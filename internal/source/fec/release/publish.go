package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type PublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
}

// Publish validates one exact plan/acquisition/stage chain under an exclusive
// filesystem lock, writes an immutable manifest, then atomically replaces the
// active pointer. It never performs acquisition or extraction.
func Publish(
	ctx context.Context,
	inventory Inventory,
	plan ReleasePlan,
	planSHA256 string,
	acquisition AcquisitionResult,
	acquisitionSHA256 string,
	stage StageResult,
	stageSHA256 string,
	runID string,
	options PublishOptions,
) (ReleaseManifest, error) {
	if options.StorageRoot == "" {
		return ReleaseManifest{}, fmt.Errorf("storage root is required")
	}
	if !ValidAcquisitionRunID(runID) || !validSHA256(planSHA256) || !validSHA256(acquisitionSHA256) || !validSHA256(stageSHA256) {
		return ReleaseManifest{}, fmt.Errorf("publication input identity is invalid")
	}
	if issues := ValidatePlan(inventory, plan); len(issues) != 0 || plan.Status != PlanUpdateAvailable {
		return ReleaseManifest{}, fmt.Errorf("publication requires a valid update_available plan")
	}
	if issues := ValidateAcquisitionResult(inventory, acquisition); len(issues) != 0 || acquisition.Status != AcquisitionAcquired {
		return ReleaseManifest{}, fmt.Errorf("publication requires a valid acquired result")
	}
	if issues := ValidateStageResult(inventory, stage); len(issues) != 0 || stage.Status != StageStaged {
		return ReleaseManifest{}, fmt.Errorf("publication requires a valid staged result")
	}
	if acquisition.PlanSHA256 != planSHA256 || stage.PlanSHA256 != planSHA256 || stage.AcquisitionSHA256 != acquisitionSHA256 ||
		acquisition.CandidateReleaseID != plan.CandidateReleaseID || stage.CandidateReleaseID != plan.CandidateReleaseID ||
		acquisition.PriorReleaseID != plan.PriorReleaseID || stage.PriorReleaseID != plan.PriorReleaseID {
		return ReleaseManifest{}, fmt.Errorf("publication input identities do not form one release chain")
	}
	if err := validateAcquisitionArtifactFiles(options.StorageRoot, acquisition); err != nil {
		return ReleaseManifest{}, fmt.Errorf("validate acquired artifacts: %w", err)
	}
	acquiredBySource := make(map[string]AcquisitionArtifact, len(acquisition.Artifacts))
	for _, artifact := range acquisition.Artifacts {
		acquiredBySource[artifact.SourceID] = artifact
	}
	for _, output := range stage.Outputs {
		artifact, exists := acquiredBySource[output.SourceID]
		if !exists || output.SourceArtifactSHA256 != artifact.SHA256 {
			return ReleaseManifest{}, fmt.Errorf("staged output %s does not reference its acquired source artifact", output.Selection)
		}
		if err := validateStagedOutputIdentity(ctx, options.StorageRoot, output); err != nil {
			return ReleaseManifest{}, fmt.Errorf("validate staged output %s: %w", output.Selection, err)
		}
	}

	releasesDirectory := filepath.Join(options.StorageRoot, "releases", "fec")
	if err := os.MkdirAll(releasesDirectory, 0o750); err != nil {
		return ReleaseManifest{}, err
	}
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(releasesDirectory, "current.json")
	}
	if err := requirePathWithinRoot(options.StorageRoot, currentPath); err != nil {
		return ReleaseManifest{}, fmt.Errorf("active release path: %w", err)
	}
	lockFile, err := os.OpenFile(filepath.Join(releasesDirectory, ".publish.lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return ReleaseManifest{}, err
	}
	defer func() { _ = lockFile.Close() }()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return ReleaseManifest{}, err
	}
	defer func() { _ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN) }()

	current, currentErr := readKnownManifestIfPresent(currentPath)
	if currentErr != nil {
		return ReleaseManifest{}, currentErr
	}
	if current != nil && current.ReleaseID == plan.CandidateReleaseID {
		if current.PlanSHA256 != planSHA256 || current.AcquisitionSHA256 != acquisitionSHA256 || current.StageSHA256 != stageSHA256 {
			return ReleaseManifest{}, fmt.Errorf("candidate release is active with different evidence")
		}
		return *current, nil
	}
	if plan.PriorReleaseID == "" && current != nil {
		return ReleaseManifest{}, fmt.Errorf("bootstrap plan cannot replace an existing active release")
	}
	if plan.PriorReleaseID != "" && (current == nil || current.ReleaseID != plan.PriorReleaseID) {
		return ReleaseManifest{}, fmt.Errorf("active release changed after planning")
	}

	historyPath := filepath.Join(releasesDirectory, "manifests", plan.CandidateReleaseID+".json")
	manifest, historyErr := readManifestIfPresent(inventory, historyPath)
	if historyErr != nil {
		return ReleaseManifest{}, historyErr
	}
	var content []byte
	if manifest != nil {
		if manifest.PlanSHA256 != planSHA256 || manifest.AcquisitionSHA256 != acquisitionSHA256 || manifest.StageSHA256 != stageSHA256 || manifest.ReleaseID != plan.CandidateReleaseID {
			return ReleaseManifest{}, fmt.Errorf("immutable release manifest exists with different evidence")
		}
		content, err = os.ReadFile(historyPath)
		if err != nil {
			return ReleaseManifest{}, err
		}
	} else {
		clock := options.Clock
		if clock == nil {
			clock = time.Now
		}
		created, err := buildPublishedManifest(inventory, plan, planSHA256, acquisition, acquisitionSHA256, stage, stageSHA256, runID, clock().UTC())
		if err != nil {
			return ReleaseManifest{}, err
		}
		manifest = &created
		content, err = json.MarshalIndent(created, "", "  ")
		if err != nil {
			return ReleaseManifest{}, err
		}
		content = append(content, '\n')
		if err := writeAtomicBytes(historyPath, content); err != nil {
			return ReleaseManifest{}, fmt.Errorf("write immutable release manifest: %w", err)
		}
	}
	if err := writeAtomicBytes(currentPath, content); err != nil {
		return ReleaseManifest{}, fmt.Errorf("replace active release manifest: %w", err)
	}
	return *manifest, nil
}

func buildPublishedManifest(inventory Inventory, plan ReleasePlan, planSHA256 string, acquisition AcquisitionResult, acquisitionSHA256 string, stage StageResult, stageSHA256, runID string, publishedAt time.Time) (ReleaseManifest, error) {
	acquiredByID := make(map[string]AcquisitionArtifact, len(acquisition.Artifacts))
	for _, artifact := range acquisition.Artifacts {
		acquiredByID[artifact.SourceID] = artifact
	}
	artifacts := make([]PublishedArtifact, 0, len(plan.SelectedSources))
	for _, selected := range plan.SelectedSources {
		acquired, exists := acquiredByID[selected.SourceID]
		if !exists {
			return ReleaseManifest{}, fmt.Errorf("acquisition has no artifact for %s", selected.SourceID)
		}
		artifacts = append(artifacts, PublishedArtifact{
			SelectedSource: selected,
			ByteCount:      acquired.ByteCount,
			SHA256:         acquired.SHA256,
			StorageKey:     acquired.StorageKey,
			AcquiredAt:     acquired.AcquiredAt,
		})
	}
	checks := append([]ReleaseCheck(nil), stage.Checks...)
	checks = append(checks,
		ReleaseCheck{ID: "publication_evidence_chain", Passed: true, Severity: "block", Detail: "plan, acquisition, and stage digests form one candidate release"},
		ReleaseCheck{ID: "publication_active_baseline", Passed: true, Severity: "block", Detail: "active release matched the plan baseline under the publication lock"},
		ReleaseCheck{ID: "publication_atomic_pointer", Passed: true, Severity: "block", Detail: "manifest is eligible for immutable write and atomic pointer replacement"},
	)
	manifest := ReleaseManifest{
		Schema:            "release-manifest.schema.json",
		SchemaVersion:     ManifestSchemaVersion,
		InventoryVersion:  inventory.InventoryVersion,
		ReleaseID:         plan.CandidateReleaseID,
		PriorReleaseID:    plan.PriorReleaseID,
		RunID:             runID,
		PlanSHA256:        planSHA256,
		AcquisitionSHA256: acquisitionSHA256,
		StageSHA256:       stageSHA256,
		State:             "published",
		SelectedAt:        plan.PlannedAt,
		PublishedAt:       publishedAt,
		Periods:           append([]string(nil), inventory.Periods...),
		Artifacts:         artifacts,
		StagedOutputs:     append([]StagedOutput(nil), stage.Outputs...),
		Checks:            checks,
	}
	if issues := validateManifest(inventory, manifest); len(issues) != 0 {
		return ReleaseManifest{}, fmt.Errorf("build invalid published manifest: %s", issues[0].Message)
	}
	return manifest, nil
}

func readManifestIfPresent(inventory Inventory, path string) (*ReleaseManifest, error) {
	return readManifestWithValidator(path, func(manifest ReleaseManifest) []Issue {
		return validateManifest(inventory, manifest)
	})
}

func readKnownManifestIfPresent(path string) (*ReleaseManifest, error) {
	return readManifestWithValidator(path, validateKnownManifest)
}

func readManifestWithValidator(path string, validate func(ReleaseManifest) []Issue) (*ReleaseManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ReleaseManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode release manifest %s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("decode release manifest %s: multiple JSON values", path)
		}
		return nil, fmt.Errorf("decode release manifest %s: %w", path, err)
	}
	if issues := validate(manifest); len(issues) != 0 {
		return nil, fmt.Errorf("invalid release manifest %s: %s", path, issues[0].Message)
	}
	return &manifest, nil
}

func requirePathWithinRoot(root, path string) error {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(absoluteRoot, absolutePath)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes the storage root")
	}
	return nil
}
