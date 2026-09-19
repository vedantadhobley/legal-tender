package committeeflows

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ProjectionBundleSchemaVersion    = "legal-tender.fec.receiver-reported-committee-flow-projection-bundle.v1"
	ProjectionBundleType             = "fec/receiver-reported-committee-flow-projection"
	ProjectionBundleVersion          = "1.0.0"
	ProjectionBundlePublisherVersion = "legal-tender.fec.receiver-reported-committee-flow-projection-bundle-publisher.v1"
)

type ProjectionBundleInput struct {
	CalculationManifestPath string
	CommitteeManifestPath   string
}

type ProjectionBundleOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	ExpectedCycle       string
	Clock               func() time.Time
}

type ProjectionBundleManifest struct {
	Schema           string                               `json:"$schema"`
	SchemaVersion    string                               `json:"schema_version"`
	BundleID         string                               `json:"bundle_id"`
	BundleType       string                               `json:"bundle_type"`
	BundleVersion    string                               `json:"bundle_version"`
	PublisherVersion string                               `json:"publisher_version"`
	Cycle            string                               `json:"cycle"`
	SourceReleaseID  string                               `json:"source_release_id"`
	InputCalculation ProjectionBundleCalculationReference `json:"input_calculation"`
	InputFactSets    []ProjectionBundleFactReference      `json:"input_fact_sets"`
	RunID            string                               `json:"run_id"`
	State            string                               `json:"state"`
	PublishedAt      time.Time                            `json:"published_at"`
	Counts           ProjectionBundleCounts               `json:"counts"`
	Checks           []Check                              `json:"checks"`
}

type ProjectionBundleCalculationReference struct {
	Role                    string `json:"role"`
	Calculation             string `json:"calculation"`
	CalculationVersion      string `json:"calculation_version"`
	CalculationSetID        string `json:"calculation_set_id"`
	ManifestSHA256          string `json:"manifest_sha256"`
	ScheduleAFactSetID      string `json:"schedule_a_fact_set_id"`
	ScheduleAManifestSHA256 string `json:"schedule_a_manifest_sha256"`
}

type ProjectionBundleFactReference struct {
	Role           string `json:"role"`
	Dataset        string `json:"dataset"`
	FactType       string `json:"fact_type"`
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type ProjectionBundleCounts struct {
	CalculationResults uint64 `json:"calculation_results"`
	CommitteeFacts     uint64 `json:"committee_facts"`
}

type ProjectionBundleResolvedInput struct {
	CalculationManifestPath string
	CommitteeManifestPath   string
}

type loadedProjectionBundleInputs struct {
	calculation          Manifest
	calculationReference ProjectionBundleCalculationReference
	factReferences       []ProjectionBundleFactReference
	counts               ProjectionBundleCounts
}

// PublishProjectionBundle freezes the exact calculation and committee master
// required by one receiver-reported committee-flow graph projection.
func PublishProjectionBundle(ctx context.Context, input ProjectionBundleInput, runID string, options ProjectionBundleOptions) (ProjectionBundleManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.CalculationManifestPath == "" || input.CommitteeManifestPath == "" {
		return ProjectionBundleManifest{}, fmt.Errorf("storage root, receiver-flow calculation, and committee master paths are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ProjectionBundleManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	loaded, err := loadProjectionBundleInputs(ctx, options.StorageRoot, input, options.ExpectedCycle)
	if err != nil {
		return ProjectionBundleManifest{}, err
	}
	bundleID := digestParts(projectionBundleIdentityParts(
		loaded.calculation.Cycle, loaded.calculation.SourceReleaseID,
		loaded.calculationReference, loaded.factReferences,
	)...)
	basePath := projectionBundleBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", loaded.calculation.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ProjectionBundleManifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+loaded.calculation.Cycle+".lock"))
	if err != nil {
		return ProjectionBundleManifest{}, err
	}
	defer unlock()

	current, err := readProjectionBundleManifestIfPresent(currentPath)
	if err != nil {
		return ProjectionBundleManifest{}, err
	}
	if current != nil {
		if err := validateProjectionBundleManifest(*current); err != nil {
			return ProjectionBundleManifest{}, fmt.Errorf("invalid current receiver-flow projection bundle: %w", err)
		}
		if err := validateProjectionBundleManifestBacking(options.StorageRoot, *current); err != nil {
			return ProjectionBundleManifest{}, err
		}
		if current.Cycle != loaded.calculation.Cycle {
			return ProjectionBundleManifest{}, fmt.Errorf("current receiver-flow projection bundle belongs to cycle %s", current.Cycle)
		}
		if current.BundleID == bundleID {
			return *current, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", bundleID+".json")
	if existing, readErr := readProjectionBundleManifestIfPresent(manifestPath); readErr != nil {
		return ProjectionBundleManifest{}, readErr
	} else if existing != nil {
		if err := validateProjectionBundleManifest(*existing); err != nil {
			return ProjectionBundleManifest{}, err
		}
		if existing.InputCalculation != loaded.calculationReference ||
			!reflect.DeepEqual(existing.InputFactSets, loaded.factReferences) || existing.Counts != loaded.counts {
			return ProjectionBundleManifest{}, fmt.Errorf("immutable receiver-flow projection bundle collision")
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ProjectionBundleManifest{}, err
		}
		return *existing, nil
	}

	manifest := ProjectionBundleManifest{
		Schema: "manifest.schema.json", SchemaVersion: ProjectionBundleSchemaVersion,
		BundleID: bundleID, BundleType: ProjectionBundleType, BundleVersion: ProjectionBundleVersion,
		PublisherVersion: ProjectionBundlePublisherVersion, Cycle: loaded.calculation.Cycle,
		SourceReleaseID: loaded.calculation.SourceReleaseID, InputCalculation: loaded.calculationReference,
		InputFactSets: loaded.factReferences, RunID: runID, State: "ready",
		PublishedAt: options.Clock().UTC(), Counts: loaded.counts,
		Checks: []Check{
			{ID: "exact_role_set", Passed: true, Severity: "block", Detail: "the bundle contains exactly one receiver-flow calculation and one committee-master fact set"},
			{ID: "cycle_coherence", Passed: true, Severity: "block", Detail: "the calculation and committee facts belong to one FEC cycle"},
			{ID: "source_release_coherence", Passed: true, Severity: "block", Detail: "the calculation and committee facts belong to one coordinated source release"},
			{ID: "manifest_immutability", Passed: true, Severity: "block", Detail: "each selected pointer matches its immutable manifest"},
			{ID: "backing_integrity", Passed: true, Severity: "block", Detail: "calculation artifacts and committee facts passed complete digest verification"},
			{ID: "projection_readiness", Passed: true, Severity: "block", Detail: "the exact input bundle is ready for the receiver-flow graph projection"},
		},
	}
	if err := validateProjectionBundleManifest(manifest); err != nil {
		return ProjectionBundleManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ProjectionBundleManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ProjectionBundleManifest{}, err
	}
	return manifest, nil
}

// LoadProjectionBundle verifies a bundle and all immutable backing, then
// returns only immutable manifest paths for graph projection.
func LoadProjectionBundle(ctx context.Context, storageRoot, path string) (ProjectionBundleManifest, string, ProjectionBundleResolvedInput, error) {
	if storageRoot == "" || path == "" {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("storage root and receiver-flow projection bundle path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	pointer, _, err := readStrictJSON[ProjectionBundleManifest](path)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if err := validateProjectionBundleManifest(pointer); err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	immutablePath := filepath.Join(storageRoot, projectionBundleBase(), "manifests", pointer.BundleID+".json")
	immutable, digest, err := readStrictJSON[ProjectionBundleManifest](immutablePath)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if !reflect.DeepEqual(pointer, immutable) {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("receiver-flow projection bundle pointer differs from immutable manifest")
	}
	resolved := resolveProjectionBundleInputPaths(storageRoot, immutable)
	loaded, err := loadProjectionBundleInputs(ctx, storageRoot, ProjectionBundleInput{
		CalculationManifestPath: resolved.CalculationManifestPath,
		CommitteeManifestPath:   resolved.CommitteeManifestPath,
	}, immutable.Cycle)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if loaded.calculation.SourceReleaseID != immutable.SourceReleaseID ||
		loaded.calculationReference != immutable.InputCalculation ||
		!reflect.DeepEqual(loaded.factReferences, immutable.InputFactSets) || loaded.counts != immutable.Counts {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("receiver-flow projection bundle inputs differ from immutable backing")
	}
	return immutable, digest, resolved, nil
}

func loadProjectionBundleInputs(ctx context.Context, storageRoot string, input ProjectionBundleInput, expectedCycle string) (loadedProjectionBundleInputs, error) {
	calculation, calculationDigest, err := LoadPublishedManifest(ctx, storageRoot, input.CalculationManifestPath)
	if err != nil {
		return loadedProjectionBundleInputs{}, fmt.Errorf("load receiver-flow calculation: %w", err)
	}
	if expectedCycle != "" && calculation.Cycle != expectedCycle {
		return loadedProjectionBundleInputs{}, fmt.Errorf("receiver-flow calculation belongs to cycle %s, expected %s", calculation.Cycle, expectedCycle)
	}
	committee, committeeDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, input.CommitteeManifestPath, "committee-master")
	if err != nil {
		return loadedProjectionBundleInputs{}, fmt.Errorf("load committee-master facts: %w", err)
	}
	if committee.Cycle != calculation.Cycle || committee.SourceReleaseID != calculation.SourceReleaseID {
		return loadedProjectionBundleInputs{}, fmt.Errorf("committee-master facts do not share the calculation cycle and source release")
	}
	artifactPath, err := storageartifact.Resolve(storageRoot, committee.Facts.StorageKey)
	if err != nil {
		return loadedProjectionBundleInputs{}, err
	}
	if err := storageartifact.Verify(ctx, artifactPath, classicFactDescriptor(committee.Facts)); err != nil {
		return loadedProjectionBundleInputs{}, fmt.Errorf("verify committee-master fact artifact: %w", err)
	}
	return loadedProjectionBundleInputs{
		calculation: calculation,
		calculationReference: ProjectionBundleCalculationReference{
			Role: "receiver_reported_committee_flows", Calculation: calculation.Calculation,
			CalculationVersion: calculation.CalculationVersion, CalculationSetID: calculation.CalculationSetID,
			ManifestSHA256: calculationDigest, ScheduleAFactSetID: calculation.InputFactSet.FactSetID,
			ScheduleAManifestSHA256: calculation.InputFactSet.ManifestSHA256,
		},
		factReferences: []ProjectionBundleFactReference{{
			Role: "committee_master", Dataset: "committee-master", FactType: committee.FactType,
			FactSetID: committee.FactSetID, ManifestSHA256: committeeDigest,
		}},
		counts: ProjectionBundleCounts{CalculationResults: calculation.ResultCounts.ResultGroups, CommitteeFacts: committee.Counts.Facts},
	}, nil
}

func projectionBundleIdentityParts(cycle, sourceReleaseID string, calculation ProjectionBundleCalculationReference, facts []ProjectionBundleFactReference) []string {
	parts := []string{
		ProjectionBundleSchemaVersion, ProjectionBundleType, ProjectionBundleVersion,
		cycle, sourceReleaseID, calculation.Role, calculation.Calculation,
		calculation.CalculationVersion, calculation.CalculationSetID,
		calculation.ManifestSHA256, calculation.ScheduleAFactSetID,
		calculation.ScheduleAManifestSHA256,
	}
	for _, reference := range facts {
		parts = append(parts, reference.Role, reference.Dataset, reference.FactType, reference.FactSetID, reference.ManifestSHA256)
	}
	return parts
}

func validateProjectionBundleManifest(manifest ProjectionBundleManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ProjectionBundleSchemaVersion ||
		manifest.BundleType != ProjectionBundleType || manifest.BundleVersion != ProjectionBundleVersion ||
		manifest.PublisherVersion != ProjectionBundlePublisherVersion || manifest.State != "ready" {
		return fmt.Errorf("unsupported receiver-flow projection bundle manifest")
	}
	if !validDigest(manifest.BundleID) || !validCycle(manifest.Cycle) || !validSourceReleaseID(manifest.SourceReleaseID) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() || len(manifest.InputFactSets) != 1 {
		return fmt.Errorf("receiver-flow projection bundle identity is incomplete")
	}
	calculation := manifest.InputCalculation
	if calculation.Role != "receiver_reported_committee_flows" || calculation.Calculation != ContractID ||
		calculation.CalculationVersion != ContractVersion || !validDigest(calculation.CalculationSetID) ||
		!validDigest(calculation.ManifestSHA256) || !validDigest(calculation.ScheduleAFactSetID) ||
		!validDigest(calculation.ScheduleAManifestSHA256) {
		return fmt.Errorf("receiver-flow projection bundle calculation reference is invalid")
	}
	fact := manifest.InputFactSets[0]
	if fact.Role != "committee_master" || fact.Dataset != "committee-master" || fact.FactType != "fec.committee_assertion.v1" ||
		!validDigest(fact.FactSetID) || !validDigest(fact.ManifestSHA256) {
		return fmt.Errorf("receiver-flow projection bundle fact role is invalid")
	}
	if manifest.BundleID != digestParts(projectionBundleIdentityParts(manifest.Cycle, manifest.SourceReleaseID, calculation, manifest.InputFactSets)...) {
		return fmt.Errorf("receiver-flow projection bundle ID does not match canonical inputs")
	}
	if manifest.Counts.CalculationResults == 0 || manifest.Counts.CommitteeFacts == 0 {
		return fmt.Errorf("receiver-flow projection bundle counts are incomplete")
	}
	expectedChecks := map[string]struct{}{
		"exact_role_set": {}, "cycle_coherence": {}, "source_release_coherence": {},
		"manifest_immutability": {}, "backing_integrity": {}, "projection_readiness": {},
	}
	if len(manifest.Checks) != len(expectedChecks) {
		return fmt.Errorf("receiver-flow projection bundle checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if _, exists := expectedChecks[check.ID]; !exists || check.Severity != "block" || !check.Passed || check.Detail == "" {
			return fmt.Errorf("receiver-flow projection bundle check %s is invalid", check.ID)
		}
		delete(expectedChecks, check.ID)
	}
	return nil
}

func validateProjectionBundleManifestBacking(storageRoot string, manifest ProjectionBundleManifest) error {
	immutablePath := filepath.Join(storageRoot, projectionBundleBase(), "manifests", manifest.BundleID+".json")
	immutable, _, err := readStrictJSON[ProjectionBundleManifest](immutablePath)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return fmt.Errorf("receiver-flow projection bundle pointer differs from immutable manifest")
	}
	return nil
}

func resolveProjectionBundleInputPaths(storageRoot string, manifest ProjectionBundleManifest) ProjectionBundleResolvedInput {
	return ProjectionBundleResolvedInput{
		CalculationManifestPath: filepath.Join(storageRoot, calculationBase(), "manifests", manifest.InputCalculation.CalculationSetID+".json"),
		CommitteeManifestPath: filepath.Join(
			storageRoot, "facts", "fec", "classic", "committee-master", "manifests", manifest.InputFactSets[0].FactSetID+".json",
		),
	}
}

func readProjectionBundleManifestIfPresent(path string) (*ProjectionBundleManifest, error) {
	manifest, _, err := readStrictJSON[ProjectionBundleManifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func projectionBundleBase() string {
	return filepath.Join("bundles", "fec", "receiver-reported-committee-flow-projection")
}

func classicFactDescriptor(artifact fecoccurrence.Artifact) storageartifact.Descriptor {
	return storageartifact.Descriptor{
		RecordCount: artifact.RecordCount, UncompressedBytes: artifact.UncompressedBytes,
		UncompressedSHA256: artifact.UncompressedSHA256, CompressedBytes: artifact.CompressedBytes,
		CompressedSHA256: artifact.CompressedSHA256, Compression: artifact.Compression, StorageKey: artifact.StorageKey,
	}
}
