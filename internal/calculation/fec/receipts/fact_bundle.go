package receipts

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	FactBundleManifestSchemaVersion = "legal-tender.fec.candidate-itemized-individual-receipts-fact-bundle.v1"
	FactBundleType                  = "fec/candidate-itemized-individual-receipts"
	FactBundleVersion               = "1.0.0"
	FactBundlePublisherVersion      = "legal-tender.fec.candidate-itemized-individual-receipts-fact-bundle-publisher.v1"
)

// FactBundleInput identifies the four exact fact publications required by the
// candidate itemized-individual receipt calculation.
type FactBundleInput struct {
	ColumnarFactManifestPath      string
	LinkageFactManifestPath       string
	AllCandidatesFactManifestPath string
	CurrentCampaignsManifestPath  string
}

type FactBundleOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	ExpectedCycle       string
	Clock               func() time.Time
}

type FactBundleManifest struct {
	Schema           string             `json:"$schema"`
	SchemaVersion    string             `json:"schema_version"`
	BundleID         string             `json:"bundle_id"`
	BundleType       string             `json:"bundle_type"`
	BundleVersion    string             `json:"bundle_version"`
	PublisherVersion string             `json:"publisher_version"`
	Cycle            string             `json:"cycle"`
	SourceReleaseID  string             `json:"source_release_id"`
	InputFactSets    []FactSetReference `json:"input_fact_sets"`
	RunID            string             `json:"run_id"`
	State            string             `json:"state"`
	PublishedAt      time.Time          `json:"published_at"`
	Counts           FactBundleCounts   `json:"counts"`
	Checks           []Check            `json:"checks"`
}

type FactBundleCounts struct {
	ScheduleAFacts             uint64 `json:"schedule_a_facts"`
	CandidateCommitteeLinkages uint64 `json:"candidate_committee_linkages"`
	AllCandidatesSummaries     uint64 `json:"all_candidates_summaries"`
	CurrentCampaignsSummaries  uint64 `json:"current_campaigns_summaries"`
}

// PublishFactBundle atomically freezes one coherent four-fact-set input. It
// performs no receipt calculation and writes no copied fact data.
func PublishFactBundle(ctx context.Context, input FactBundleInput, runID string, options FactBundleOptions) (FactBundleManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" {
		return FactBundleManifest{}, fmt.Errorf("storage root is required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return FactBundleManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	for name, path := range map[string]string{
		"columnar Schedule A fact manifest": input.ColumnarFactManifestPath,
		"linkage fact manifest":             input.LinkageFactManifestPath,
		"all-candidates fact manifest":      input.AllCandidatesFactManifestPath,
		"current-campaigns fact manifest":   input.CurrentCampaignsManifestPath,
	} {
		if path == "" {
			return FactBundleManifest{}, fmt.Errorf("%s path is required", name)
		}
	}

	scheduleManifest, scheduleDigest, err := loadCompactColumnarManifest(ctx, options.StorageRoot, input.ColumnarFactManifestPath)
	if err != nil {
		return FactBundleManifest{}, fmt.Errorf("load columnar Schedule A facts: %w", err)
	}
	if options.ExpectedCycle != "" && scheduleManifest.Cycle != options.ExpectedCycle {
		return FactBundleManifest{}, fmt.Errorf("Schedule A facts belong to cycle %s, expected %s", scheduleManifest.Cycle, options.ExpectedCycle)
	}
	references := []FactSetReference{{
		Role: "schedule_a_receipts", Dataset: "schedule-a", FactType: scheduleManifest.FactType,
		FactSetID: scheduleManifest.FactSetID, ManifestSHA256: scheduleDigest,
	}}
	counts := FactBundleCounts{ScheduleAFacts: scheduleManifest.Counts.Facts}
	classicInputs := []struct {
		role    string
		dataset string
		path    string
		count   *uint64
	}{
		{"candidate_committee_linkage", "candidate-committee-linkage", input.LinkageFactManifestPath, &counts.CandidateCommitteeLinkages},
		{"all_candidates_summary", "all-candidates-summary", input.AllCandidatesFactManifestPath, &counts.AllCandidatesSummaries},
		{"current_campaigns_summary", "current-campaigns-summary", input.CurrentCampaignsManifestPath, &counts.CurrentCampaignsSummaries},
	}
	for _, selected := range classicInputs {
		manifest, digest, loadErr := loadClassicFactManifest(options.StorageRoot, selected.path, selected.dataset)
		if loadErr != nil {
			return FactBundleManifest{}, fmt.Errorf("load %s facts: %w", selected.dataset, loadErr)
		}
		if manifest.Cycle != scheduleManifest.Cycle || manifest.SourceReleaseID != scheduleManifest.SourceReleaseID {
			return FactBundleManifest{}, fmt.Errorf("%s facts do not share Schedule A cycle and source release", selected.dataset)
		}
		for _, check := range manifest.Checks {
			if check.Severity == "block" && !check.Passed {
				return FactBundleManifest{}, fmt.Errorf("%s blocking fact check %s failed", selected.dataset, check.ID)
			}
		}
		artifactPath, resolveErr := storageartifact.Resolve(options.StorageRoot, manifest.Facts.StorageKey)
		if resolveErr != nil {
			return FactBundleManifest{}, resolveErr
		}
		if verifyErr := storageartifact.Verify(ctx, artifactPath, descriptor(manifest.Facts)); verifyErr != nil {
			return FactBundleManifest{}, fmt.Errorf("verify %s fact artifact: %w", selected.dataset, verifyErr)
		}
		*selected.count = manifest.Counts.Facts
		references = append(references, FactSetReference{
			Role: selected.role, Dataset: selected.dataset, FactType: manifest.FactType,
			FactSetID: manifest.FactSetID, ManifestSHA256: digest,
		})
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Role < references[right].Role })
	identityParts := []string{
		FactBundleManifestSchemaVersion, FactBundleType, FactBundleVersion,
		scheduleManifest.Cycle, scheduleManifest.SourceReleaseID,
	}
	for _, reference := range references {
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	bundleID := digestParts(identityParts...)
	basePath := factBundleBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", scheduleManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return FactBundleManifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+scheduleManifest.Cycle+".lock"))
	if err != nil {
		return FactBundleManifest{}, err
	}
	defer unlock()
	current, err := readFactBundleManifestIfPresent(currentPath)
	if err != nil {
		return FactBundleManifest{}, err
	}
	if current != nil {
		if err := validateFactBundleManifest(*current); err != nil {
			return FactBundleManifest{}, fmt.Errorf("invalid current fact bundle manifest: %w", err)
		}
		if err := validateFactBundleManifestBacking(options.StorageRoot, *current); err != nil {
			return FactBundleManifest{}, err
		}
		if current.Cycle != scheduleManifest.Cycle {
			return FactBundleManifest{}, fmt.Errorf("current fact bundle belongs to cycle %s", current.Cycle)
		}
		if current.BundleID == bundleID {
			return *current, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", bundleID+".json")
	if existing, readErr := readFactBundleManifestIfPresent(manifestPath); readErr != nil {
		return FactBundleManifest{}, readErr
	} else if existing != nil {
		if err := validateFactBundleManifest(*existing); err != nil {
			return FactBundleManifest{}, err
		}
		if !reflect.DeepEqual(existing.InputFactSets, references) || existing.Counts != counts {
			return FactBundleManifest{}, fmt.Errorf("immutable fact bundle manifest collision")
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return FactBundleManifest{}, err
		}
		return *existing, nil
	}

	manifest := FactBundleManifest{
		Schema: "manifest.schema.json", SchemaVersion: FactBundleManifestSchemaVersion,
		BundleID: bundleID, BundleType: FactBundleType, BundleVersion: FactBundleVersion,
		PublisherVersion: FactBundlePublisherVersion, Cycle: scheduleManifest.Cycle,
		SourceReleaseID: scheduleManifest.SourceReleaseID, InputFactSets: references,
		RunID: runID, State: "ready", PublishedAt: options.Clock().UTC(), Counts: counts,
		Checks: []Check{
			{ID: "exact_role_set", Passed: true, Severity: "block", Detail: "the bundle contains exactly the four fact roles required by the calculation"},
			{ID: "cycle_coherence", Passed: true, Severity: "block", Detail: "all four fact sets belong to one FEC cycle"},
			{ID: "source_release_coherence", Passed: true, Severity: "block", Detail: "all four fact sets belong to one coordinated source release"},
			{ID: "manifest_immutability", Passed: true, Severity: "block", Detail: "every selected pointer matches its immutable fact manifest"},
			{ID: "backing_integrity", Passed: true, Severity: "block", Detail: "all referenced Parquet shards and classic fact artifacts passed complete digest verification"},
			{ID: "calculation_readiness", Passed: true, Severity: "block", Detail: "the exact input bundle is ready for the compact candidate receipt calculation"},
		},
	}
	if err := validateFactBundleManifest(manifest); err != nil {
		return FactBundleManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return FactBundleManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return FactBundleManifest{}, err
	}
	return manifest, nil
}

// PublishCompactFromFactBundle resolves only immutable manifests named by one
// accepted bundle. Mutable current pointers do not participate in calculation.
func PublishCompactFromFactBundle(ctx context.Context, factBundlePath, runID string, options CompactPublishOptions) (CompactManifest, error) {
	if options.StorageRoot == "" {
		return CompactManifest{}, fmt.Errorf("storage root is required")
	}
	bundle, _, err := readStrictJSON[FactBundleManifest](factBundlePath)
	if err != nil {
		return CompactManifest{}, fmt.Errorf("read fact bundle manifest: %w", err)
	}
	if err := validateFactBundleManifest(bundle); err != nil {
		return CompactManifest{}, err
	}
	if err := validateFactBundleManifestBacking(options.StorageRoot, bundle); err != nil {
		return CompactManifest{}, err
	}
	paths := make(map[string]string, len(bundle.InputFactSets))
	for _, reference := range bundle.InputFactSets {
		var path string
		if reference.Role == "schedule_a_receipts" {
			path = filepath.Join(options.StorageRoot, "facts", "fec", "schedule-a", "columnar", "manifests", reference.FactSetID+".json")
		} else {
			path = filepath.Join(options.StorageRoot, "facts", "fec", "classic", reference.Dataset, "manifests", reference.FactSetID+".json")
		}
		paths[reference.Role] = path
	}
	manifest, err := PublishCompact(ctx, CompactPublishInput{
		ColumnarFactManifestPath:      paths["schedule_a_receipts"],
		LinkageFactManifestPath:       paths["candidate_committee_linkage"],
		AllCandidatesFactManifestPath: paths["all_candidates_summary"],
		CurrentCampaignsManifestPath:  paths["current_campaigns_summary"],
	}, runID, options)
	if err != nil {
		return CompactManifest{}, err
	}
	if manifest.Cycle != bundle.Cycle || manifest.SourceReleaseID != bundle.SourceReleaseID || !reflect.DeepEqual(manifest.InputFactSets, bundle.InputFactSets) {
		return CompactManifest{}, fmt.Errorf("compact calculation inputs differ from fact bundle")
	}
	return manifest, nil
}

func validateFactBundleManifest(manifest FactBundleManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != FactBundleManifestSchemaVersion ||
		manifest.BundleType != FactBundleType || manifest.BundleVersion != FactBundleVersion ||
		manifest.PublisherVersion != FactBundlePublisherVersion || manifest.State != "ready" {
		return fmt.Errorf("unsupported fact bundle manifest")
	}
	if !validDigest(manifest.BundleID) || manifest.Cycle == "" || !strings.HasPrefix(manifest.SourceReleaseID, "fec-") ||
		!validDigest(strings.TrimPrefix(manifest.SourceReleaseID, "fec-")) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) ||
		manifest.PublishedAt.IsZero() || len(manifest.InputFactSets) != 4 {
		return fmt.Errorf("fact bundle identity is incomplete")
	}
	expected := []struct{ role, dataset, factType string }{
		{"all_candidates_summary", "all-candidates-summary", "fec.candidate_summary_all.v1"},
		{"candidate_committee_linkage", "candidate-committee-linkage", "fec.candidate_committee_linkage.v1"},
		{"current_campaigns_summary", "current-campaigns-summary", "fec.campaign_summary.v1"},
		{"schedule_a_receipts", "schedule-a", fecoccurrence.ScheduleAFactType},
	}
	identityParts := []string{FactBundleManifestSchemaVersion, FactBundleType, FactBundleVersion, manifest.Cycle, manifest.SourceReleaseID}
	for index, reference := range manifest.InputFactSets {
		want := expected[index]
		if reference.Role != want.role || reference.Dataset != want.dataset || reference.FactType != want.factType ||
			!validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) {
			return fmt.Errorf("fact bundle input role %d is invalid", index)
		}
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	if manifest.BundleID != digestParts(identityParts...) {
		return fmt.Errorf("fact bundle ID does not match canonical inputs")
	}
	if manifest.Counts.ScheduleAFacts == 0 || len(manifest.Checks) != 6 {
		return fmt.Errorf("fact bundle counts or checks are incomplete")
	}
	expectedChecks := map[string]struct{}{
		"exact_role_set": {}, "cycle_coherence": {}, "source_release_coherence": {},
		"manifest_immutability": {}, "backing_integrity": {}, "calculation_readiness": {},
	}
	for _, check := range manifest.Checks {
		if _, exists := expectedChecks[check.ID]; !exists || check.Severity != "block" || !check.Passed {
			return fmt.Errorf("required fact bundle check %s is invalid", check.ID)
		}
		delete(expectedChecks, check.ID)
	}
	if len(expectedChecks) != 0 {
		return fmt.Errorf("fact bundle required checks are incomplete")
	}
	return nil
}

func validateFactBundleManifestBacking(storageRoot string, manifest FactBundleManifest) error {
	immutablePath := filepath.Join(storageRoot, factBundleBase(), "manifests", manifest.BundleID+".json")
	immutable, _, err := readStrictJSON[FactBundleManifest](immutablePath)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return fmt.Errorf("fact bundle pointer differs from immutable manifest")
	}
	return nil
}

func readFactBundleManifestIfPresent(path string) (*FactBundleManifest, error) {
	manifest, _, err := readStrictJSON[FactBundleManifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func factBundleBase() string {
	return filepath.Join("bundles", "fec", "candidate-itemized-individual-receipts")
}
