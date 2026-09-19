package independentexpenditures

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ProjectionBundleSchemaVersion    = "legal-tender.fec.independent-expenditure-projection-bundle.v1"
	ProjectionBundleType             = "fec/independent-expenditure-projection"
	ProjectionBundleVersion          = "1.0.0"
	ProjectionBundlePublisherVersion = "legal-tender.fec.independent-expenditure-projection-bundle-publisher.v1"
)

type ProjectionBundleInput struct {
	CalculationManifestPath string
	CandidateManifestPath   string
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
	ScheduleEFactSetID      string `json:"schedule_e_fact_set_id"`
	ScheduleEManifestSHA256 string `json:"schedule_e_manifest_sha256"`
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
	CandidateFacts     uint64 `json:"candidate_facts"`
	CommitteeFacts     uint64 `json:"committee_facts"`
}

// ProjectionBundleResolvedInput contains only immutable manifest paths derived
// from one accepted readiness bundle.
type ProjectionBundleResolvedInput struct {
	CalculationManifestPath string
	CandidateManifestPath   string
	CommitteeManifestPath   string
}

type loadedProjectionBundleInputs struct {
	calculation          Manifest
	calculationReference ProjectionBundleCalculationReference
	factReferences       []ProjectionBundleFactReference
	counts               ProjectionBundleCounts
}

// PublishProjectionBundle freezes the exact calculation and two master fact
// publications required by one independent-expenditure graph projection.
func PublishProjectionBundle(ctx context.Context, input ProjectionBundleInput, runID string, options ProjectionBundleOptions) (ProjectionBundleManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" {
		return ProjectionBundleManifest{}, fmt.Errorf("storage root is required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ProjectionBundleManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	for name, path := range map[string]string{
		"effective independent-expenditure calculation": input.CalculationManifestPath,
		"candidate master facts":                        input.CandidateManifestPath,
		"committee master facts":                        input.CommitteeManifestPath,
	} {
		if path == "" {
			return ProjectionBundleManifest{}, fmt.Errorf("%s path is required", name)
		}
	}

	loaded, err := loadProjectionBundleInputs(ctx, options.StorageRoot, input, options.ExpectedCycle)
	if err != nil {
		return ProjectionBundleManifest{}, err
	}
	identityParts := projectionBundleIdentityParts(
		loaded.calculation.Cycle,
		loaded.calculation.SourceReleaseID,
		loaded.calculationReference,
		loaded.factReferences,
	)
	bundleID := digestParts(identityParts...)
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
			return ProjectionBundleManifest{}, fmt.Errorf("invalid current projection bundle manifest: %w", err)
		}
		if err := validateProjectionBundleManifestBacking(options.StorageRoot, *current); err != nil {
			return ProjectionBundleManifest{}, err
		}
		if current.Cycle != loaded.calculation.Cycle {
			return ProjectionBundleManifest{}, fmt.Errorf("current projection bundle belongs to cycle %s", current.Cycle)
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
			return ProjectionBundleManifest{}, fmt.Errorf("immutable projection bundle manifest collision")
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
		SourceReleaseID:  loaded.calculation.SourceReleaseID,
		InputCalculation: loaded.calculationReference, InputFactSets: loaded.factReferences,
		RunID: runID, State: "ready", PublishedAt: options.Clock().UTC(), Counts: loaded.counts,
		Checks: []Check{
			{ID: "exact_role_set", Passed: true, Severity: "block", Detail: "the bundle contains exactly one effective calculation and the two master fact roles required by the graph projection"},
			{ID: "cycle_coherence", Passed: true, Severity: "block", Detail: "the calculation and both master fact sets belong to one FEC cycle"},
			{ID: "source_release_coherence", Passed: true, Severity: "block", Detail: "the calculation and both master fact sets belong to one coordinated source release"},
			{ID: "manifest_immutability", Passed: true, Severity: "block", Detail: "every selected pointer matches its immutable manifest"},
			{ID: "backing_integrity", Passed: true, Severity: "block", Detail: "the calculation result and exception artifacts and both master fact artifacts passed complete digest verification"},
			{ID: "projection_readiness", Passed: true, Severity: "block", Detail: "the exact input bundle is ready for the independent-expenditure graph projection"},
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

// LoadProjectionBundle validates a bundle, every immutable input manifest,
// and all backing artifacts, then returns only immutable paths for projection.
func LoadProjectionBundle(ctx context.Context, storageRoot, path string) (ProjectionBundleManifest, string, ProjectionBundleResolvedInput, error) {
	if storageRoot == "" || path == "" {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("storage root and projection bundle path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	manifest, _, err := readStrictJSON[ProjectionBundleManifest](path)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if err := validateProjectionBundleManifest(manifest); err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	immutablePath := filepath.Join(storageRoot, projectionBundleBase(), "manifests", manifest.BundleID+".json")
	immutable, digest, err := readStrictJSON[ProjectionBundleManifest](immutablePath)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("projection bundle pointer differs from immutable manifest")
	}
	resolved := resolveProjectionBundleInputPaths(storageRoot, manifest)
	loaded, err := loadProjectionBundleInputs(ctx, storageRoot, ProjectionBundleInput{
		CalculationManifestPath: resolved.CalculationManifestPath,
		CandidateManifestPath:   resolved.CandidateManifestPath,
		CommitteeManifestPath:   resolved.CommitteeManifestPath,
	}, manifest.Cycle)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if loaded.calculation.SourceReleaseID != manifest.SourceReleaseID ||
		loaded.calculationReference != manifest.InputCalculation ||
		!reflect.DeepEqual(loaded.factReferences, manifest.InputFactSets) || loaded.counts != manifest.Counts {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("projection bundle inputs differ from immutable backing")
	}
	return immutable, digest, resolved, nil
}

func loadProjectionBundleInputs(ctx context.Context, storageRoot string, input ProjectionBundleInput, expectedCycle string) (loadedProjectionBundleInputs, error) {
	calculation, calculationDigest, err := LoadPublishedManifest(ctx, storageRoot, input.CalculationManifestPath)
	if err != nil {
		return loadedProjectionBundleInputs{}, fmt.Errorf("load effective independent-expenditure calculation: %w", err)
	}
	if expectedCycle != "" && calculation.Cycle != expectedCycle {
		return loadedProjectionBundleInputs{}, fmt.Errorf("independent-expenditure calculation belongs to cycle %s, expected %s", calculation.Cycle, expectedCycle)
	}
	loaded := loadedProjectionBundleInputs{
		calculation: calculation,
		calculationReference: ProjectionBundleCalculationReference{
			Role: "effective_independent_expenditures", Calculation: calculation.Calculation,
			CalculationVersion: calculation.CalculationVersion, CalculationSetID: calculation.CalculationSetID,
			ManifestSHA256: calculationDigest, ScheduleEFactSetID: calculation.InputFactSet.FactSetID,
			ScheduleEManifestSHA256: calculation.InputFactSet.ManifestSHA256,
		},
		counts: ProjectionBundleCounts{CalculationResults: calculation.RouteCounts.ResultGroups},
	}
	classicInputs := []struct {
		role    string
		dataset string
		path    string
		count   *uint64
	}{
		{role: "candidate_master", dataset: "candidate-master", path: input.CandidateManifestPath, count: &loaded.counts.CandidateFacts},
		{role: "committee_master", dataset: "committee-master", path: input.CommitteeManifestPath, count: &loaded.counts.CommitteeFacts},
	}
	for _, selected := range classicInputs {
		manifest, digest, loadErr := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, selected.path, selected.dataset)
		if loadErr != nil {
			return loadedProjectionBundleInputs{}, fmt.Errorf("load %s facts: %w", selected.dataset, loadErr)
		}
		if manifest.Cycle != calculation.Cycle || manifest.SourceReleaseID != calculation.SourceReleaseID {
			return loadedProjectionBundleInputs{}, fmt.Errorf("%s facts do not share the calculation cycle and source release", selected.dataset)
		}
		artifactPath, resolveErr := storageartifact.Resolve(storageRoot, manifest.Facts.StorageKey)
		if resolveErr != nil {
			return loadedProjectionBundleInputs{}, resolveErr
		}
		if verifyErr := storageartifact.Verify(ctx, artifactPath, factDescriptor(manifest.Facts)); verifyErr != nil {
			return loadedProjectionBundleInputs{}, fmt.Errorf("verify %s fact artifact: %w", selected.dataset, verifyErr)
		}
		*selected.count = manifest.Counts.Facts
		loaded.factReferences = append(loaded.factReferences, ProjectionBundleFactReference{
			Role: selected.role, Dataset: selected.dataset, FactType: manifest.FactType,
			FactSetID: manifest.FactSetID, ManifestSHA256: digest,
		})
	}
	sort.Slice(loaded.factReferences, func(left, right int) bool {
		return loaded.factReferences[left].Role < loaded.factReferences[right].Role
	})
	return loaded, nil
}

func projectionBundleIdentityParts(cycle, sourceReleaseID string, calculation ProjectionBundleCalculationReference, facts []ProjectionBundleFactReference) []string {
	parts := []string{
		ProjectionBundleSchemaVersion, ProjectionBundleType, ProjectionBundleVersion,
		cycle, sourceReleaseID,
		calculation.Role, calculation.Calculation, calculation.CalculationVersion,
		calculation.CalculationSetID, calculation.ManifestSHA256,
		calculation.ScheduleEFactSetID, calculation.ScheduleEManifestSHA256,
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
		return fmt.Errorf("unsupported independent-expenditure projection bundle manifest")
	}
	if !validDigest(manifest.BundleID) || !validCycle(manifest.Cycle) || !validSourceReleaseID(manifest.SourceReleaseID) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() || len(manifest.InputFactSets) != 2 {
		return fmt.Errorf("independent-expenditure projection bundle identity is incomplete")
	}
	calculation := manifest.InputCalculation
	if calculation.Role != "effective_independent_expenditures" || calculation.Calculation != ContractID ||
		calculation.CalculationVersion != ContractVersion || !validDigest(calculation.CalculationSetID) ||
		!validDigest(calculation.ManifestSHA256) || !validDigest(calculation.ScheduleEFactSetID) ||
		!validDigest(calculation.ScheduleEManifestSHA256) {
		return fmt.Errorf("projection bundle calculation reference is invalid")
	}
	expectedFacts := []struct{ role, dataset, factType string }{
		{role: "candidate_master", dataset: "candidate-master", factType: "fec.candidate_assertion.v1"},
		{role: "committee_master", dataset: "committee-master", factType: "fec.committee_assertion.v1"},
	}
	for index, reference := range manifest.InputFactSets {
		want := expectedFacts[index]
		if reference.Role != want.role || reference.Dataset != want.dataset || reference.FactType != want.factType ||
			!validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) {
			return fmt.Errorf("projection bundle fact role %d is invalid", index)
		}
	}
	if manifest.BundleID != digestParts(projectionBundleIdentityParts(manifest.Cycle, manifest.SourceReleaseID, calculation, manifest.InputFactSets)...) {
		return fmt.Errorf("projection bundle ID does not match canonical inputs")
	}
	if manifest.Counts.CalculationResults == 0 || manifest.Counts.CandidateFacts == 0 || manifest.Counts.CommitteeFacts == 0 {
		return fmt.Errorf("projection bundle counts are incomplete")
	}
	expectedChecks := map[string]struct{}{
		"exact_role_set": {}, "cycle_coherence": {}, "source_release_coherence": {},
		"manifest_immutability": {}, "backing_integrity": {}, "projection_readiness": {},
	}
	if len(manifest.Checks) != len(expectedChecks) {
		return fmt.Errorf("projection bundle checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if _, exists := expectedChecks[check.ID]; !exists || check.Severity != "block" || !check.Passed {
			return fmt.Errorf("required projection bundle check %s is invalid", check.ID)
		}
		delete(expectedChecks, check.ID)
	}
	if len(expectedChecks) != 0 {
		return fmt.Errorf("projection bundle required checks are incomplete")
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
		return fmt.Errorf("projection bundle pointer differs from immutable manifest")
	}
	return nil
}

func resolveProjectionBundleInputPaths(storageRoot string, manifest ProjectionBundleManifest) ProjectionBundleResolvedInput {
	paths := ProjectionBundleResolvedInput{
		CalculationManifestPath: filepath.Join(storageRoot, calculationBase(), "manifests", manifest.InputCalculation.CalculationSetID+".json"),
	}
	for _, reference := range manifest.InputFactSets {
		path := filepath.Join(storageRoot, "facts", "fec", "classic", reference.Dataset, "manifests", reference.FactSetID+".json")
		switch reference.Role {
		case "candidate_master":
			paths.CandidateManifestPath = path
		case "committee_master":
			paths.CommitteeManifestPath = path
		}
	}
	return paths
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
	return filepath.Join("bundles", "fec", "independent-expenditure-projection")
}
