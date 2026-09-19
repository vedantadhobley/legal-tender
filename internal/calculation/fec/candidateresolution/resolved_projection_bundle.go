package candidateresolution

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
	ResolvedProjectionBundleSchemaVersion    = "legal-tender.fec.resolved-independent-expenditure-projection-bundle.v1"
	ResolvedProjectionBundleType             = "fec/resolved-independent-expenditure-projection"
	ResolvedProjectionBundleVersion          = "1.0.0"
	ResolvedProjectionBundlePublisherVersion = "legal-tender.fec.resolved-independent-expenditure-projection-bundle-publisher.v1"
)

type ResolvedProjectionBundleInput struct {
	AggregateManifestPath string
	CandidateManifestPath string
	CommitteeManifestPath string
}

type ResolvedProjectionBundleOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	ExpectedCycle       string
	Clock               func() time.Time
}

type ResolvedProjectionBundleManifest struct {
	Schema           string                                       `json:"$schema"`
	SchemaVersion    string                                       `json:"schema_version"`
	BundleID         string                                       `json:"bundle_id"`
	BundleType       string                                       `json:"bundle_type"`
	BundleVersion    string                                       `json:"bundle_version"`
	PublisherVersion string                                       `json:"publisher_version"`
	Cycle            string                                       `json:"cycle"`
	SourceReleaseID  string                                       `json:"source_release_id"`
	InputCalculation ResolvedProjectionBundleCalculationReference `json:"input_calculation"`
	InputFactSets    []ResolvedProjectionBundleFactReference      `json:"input_fact_sets"`
	RunID            string                                       `json:"run_id"`
	State            string                                       `json:"state"`
	PublishedAt      time.Time                                    `json:"published_at"`
	Counts           ResolvedProjectionBundleCounts               `json:"counts"`
	Checks           []Check                                      `json:"checks"`
}

type ResolvedProjectionBundleCalculationReference struct {
	Role                                string `json:"role"`
	Calculation                         string `json:"calculation"`
	CalculationVersion                  string `json:"calculation_version"`
	CalculationSetID                    string `json:"calculation_set_id"`
	ManifestSHA256                      string `json:"manifest_sha256"`
	CandidateResolutionCalculationSetID string `json:"candidate_resolution_calculation_set_id"`
	CandidateResolutionManifestSHA256   string `json:"candidate_resolution_manifest_sha256"`
	ResultsSHA256                       string `json:"results_sha256"`
	ExceptionsSHA256                    string `json:"exceptions_sha256"`
}

type ResolvedProjectionBundleFactReference struct {
	Role           string `json:"role"`
	Dataset        string `json:"dataset"`
	FactType       string `json:"fact_type"`
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type ResolvedProjectionBundleCounts struct {
	CalculationResults     uint64 `json:"calculation_results"`
	CalculationExceptions  uint64 `json:"calculation_exceptions"`
	ProjectableDecisions   uint64 `json:"projectable_decisions"`
	UnprojectableDecisions uint64 `json:"unprojectable_decisions"`
	CandidateFacts         uint64 `json:"candidate_facts"`
	CommitteeFacts         uint64 `json:"committee_facts"`
}

type ResolvedProjectionBundleResolvedInput struct {
	AggregateManifestPath  string
	ResolutionManifestPath string
	CandidateManifestPath  string
	CommitteeManifestPath  string
}

type loadedResolvedProjectionBundleInputs struct {
	aggregate          AggregateManifest
	aggregateReference ResolvedProjectionBundleCalculationReference
	factReferences     []ResolvedProjectionBundleFactReference
	counts             ResolvedProjectionBundleCounts
}

// PublishResolvedProjectionBundle freezes the resolved aggregate and the
// exact candidate and committee masters required by its ArangoDB projection.
func PublishResolvedProjectionBundle(ctx context.Context, input ResolvedProjectionBundleInput, runID string, options ResolvedProjectionBundleOptions) (ResolvedProjectionBundleManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" {
		return ResolvedProjectionBundleManifest{}, fmt.Errorf("storage root is required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ResolvedProjectionBundleManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	for name, path := range map[string]string{
		"resolved independent-expenditure calculation": input.AggregateManifestPath,
		"candidate master facts":                       input.CandidateManifestPath,
		"committee master facts":                       input.CommitteeManifestPath,
	} {
		if path == "" {
			return ResolvedProjectionBundleManifest{}, fmt.Errorf("%s path is required", name)
		}
	}

	loaded, err := loadResolvedProjectionBundleInputs(ctx, options.StorageRoot, input, options.ExpectedCycle)
	if err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	bundleID := digestParts(resolvedProjectionBundleIdentityParts(
		loaded.aggregate.Cycle, loaded.aggregate.SourceReleaseID, loaded.aggregateReference, loaded.factReferences,
	)...)
	basePath := resolvedProjectionBundleBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", loaded.aggregate.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+loaded.aggregate.Cycle+".lock"))
	if err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	defer unlock()
	current, err := readResolvedProjectionBundleIfPresent(currentPath)
	if err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	if current != nil {
		if err := validateResolvedProjectionBundle(*current); err != nil {
			return ResolvedProjectionBundleManifest{}, err
		}
		if current.Cycle != loaded.aggregate.Cycle {
			return ResolvedProjectionBundleManifest{}, fmt.Errorf("current resolved projection bundle belongs to cycle %s", current.Cycle)
		}
		if current.BundleID == bundleID {
			return *current, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", bundleID+".json")
	if existing, readErr := readResolvedProjectionBundleIfPresent(manifestPath); readErr != nil {
		return ResolvedProjectionBundleManifest{}, readErr
	} else if existing != nil {
		if err := validateResolvedProjectionBundle(*existing); err != nil {
			return ResolvedProjectionBundleManifest{}, err
		}
		if existing.InputCalculation != loaded.aggregateReference ||
			!reflect.DeepEqual(existing.InputFactSets, loaded.factReferences) || existing.Counts != loaded.counts {
			return ResolvedProjectionBundleManifest{}, fmt.Errorf("immutable resolved projection bundle collision")
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ResolvedProjectionBundleManifest{}, err
		}
		return *existing, nil
	}

	manifest := ResolvedProjectionBundleManifest{
		Schema: "manifest.schema.json", SchemaVersion: ResolvedProjectionBundleSchemaVersion,
		BundleID: bundleID, BundleType: ResolvedProjectionBundleType, BundleVersion: ResolvedProjectionBundleVersion,
		PublisherVersion: ResolvedProjectionBundlePublisherVersion, Cycle: loaded.aggregate.Cycle,
		SourceReleaseID: loaded.aggregate.SourceReleaseID, InputCalculation: loaded.aggregateReference,
		InputFactSets: loaded.factReferences, RunID: runID, State: "ready", PublishedAt: options.Clock().UTC(),
		Counts: loaded.counts,
		Checks: []Check{
			{ID: "exact_role_set", Passed: true, Severity: "block", Detail: "the bundle contains exactly one resolved aggregate and the two master fact roles required by the graph"},
			{ID: "resolution_lineage", Passed: true, Severity: "block", Detail: "the aggregate's exact candidate-resolution manifest and decision artifact passed immutable backing verification"},
			{ID: "candidate_identity", Passed: true, Severity: "block", Detail: "the candidate master exactly matches the fact set used by candidate resolution"},
			{ID: "cycle_coherence", Passed: true, Severity: "block", Detail: "the aggregate and both master fact sets belong to one FEC cycle"},
			{ID: "source_release_coherence", Passed: true, Severity: "block", Detail: "the aggregate and both master fact sets belong to one coordinated source release"},
			{ID: "backing_integrity", Passed: true, Severity: "block", Detail: "all calculation, decision, result, exception, and master-fact artifacts passed complete digest verification"},
			{ID: "projection_readiness", Passed: true, Severity: "block", Detail: "projectable groups and explicit unprojectable coverage are ready for the resolved graph projection"},
		},
	}
	if err := validateResolvedProjectionBundle(manifest); err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ResolvedProjectionBundleManifest{}, err
	}
	return manifest, nil
}

// LoadResolvedProjectionBundle verifies the bundle and all immutable inputs,
// then returns only immutable paths for graph construction.
func LoadResolvedProjectionBundle(ctx context.Context, storageRoot, path string) (ResolvedProjectionBundleManifest, string, ResolvedProjectionBundleResolvedInput, error) {
	if storageRoot == "" || path == "" {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, fmt.Errorf("storage root and resolved projection bundle path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, err
	}
	pointer, _, err := readStrictJSON[ResolvedProjectionBundleManifest](path)
	if err != nil {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, err
	}
	if err := validateResolvedProjectionBundle(pointer); err != nil {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, err
	}
	immutablePath := filepath.Join(storageRoot, resolvedProjectionBundleBase(), "manifests", pointer.BundleID+".json")
	immutable, digest, err := readStrictJSON[ResolvedProjectionBundleManifest](immutablePath)
	if err != nil {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, err
	}
	if !reflect.DeepEqual(pointer, immutable) {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, fmt.Errorf("resolved projection bundle pointer differs from immutable manifest")
	}
	resolved := resolveResolvedProjectionBundlePaths(storageRoot, immutable)
	loaded, err := loadResolvedProjectionBundleInputs(ctx, storageRoot, ResolvedProjectionBundleInput{
		AggregateManifestPath: resolved.AggregateManifestPath,
		CandidateManifestPath: resolved.CandidateManifestPath,
		CommitteeManifestPath: resolved.CommitteeManifestPath,
	}, immutable.Cycle)
	if err != nil {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, err
	}
	if loaded.aggregate.SourceReleaseID != immutable.SourceReleaseID || loaded.aggregateReference != immutable.InputCalculation ||
		!reflect.DeepEqual(loaded.factReferences, immutable.InputFactSets) || loaded.counts != immutable.Counts {
		return ResolvedProjectionBundleManifest{}, "", ResolvedProjectionBundleResolvedInput{}, fmt.Errorf("resolved projection bundle inputs differ from immutable backing")
	}
	return immutable, digest, resolved, nil
}

func loadResolvedProjectionBundleInputs(ctx context.Context, storageRoot string, input ResolvedProjectionBundleInput, expectedCycle string) (loadedResolvedProjectionBundleInputs, error) {
	aggregate, aggregateDigest, err := LoadPublishedAggregateManifest(ctx, storageRoot, input.AggregateManifestPath)
	if err != nil {
		return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("load resolved independent expenditures: %w", err)
	}
	if expectedCycle != "" && aggregate.Cycle != expectedCycle {
		return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("resolved independent expenditures belong to cycle %s, expected %s", aggregate.Cycle, expectedCycle)
	}
	resolutionPath := filepath.Join(storageRoot, calculationBase(), "manifests", aggregate.InputResolution.CalculationSetID+".json")
	resolution, resolutionDigest, err := LoadPublishedManifest(ctx, storageRoot, resolutionPath)
	if err != nil {
		return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("load aggregate candidate-resolution ancestry: %w", err)
	}
	if resolutionDigest != aggregate.InputResolution.ManifestSHA256 || resolution.CalculationSetID != aggregate.InputResolution.CalculationSetID ||
		resolution.Decisions.CompressedSHA256 != aggregate.InputResolution.DecisionsSHA256 || resolution.Cycle != aggregate.Cycle ||
		resolution.SourceReleaseID != aggregate.SourceReleaseID {
		return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("resolved aggregate does not match candidate-resolution ancestry")
	}
	loaded := loadedResolvedProjectionBundleInputs{
		aggregate: aggregate,
		aggregateReference: ResolvedProjectionBundleCalculationReference{
			Role: "resolved_independent_expenditures", Calculation: aggregate.Calculation,
			CalculationVersion: aggregate.CalculationVersion, CalculationSetID: aggregate.CalculationSetID,
			ManifestSHA256: aggregateDigest, CandidateResolutionCalculationSetID: resolution.CalculationSetID,
			CandidateResolutionManifestSHA256: resolutionDigest, ResultsSHA256: aggregate.Results.CompressedSHA256,
			ExceptionsSHA256: aggregate.Exceptions.CompressedSHA256,
		},
		counts: ResolvedProjectionBundleCounts{
			CalculationResults: aggregate.Counts.ResultGroups, CalculationExceptions: aggregate.Counts.Exceptions,
			ProjectableDecisions:   aggregate.Counts.ProjectableDecisions,
			UnprojectableDecisions: aggregate.Counts.UnprojectableDecisions,
		},
	}
	for _, selected := range []struct {
		role, dataset, path string
		count               *uint64
	}{
		{role: "candidate_master", dataset: "candidate-master", path: input.CandidateManifestPath, count: &loaded.counts.CandidateFacts},
		{role: "committee_master", dataset: "committee-master", path: input.CommitteeManifestPath, count: &loaded.counts.CommitteeFacts},
	} {
		manifest, digest, loadErr := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, selected.path, selected.dataset)
		if loadErr != nil {
			return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("load %s facts: %w", selected.dataset, loadErr)
		}
		if manifest.Cycle != aggregate.Cycle || manifest.SourceReleaseID != aggregate.SourceReleaseID {
			return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("%s facts do not share the aggregate cycle and source release", selected.dataset)
		}
		if selected.dataset == "candidate-master" &&
			(manifest.FactSetID != resolution.InputCandidateFactSet.FactSetID || digest != resolution.InputCandidateFactSet.ManifestSHA256) {
			return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("candidate master differs from the exact candidate-resolution input")
		}
		artifactPath, resolveErr := storageartifact.Resolve(storageRoot, manifest.Facts.StorageKey)
		if resolveErr != nil {
			return loadedResolvedProjectionBundleInputs{}, resolveErr
		}
		if verifyErr := storageartifact.Verify(ctx, artifactPath, classicDescriptor(manifest.Facts)); verifyErr != nil {
			return loadedResolvedProjectionBundleInputs{}, fmt.Errorf("verify %s fact artifact: %w", selected.dataset, verifyErr)
		}
		*selected.count = manifest.Counts.Facts
		loaded.factReferences = append(loaded.factReferences, ResolvedProjectionBundleFactReference{
			Role: selected.role, Dataset: selected.dataset, FactType: manifest.FactType,
			FactSetID: manifest.FactSetID, ManifestSHA256: digest,
		})
	}
	sort.Slice(loaded.factReferences, func(left, right int) bool {
		return loaded.factReferences[left].Role < loaded.factReferences[right].Role
	})
	return loaded, nil
}

func resolvedProjectionBundleIdentityParts(cycle, sourceReleaseID string, calculation ResolvedProjectionBundleCalculationReference, facts []ResolvedProjectionBundleFactReference) []string {
	parts := []string{
		ResolvedProjectionBundleSchemaVersion, ResolvedProjectionBundleType, ResolvedProjectionBundleVersion,
		cycle, sourceReleaseID, calculation.Role, calculation.Calculation, calculation.CalculationVersion,
		calculation.CalculationSetID, calculation.ManifestSHA256, calculation.CandidateResolutionCalculationSetID,
		calculation.CandidateResolutionManifestSHA256, calculation.ResultsSHA256, calculation.ExceptionsSHA256,
	}
	for _, reference := range facts {
		parts = append(parts, reference.Role, reference.Dataset, reference.FactType, reference.FactSetID, reference.ManifestSHA256)
	}
	return parts
}

func validateResolvedProjectionBundle(manifest ResolvedProjectionBundleManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ResolvedProjectionBundleSchemaVersion ||
		manifest.BundleType != ResolvedProjectionBundleType || manifest.BundleVersion != ResolvedProjectionBundleVersion ||
		manifest.PublisherVersion != ResolvedProjectionBundlePublisherVersion || manifest.State != "ready" {
		return fmt.Errorf("unsupported resolved independent-expenditure projection bundle")
	}
	if !validDigest(manifest.BundleID) || !validCycle(manifest.Cycle) || !validSourceReleaseID(manifest.SourceReleaseID) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() || len(manifest.InputFactSets) != 2 {
		return fmt.Errorf("resolved projection bundle identity is incomplete")
	}
	calculation := manifest.InputCalculation
	if calculation.Role != "resolved_independent_expenditures" || calculation.Calculation != AggregateContractID ||
		calculation.CalculationVersion != AggregateContractVersion || !validDigest(calculation.CalculationSetID) ||
		!validDigest(calculation.ManifestSHA256) || !validDigest(calculation.CandidateResolutionCalculationSetID) ||
		!validDigest(calculation.CandidateResolutionManifestSHA256) || !validDigest(calculation.ResultsSHA256) ||
		!validDigest(calculation.ExceptionsSHA256) {
		return fmt.Errorf("resolved projection bundle calculation reference is invalid")
	}
	expectedFacts := []struct{ role, dataset, factType string }{
		{role: "candidate_master", dataset: "candidate-master", factType: "fec.candidate_assertion.v1"},
		{role: "committee_master", dataset: "committee-master", factType: "fec.committee_assertion.v1"},
	}
	for index, reference := range manifest.InputFactSets {
		want := expectedFacts[index]
		if reference.Role != want.role || reference.Dataset != want.dataset || reference.FactType != want.factType ||
			!validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) {
			return fmt.Errorf("resolved projection bundle fact role %d is invalid", index)
		}
	}
	if manifest.BundleID != digestParts(resolvedProjectionBundleIdentityParts(manifest.Cycle, manifest.SourceReleaseID, calculation, manifest.InputFactSets)...) {
		return fmt.Errorf("resolved projection bundle ID does not match canonical inputs")
	}
	if manifest.Counts.CalculationResults == 0 || manifest.Counts.ProjectableDecisions == 0 ||
		manifest.Counts.CandidateFacts == 0 || manifest.Counts.CommitteeFacts == 0 ||
		manifest.Counts.CalculationExceptions != manifest.Counts.UnprojectableDecisions {
		return fmt.Errorf("resolved projection bundle counts are incomplete")
	}
	expectedChecks := map[string]struct{}{
		"exact_role_set": {}, "resolution_lineage": {}, "candidate_identity": {}, "cycle_coherence": {},
		"source_release_coherence": {}, "backing_integrity": {}, "projection_readiness": {},
	}
	if len(manifest.Checks) != len(expectedChecks) {
		return fmt.Errorf("resolved projection bundle checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if _, ok := expectedChecks[check.ID]; !ok || check.Severity != "block" || !check.Passed {
			return fmt.Errorf("resolved projection bundle check %s is invalid", check.ID)
		}
		delete(expectedChecks, check.ID)
	}
	return nil
}

func resolveResolvedProjectionBundlePaths(storageRoot string, manifest ResolvedProjectionBundleManifest) ResolvedProjectionBundleResolvedInput {
	resolved := ResolvedProjectionBundleResolvedInput{
		AggregateManifestPath:  filepath.Join(storageRoot, aggregateCalculationBase(), "manifests", manifest.InputCalculation.CalculationSetID+".json"),
		ResolutionManifestPath: filepath.Join(storageRoot, calculationBase(), "manifests", manifest.InputCalculation.CandidateResolutionCalculationSetID+".json"),
	}
	for _, reference := range manifest.InputFactSets {
		path := filepath.Join(storageRoot, "facts", "fec", "classic", reference.Dataset, "manifests", reference.FactSetID+".json")
		if reference.Role == "candidate_master" {
			resolved.CandidateManifestPath = path
		} else if reference.Role == "committee_master" {
			resolved.CommitteeManifestPath = path
		}
	}
	return resolved
}

func readResolvedProjectionBundleIfPresent(path string) (*ResolvedProjectionBundleManifest, error) {
	manifest, _, err := readStrictJSON[ResolvedProjectionBundleManifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func resolvedProjectionBundleBase() string {
	return filepath.Join("bundles", "fec", "resolved-independent-expenditure-projection")
}
