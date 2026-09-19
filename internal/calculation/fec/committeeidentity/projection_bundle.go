package committeeidentity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

const (
	ProjectionBundleSchemaVersion    = "legal-tender.fec.receiver-reported-committee-flow-projection-bundle.v2"
	ProjectionBundleType             = "fec/receiver-reported-committee-flow-projection"
	ProjectionBundleVersion          = "2.0.0"
	ProjectionBundlePublisherVersion = "legal-tender.fec.receiver-reported-committee-flow-projection-bundle-publisher.v2"
)

type ProjectionBundleInput struct {
	BaseBundlePath          string
	IdentityCalculationPath string
}

type ProjectionBundleOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	ExpectedCycle       string
	Clock               func() time.Time
}

type ProjectionBundleManifest struct {
	Schema           string                       `json:"$schema"`
	SchemaVersion    string                       `json:"schema_version"`
	BundleID         string                       `json:"bundle_id"`
	BundleType       string                       `json:"bundle_type"`
	BundleVersion    string                       `json:"bundle_version"`
	PublisherVersion string                       `json:"publisher_version"`
	Cycle            string                       `json:"cycle"`
	SourceReleaseID  string                       `json:"source_release_id"`
	BaseFlowBundle   BaseFlowBundleReference      `json:"base_flow_bundle"`
	IdentityCoverage IdentityCalculationReference `json:"identity_coverage"`
	RunID            string                       `json:"run_id"`
	State            string                       `json:"state"`
	PublishedAt      time.Time                    `json:"published_at"`
	Counts           ProjectionBundleCounts       `json:"counts"`
	Checks           []Check                      `json:"checks"`
}

type BaseFlowBundleReference struct {
	Role           string `json:"role"`
	BundleID       string `json:"bundle_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type IdentityCalculationReference struct {
	Role             string `json:"role"`
	CalculationSetID string `json:"calculation_set_id"`
	ManifestSHA256   string `json:"manifest_sha256"`
	DecisionsSHA256  string `json:"decisions_sha256"`
}

type ProjectionBundleCounts struct {
	FlowResultGroups              uint64 `json:"flow_result_groups"`
	CommitteeFacts                uint64 `json:"committee_facts"`
	ReferencedCommittees          uint64 `json:"referenced_committees"`
	CurrentCycleMasters           uint64 `json:"current_cycle_masters"`
	HistoricalRegistrations       uint64 `json:"historical_registrations"`
	AlternateReleaseRegistrations uint64 `json:"alternate_release_registrations"`
	UnresolvedReportedIDs         uint64 `json:"unresolved_reported_ids"`
}

type ProjectionBundleResolvedInput struct {
	BaseBundlePath          string
	IdentityCalculationPath string
	BaseFlowInputs          fecflows.ProjectionBundleResolvedInput
}

type loadedProjectionBundleInputs struct {
	base              fecflows.ProjectionBundleManifest
	baseDigest        string
	baseResolved      fecflows.ProjectionBundleResolvedInput
	identity          Manifest
	identityDigest    string
	baseReference     BaseFlowBundleReference
	identityReference IdentityCalculationReference
	counts            ProjectionBundleCounts
}

// PublishProjectionBundle freezes the shipped v1 flow inputs together with
// one exact committee-identity coverage calculation. The v1 bundle remains an
// immutable input rather than being rewritten in place.
func PublishProjectionBundle(ctx context.Context, input ProjectionBundleInput, runID string, options ProjectionBundleOptions) (ProjectionBundleManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.BaseBundlePath == "" || input.IdentityCalculationPath == "" {
		return ProjectionBundleManifest{}, fmt.Errorf("storage root, base flow bundle, and identity calculation are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ProjectionBundleManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	loaded, err := loadProjectionBundleInputs(ctx, options.StorageRoot, input, options.ExpectedCycle)
	if err != nil {
		return ProjectionBundleManifest{}, err
	}
	bundleID := projectionBundleIdentity(
		loaded.base.Cycle, loaded.base.SourceReleaseID,
		loaded.baseReference, loaded.identityReference,
	)
	basePath := projectionBundleBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", loaded.base.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ProjectionBundleManifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+loaded.base.Cycle+".lock"))
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
			return ProjectionBundleManifest{}, fmt.Errorf("invalid current v2 receiver-flow projection bundle: %w", err)
		}
		if err := validateProjectionBundleBacking(ctx, options.StorageRoot, *current); err != nil {
			return ProjectionBundleManifest{}, err
		}
		if current.Cycle != loaded.base.Cycle {
			return ProjectionBundleManifest{}, fmt.Errorf("current v2 receiver-flow bundle belongs to cycle %s", current.Cycle)
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
		if existing.BaseFlowBundle != loaded.baseReference || existing.IdentityCoverage != loaded.identityReference || existing.Counts != loaded.counts {
			return ProjectionBundleManifest{}, fmt.Errorf("immutable v2 receiver-flow projection bundle collision")
		}
		if err := validateProjectionBundleBacking(ctx, options.StorageRoot, *existing); err != nil {
			return ProjectionBundleManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ProjectionBundleManifest{}, err
		}
		return *existing, nil
	}

	manifest := ProjectionBundleManifest{
		Schema: "manifest.schema.json", SchemaVersion: ProjectionBundleSchemaVersion,
		BundleID: bundleID, BundleType: ProjectionBundleType, BundleVersion: ProjectionBundleVersion,
		PublisherVersion: ProjectionBundlePublisherVersion, Cycle: loaded.base.Cycle,
		SourceReleaseID: loaded.base.SourceReleaseID, BaseFlowBundle: loaded.baseReference,
		IdentityCoverage: loaded.identityReference, RunID: runID, State: "ready",
		PublishedAt: options.Clock().UTC(), Counts: loaded.counts,
		Checks: []Check{
			{ID: "base_bundle_lineage", Passed: true, Severity: "block", Detail: "the shipped v1 flow readiness bundle and all backing remain immutable verified inputs"},
			{ID: "identity_lineage", Passed: true, Severity: "block", Detail: "the exact committee-identity manifest and complete decision artifact passed verification"},
			{ID: "cycle_release_coherence", Passed: true, Severity: "block", Detail: "the flow and identity inputs bind one cycle and coordinated FEC release"},
			{ID: "flow_ancestry_coherence", Passed: true, Severity: "block", Detail: "the identity calculation names this exact base flow bundle, calculation, and selected committee master"},
			{ID: "identity_membership", Passed: loaded.counts.CurrentCycleMasters+loaded.counts.HistoricalRegistrations+loaded.counts.AlternateReleaseRegistrations+loaded.counts.UnresolvedReportedIDs == loaded.counts.ReferencedCommittees, Severity: "block", Detail: "every referenced committee has one graph identity-coverage state"},
			{ID: "projection_readiness", Passed: true, Severity: "block", Detail: "the exact v2 input bundle is ready for the identity-aware receiver-flow projection"},
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

// LoadProjectionBundle verifies the v2 pointer, immutable manifest, v1 base
// bundle, identity calculation, and their ancestry coherence.
func LoadProjectionBundle(ctx context.Context, storageRoot, path string) (ProjectionBundleManifest, string, ProjectionBundleResolvedInput, error) {
	if storageRoot == "" || path == "" {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("storage root and v2 receiver-flow bundle path are required")
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
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("v2 receiver-flow bundle pointer differs from immutable manifest")
	}
	loaded, err := loadProjectionBundleInputs(ctx, storageRoot, ProjectionBundleInput{
		BaseBundlePath:          filepath.Join(storageRoot, baseProjectionBundleBase(), "manifests", immutable.BaseFlowBundle.BundleID+".json"),
		IdentityCalculationPath: filepath.Join(storageRoot, calculationBase(), "manifests", immutable.IdentityCoverage.CalculationSetID+".json"),
	}, immutable.Cycle)
	if err != nil {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, err
	}
	if loaded.baseReference != immutable.BaseFlowBundle || loaded.identityReference != immutable.IdentityCoverage || loaded.counts != immutable.Counts {
		return ProjectionBundleManifest{}, "", ProjectionBundleResolvedInput{}, fmt.Errorf("v2 receiver-flow bundle inputs differ from immutable backing")
	}
	return immutable, digest, ProjectionBundleResolvedInput{
		BaseBundlePath:          filepath.Join(storageRoot, baseProjectionBundleBase(), "manifests", immutable.BaseFlowBundle.BundleID+".json"),
		IdentityCalculationPath: filepath.Join(storageRoot, calculationBase(), "manifests", immutable.IdentityCoverage.CalculationSetID+".json"),
		BaseFlowInputs:          loaded.baseResolved,
	}, nil
}

func loadProjectionBundleInputs(ctx context.Context, storageRoot string, input ProjectionBundleInput, expectedCycle string) (loadedProjectionBundleInputs, error) {
	base, baseDigest, baseResolved, err := fecflows.LoadProjectionBundle(ctx, storageRoot, input.BaseBundlePath)
	if err != nil {
		return loadedProjectionBundleInputs{}, fmt.Errorf("load v1 receiver-flow base bundle: %w", err)
	}
	identity, identityDigest, err := LoadPublishedManifest(ctx, storageRoot, input.IdentityCalculationPath)
	if err != nil {
		return loadedProjectionBundleInputs{}, fmt.Errorf("load committee-identity calculation: %w", err)
	}
	if expectedCycle != "" && base.Cycle != expectedCycle {
		return loadedProjectionBundleInputs{}, fmt.Errorf("base flow bundle belongs to cycle %s, expected %s", base.Cycle, expectedCycle)
	}
	if identity.Cycle != base.Cycle || identity.SourceReleaseID != base.SourceReleaseID {
		return loadedProjectionBundleInputs{}, fmt.Errorf("flow and identity inputs do not share cycle and source release")
	}
	if identity.Inputs.ReadinessBundle.BundleID != base.BundleID || identity.Inputs.ReadinessBundle.ManifestSHA256 != baseDigest ||
		identity.Inputs.Calculation.CalculationSetID != base.InputCalculation.CalculationSetID ||
		identity.Inputs.Calculation.ManifestSHA256 != base.InputCalculation.ManifestSHA256 ||
		identity.Inputs.CurrentCommitteeMaster.FactSetID != base.InputFactSets[0].FactSetID ||
		identity.Inputs.CurrentCommitteeMaster.ManifestSHA256 != base.InputFactSets[0].ManifestSHA256 {
		return loadedProjectionBundleInputs{}, fmt.Errorf("committee-identity ancestry does not match the base flow bundle")
	}
	baseReference := BaseFlowBundleReference{Role: "receiver_reported_committee_flow_v1", BundleID: base.BundleID, ManifestSHA256: baseDigest}
	identityReference := IdentityCalculationReference{
		Role: "committee_identity_coverage", CalculationSetID: identity.CalculationSetID,
		ManifestSHA256: identityDigest, DecisionsSHA256: identity.Decisions.CompressedSHA256,
	}
	counts := ProjectionBundleCounts{
		FlowResultGroups: base.Counts.CalculationResults, CommitteeFacts: base.Counts.CommitteeFacts,
		ReferencedCommittees: identity.Counts.ReferencedCommittees, CurrentCycleMasters: identity.Counts.CurrentCycleMasters,
		HistoricalRegistrations:       identity.Counts.HistoricalRegistrations,
		AlternateReleaseRegistrations: identity.Counts.AlternateReleaseRegistrations,
		UnresolvedReportedIDs:         identity.Counts.UnresolvedReportedIDs,
	}
	return loadedProjectionBundleInputs{
		base: base, baseDigest: baseDigest, baseResolved: baseResolved, identity: identity,
		identityDigest: identityDigest, baseReference: baseReference,
		identityReference: identityReference, counts: counts,
	}, nil
}

func validateProjectionBundleBacking(ctx context.Context, storageRoot string, manifest ProjectionBundleManifest) error {
	_, _, _, err := LoadProjectionBundle(ctx, storageRoot, filepath.Join(storageRoot, projectionBundleBase(), "manifests", manifest.BundleID+".json"))
	return err
}

func validateProjectionBundleManifest(manifest ProjectionBundleManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ProjectionBundleSchemaVersion ||
		manifest.BundleType != ProjectionBundleType || manifest.BundleVersion != ProjectionBundleVersion ||
		manifest.PublisherVersion != ProjectionBundlePublisherVersion || manifest.State != "ready" ||
		!validDigest(manifest.BundleID) || manifest.Cycle == "" || manifest.SourceReleaseID == "" || manifest.RunID == "" || manifest.PublishedAt.IsZero() ||
		manifest.BaseFlowBundle.Role != "receiver_reported_committee_flow_v1" || !validDigest(manifest.BaseFlowBundle.BundleID) || !validDigest(manifest.BaseFlowBundle.ManifestSHA256) ||
		manifest.IdentityCoverage.Role != "committee_identity_coverage" || !validDigest(manifest.IdentityCoverage.CalculationSetID) ||
		!validDigest(manifest.IdentityCoverage.ManifestSHA256) || !validDigest(manifest.IdentityCoverage.DecisionsSHA256) {
		return fmt.Errorf("invalid v2 receiver-flow projection bundle")
	}
	if manifest.Counts.CurrentCycleMasters+manifest.Counts.HistoricalRegistrations+manifest.Counts.AlternateReleaseRegistrations+manifest.Counts.UnresolvedReportedIDs != manifest.Counts.ReferencedCommittees {
		return fmt.Errorf("v2 receiver-flow projection bundle identity counts do not conserve")
	}
	if manifest.BundleID != projectionBundleIdentity(manifest.Cycle, manifest.SourceReleaseID, manifest.BaseFlowBundle, manifest.IdentityCoverage) {
		return fmt.Errorf("v2 receiver-flow projection bundle ID does not match its inputs")
	}
	if len(manifest.Checks) == 0 {
		return fmt.Errorf("v2 receiver-flow projection bundle contains no checks")
	}
	for _, check := range manifest.Checks {
		if check.ID == "" || check.Severity != "block" || !check.Passed {
			return fmt.Errorf("v2 receiver-flow projection bundle contains a failed or invalid check")
		}
	}
	return nil
}

func readProjectionBundleManifestIfPresent(path string) (*ProjectionBundleManifest, error) {
	manifest, _, err := readStrictJSON[ProjectionBundleManifest](path)
	if err == nil {
		return &manifest, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return nil, err
}

func projectionBundleBase() string {
	return filepath.Join("bundles", "fec", "receiver-reported-committee-flow-projection", "v2")
}

func baseProjectionBundleBase() string {
	return filepath.Join("bundles", "fec", "receiver-reported-committee-flow-projection")
}

func projectionBundleIdentity(cycle, sourceReleaseID string, base BaseFlowBundleReference, identity IdentityCalculationReference) string {
	return digestParts(
		ProjectionBundleSchemaVersion, ProjectionBundleType, ProjectionBundleVersion,
		cycle, sourceReleaseID, base.BundleID, base.ManifestSHA256,
		identity.CalculationSetID, identity.ManifestSHA256, identity.DecisionsSHA256,
	)
}
