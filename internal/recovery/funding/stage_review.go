package funding

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"syscall"

	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const StageReviewVersion = "legal-tender.release-stage-evidence-review.v1"

type StageReviewOptions struct {
	StorageRoot, ReleasePath, ReleaseSHA256, StagePath, StageSHA256, BuildSHA256 string
}

// StageReview verifies a candidate control record against published metadata.
// It never substitutes that record for a release's exact original byte pin.
type StageReview struct {
	Version                 string    `json:"schema_version"`
	ID                      string    `json:"review_id"`
	BuildSHA256             string    `json:"executable_sha256"`
	Release                 Reference `json:"release"`
	Plan                    Reference `json:"plan"`
	Acquisition             Reference `json:"acquisition"`
	CandidateStage          Reference `json:"candidate_stage"`
	OriginalStageSHA256     string    `json:"release_referenced_stage_sha256"`
	OriginalStageBytesEqual bool      `json:"candidate_is_exact_referenced_stage"`
	SelectionMatches        bool      `json:"published_source_and_output_descriptors_match"`
	Sources                 uint64    `json:"source_descriptors"`
	Outputs                 uint64    `json:"staged_output_descriptors"`
	Checks                  []string  `json:"checks"`
	SourceBytesVerified     bool      `json:"source_bytes_verified"`
	RecoveryReady           bool      `json:"recovery_ready"`
	Limitations             []string  `json:"limitations"`
}

// ReviewStage reads only four pinned metadata files. The plan and acquisition
// locators come from the release-control publication contract, never discovery.
func ReviewStage(ctx context.Context, o StageReviewOptions) (StageReview, error) {
	var result StageReview
	if !validDigest(o.BuildSHA256) {
		return result, fmt.Errorf("exact executable digest required")
	}
	root, err := os.OpenRoot(o.StorageRoot)
	if err != nil {
		return result, err
	}
	defer root.Close()
	var m rel.ReleaseManifest
	if err = readReviewMetadata(ctx, root, o.ReleasePath, o.ReleaseSHA256, &m); err != nil {
		return result, fmt.Errorf("release: %w", err)
	}
	if issues := rel.ValidateKnownManifest(m); len(issues) != 0 {
		return result, fmt.Errorf("invalid release metadata: %s", issues[0].Code)
	}
	if !rel.ValidReleaseID(m.ReleaseID) {
		return result, fmt.Errorf("invalid release identity")
	}
	planRef := Reference{Kind: "plan", ID: m.PlanSHA256, SHA256: m.PlanSHA256}
	planRef.Path = defaultPath(planRef)
	acqRef := Reference{Kind: "acquisition", ID: m.AcquisitionSHA256, SHA256: m.AcquisitionSHA256}
	acqRef.Path = defaultPath(acqRef)
	var p rel.ReleasePlan
	var a rel.AcquisitionResult
	var s rel.StageResult
	if err = readReviewMetadata(ctx, root, planRef.Path, planRef.SHA256, &p); err != nil {
		return result, fmt.Errorf("plan: %w", err)
	}
	if err = readReviewMetadata(ctx, root, acqRef.Path, acqRef.SHA256, &a); err != nil {
		return result, fmt.Errorf("acquisition: %w", err)
	}
	if err = readReviewMetadata(ctx, root, o.StagePath, o.StageSHA256, &s); err != nil {
		return result, fmt.Errorf("candidate stage: %w", err)
	}
	inv, _ := rel.InventoryForVersion(m.InventoryVersion)
	if issues := rel.ValidatePlan(inv, p); len(issues) != 0 || p.Status != rel.PlanUpdateAvailable {
		return result, fmt.Errorf("invalid successful plan metadata")
	}
	if issues := rel.ValidateAcquisitionResult(inv, a); len(issues) != 0 || a.Status != rel.AcquisitionAcquired {
		return result, fmt.Errorf("invalid successful acquisition metadata")
	}
	if issues := rel.ValidateStageResult(inv, s); len(issues) != 0 || s.Status != rel.StageStaged {
		return result, fmt.Errorf("invalid successful candidate stage metadata")
	}
	if err = compareStageEvidence(m, p, a, s); err != nil {
		return result, err
	}
	result = StageReview{
		Version: StageReviewVersion, BuildSHA256: o.BuildSHA256,
		Release: Reference{Kind: "release", ID: m.ReleaseID, SHA256: o.ReleaseSHA256, Path: o.ReleasePath}, Plan: planRef, Acquisition: acqRef,
		CandidateStage:      Reference{Kind: "stage", ID: o.StageSHA256, SHA256: o.StageSHA256, Path: o.StagePath},
		OriginalStageSHA256: m.StageSHA256, OriginalStageBytesEqual: o.StageSHA256 == m.StageSHA256,
		SelectionMatches: true, Sources: uint64(len(m.Artifacts)), Outputs: uint64(len(m.StagedOutputs)),
		Checks:      []string{"four_exact_metadata_byte_hashes", "closed_known_metadata_contracts", "release_plan_acquisition_candidate_stage_identity", "complete_selected_source_metadata", "complete_acquired_source_descriptors", "complete_staged_output_descriptors", "every_staged_output_references_its_acquired_source"},
		Limitations: []string{"candidate_retains_its_own_byte_identity_never_replaces_release_pin", "original_stage_only_compared_by_expected_byte_digest_not_unavailable_fields", "candidate_run_times_storage_observations_and_checks_do_not_reconstruct_original_run", "output_order_ignored_all_descriptor_fields_compared", "no_source_body_hash_decompression_calculation_graph_or_retention_check"},
	}
	result.ID = identity(result)
	return result, nil
}

func readReviewMetadata(ctx context.Context, root *os.Root, file, sha string, target any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !relative(file) || !validDigest(sha) {
		return fmt.Errorf("exact digest and immutable relative path required")
	}
	info, err := root.Lstat(file)
	if err != nil {
		return fmt.Errorf("metadata unavailable")
	}
	if !info.Mode().IsRegular() || info.Size() > maxManifestBytes {
		return fmt.Errorf("bounded regular metadata file required")
	}
	f, err := root.OpenFile(file, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("metadata unreadable")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return fmt.Errorf("metadata changed before open")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxManifestBytes+1))
	if err != nil {
		return err
	}
	if len(b) > maxManifestBytes || digest(b) != sha {
		return fmt.Errorf("metadata bytes differ from expected digest")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = strictjson.Decode(b, target); err != nil {
		return fmt.Errorf("invalid closed metadata JSON")
	}
	return nil
}

// All fields in each published descriptor are compared. No particular release,
// source ID, cycle, selected relation or output count is special-cased.
func compareStageEvidence(m rel.ReleaseManifest, p rel.ReleasePlan, a rel.AcquisitionResult, s rel.StageResult) error {
	if p.CandidateReleaseID != m.ReleaseID || a.CandidateReleaseID != m.ReleaseID || s.CandidateReleaseID != m.ReleaseID || p.PriorReleaseID != m.PriorReleaseID || a.PriorReleaseID != m.PriorReleaseID || s.PriorReleaseID != m.PriorReleaseID || p.InventoryVersion != m.InventoryVersion || a.InventoryVersion != m.InventoryVersion || s.InventoryVersion != m.InventoryVersion || a.PlanSHA256 != m.PlanSHA256 || s.PlanSHA256 != m.PlanSHA256 || s.AcquisitionSHA256 != m.AcquisitionSHA256 {
		return fmt.Errorf("release input chain differs")
	}
	if len(m.Artifacts) != len(p.SelectedSources) || len(m.Artifacts) != len(a.Artifacts) {
		return fmt.Errorf("source descriptor membership differs")
	}
	selected := map[string]rel.SelectedSource{}
	acquired := map[string]rel.AcquisitionArtifact{}
	published := map[string]rel.PublishedArtifact{}
	for _, v := range p.SelectedSources {
		if _, ok := selected[v.SourceID]; ok {
			return fmt.Errorf("duplicate selected source")
		}
		selected[v.SourceID] = v
	}
	for _, v := range a.Artifacts {
		if _, ok := acquired[v.SourceID]; ok {
			return fmt.Errorf("duplicate acquired source")
		}
		acquired[v.SourceID] = v
	}
	for _, v := range m.Artifacts {
		if _, ok := published[v.SourceID]; ok {
			return fmt.Errorf("duplicate published source")
		}
		published[v.SourceID] = v
		selectedSource, ok := selected[v.SourceID]
		if !ok || !reflect.DeepEqual(v.SelectedSource, selectedSource) {
			return fmt.Errorf("selected source metadata differs")
		}
		acq, ok := acquired[v.SourceID]
		if !ok || v.VersionIdentity != acq.VersionIdentity || v.ByteCount != acq.ByteCount || v.SHA256 != acq.SHA256 || v.StorageKey != acq.StorageKey || !v.AcquiredAt.Equal(acq.AcquiredAt) {
			return fmt.Errorf("acquired source descriptor differs")
		}
	}
	if err := compareStageOutputs(m.StagedOutputs, s.Outputs); err != nil {
		return err
	}
	for _, v := range s.Outputs {
		a, ok := acquired[v.SourceID]
		if !ok || a.SHA256 != v.SourceArtifactSHA256 {
			return fmt.Errorf("staged output source differs")
		}
	}
	return nil
}

type stageOutputKey struct{ SourceID, Kind, Selection string }

func compareStageOutputs(a, b []rel.StagedOutput) error {
	if len(a) != len(b) {
		return fmt.Errorf("staged output membership differs")
	}
	key := func(v rel.StagedOutput) stageOutputKey {
		return stageOutputKey{v.SourceID, v.SelectionKind, v.Selection}
	}
	index := map[stageOutputKey]rel.StagedOutput{}
	for _, v := range a {
		if _, ok := index[key(v)]; ok {
			return fmt.Errorf("duplicate published staged output")
		}
		index[key(v)] = v
	}
	for _, v := range b {
		prior, ok := index[key(v)]
		if !ok || !reflect.DeepEqual(prior, v) {
			return fmt.Errorf("staged output descriptor differs")
		}
		delete(index, key(v))
	}
	if len(index) != 0 {
		return fmt.Errorf("staged output membership differs")
	}
	return nil
}
