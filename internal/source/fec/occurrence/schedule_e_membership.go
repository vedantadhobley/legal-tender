package occurrence

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
)

const ScheduleEMembershipPolicy = "fec/schedule-e-exact-release-membership@1.0.0"

// ScheduleEReleaseMembership proves exact published source-version membership,
// not economic equivalence or a fresh acquisition of the archive. The ordinary
// fact loader verifies the complete retained fact artifact before this is emitted.
type ScheduleEReleaseMembership struct {
	Policy               string            `json:"policy"`
	ProofID              string            `json:"proof_id"`
	Cycle                string            `json:"cycle"`
	FactSet              ReferenceIdentity `json:"fact_set"`
	SourceRelease        ReferenceIdentity `json:"source_release"`
	TargetRelease        ReferenceIdentity `json:"target_release"`
	SourceArtifactSHA256 string            `json:"source_artifact_sha256"`
	Relation             string            `json:"relation"`
	StagedSHA256         string            `json:"staged_sha256"`
	RelationSHA256       string            `json:"relation_sha256"`
	FactSchema           string            `json:"fact_schema"`
	Rows                 uint64            `json:"source_occurrences"`
}

func ProveScheduleEReleaseMembership(ctx context.Context, root, facts, targetID, targetSHA string) (ScheduleEReleaseMembership, error) {
	var out ScheduleEReleaseMembership
	m, md, err := LoadPublishedScheduleEFactManifest(ctx, root, facts)
	if err != nil {
		return out, err
	}
	op := filepath.Join(root, scheduleEEvidenceBase(), "manifests", m.OccurrenceSetID+".json")
	o, od, err := readScheduleEOccurrenceManifestWithSHA256(op)
	if err != nil {
		return out, err
	}
	if err := validateScheduleEOccurrenceManifest(o); err != nil {
		return out, err
	}
	if od != m.OccurrenceManifestSHA256 || o.SourceReleaseID != m.SourceReleaseID || o.SourceReleaseManifestSHA256 != m.SourceReleaseManifestSHA256 || o.Cycle != m.Cycle || o.Counts.SelectedRows != m.Counts.SourceOccurrences {
		return out, fmt.Errorf("Schedule E occurrence ancestry differs")
	}
	origin, err := referenceRelease(root, m.SourceReleaseID, m.SourceReleaseManifestSHA256)
	if err != nil {
		return out, err
	}
	target, err := referenceRelease(root, targetID, targetSHA)
	if err != nil {
		return out, err
	}
	a, ash, err := selectedScheduleEOutput(origin)
	if err != nil {
		return out, err
	}
	b, bsh, err := selectedScheduleEOutput(target)
	if err != nil {
		return out, err
	}
	if ash != bsh || ash != o.SourceArtifactSHA256 || a.Selection != o.Relation || a.Selection != b.Selection || a.CompressedSHA256 != o.StagedOutputSHA256 || a.CompressedSHA256 != b.CompressedSHA256 || a.UncompressedSHA256 != b.UncompressedSHA256 || a.CompressedByteCount != b.CompressedByteCount || a.UncompressedByteCount != b.UncompressedByteCount {
		return out, fmt.Errorf("Schedule E source version is not an exact member of the target release")
	}
	cyclePresent := false
	for _, cycle := range target.Periods {
		cyclePresent = cyclePresent || cycle == m.Cycle
	}
	if !cyclePresent {
		return out, fmt.Errorf("Schedule E cycle is outside target release")
	}
	out = ScheduleEReleaseMembership{Policy: ScheduleEMembershipPolicy, Cycle: m.Cycle, FactSet: ReferenceIdentity{m.FactSetID, md}, SourceRelease: ReferenceIdentity{m.SourceReleaseID, m.SourceReleaseManifestSHA256}, TargetRelease: ReferenceIdentity{targetID, targetSHA}, SourceArtifactSHA256: ash, Relation: a.Selection, StagedSHA256: a.CompressedSHA256, RelationSHA256: a.UncompressedSHA256, FactSchema: m.FactSchemaVersion, Rows: m.Counts.SourceOccurrences}
	raw, err := json.Marshal(out)
	if err != nil {
		return ScheduleEReleaseMembership{}, err
	}
	out.ProofID = referenceHash(raw)
	return out, nil
}
