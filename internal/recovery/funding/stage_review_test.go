package funding

import (
	"context"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func stageEvidenceFixture() (rel.ReleaseManifest, rel.ReleasePlan, rel.AcquisitionResult, rel.StageResult) {
	sha := digest([]byte("source"))
	now := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	sel := rel.SelectedSource{SourceID: "fixture:one", VersionIdentity: "version_id:fixture", RequestURL: "https://example.test/source", ObservedAt: now}
	m := rel.ReleaseManifest{ReleaseID: "fec-" + sha, InventoryVersion: "fixture", PlanSHA256: digest([]byte("plan")), AcquisitionSHA256: digest([]byte("acquisition")), StageSHA256: digest([]byte("original stage")), Artifacts: []rel.PublishedArtifact{{SelectedSource: sel, SHA256: sha, ByteCount: 10, StorageKey: "raw/source", AcquiredAt: now}}}
	p := rel.ReleasePlan{InventoryVersion: m.InventoryVersion, CandidateReleaseID: m.ReleaseID, SelectedSources: []rel.SelectedSource{sel}}
	a := rel.AcquisitionResult{InventoryVersion: m.InventoryVersion, CandidateReleaseID: m.ReleaseID, PlanSHA256: m.PlanSHA256, Artifacts: []rel.AcquisitionArtifact{{SourceID: sel.SourceID, VersionIdentity: sel.VersionIdentity, SHA256: sha, ByteCount: 10, StorageKey: "raw/source", AcquiredAt: now}}}
	rows := uint64(3)
	fields := 5
	output := rel.StagedOutput{SourceID: sel.SourceID, Disposition: "staged", SelectionKind: "relation", Selection: "fixture.relation", Period: "2024", Representation: "postgresql_copy_text_data_rows_zstd", SourceArtifactSHA256: sha, RowCount: &rows, ContractedFieldCount: &fields, UncompressedByteCount: 20, UncompressedSHA256: sha, Compression: "zstd", CompressionLevel: 3, CompressedByteCount: 10, CompressedSHA256: sha, StorageKey: "raw/output", DecompressionValidated: true, StagedAt: now}
	m.StagedOutputs = []rel.StagedOutput{output}
	s := rel.StageResult{InventoryVersion: m.InventoryVersion, CandidateReleaseID: m.ReleaseID, PlanSHA256: m.PlanSHA256, AcquisitionSHA256: m.AcquisitionSHA256, Outputs: slices.Clone(m.StagedOutputs)}
	return m, p, a, s
}

func TestStageEvidenceMatchesWithoutReplacingOriginal(t *testing.T) {
	m, p, a, s := stageEvidenceFixture()
	if err := compareStageEvidence(m, p, a, s); err != nil {
		t.Fatal(err)
	}
	// The comparison does not invent original operational observations from a
	// replacement execution. Those fields stay separate from source descriptors.
	s.RunID = "replacement-run"
	s.StartedAt = time.Now().UTC()
	s.Storage.FreeBytesBefore = 1234
	if err := compareStageEvidence(m, p, a, s); err != nil {
		t.Fatal(err)
	}
	if m.StageSHA256 == identity(s) {
		t.Fatal("fixture accidentally recreates original bytes")
	}
}

func TestEveryStageOutputFieldIsCompared(t *testing.T) {
	m, _, _, s := stageEvidenceFixture()
	typ := reflect.TypeFor[rel.StagedOutput]()
	for i := 0; i < typ.NumField(); i++ {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			changed := slices.Clone(s.Outputs)
			field := reflect.ValueOf(&changed[0]).Elem().Field(i)
			switch field.Kind() {
			case reflect.String:
				field.SetString(field.String() + "-changed")
			case reflect.Bool:
				field.SetBool(!field.Bool())
			case reflect.Int:
				field.SetInt(field.Int() + 1)
			case reflect.Uint64:
				field.SetUint(field.Uint() + 1)
			case reflect.Pointer:
				field.SetZero()
			case reflect.Struct:
				if field.Type() != reflect.TypeFor[time.Time]() {
					t.Fatal("new struct needs a mutation test")
				}
				field.Set(reflect.ValueOf(changed[0].StagedAt.Add(time.Second)))
			default:
				t.Fatalf("new field type requires a mutation test: %s", field.Type())
			}
			if compareStageOutputs(m.StagedOutputs, changed) == nil {
				t.Fatal("changed output field accepted")
			}
		})
	}
	// Test pointer values independently from nullability.
	changed := slices.Clone(s.Outputs)
	rows := *changed[0].RowCount + 1
	changed[0].RowCount = &rows
	if compareStageOutputs(m.StagedOutputs, changed) == nil {
		t.Fatal("changed row count accepted")
	}
	changed = slices.Clone(s.Outputs)
	fields := *changed[0].ContractedFieldCount + 1
	changed[0].ContractedFieldCount = &fields
	if compareStageOutputs(m.StagedOutputs, changed) == nil {
		t.Fatal("changed column count accepted")
	}
}

func TestStageReviewRejectsDifferentInputsAndMembership(t *testing.T) {
	for _, mutate := range []func(*rel.ReleaseManifest, *rel.ReleasePlan, *rel.AcquisitionResult, *rel.StageResult){
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, _ *rel.AcquisitionResult, s *rel.StageResult) {
			s.PlanSHA256 = digest(nil)
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, _ *rel.AcquisitionResult, s *rel.StageResult) {
			s.AcquisitionSHA256 = digest(nil)
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, _ *rel.AcquisitionResult, s *rel.StageResult) {
			s.CandidateReleaseID = "other"
		},
		func(_ *rel.ReleaseManifest, p *rel.ReleasePlan, _ *rel.AcquisitionResult, _ *rel.StageResult) {
			p.PriorReleaseID = "other"
		},
		func(_ *rel.ReleaseManifest, p *rel.ReleasePlan, _ *rel.AcquisitionResult, _ *rel.StageResult) {
			p.SelectedSources[0].RequestURL += "/changed"
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, a *rel.AcquisitionResult, _ *rel.StageResult) {
			a.Artifacts[0].SHA256 = digest(nil)
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, a *rel.AcquisitionResult, _ *rel.StageResult) {
			a.Artifacts[0].ByteCount++
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, a *rel.AcquisitionResult, _ *rel.StageResult) {
			a.Artifacts[0].StorageKey = "other"
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, a *rel.AcquisitionResult, _ *rel.StageResult) {
			a.Artifacts[0].AcquiredAt = a.Artifacts[0].AcquiredAt.Add(time.Second)
		},
		func(_ *rel.ReleaseManifest, _ *rel.ReleasePlan, _ *rel.AcquisitionResult, s *rel.StageResult) {
			s.Outputs = nil
		},
		func(m *rel.ReleaseManifest, _ *rel.ReleasePlan, _ *rel.AcquisitionResult, s *rel.StageResult) {
			m.StagedOutputs = append(m.StagedOutputs, m.StagedOutputs[0])
			s.Outputs = append(s.Outputs, s.Outputs[0])
		},
		func(m *rel.ReleaseManifest, _ *rel.ReleasePlan, _ *rel.AcquisitionResult, s *rel.StageResult) {
			m.StagedOutputs[0].SourceArtifactSHA256 = digest(nil)
			s.Outputs[0].SourceArtifactSHA256 = digest(nil)
		},
	} {
		m, p, a, s := stageEvidenceFixture()
		mutate(&m, &p, &a, &s)
		if compareStageEvidence(m, p, a, s) == nil {
			t.Fatal("different input or descriptor accepted")
		}
	}
	m, _, _, s := stageEvidenceFixture()
	other := s.Outputs[0]
	other.Selection = "second"
	m.StagedOutputs = append(m.StagedOutputs, other)
	s.Outputs = append([]rel.StagedOutput{other}, s.Outputs...)
	if err := compareStageOutputs(m.StagedOutputs, s.Outputs); err != nil {
		t.Fatal("order-only difference rejected:", err)
	}
}

func TestReadStageReviewMetadata(t *testing.T) {
	dir := t.TempDir()
	b := []byte(`{"schema_version":"legal-tender.fec.staged-release.v1"}`)
	writeFixture(t, dir, "candidate.json", b)
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var out rel.StageResult
	if err = readReviewMetadata(context.Background(), root, "candidate.json", digest(b), &out); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, sha string }{{"../outside", digest(b)}, {"candidate.json", digest(nil)}, {"missing.json", digest(b)}, {"current.json", digest(b)}} {
		if readReviewMetadata(context.Background(), root, tc.path, tc.sha, &out) == nil {
			t.Fatal("unsafe or changed input accepted")
		}
	}
	bad := []byte(`{"schema_version":"x","extra_input":{}}`)
	writeFixture(t, dir, "unknown.json", bad)
	if readReviewMetadata(context.Background(), root, "unknown.json", digest(bad), &out) == nil {
		t.Fatal("unknown dependency field accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if readReviewMetadata(ctx, root, "candidate.json", digest(b), &out) != context.Canceled {
		t.Fatal("cancellation ignored")
	}
}
