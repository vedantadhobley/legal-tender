package funding

import (
	"encoding/json"
	"path"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	fg "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	rg "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	art "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func TestSourceAdaptersEnumerateEveryArtifact(t *testing.T) {
	id, sha := digest([]byte("id")), digest([]byte("bytes"))
	releaseID := "fec-" + id
	blob := occ.Artifact{StorageKey: "blobs/a", CompressedSHA256: sha, CompressedBytes: 5}
	artifacts := occ.ArtifactSet{Occurrences: blob, Issues: blob, NaturalIndex: blob, Changes: blob}
	for _, tc := range []struct {
		kind, dataset, schema string
		value                 any
		children              int
	}{
		{"a_facts", "", occ.ScheduleAColumnarFactSetSchemaVersion, occ.ScheduleAColumnarManifest{SchemaVersion: occ.ScheduleAColumnarFactSetSchemaVersion, FactSetID: id, SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, OccurrenceSetID: id, OccurrenceManifestSHA256: sha, Shards: []occ.ScheduleAColumnarShard{{StorageKey: "shard.parquet", SHA256: sha, Bytes: 5}}}, 3},
		{"b_facts", "", occ.ScheduleBColumnarFactSetSchemaVersion, occ.ScheduleBColumnarManifest{SchemaVersion: occ.ScheduleBColumnarFactSetSchemaVersion, FactSetID: id, SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, SourceArtifactStorageKey: "archive", SourceArtifactSHA256: sha, SourceArtifactByteCount: 5, Shards: []occ.ScheduleBColumnarShard{{StorageKey: "shard.parquet", SHA256: sha, Bytes: 5}}}, 3},
		{"e_facts", "", occ.ScheduleEFactSetSchemaVersion, occ.ScheduleEFactManifest{SchemaVersion: occ.ScheduleEFactSetSchemaVersion, FactSetID: id, SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, OccurrenceSetID: id, OccurrenceManifestSHA256: sha, Facts: blob}, 3},
		{"classic_facts", "committee-master", occ.ClassicFactSetSchemaVersion, occ.ClassicFactManifest{SchemaVersion: occ.ClassicFactSetSchemaVersion, FactSetID: id, Dataset: "committee-master", SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, OccurrenceSetID: id, OccurrenceManifestSHA256: sha, Facts: blob}, 3},
		{"a_occurrences", "", occ.ManifestSchemaVersion, occ.Manifest{SchemaVersion: occ.ManifestSchemaVersion, OccurrenceSetID: id, SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, Artifacts: artifacts}, 5},
		{"a_occurrences", "", occ.ScheduleACompactManifestSchemaVersion, occ.ScheduleACompactManifest{SchemaVersion: occ.ScheduleACompactManifestSchemaVersion, OccurrenceSetID: id, SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, RowExceptions: blob, Deltas: blob, IndexPartitions: []occ.ScheduleACompactPartition{{Index: occ.ScheduleACompactArtifact{StorageKey: "index", CompressedSHA256: sha, CompressedBytes: 5}, KeyExceptions: blob}}}, 5},
		{"e_occurrences", "", occ.ScheduleEOccurrenceSetSchemaVersion, occ.ScheduleEOccurrenceManifest{SchemaVersion: occ.ScheduleEOccurrenceSetSchemaVersion, OccurrenceSetID: id, SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, Occurrences: blob}, 2},
		{"classic_occurrences", "candidate-master", occ.ClassicManifestSchemaVersion, occ.ClassicManifest{SchemaVersion: occ.ClassicManifestSchemaVersion, OccurrenceSetID: id, Dataset: "candidate-master", SourceReleaseID: releaseID, SourceReleaseManifestSHA256: sha, Artifacts: artifacts, PriorOccurrenceSetID: sha}, 6},
	} {
		t.Run(tc.kind+"/"+tc.schema, func(t *testing.T) {
			b, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			r := Reference{Kind: tc.kind, ID: id, SHA256: digest(b), Dataset: tc.dataset, Path: "manifests/source.json"}
			deps, _, _, schema, err := expand(r, b)
			if err != nil || schema != tc.schema || len(deps) != tc.children {
				t.Fatalf("dependencies %d: %v", len(deps), err)
			}
			for _, d := range deps {
				if d.Ref.Kind == "file" && (d.Ref.SHA256 != sha || d.Ref.Bytes == nil || *d.Ref.Bytes != 5) {
					t.Fatal("artifact identity lost")
				}
			}
			// Reject newly introduced fields instead of silently losing dependencies.
			changed := append(append([]byte{}, b[:len(b)-1]...), []byte(`,"future_dependency":{}}`)...)
			if _, _, _, _, err = expand(r, changed); err == nil {
				t.Fatal("unknown dependency field accepted")
			}
		})
	}
}

func TestPublisherSpecificArtifactRoots(t *testing.T) {
	id := digest([]byte("id"))
	sha := digest([]byte("sha"))
	a := adapter{r: Reference{Path: "audit/manifest.json"}}
	a.local("participant_shard", "participants.parquet", sha, 5)
	a.sorted("decisions", xsort.File{Name: "run.zst", SHA256: sha, Bytes: 5})
	a.artifact("root_relative", art.Descriptor{StorageKey: "facts/blob", CompressedSHA256: sha, CompressedBytes: 5})
	a.flowArtifact("flow", art.Descriptor{StorageKey: "evidence/" + id + "/blob", CompressedSHA256: sha, CompressedBytes: 5})
	want := []string{"audit/data/participants.parquet", "audit/data/run.zst", "facts/blob", path.Join(flow.PublicationBase, "evidence", id, "blob")}
	if a.err != nil {
		t.Fatal(a.err)
	}
	for i, d := range a.deps {
		if d.Ref.Path != want[i] {
			t.Fatalf("path %s != %s", d.Ref.Path, want[i])
		}
	}
	for _, kind := range []string{"plan", "acquisition", "stage"} {
		r := Reference{Kind: kind, ID: id}
		if defaultPath(r) == "" {
			t.Fatal("lost release control layout")
		}
	}
	a.local("bad", "../escape", sha, 0)
	if a.err == nil {
		t.Fatal("local path traversal accepted")
	}
}

func TestSharedGenerationAndTopology(t *testing.T) {
	id := digest([]byte("id"))
	sha := digest([]byte("sha"))
	g := fg.SharedGeneration{SchemaVersion: "legal-tender.funding-evidence-generation.shared-conduits.v1", Base: fg.Result{GenerationID: id}, BaseSHA256: sha, Extension: rg.SharedView{Projection: rg.Reference{ID: id, SHA256: sha}, Calculation: rg.Reference{ID: sha, SHA256: sha}}}
	g.GenerationID = identity(g)
	b, _ := json.Marshal(g)
	deps, _, _, _, err := expand(Reference{Kind: "shared_generation", ID: g.GenerationID}, b)
	if err != nil || len(deps) != 3 || deps[0].Ref.Kind != "generation" || deps[0].Ref.SHA256 != sha {
		t.Fatalf("shared dependencies: %+v %v", deps, err)
	}
	if _, _, _, _, err = expand(Reference{Kind: "shared_generation", ID: sha}, b); err == nil {
		t.Fatal("wrong generation identity accepted")
	}
	v := refs.TopologyResult{SchemaVersion: refs.TopologyVersion, CalculationID: id, ReferenceCalculationID: id, ReferenceManifestSHA256: sha, FactSetID: id, FactManifestSHA256: sha, Endpoints: xsort.File{Name: "endpoints.zst", SHA256: sha, Bytes: 5}}
	b, _ = json.Marshal(v)
	deps, _, _, _, err = expand(Reference{Kind: "topology", ID: id, Path: "topology/manifest.json"}, b)
	if err != nil || len(deps) != 3 || deps[2].Ref.Path != "topology/data/endpoints.zst" {
		t.Fatalf("topology: %+v %v", deps, err)
	}
}

func TestUnknownVersionsAndNegativeSizes(t *testing.T) {
	for _, kind := range []string{"generation", "shared_generation", "receipt_projection", "shared_projection", "participants", "conduits", "topology", "references", "flow_bundle", "flow_calculation", "resolved_bundle", "resolved_aggregate", "resolution", "effective", "a_facts", "b_facts", "e_facts", "classic_facts", "a_occurrences", "e_occurrences", "classic_occurrences", "release", "plan", "acquisition", "stage", "unknown"} {
		if _, _, _, _, err := expand(Reference{Kind: kind}, []byte(`{"schema_version":"future"}`)); err == nil {
			t.Errorf("accepted unknown %s version", kind)
		}
	}
	a := adapter{}
	a.acquired("source", digest(nil), -1)
	if a.err == nil {
		t.Fatal("negative size accepted")
	}
	a = adapter{}
	a.staged(rel.StagedOutput{Representation: "future"})
	if a.err == nil {
		t.Fatal("unknown stage representation accepted")
	}
}
