package organizationresolution

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestCommitteeQueryGrainAndSelection(t *testing.T) {
	root := t.TempDir()
	m := occ.ClassicFactManifest{FactSetID: "fixture", Dataset: "committee-master", Cycle: "2024", FactType: "fec.committee.v1", FactSchemaVersion: occ.ClassicFactSchemaVersion, OccurrenceSetID: "occurrences", SourceReleaseID: "release", SourceContract: "contract"}
	w, err := artifact.NewWriter(context.Background(), root, filepath.Join(root, "scratch"), "fixtures", "committee")
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"Z Corporation", "A Corporation", "A Corporation", " A Corporation ", ""} {
		f := occ.ClassicFact{FactID: fmt.Sprint(i), OccurrenceID: fmt.Sprint("occ-", i), State: "valid", Dataset: m.Dataset, Cycle: m.Cycle, FactType: m.FactType, SchemaVersion: m.FactSchemaVersion, OccurrenceSetID: m.OccurrenceSetID, SourceReleaseID: m.SourceReleaseID, SourceContract: m.SourceContract, SourceFields: map[string]string{"CONNECTED_ORG_NM": name}}
		if err = w.WriteJSON(f); err != nil {
			t.Fatal(err)
		}
	}
	a, err := w.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	m.Counts.Facts = a.RecordCount
	m.Facts = occ.Artifact{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	sha := wikimedia.Hash([]byte("fixture"))
	build := wikimedia.Hash([]byte("build"))
	q, err := committeeQueries(context.Background(), root, m, sha, build, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Queries) != 2 || q.Queries[0].Text != " A Corporation " || q.Queries[1].Text != "A Corporation" || len(q.Queries[1].References) != 2 {
		t.Fatal("raw text or reference grain lost", q)
	}
	if q.Queries[1].References[0].FactID != "1" || q.Queries[1].References[1].FactID != "2" {
		t.Fatal("membership order")
	}
	replay, err := committeeQueries(context.Background(), root, m, sha, build, 2)
	if err != nil || !reflect.DeepEqual(q, replay) {
		t.Fatal("unstable source selection", err)
	}
	m.Counts.Facts++
	if _, err = committeeQueries(context.Background(), root, m, sha, build, 2); err == nil {
		t.Fatal("count mismatch accepted")
	}
	m.Counts.Facts--
	m.Cycle = "2022"
	if _, err = committeeQueries(context.Background(), root, m, sha, build, 2); err == nil {
		t.Fatal("source envelope mismatch accepted")
	}
}

func TestEmployerQueryGrain(t *testing.T) {
	rows := []fundingbasis.Receipt{{Ordinal: 9, Fields: map[string]any{"contbr_employer": "Example Corp", "contbr_nm": "not transmitted", "contbr_st1": "not transmitted"}}, {Ordinal: 1, Fields: map[string]any{"contbr_employer": "Example Corp"}}}
	sha := wikimedia.Hash([]byte("fixture"))
	build := wikimedia.Hash([]byte("build"))
	q, err := employerQueries(rows, "facts", sha, build)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Queries) != 1 || len(q.Queries[0].References) != 2 || q.Queries[0].References[0].Ordinal != 1 || q.Queries[0].References[1].Ordinal != 9 {
		t.Fatal("reference membership collapsed", q)
	}
	rows[0], rows[1] = rows[1], rows[0]
	r, err := employerQueries(rows, "facts", sha, build)
	if err != nil || !reflect.DeepEqual(q, r) {
		t.Fatal("input order changed queries", err)
	}
	for _, v := range []any{nil, "", 19} {
		rows[0].Fields["contbr_employer"] = v
		if _, err = employerQueries(rows, "facts", sha, build); err == nil {
			t.Fatal("missing/invalid employer silently dropped")
		}
	}
}
