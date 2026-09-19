package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fileSHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func TestReadGenerationRequiresBytesAndOriginalIdentity(t *testing.T) {
	v := Result{SchemaVersion: Version, Policy: Policy, State: "verified_declared_evidence_families", BuildSHA256: strings.Repeat("a", 64), Cycle: "2028"}
	v.GenerationID = identity(v)
	raw, _ := json.Marshal(v)
	p := filepath.Join(t.TempDir(), "generation.json")
	if e := os.WriteFile(p, raw, 0600); e != nil {
		t.Fatal(e)
	}
	got, e := readGeneration(p, fileSHA(raw))
	if e != nil || got.GenerationID != v.GenerationID {
		t.Fatal(e)
	}
	if _, e := readGeneration(p, strings.Repeat("b", 64)); e == nil {
		t.Fatal("accepted changed bytes")
	}
	for _, mutate := range []func(*Result){func(v *Result) { v.Cycle = "2026" }, func(v *Result) { v.Policy = "other" }, func(v *Result) { v.TerminalEligible = true }, func(v *Result) { v.SchemaVersion = "unknown" }} {
		x := v
		mutate(&x)
		b, _ := json.Marshal(x)
		_ = os.WriteFile(p, b, 0600)
		if _, e := readGeneration(p, fileSHA(b)); e == nil {
			t.Fatal("accepted changed contract")
		}
	}
	// A serialized claim, even resealed, cannot open missing backing.
	_ = os.WriteFile(p, raw, 0600)
	if _, e := OpenReader(context.Background(), ReadOptions{Generation: p, GenerationSHA256: fileSHA(raw), BuildSHA256: strings.Repeat("b", 64), StorageRoot: t.TempDir(), GraphManifest: "absent", Participants: "absent", Conduits: "absent"}); e == nil {
		t.Fatal("accepted serialized approval")
	}
}

func TestReaderDerivesInputsAndSeparatesProducerConsumer(t *testing.T) {
	v := Result{BuildSHA256: strings.Repeat("a", 64), GenerationID: strings.Repeat("f", 64)}
	v.Receipts.Inputs.Facts.ID = strings.Repeat("b", 64)
	v.Receipts.Inputs.Candidates.ID = strings.Repeat("c", 64)
	v.Receipts.Inputs.Committees.ID = strings.Repeat("d", 64)
	v.Receipts.Inputs.Linkages.ID = strings.Repeat("e", 64)
	v.CommitteeFlow.BundleID = strings.Repeat("1", 64)
	v.OutsideSpending.BundleID = strings.Repeat("2", 64)
	o := ReadOptions{StorageRoot: "/retained", BuildSHA256: strings.Repeat("3", 64), GraphManifest: "/physical/graph", Participants: "/physical/participants", Conduits: "/physical/conduits"}
	opt, e := readerInputs(o, v)
	if e != nil {
		t.Fatal(e)
	}
	if opt.BuildSHA256 != v.BuildSHA256 || opt.BuildSHA256 == o.BuildSHA256 || opt.ExpectedGenerationID != v.GenerationID || opt.Receipt.Facts != "/retained/facts/fec/schedule-a/columnar/manifests/"+v.Receipts.Inputs.Facts.ID+".json" || opt.ReceiptManifest != o.GraphManifest {
		t.Fatal("lost pinned identity or physical locator")
	}
	v.CommitteeFlow.BundleID = "../../other"
	if _, e := readerInputs(o, v); e == nil {
		t.Fatal("accepted path traversal")
	}
}

func TestNeighborhoodCursorScopeAndIdentity(t *testing.T) {
	generation := strings.Repeat("a", 64)
	after := strings.Repeat("b", 64)
	q := Query{Entity: "C00000001", Family: "reported_receipt", Limit: 2}
	q.Cursor = encodeCursor(generation, q.Entity, q.Family, after)
	if got, e := decodeCursor(q, generation); e != nil || got != after {
		t.Fatal(got, e)
	}
	for _, mutate := range []func(*Query){func(q *Query) { q.Entity = "C00000002" }, func(q *Query) { q.Family = "conduit_association" }, func(q *Query) { q.Family = "" }, func(q *Query) { q.Limit = 11 }, func(q *Query) { q.Cursor = "bad" }} {
		bad := q
		mutate(&bad)
		if _, e := decodeCursor(bad, generation); e == nil {
			t.Fatal("accepted foreign cursor")
		}
	}
	if _, e := decodeCursor(q, strings.Repeat("c", 64)); e == nil {
		t.Fatal("accepted other generation")
	}
	// Invalid scope fails before any backend access.
	r := Reader{generation: Result{GenerationID: generation}}
	bad := q
	bad.Entity = "C00000002"
	if _, e := r.Neighborhood(context.Background(), bad); e == nil {
		t.Fatal("accepted foreign query")
	}
	n := Neighborhood{SchemaVersion: NeighborhoodVersion, GenerationID: generation, Query: q, ConsumerBuildSHA256: after}
	id := neighborhoodID(n)
	n.ConsumerBuildSHA256 = generation
	if id == neighborhoodID(n) {
		t.Fatal("consumer change lost")
	}
}

func TestWitnessSelectionIsStableWithoutNamedCases(t *testing.T) {
	if firstID(map[string]bool{"C00000002": true, "C00000001": true}) != "C00000001" || firstID(nil) != "" {
		t.Fatal("selection")
	}
	if firstShared([]string{"C00000001", "C00000002"}, map[string]bool{"C00000002": true}) != "C00000002" {
		t.Fatal("intersection")
	}
}
