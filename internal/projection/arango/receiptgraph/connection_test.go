package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

func connectionFixture(cycle string) (completion, upstream.Result) {
	ref := func(s string) Reference {
		return Reference{digest([]byte(s + cycle)), digest([]byte("manifest" + s + cycle))}
	}
	in := Inputs{Participants: ref("participant"), Conduits: ref("conduit"), Facts: ref("facts"), Committees: ref("masters"), Candidates: ref("candidates"), Linkages: ref("linkages"), SourceRelease: "fec-" + digest([]byte("original"+cycle)), Cycle: cycle}
	v := completion{definition: definition{Version: CycleVersion, State: CycleState, Inputs: in, Build: digest([]byte("publisher")), First: 1, Rows: 12, SourceRows: 12},
		Counts: map[string]uint64{appearances: 12, receipts: 11, conduits: 2, entities: 3, authorizations: 1}, Unrouted: 1, States: map[string]uint64{"qualified": 2, "other": 10}, Digests: map[string]string{}, SourceEvidenceSHA256: map[string]string{}}
	for _, name := range collections {
		v.Digests[name], v.SourceEvidenceSHA256[name] = digest(nil), digest(nil)
	}
	b, _ := json.Marshal(v.definition)
	v.Key = digest(b)
	f := func(ref Reference) flow.FactReference {
		return flow.FactReference{FactSetID: ref.ID, ManifestSHA256: ref.SHA256, SourceReleaseID: in.SourceRelease}
	}
	u := upstream.Result{SchemaVersion: upstream.Version, Policy: upstream.Policy, Cycle: cycle, Ledger: "schedule_a", Candidate: "H0ZZ00001"}
	u.Inputs.Sources = flow.Inputs{ReleaseID: "fec-" + digest([]byte("coordinated"+cycle)), ReleaseSHA256: digest([]byte("release manifest")), A: f(in.Facts)}
	u.Inputs.Sources.A.Facts = 12
	u.Inputs.CommitteeMaster, u.Inputs.Linkages = f(in.Committees), f(in.Linkages)
	return v, u
}

func TestConnectionAncestryAcrossCyclesAndReusedSources(t *testing.T) {
	for _, cycle := range []string{"2020", "2022", "2024", "2026", "2028"} {
		v, u := connectionFixture(cycle)
		b, _ := json.Marshal(v)
		got, err := decodeCycleCompletion(b, digest(b))
		if err != nil || !reflect.DeepEqual(got, v) {
			t.Fatalf("%s: %v", cycle, err)
		}
		// The coordinated A/B release may differ: its existing loader proves
		// unchanged source bytes. Exact shared facts and masters remain required.
		if err := compatibleUpstream(v, u); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*upstream.Result){
			func(r *upstream.Result) { r.Cycle = "1998" },
			func(r *upstream.Result) { r.Inputs.Sources.A.FactSetID = digest([]byte("other")) },
			func(r *upstream.Result) { r.Inputs.Sources.A.ManifestSHA256 = digest([]byte("other")) },
			func(r *upstream.Result) { r.Inputs.Sources.A.Facts-- },
			func(r *upstream.Result) { r.Inputs.Sources.A.SourceReleaseID = "other" },
			func(r *upstream.Result) { r.Inputs.CommitteeMaster.ManifestSHA256 = digest([]byte("other")) },
			func(r *upstream.Result) { r.Inputs.Linkages.FactSetID = digest([]byte("other")) },
			func(r *upstream.Result) { r.Ledger = "schedule_b" },
			func(r *upstream.Result) { r.TerminalEligible = true },
		} {
			x := u
			mutate(&x)
			if compatibleUpstream(v, x) == nil {
				t.Fatalf("accepted mixed ancestry for %s", cycle)
			}
		}
	}
}

func TestConnectionPreflightRequiresExactSharedReferencesAcrossCycles(t *testing.T) {
	for _, cycle := range []string{"2020", "2022", "2024", "2026", "2028"} {
		v, u := connectionFixture(cycle)
		b := flow.Bundle{Cycle: cycle, Input: u.Inputs.Sources, Committee: u.Inputs.CommitteeMaster}
		if err := compatibleConnectionBundle(v, b); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*flow.Bundle){
			func(b *flow.Bundle) { b.Cycle = "1998" },
			func(b *flow.Bundle) { b.Input.A.FactSetID = digest([]byte("changed")) },
			func(b *flow.Bundle) { b.Input.A.ManifestSHA256 = digest([]byte("changed")) },
			func(b *flow.Bundle) { b.Input.A.SourceReleaseID = "changed" },
			func(b *flow.Bundle) { b.Input.A.Facts-- },
		} {
			changed := b
			mutate(&changed)
			if compatibleConnectionBundle(v, changed) == nil {
				t.Fatal("accepted mismatched reference", cycle)
			}
		}
	}
}

func TestConnectionRejectsPartialOrChangedCompletion(t *testing.T) {
	for _, mutate := range []func(*completion){
		func(v *completion) { v.Version = CompactVersion },
		func(v *completion) { v.Rows-- },
		func(v *completion) { v.First = 2 },
		func(v *completion) { v.FinancialEligibility = true },
		func(v *completion) { v.Counts[receipts]-- },
		func(v *completion) { v.Unrouted = ^uint64(0) },
		func(v *completion) { v.States["other"] = ^uint64(0) },
		func(v *completion) { delete(v.Digests, receipts) },
		func(v *completion) { v.SourceEvidenceSHA256[metadata] = digest([]byte("self")) },
		func(v *completion) { v.Counts["foreign"] = 1 },
	} {
		v, _ := connectionFixture("2024")
		mutate(&v)
		b, _ := json.Marshal(v)
		if _, err := decodeCycleCompletion(b, digest(b)); err == nil {
			t.Fatal("accepted broken completion")
		}
	}
	v, _ := connectionFixture("2024")
	b, _ := json.Marshal(v)
	if _, err := decodeCycleCompletion(b, digest([]byte("other"))); err == nil {
		t.Fatal("accepted changed bytes")
	}
	b = append(b, []byte(" {}")...)
	if _, err := decodeCycleCompletion(b, digest(b)); err == nil {
		t.Fatal("accepted trailing data")
	}
}

func TestConnectionPathIsCycleIndependentAndNotDepthTruncated(t *testing.T) {
	for _, cycle := range []string{"2020", "2022", "2024", "2026"} {
		_, u := connectionFixture(cycle)
		edges := []flow.Observation{}
		for i := 0; i <= 12; i++ {
			id := fmt.Sprintf("C%08d", i+1)
			n := upstream.Node{CommitteeID: id, Hops: i, Authorized: i == 0}
			if i > 0 {
				n.WitnessOrdinal = ptr(uint64(i))
				edges = append(edges, flow.Observation{Ordinal: uint64(i), Sender: id, Recipient: fmt.Sprintf("C%08d", i), Amount: -103})
			}
			u.Nodes = append(u.Nodes, n)
		}
		path, root, state, err := connectionPath(u, edges, "C00000013")
		if err != nil || len(path) != 12 || root != "C00000001" || state != "connected_observation_path_not_attributed_money" || path[0].Amount != -103 {
			t.Fatal(path, root, state, err)
		}
		p2, r2, s2, err := connectionPath(u, edges, "C00000013")
		if err != nil || !reflect.DeepEqual(path, p2) || root != r2 || state != s2 {
			t.Fatal("non-reproducible path")
		}
		path, root, _, err = connectionPath(u, edges, "C00000001")
		if err != nil || len(path) != 0 || root != "C00000001" {
			t.Fatal("direct authorization lost")
		}
		for _, recipient := range []string{"", "C99999999"} {
			path, root, state, err = connectionPath(u, edges, recipient)
			if err != nil || len(path) != 0 || root != "" || strings.HasPrefix(state, "connected") {
				t.Fatal("gap promoted to connected")
			}
		}
		edges[11].Recipient = "C00000013"
		if _, _, _, err := connectionPath(u, edges, "C00000013"); err == nil {
			t.Fatal("cyclic witness accepted")
		}
	}
}

func TestConnectionReadbackNeverRepairsAndChecksAbsentEdges(t *testing.T) {
	for _, changed := range []bool{false, true} {
		v, _ := connectionFixture("2024")
		raw, _ := json.Marshal(v)
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/count") {
				parts := strings.Split(r.URL.Path, "/")
				name := parts[len(parts)-2]
				n := v.Counts[name]
				if name == metadata {
					n = 1
				}
				if changed && name == receipts {
					n++
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"count": n})
				return
			}
			if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/_api/cursor") {
				t.Error("mutation attempted", r.Method, r.URL.Path)
				http.Error(w, "unexpected", 500)
				return
			}
			var q struct {
				Query string                     `json:"query"`
				Bind  map[string]json.RawMessage `json:"bindVars"`
			}
			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
				t.Error(err)
			}
			if !strings.HasPrefix(q.Query, "FOR k IN @keys LET d = DOCUMENT(") {
				t.Error("unexpected query", q.Query)
			}
			var collection string
			_ = json.Unmarshal(q.Bind["collection"], &collection)
			value := json.RawMessage("null")
			if collection == metadata {
				value = raw
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"result": []json.RawMessage{value}, "hasMore": false})
		}))
		cl, _ := newClient(Options{Endpoint: s.URL, Username: "fixture", Password: "fixture"}, "lt_receipt_cycle_fixture")
		err := verifyCycleCompletion(context.Background(), cl, raw, v)
		if (err != nil) != changed {
			t.Fatal("count gate", err)
		}
		if _, err := verifyConnectionDocument(context.Background(), cl, receipts, "missing", (*receipt)(nil)); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyConnectionDocument(context.Background(), cl, receipts, "missing", receipt{Key: "missing"}); err == nil {
			t.Fatal("missing edge accepted")
		}
		s.Close()
	}
}
