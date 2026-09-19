package flowevidence

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"
)

func readerFixture(t *testing.T) *Reader {
	t.Helper()
	m := fixture()
	m.id = hash("fixture")
	m.bundle.Cycle = "2024"
	m.components[0].Key = hash("component")
	m.components[0].ID = m.components[0].Key
	if err := bindComponents(&m); err != nil {
		t.Fatal(err)
	}
	for _, es := range [][]observation{m.a, m.b} {
		sort.Slice(es, func(i, j int) bool { return es[i].Key < es[j].Key })
	}
	m.entities = []entity{{Key: "C00000001", CommitteeID: "C00000001", IdentityState: "unresolved_same_cycle_master"}, {Key: "C00000002", CommitteeID: "C00000002", IdentityState: "same_cycle_master"}}
	return &Reader{m: m}
}
func readerResponse(t *testing.T, r *Reader, rows any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"result": rows, "hasMore": false})
	if err != nil {
		t.Fatal(err)
	}
	r.c = mockClient(t, func(req *http.Request) (*http.Response, error) {
		if req.Method != "POST" || !strings.HasSuffix(req.URL.Path, "/_api/cursor") {
			t.Fatal("reader attempted mutation", req.Method, req.URL.Path)
		}
		var q struct {
			Query   string         `json:"query"`
			Options map[string]any `json:"options"`
		}
		if err := json.NewDecoder(req.Body).Decode(&q); err != nil {
			t.Fatal(err)
		}
		for _, word := range []string{"INSERT ", "UPDATE ", "REPLACE ", "REMOVE ", "UPSERT "} {
			if strings.Contains(q.Query, word) {
				t.Fatal("write query")
			}
		}
		if q.Options["maxRuntime"] != float64(5) {
			t.Fatal("missing query cap")
		}
		return response(string(body)), nil
	})
}

func TestReadQueryBoundsAndLedgerIsolation(t *testing.T) {
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		q := ReadQuery{Kind: "paths", Ledger: side, Committee: "C00000001", Target: "C00000002", Depth: 2, Limit: 1}
		sql, bind, err := readQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		collection, _ := edgeCollection(side)
		other, _ := edgeCollection(ScheduleB)
		if side == ScheduleB {
			other, _ = edgeCollection(ScheduleA)
		}
		if !strings.Contains(sql, collection) || strings.Contains(sql, other) || strings.Contains(sql, "GRAPH ") || strings.Contains(sql, "reconciliation_components") || !strings.Contains(sql, "SORT key LIMIT @limit") || bind["limit"] != 2 {
			t.Fatal(sql, bind)
		}
		for _, change := range []func(*ReadQuery){func(q *ReadQuery) { q.Ledger = "both" }, func(q *ReadQuery) { q.Committee = "C1' REMOVE" }, func(q *ReadQuery) { q.Depth = 9 }, func(q *ReadQuery) { q.Depth = 0 }, func(q *ReadQuery) { q.Limit = 26 }, func(q *ReadQuery) { q.Direction = "any" }, func(q *ReadQuery) { q.Component = hash("other") }} {
			bad := q
			change(&bad)
			if _, _, err := readQuery(bad); err == nil {
				t.Fatal("unsafe query accepted", bad)
			}
		}
	}
}

func TestReaderSelectionMembershipAndCompletionRecheck(t *testing.T) {
	r := readerFixture(t)
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		got, err := r.Recipients(side)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range r.m.edges(side) {
			if !got[e.Recipient] {
				t.Fatal("lost recipient")
			}
		}
		got["foreign"] = true
		again, _ := r.Recipients(side)
		if again["foreign"] {
			t.Fatal("mutable reader state exposed")
		}
	}
	if _, err := r.Recipients("both"); err == nil {
		t.Fatal("ledgers merged")
	}
	if r.VerifyCompletion(context.Background()) == nil {
		t.Fatal("unverified completion accepted")
	}
	r.proof = completion{Key: r.m.id, State: "complete"}
	readerResponse(t, r, []completion{r.proof})
	if err := r.VerifyCompletion(context.Background()); err != nil {
		t.Fatal(err)
	}
	changed := r.proof
	changed.State = "changed"
	readerResponse(t, r, []completion{changed})
	if r.VerifyCompletion(context.Background()) == nil {
		t.Fatal("changed completion accepted")
	}
}

func TestObservationAtUsesPinnedFactSetAndLedger(t *testing.T) {
	r := readerFixture(t)
	r.m.calculation.Input.A.FactSetID = r.m.a[0].FactSetID
	r.m.calculation.Input.B.FactSetID = r.m.b[0].FactSetID
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		es := r.m.edges(side)
		want := es[0]
		readerResponse(t, r, []observation{want})
		raw, err := r.ObservationAt(context.Background(), side, want.Ordinal)
		if err != nil || !equalDocument(raw, want) {
			t.Fatal(string(raw), err)
		}
		canonical, _ := json.Marshal(want)
		var reordered map[string]json.RawMessage
		if err := json.Unmarshal(canonical, &reordered); err != nil {
			t.Fatal(err)
		}
		readerResponse(t, r, []map[string]json.RawMessage{reordered})
		again, err := r.ObservationAt(context.Background(), side, want.Ordinal)
		if err != nil || string(again) != string(canonical) || string(again) != string(raw) {
			t.Fatal("backend property order changed canonical evidence", err)
		}
		changed := want
		changed.Amount++
		readerResponse(t, r, []observation{changed})
		if _, err := r.ObservationAt(context.Background(), side, want.Ordinal); err == nil {
			t.Fatal("changed observation accepted")
		}
		if _, err := r.ObservationAt(context.Background(), side, 0); err != ErrInvalidQuery {
			t.Fatal(err)
		}
		if _, err := r.ObservationAt(context.Background(), side, ^uint64(0)); err != ErrNotFound {
			t.Fatal(err)
		}
	}
	if _, err := r.ObservationAt(context.Background(), "both", 1); err != ErrInvalidQuery {
		t.Fatal(err)
	}
	// Same ordinal in another fact set cannot reuse the existing observation.
	r.m.calculation.Input.A.FactSetID = hash("other facts")
	if _, err := r.ObservationAt(context.Background(), ScheduleA, r.m.a[0].Ordinal); err != ErrNotFound {
		t.Fatal(err)
	}
}

func TestObservationPaginationConservesOccurrencesAndRejectsDrift(t *testing.T) {
	r := readerFixture(t)
	q := ReadQuery{Kind: "observations", Ledger: ScheduleA, Committee: "C00000001", Direction: "outbound", Limit: 2}
	readerResponse(t, r, r.m.a)
	first, err := r.Query(context.Background(), q)
	if err != nil || !first.HasMore || len(first.Items) != 2 || first.Last != r.m.a[1].Key {
		t.Fatal(first, err)
	}
	q.After = first.Last
	readerResponse(t, r, r.m.a[2:])
	last, err := r.Query(context.Background(), q)
	if err != nil || last.HasMore || len(last.Items) != 1 {
		t.Fatal(last, err)
	}
	joined := append(first.Items, last.Items...)
	seen := map[string]bool{}
	var amounts []string
	for _, raw := range joined {
		var e map[string]any
		_ = json.Unmarshal(raw, &e)
		key := e["_key"].(string)
		if seen[key] {
			t.Fatal("repeated edge")
		}
		seen[key] = true
		amounts = append(amounts, e["signed_amount_minor_units"].(string))
	}
	sort.Strings(amounts)
	if strings.Join(amounts, ",") != "-3,0,9007199254740993" {
		t.Fatal(amounts)
	}
	q.After = ""
	for _, bad := range []any{r.m.b, []observation{r.m.a[1], r.m.a[0]}, append(r.m.a, r.m.a[0])} {
		readerResponse(t, r, bad)
		if _, err := r.Query(context.Background(), q); err == nil {
			t.Fatal("accepted opposite, duplicate or unordered observations")
		}
	}
	e := r.m.a[0]
	e.Amount++
	readerResponse(t, r, []observation{e})
	if _, err := r.Query(context.Background(), q); err == nil {
		t.Fatal("accepted amount drift")
	}
}

func TestPathDocumentsAndComponentMembershipRemainSeparate(t *testing.T) {
	r := readerFixture(t)
	q := ReadQuery{Kind: "paths", Ledger: ScheduleA, Committee: "C00000001", Target: "C00000002", Depth: 1, Limit: 1}
	e := r.m.a[0]
	raw, _ := json.Marshal(e)
	p := ReadPath{Key: "1:" + receivers + "/" + e.Key, Observations: []json.RawMessage{raw}}
	for _, v := range r.m.entities {
		raw, _ := json.Marshal(v)
		p.Vertices = append(p.Vertices, raw)
	}
	readerResponse(t, r, []ReadPath{p})
	if _, err := r.Query(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	foreign, _ := json.Marshal(r.m.b[0])
	p.Observations[0] = foreign
	if err := r.validateReadPath(p, q); err == nil {
		t.Fatal("opposite ledger entered path")
	}
	readerResponse(t, r, r.m.components)
	summary, err := r.Component(context.Background(), r.m.components[0].Key)
	if err != nil || strings.Contains(string(summary), "schedule_a_ordinals") || !strings.Contains(string(summary), `"schedule_a_members":3`) {
		t.Fatal(string(summary), err)
	}
	q = ReadQuery{Kind: "members", Ledger: ScheduleB, Component: r.m.components[0].Key, Limit: 10}
	readerResponse(t, r, r.m.b)
	if _, err := r.Query(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	q.Component = hash("unknown")
	if _, err := r.Query(context.Background(), q); err != ErrNotFound {
		t.Fatal(err)
	}
}
