package receiptreferences

import (
	"context"
	"encoding/json"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"io"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func ptr(s string) *string { return &s }
func fixture() []Row {
	rows := []Row{}
	add := func(tx, back, sched string) {
		ord := int64(len(rows) + 1)
		rows = append(rows, Row{Ordinal: ord, Cycle: 2024, Normalization: "valid", Recipient: ptr("C00000001"), File: ptr("42"), Transaction: ptr(tx), BackReference: ptr(back), BackSchedule: ptr(sched), Schedule: ptr("SA"), Line: ptr("11AI")})
	}
	add("a", "b", "SA")
	add("b", "a", "SA11AI") // reciprocal, one peer each
	add("fan", "b", "SA")   // fan-out at b, not a second b->a link
	add("dup", "", "")
	add("dup", "", "")
	add("askdup", "dup", "SA") // unreferenced duplicates
	add("same", "b", "SA")
	add("same", "", "") // duplicated source, even though only one references
	add("missing", "absent", "SA")
	add("incomplete", "", "SA")
	add("self", "self", "SA")
	add("wrong", "b", "SB")
	add("no-sched", "b", "")
	add("other-report", "a", "SA")
	rows[len(rows)-1].File = ptr("43")
	add("invalid", "a", "SA")
	rows[len(rows)-1].Recipient = nil
	add("no-own-id", "b", "SA")
	rows[len(rows)-1].Transaction = nil
	add("unrelated", "", "")
	return rows
}
func runRows(t *testing.T, rows []Row, size, filterBytes int) (*engine, []Decision, []Neighbors) {
	t.Helper()
	s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "work"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	e, err := newEngine(context.Background(), s, Options{RunRows: size, FanIn: 2, FilterBytes: filterBytes})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err = e.request(r); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(rows) - 1; i >= 0; i-- {
		if err = e.membership(rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.join(); err != nil {
		t.Fatal(err)
	}
	if err = e.decisions(); err != nil {
		t.Fatal(err)
	}
	if err = e.neighbors(); err != nil {
		t.Fatal(err)
	}
	d := []Decision{}
	n := []Neighbors{}
	for i, f := range []xsort.File{e.out.Decisions, e.out.Neighbors} {
		r, err := xsort.Open(e.ctx, s.Dir, f)
		if err != nil {
			t.Fatal(err)
		}
		for {
			v, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				var value Decision
				if err = json.Unmarshal(v.Data, &value); err != nil {
					t.Fatal(err)
				}
				d = append(d, value)
			} else {
				var value Neighbors
				if err = json.Unmarshal(v.Data, &value); err != nil {
					t.Fatal(err)
				}
				n = append(n, value)
			}
		}
		r.Close()
	}
	return e, d, n
}
func TestCompleteLookupAndReverseIncidenceIndependentOfRunsAndFilter(t *testing.T) {
	rows := fixture()
	var wantD []Decision
	var wantN []Neighbors
	var digest string
	for _, size := range []int{1, 3, 100} {
		for _, bits := range []int{1, 1024} {
			e, d, n := runRows(t, rows, size, bits)
			if wantD != nil && (!reflect.DeepEqual(wantD, d) || !reflect.DeepEqual(wantN, n) || digest != e.out.Decisions.ValuesSHA256) {
				t.Fatal("geometry/filter affected decisions")
			}
			wantD, wantN, digest = d, n, e.out.Decisions.ValuesSHA256
			var total uint64
			for _, count := range e.out.States {
				total += count
			}
			if total != uint64(len(rows)) {
				t.Fatal("conservation")
			}
			for _, v := range d {
				want := map[int64]string{1: "exact_same_report_reference", 2: "exact_same_report_reference", 3: "exact_same_report_reference", 6: "ambiguous_target_transaction_id", 7: "duplicate_source_transaction_id", 9: "target_absent_from_cycle_report", 10: "incomplete_report_reference", 11: "self_reference", 12: "reference_schedule_mismatch", 13: "missing_reference_schedule", 14: "target_absent_from_cycle_report", 15: "invalid_report_scope", 16: "incomplete_report_reference"}[v.Source.Ordinal]
				if v.State != want {
					t.Fatal(v.Source.Ordinal, want, v.State)
				}
			}
			if !reflect.DeepEqual(n, []Neighbors{{Ordinal: 1, Peers: 1, Incoming: 1, Outgoing: 1}, {Ordinal: 2, Peers: 2, Incoming: 2, Outgoing: 1}, {Ordinal: 3, Peers: 1, Outgoing: 1}}) {
				t.Fatal(n)
			}
		}
	}
}
func TestFilterNeverDropsInsertedKeysAndKeyFraming(t *testing.T) {
	f := make(filter, 1)
	for _, r := range fixture() {
		k := key(r.Recipient, r.File, r.Transaction)
		f.visit(k, true)
		if !f.visit(k, false) {
			t.Fatal("false negative")
		}
	}
	if key(nil, ptr(""), ptr("x")) == key(ptr(""), nil, ptr("x")) || key(ptr("ab"), ptr("c"), ptr("")) == key(ptr("a"), ptr("bc"), ptr("")) {
		t.Fatal("key collision")
	}
	_, d, n := runRows(t, []Row{{Ordinal: 1}}, 2, 1)
	if len(d) != 0 || len(n) != 0 {
		t.Fatal("empty-reference population")
	}
}

func TestJoinMatchesBruteForceMembershipAndPeerSets(t *testing.T) {
	// The oracle deliberately uses direct nullable field comparisons, not the
	// framed-key encoder, Bloom filter, sort order or shared reference policy.
	for seed := uint64(1); seed <= 8; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed+1))
		rows := make([]Row, 64)
		ids := []*string{nil, ptr(""), ptr("a"), ptr("b"), ptr("ab"), ptr("a|b"), ptr("b|c"), ptr("c")}
		files := []*string{ptr("1"), ptr("12"), ptr("23")}
		for i := range rows {
			rows[i] = Row{Ordinal: int64(i + 1), Cycle: 2024, Normalization: "valid", Recipient: ptr("C00000001"), File: files[rng.IntN(len(files))], Transaction: ids[rng.IntN(len(ids))], BackReference: ids[rng.IntN(len(ids))], BackSchedule: ptr("SA"), Schedule: ptr("SA"), Line: ptr("11AI")}
			if rng.IntN(4) == 0 {
				rows[i].BackSchedule = nil
			}
		}
		rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
		_, decisions, neighbors := runRows(t, rows, 13, 1)
		peerSets := map[uint64]map[uint64]bool{}
		incoming, outgoing := map[uint64]uint64{}, map[uint64]uint64{}
		for _, d := range decisions {
			for i, transaction := range []*string{d.Source.Transaction, d.Source.BackReference} {
				var count uint64
				for _, r := range rows {
					if transaction != nil && *transaction != "" && reflect.DeepEqual(d.Source.Recipient, r.Recipient) && reflect.DeepEqual(d.Source.File, r.File) && reflect.DeepEqual(transaction, r.Transaction) {
						count++
					}
				}
				got := d.SourceMultiplicity
				if i == 1 {
					got = d.TargetMultiplicity
				}
				if transaction == nil || *transaction == "" {
					if got != nil {
						t.Fatal("unrequested lookup acquired a count", seed, d)
					}
				} else if got == nil || *got != count {
					t.Fatal("brute force membership differs", seed, count, d)
				}
			}
			if d.Target == nil {
				continue
			}
			from, to := uint64(d.Source.Ordinal), *d.Target
			if peerSets[from] == nil {
				peerSets[from] = map[uint64]bool{}
			}
			if peerSets[to] == nil {
				peerSets[to] = map[uint64]bool{}
			}
			peerSets[from][to], peerSets[to][from] = true, true
			outgoing[from]++
			incoming[to]++
		}
		want := []Neighbors{}
		for ordinal, peers := range peerSets {
			want = append(want, Neighbors{Ordinal: ordinal, Peers: uint64(len(peers)), Incoming: incoming[ordinal], Outgoing: outgoing[ordinal]})
		}
		slices.SortFunc(want, func(a, b Neighbors) int {
			if a.Ordinal < b.Ordinal {
				return -1
			}
			if a.Ordinal > b.Ordinal {
				return 1
			}
			return 0
		})
		if !reflect.DeepEqual(want, neighbors) {
			t.Fatal("brute force distinct-peer sets differ", seed, want, neighbors)
		}
	}
}
