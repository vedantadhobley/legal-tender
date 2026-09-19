package fundinggeneration

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

func fixturePathLink(key int, from, to string) graphread.Link {
	return graphread.Link{Family: "receiver_reported_committee_observation", Key: fmt.Sprintf("%064x", key), From: from, To: to}
}

func TestPathSearchMatchesIndependentSmallGraphEnumeration(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 40; trial++ {
		chain := pathTopology{}
		for from := 1; from <= 5; from++ {
			for to := 1; to <= 5; to++ {
				if rng.Intn(4) == 0 {
					_ = chain.add(fixturePathLink(from*10+to, fmt.Sprintf("C%08d", from), fmt.Sprintf("C%08d", to)))
				}
			}
		}
		_ = chain.order()
		// An independent breadth-first enumeration, without the production DFS or
		// completion/counter code, establishes the complete bounded path membership.
		type walk struct {
			at    string
			nodes map[string]bool
			ids   []string
		}
		queue := []walk{{at: "C00000001", nodes: map[string]bool{"C00000001": true}}}
		want := map[string]bool{}
		for len(queue) > 0 {
			w := queue[0]
			queue = queue[1:]
			if w.at == "C00000005" {
				want[valueID(w.ids)] = true
				continue
			}
			if len(w.ids) == 3 {
				continue
			}
			for _, l := range chain[w.at] {
				if w.nodes[l.To] {
					continue
				}
				nodes := map[string]bool{}
				for id := range w.nodes {
					nodes[id] = true
				}
				nodes[l.To] = true
				ids := append(append([]string{}, w.ids...), l.ID())
				queue = append(queue, walk{l.To, nodes, ids})
			}
		}
		paths, stats, err := searchPaths(context.Background(), chain, nil, "C00000001", "C00000005", 3, 10, 100000)
		if err != nil {
			t.Fatal(err)
		}
		if len(want) > 10 {
			if stats.MorePaths != "yes" || len(paths) != 10 {
				t.Fatal("lost truncation")
			}
			continue
		}
		got := map[string]bool{}
		for _, p := range paths {
			ids := []string{}
			for _, l := range p {
				ids = append(ids, l.ID())
			}
			got[valueID(ids)] = true
		}
		if !reflect.DeepEqual(got, want) || stats.MorePaths != "no" {
			t.Fatal("enumeration differs", trial, len(got), len(want), stats)
		}
	}
}
func pathFixture(t *testing.T) (pathTopology, pathTopology) {
	t.Helper()
	chain := pathTopology{}
	for _, l := range []graphread.Link{
		fixturePathLink(1, "C00000001", "C00000002"), fixturePathLink(2, "C00000001", "C00000002"),
		fixturePathLink(3, "C00000002", "C00000001"), fixturePathLink(4, "C00000002", "C00000003"),
	} {
		if err := chain.add(l); err != nil {
			t.Fatal(err)
		}
	}
	end := fixturePathLink(5, "C00000003", "H0ZZ00001")
	end.Family = "candidate_authorization_context"
	endings := pathTopology{}
	_ = endings.add(end)
	if err := chain.order(); err != nil {
		t.Fatal(err)
	}
	return chain, endings
}

func TestPathSearchPreservesParallelEvidenceAndSkipsCycles(t *testing.T) {
	chain, endings := pathFixture(t)
	p, s, e := searchPaths(context.Background(), chain, endings, "C00000001", "H0ZZ00001", 8, 10, 100)
	if e != nil || len(p) != 2 || s.State != "complete_within_hop_bound" || s.CycleClosures != 2 || s.MorePaths != "no" {
		t.Fatal(p, s, e)
	}
	if len(p[0]) != 3 || p[0][0].ID() == p[1][0].ID() || p[0][2].Family != "candidate_authorization_context" {
		t.Fatal("collapsed parallel observations or lost ending")
	}
	// Input insertion order does not alter output or counters after normalization.
	other := pathTopology{}
	for _, es := range chain {
		for i := len(es) - 1; i >= 0; i-- {
			_ = other.add(es[i])
		}
	}
	_ = other.order()
	again, stats, e := searchPaths(context.Background(), other, endings, "C00000001", "H0ZZ00001", 8, 10, 100)
	if e != nil || !reflect.DeepEqual(p, again) || s != stats {
		t.Fatal("unstable replay")
	}
}

func TestPathSearchBoundsNeverBecomeTerminalEvidence(t *testing.T) {
	chain, endings := pathFixture(t)
	p, s, e := searchPaths(context.Background(), chain, endings, "C00000001", "H0ZZ00001", 8, 1, 100)
	if e != nil || len(p) != 1 || s.State != "truncated_path_limit" || s.MorePaths != "yes" {
		t.Fatal(p, s, e)
	}
	p, s, e = searchPaths(context.Background(), chain, endings, "C00000001", "H0ZZ00001", 1, 10, 100)
	if e != nil || len(p) != 0 || s.HopFrontiers != 2 || s.MorePaths != "no" || s.State != "complete_within_hop_bound" {
		t.Fatal(p, s, e)
	}
	p, s, e = searchPaths(context.Background(), chain, endings, "C00000001", "H0ZZ00001", 8, 10, 1)
	if e != nil || len(p) != 0 || s.Examined != 1 || s.State != "truncated_expansion_budget" || s.MorePaths != "unknown" {
		t.Fatal(p, s, e)
	}
	p, s, e = searchPaths(context.Background(), chain, endings, "C99999999", "H0ZZ00001", 8, 10, 100)
	if e != nil || len(p) != 0 || s.NoOutgoing != 1 || s.MorePaths != "no" {
		t.Fatal(p, s, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := searchPaths(ctx, chain, endings, "C00000001", "H0ZZ00001", 8, 10, 100); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestPathDirectEndingAndIdentityDoNotRequireFlowMembership(t *testing.T) {
	_, endings := pathFixture(t)
	p, _, e := searchPaths(context.Background(), pathTopology{}, endings, "C00000003", "H0ZZ00001", 0, 2, 100)
	if e != nil || len(p) != 1 || len(p[0]) != 1 {
		t.Fatal(p, e)
	}
	p, _, e = searchPaths(context.Background(), pathTopology{}, nil, "C00000003", "C00000003", 0, 2, 100)
	if e != nil || len(p) != 1 || len(p[0]) != 0 {
		t.Fatal(p, e)
	}
}

func TestPathsRejectEmptySelfPathEvenForUnknownID(t *testing.T) {
	q := PathQuery{From: "C99999999", Target: "C99999999", Ledger: flow.ScheduleA, MaxHops: 2, Limit: 2, Budget: 100}
	if q.Validate() == nil {
		t.Fatal("accepted empty self-path without any source evidence")
	}
	r := Reader{}
	if _, err := r.Paths(context.Background(), q); err == nil {
		t.Fatal("opened unsupported self query")
	}
	// An exact receipt may still end directly at its reported committee.
	q.From = ""
	q.ReceiptOrdinal = 1
	q.EntryFamily = "reported_receipt"
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPathQueryRejectsMixedLedgersAmbiguousStartsAndContextHops(t *testing.T) {
	valid := PathQuery{From: "C00000001", Ledger: flow.ScheduleA, Target: "H0ZZ00001", Ending: "candidate_authorization_context", MaxHops: 2, Limit: 2, Budget: 100}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*PathQuery){
		func(q *PathQuery) { q.Ledger = "both" }, func(q *PathQuery) { q.From = "H0ZZ00001" }, func(q *PathQuery) { q.ReceiptOrdinal = 2 }, func(q *PathQuery) { q.EntryFamily = "reported_receipt" },
		func(q *PathQuery) { q.Ending = "reconciliation_candidate" }, func(q *PathQuery) { q.Ending = "" }, func(q *PathQuery) { q.Target = "C00000002" }, func(q *PathQuery) { q.MaxHops = 9 }, func(q *PathQuery) { q.Limit = 11 }, func(q *PathQuery) { q.Budget = 0 },
	} {
		q := valid
		mutate(&q)
		if q.Validate() == nil {
			t.Fatal("accepted invalid query", q)
		}
		r := Reader{}
		if _, err := r.Paths(context.Background(), q); err == nil {
			t.Fatal("opened invalid query")
		}
	}
	q := valid
	q.From = ""
	q.ReceiptOrdinal = 2
	q.EntryFamily = "conduit_association"
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	r := Reader{}
	if _, err := r.pathEvidence(context.Background(), flow.ScheduleB, fixturePathLink(1, "C00000001", "C00000002"), nil); err == nil {
		t.Fatal("accepted foreign ledger")
	}
}

func TestPathWitnessSelectionRejectsDirectOnlyAndCycles(t *testing.T) {
	chain, ends := pathFixture(t)
	from, target, ok := selectPathWitness(chain, ends["C00000003"])
	if !ok || from != "C00000001" || target != "H0ZZ00001" {
		t.Fatal(from, target, ok)
	}
	direct := fixturePathLink(6, "C00000001", "H0ZZ00001")
	direct.Family = "candidate_authorization_context"
	if _, _, ok := selectPathWitness(chain, append(ends["C00000003"], direct)); ok {
		t.Fatal("selected direct ending as multi-hop witness")
	}
	if err := chain.add(fixturePathLink(1, "bad", "C00000002")); err == nil {
		t.Fatal("invalid endpoint")
	}
	_ = chain.add(fixturePathLink(1, "C00000001", "C00000002"))
	if chain.order() == nil {
		t.Fatal("duplicate topology")
	}
}
