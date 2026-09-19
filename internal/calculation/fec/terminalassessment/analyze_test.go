package terminalassessment

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

func cm(n int) string  { return fmt.Sprintf("C%08d", n) }
func key(n int) string { return fmt.Sprintf("%064x", n) }
func masters(n int) []Committee {
	out := []Committee{}
	for i := 1; i <= n; i++ {
		id := key(i)
		out = append(out, Committee{cm(i), SameCycleMaster, key(99), &id})
	}
	return out
}
func edge(k, a, b int) Observation { return Observation{key(k), cm(a), cm(b)} }

func TestSeparateLedgersMissingIdentityCyclesAndAbsence(t *testing.T) {
	ids := masters(8)
	ids[0].IdentityState, ids[0].MasterFactID = MissingMaster, nil
	a := []Observation{edge(1, 1, 2), edge(2, 1, 2), edge(3, 3, 4), edge(4, 4, 3), edge(5, 5, 5), edge(6, 6, 7), edge(7, 7, 6), edge(8, 2, 6)}
	b := []Observation{edge(1, 2, 1), edge(2, 8, 2)} // Same keys in a different ledger are legal.
	v, err := Analyze(context.Background(), ids, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if v.TerminalEligible || v.OriginState != "not_established" || v.A.Observations != 8 || v.B.Observations != 2 || v.A.Present != 7 || v.A.MissingMasters != 1 {
		t.Fatal(v)
	}
	if v.A.CyclicComponents != 3 || v.A.CyclicRootComponents != 2 || v.A.RootComponents != 3 {
		t.Fatal(v.A.Components)
	}
	if v.A.Rules[0].Matched != 1 || v.A.Rules[1].Matched != 0 || v.A.Rules[2].Matched != 4 {
		t.Fatal(v.A.Rules)
	}
	if v.A.Nodes[0].Incoming != 0 || v.A.Nodes[0].Outgoing != 2 || *v.A.Nodes[0].IdentityFrontier {
		t.Fatal(v.A.Nodes[0])
	}
	if v.A.Nodes[7].State != Absent || v.A.Nodes[7].Frontier != nil || v.A.Nodes[7].ComponentID != "" {
		t.Fatal("absent endpoint became frontier")
	}
	if v.A.Nodes[4].SelfLoops != 1 || *v.A.Nodes[4].Frontier || !*v.A.Nodes[4].RootComponent {
		t.Fatal("self-loop semantics")
	}
	for _, l := range []Ledger{v.A, v.B} {
		for _, rule := range l.Rules {
			if rule.Matched+rule.NotMatched+rule.NotApplicable != 8 {
				t.Fatal("rule count loss")
			}
		}
	}
	var total uint64
	for _, c := range v.Comparison {
		total += c.Committees
	}
	if total != 8 {
		t.Fatal("comparison count loss")
	}
	// Change order, not evidence; IDs and every result field must remain equal.
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
	}
	b[0], b[1] = b[1], b[0]
	replay, err := Analyze(context.Background(), ids, a, b)
	if err != nil || !reflect.DeepEqual(v, replay) {
		t.Fatal("order changed assessment", err)
	}
}

func TestSCCAndFrontiersAgainstIndependentReachability(t *testing.T) {
	rng := rand.New(rand.NewPCG(101, 209))
	for trial := 0; trial < 200; trial++ {
		n := 1 + rng.IntN(14)
		reach := make([][]bool, n)
		present, incoming, outgoing := make([]bool, n), make([]uint64, n), make([]uint64, n)
		for i := range reach {
			reach[i] = make([]bool, n)
			reach[i][i] = true
		}
		es := []Observation{}
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if rng.IntN(5) == 0 {
					for p := 0; p < 1+rng.IntN(3); p++ {
						es = append(es, edge(len(es)+1, i+1, j+1))
						incoming[j]++
						outgoing[i]++
					}
					reach[i][j] = true
					present[i] = true
					present[j] = true
				}
			}
		}
		for k := 0; k < n; k++ {
			for i := 0; i < n; i++ {
				for j := 0; j < n; j++ {
					reach[i][j] = reach[i][j] || reach[i][k] && reach[k][j]
				}
			}
		}
		ids := []Committee{}
		for i, c := range masters(n) {
			if present[i] {
				ids = append(ids, c)
			}
		}
		v, err := Analyze(context.Background(), ids, es, nil)
		if err != nil {
			t.Fatal(err)
		}
		index := map[string]Node{}
		for _, node := range v.A.Nodes {
			index[node.CommitteeID] = node
		}
		for i := 0; i < n; i++ {
			if !present[i] {
				continue
			}
			node := index[cm(i+1)]
			if node.Incoming != incoming[i] || node.Outgoing != outgoing[i] || *node.Frontier != (incoming[i] == 0) {
				t.Fatal("degree mismatch")
			}
			root := true
			for j := 0; j < n; j++ {
				if !present[j] {
					continue
				}
				same := reach[i][j] && reach[j][i]
				if (node.ComponentID == index[cm(j+1)].ComponentID) != same {
					t.Fatal("SCC mismatch", trial, i, j)
				}
				if reach[j][i] && !same {
					root = false
				}
			}
			if *node.RootComponent != root {
				t.Fatal("condensation root mismatch")
			}
		}
	}
}

func TestLongChainHasNoQueryDepthCutoff(t *testing.T) {
	const size = 30000
	es := make([]Observation, 0, size-1)
	for i := 1; i < size; i++ {
		es = append(es, edge(i, i, i+1))
	}
	v, err := Analyze(context.Background(), masters(size), es, nil)
	if err != nil || v.A.Present != size || len(v.A.Components) != size || v.A.Rules[0].Matched != 1 || v.A.CyclicComponents != 0 {
		t.Fatal("long-chain failure", err)
	}
}

func TestRejectsUnboundIdentityDuplicateKeysAndCancelledEmptyGraph(t *testing.T) {
	for _, es := range [][]Observation{{edge(1, 1, 3)}, {edge(1, 1, 2), edge(1, 2, 1)}, {{"bad", cm(1), cm(2)}}} {
		if _, err := Analyze(context.Background(), masters(2), es, nil); err == nil {
			t.Fatal("accepted bad observation")
		}
	}
	for _, mutate := range []func([]Committee) []Committee{
		func(cs []Committee) []Committee { return append(cs, cs[0]) },
		func(cs []Committee) []Committee { cs[0].IdentityState = "other"; return cs },
		func(cs []Committee) []Committee { cs[0].MasterFactID = nil; return cs },
		func(cs []Committee) []Committee { cs[0].IdentityState = MissingMaster; return cs },
		func(cs []Committee) []Committee { cs[0].MasterFactSetID = "bad"; return cs },
		func(cs []Committee) []Committee { cs[0].ID = "H0AA00001"; return cs },
	} {
		if _, err := Analyze(context.Background(), mutate(masters(2)), []Observation{edge(1, 1, 2)}, nil); err == nil {
			t.Fatal("accepted bad identity")
		}
	}
	if _, err := Analyze(context.Background(), masters(1), nil, nil); err == nil {
		t.Fatal("accepted entity outside endpoint union")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Analyze(ctx, nil, nil, nil); err == nil {
		t.Fatal("ignored cancellation")
	}
	v, err := Analyze(context.Background(), nil, nil, nil)
	if err != nil || len(v.Committees) != 0 || v.A.Observations != 0 || v.TerminalEligible {
		t.Fatal("empty topology", err)
	}
}
