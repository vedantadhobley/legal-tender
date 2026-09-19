package flowevidence

import (
	"strings"
	"testing"
)

func TestFinancialQueriesCannotTraverseComponentsOrOppositeLedger(t *testing.T) {
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		for _, kind := range []string{"neighborhood", "paths", "shortest", "cycles"} {
			q, bind, err := pathQuery(pathRequest{side, kind, "entities/C00000001", "entities/C00000002", 4, 25})
			if err != nil {
				t.Fatal(err)
			}
			wanted, _ := edgeCollection(side)
			opposite := senders
			if side == ScheduleB {
				opposite = receivers
			}
			if !strings.Contains(q, wanted) || strings.Contains(q, opposite) || strings.Contains(q, components) || strings.Contains(q, "GRAPH") || strings.Contains(q, "SUM") || bind["depth"] != 4 {
				t.Fatal("query crossed evidence boundary", q)
			}
		}
	}
	for _, r := range []pathRequest{{"", "paths", "a", "b", 4, 25}, {"both", "paths", "a", "b", 4, 25}, {ScheduleA, "paths", "a", "b", 9, 25}, {ScheduleA, "paths", "a", "b", 4, 26}, {ScheduleA, "unsupported", "a", "b", 4, 25}} {
		if _, _, err := pathQuery(r); err == nil {
			t.Fatal("accepted invalid query")
		}
	}
	m := fixture()
	r := pathRequest{ScheduleA, "paths", m.a[0].From, m.a[0].To, 4, 25}
	index := map[string]observation{m.a[0].Key: m.a[0]}
	p := pathRow{[]string{r.start, r.target}, []string{receivers + "/" + m.a[0].Key}}
	if err := validatePath(p, r, index); err != nil {
		t.Fatal(err)
	}
	for _, collection := range []string{senders, components} {
		p.Edges[0] = collection + "/" + m.a[0].Key
		if err := validatePath(p, r, index); err == nil {
			t.Fatal("foreign evidence entered path")
		}
	}
	p.Edges[0] = receivers + "/" + m.a[0].Key
	p.Vertices[1] = "entities/C00000003"
	if err := validatePath(p, r, index); err == nil {
		t.Fatal("discontinuous path accepted")
	}
}
func TestGraphDefinitionExcludesComponentMembership(t *testing.T) {
	g := graphDefinition()
	if len(g.Edges) != 2 || len(g.Orphans) != 0 {
		t.Fatal(g)
	}
	for _, e := range g.Edges {
		if e.Collection == components || len(e.From) != 1 || e.From[0] != entities || e.To[0] != entities {
			t.Fatal(g)
		}
	}
	g.Edges = append(g.Edges, edgeDefinition{components, []string{entities}, []string{entities}})
	if equalGraph(g, graphDefinition()) {
		t.Fatal("accepted membership graph")
	}
}
func TestCycleAndShortestSamples(t *testing.T) {
	adj := map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"a", "d"}}
	if shortestHops(adj, "a", "d", 4) != 3 || shortestHops(adj, "a", "d", 2) != -1 {
		t.Fatal("bad bounded shortest")
	}
	start, n := sampleCycle(adj)
	if start != "a" || n != 3 {
		t.Fatal(start, n)
	}
	if s, n := sampleCycle(map[string][]string{"a": {"b"}}); s != "" || n != 0 {
		t.Fatal("invented cycle")
	}
}

func TestSelfObservationsRemainQueryable(t *testing.T) {
	for _, kind := range []string{"neighborhood", "paths", "shortest", "cycles"} {
		q, _, err := pathQuery(pathRequest{ScheduleA, kind, "entities/C00000001", "entities/C00000001", 3, 25})
		if err != nil || !strings.Contains(q, "uniqueVertices: 'none'") {
			t.Fatal("self observation omitted", q, err)
		}
	}
	if shortestHops(map[string][]string{"a": {"a"}}, "a", "a", 1) != 1 {
		t.Fatal("self hop lost")
	}
}
