package independentexpenditures

import (
	"context"
	"strings"
	"testing"
)

func TestOutsideCandidatePathEndpointsAndStancesRemainSeparate(t *testing.T) {
	r := ResolvedReader{m: resolvedProjection{Edges: []resolvedExpenditureEdge{
		{ResultID: strings.Repeat("a", 64), From: entitiesCollection + "/committee_C00000001", To: entitiesCollection + "/candidate_H0ZZ00001", SupportOppose: "S"},
		{ResultID: strings.Repeat("b", 64), From: entitiesCollection + "/committee_C00000001", To: entitiesCollection + "/candidate_H0ZZ00001", SupportOppose: "O"},
	}}}
	for _, family := range []string{"independent_support", "independent_opposition"} {
		links, e := r.CandidateLinks(family)
		if e != nil || len(links) != 1 || links[0].Family != family || links[0].From != "C00000001" || links[0].To != "H0ZZ00001" {
			t.Fatal(links, e)
		}
	}
	if _, err := r.CandidateLinks("combined_outside"); err == nil {
		t.Fatal("combined stances")
	}
	if _, err := r.PathEvidence(context.Background(), "independent_opposition", strings.Repeat("a", 64)); err == nil {
		t.Fatal("accepted other stance")
	}
}
