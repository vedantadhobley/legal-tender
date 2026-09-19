package flowevidence

import (
	"context"
	"strings"
	"testing"
)

func TestFlowNeighborhoodScopeAndMissingFacets(t *testing.T) {
	r := readerFixture(t)
	ctx := context.Background()
	for _, kind := range []string{"receiver_reported_committee_observation", "sender_reported_committee_observation", "reconciliation_candidate"} {
		p, e := r.NeighborhoodPage(ctx, kind, "H0ZZ00001", "", 1)
		if e != nil || p.State != "not_applicable" {
			t.Fatal(p, e)
		}
		p, e = r.NeighborhoodPage(ctx, kind, "C99999999", "", 1)
		if e != nil || p.State != "not_present_in_projection" {
			t.Fatal(p, e)
		}
	}
	if _, e := r.NeighborhoodPage(ctx, "combined_money", "C00000001", "", 1); e == nil {
		t.Fatal("accepted combined ledger")
	}
	readerResponse(t, r, []entity{r.m.entities[0]})
	f, e := r.Facet(ctx, "C00000001")
	if e != nil || f.State != "present" || !strings.Contains(string(f.Document), "unresolved_same_cycle_master") {
		t.Fatal(f, e)
	}
	f, e = r.Facet(ctx, "C99999999")
	if e != nil || f.State != "not_present_in_projection" {
		t.Fatal(f, e)
	}
}
