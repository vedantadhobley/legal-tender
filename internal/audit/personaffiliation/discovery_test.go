package personaffiliation

import (
	"context"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestCorpusDiscoveryPlanIgnoresReviewedRoles(t *testing.T) {
	build := hash([]byte("build"))
	p, err := DiscoveryPlan(context.Background(), fixtureDir, corpusPin, build)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Appearances) != 4 || len(p.Searches) != 9 {
		t.Fatal("source/query conservation")
	}
	shared := 0
	for _, s := range p.Searches {
		if len(s.Appearances) == 2 {
			shared++
		}
	}
	if shared != 3 {
		t.Fatal("repeated appearances lost")
	}
	dir, c := fixtureCopy(t)
	for i := range c.Roles {
		c.Roles[i].Reason = "Different review prose"
		c.Roles[i].Claim.PersonName = "Different reviewed subject"
	}
	changedPin := writeCorpus(t, dir, c)
	again, err := DiscoveryPlan(context.Background(), dir, changedPin, build)
	if err != nil {
		t.Fatal(err)
	}
	// Provenance changes, but annotations do not steer source-derived searches.
	again.Selection = p.Selection
	if !reflect.DeepEqual(p, again) {
		t.Fatal("reviewed labels steered discovery")
	}
}

func TestRetainedDiscoveryKeepsRivalsAndCoverageGaps(t *testing.T) {
	r, err := wikimedia.ReadDiscovery(fixtureDir+"/discovery-v1", "c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c")
	if err != nil {
		t.Fatal(err)
	}
	want, err := DiscoveryPlan(context.Background(), fixtureDir, corpusPin, r.Plan.BuildSHA256)
	if err != nil || !reflect.DeepEqual(want, r.Plan) {
		t.Fatal("capture not bound to unchanged FEC appearances", err)
	}
	if !r.CaptureUsable || r.DiscoveryComplete || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
		t.Fatal("capture became approval")
	}
	if len(r.Observations) != 9 {
		t.Fatal("search conservation")
	}
	for i, expected := range []struct {
		pages  int
		second string
	}{{5, "Q5233095"}, {5, "Q1393271"}, {5, ""}, {1, ""}, {0, ""}, {1, ""}, {0, ""}, {5, ""}, {5, ""}} {
		o := r.Observations[i]
		if len(o.Pages) != expected.pages || len(o.Candidates) != expected.pages {
			t.Fatal("page evidence changed", i)
		}
		if expected.second != "" && o.Candidates[1].QID != expected.second {
			t.Fatal("candidate rank changed", i)
		}
		if o.Issue != "" {
			t.Fatal("source failure", i)
		}
	}
	if r.Observations[3].Candidates[0].QID != "Q5233095" || r.Observations[5].Candidates[0].HumanStatementObserved {
		t.Fatal("context search evidence changed")
	}
	// No empty search or unrelated hit becomes a verified ordinary employee.
	if r.Observations[4].State != "no_pages_in_search_window" || r.Observations[6].State != "no_pages_in_search_window" {
		t.Fatal("empty windows mislabeled")
	}
}

func TestRetainedVariantsUseSameSourceAppearances(t *testing.T) {
	r, err := wikimedia.ReadDiscovery(fixtureDir+"/discovery-v2", "5ff30d1c86bd6e4b2b7746f88628057f30d03220a6b27a6853fc8124e04a9c81")
	if err != nil {
		t.Fatal(err)
	}
	want, err := DiscoveryPlanWithPolicy(context.Background(), fixtureDir, corpusPin, r.Plan.BuildSHA256, wikimedia.DiscoveryVariantsPolicy)
	if err != nil || !reflect.DeepEqual(want, r.Plan) || len(want.Searches) != 15 || !r.CaptureUsable {
		t.Fatal("source-derived variants changed", err)
	}
	// The broader searches return more pages, not the missing company items.
	for i, count := range []int{5, 5, 5, 1, 1, 5, 0, 2, 1, 5, 0, 5, 5, 5, 5} {
		if len(r.Observations[i].Pages) != count {
			t.Fatal("retained search window changed", i)
		}
	}
	dir, c := fixtureCopy(t)
	for i := range c.Roles {
		c.Roles[i].Claim.PersonName = "Unrelated reviewed subject"
	}
	changedPin := writeCorpus(t, dir, c)
	again, err := DiscoveryPlanWithPolicy(context.Background(), dir, changedPin, r.Plan.BuildSHA256, wikimedia.DiscoveryVariantsPolicy)
	if err != nil {
		t.Fatal(err)
	}
	again.Selection = want.Selection
	if !reflect.DeepEqual(want, again) {
		t.Fatal("review annotations steer v2 searches")
	}
}
