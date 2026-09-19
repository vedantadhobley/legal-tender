package personaffiliation

import (
	"context"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestSourceOnlySelectionAndAdditionalSample(t *testing.T) {
	ctx := context.Background()
	baseline, err := Run(ctx, fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := SelectAppearances(ctx, fixtureDir, corpusPin, 0)
	if err != nil || len(selected.Appearances) != len(baseline.Cases) {
		t.Fatal(err)
	}
	excluded := map[string]bool{}
	for i, a := range selected.Appearances {
		if !reflect.DeepEqual(a, baseline.Cases[i].Assessment.Appearance) {
			t.Fatal("FEC source projection changed")
		}
		excluded[searchInputKey(a)] = true
	}
	sample, err := SelectAppearances(ctx, fixtureDir, corpusPin, 3)
	if err != nil || len(sample.Appearances) != 3 || sample.EligibleOccurrences == 0 || sample.DistinctInputs < 3 {
		t.Fatal(sample, err)
	}
	seen := map[string]bool{}
	for _, a := range sample.Appearances {
		key := searchInputKey(a)
		if excluded[key] || seen[key] {
			t.Fatal("sample repeated reviewed or selected pair")
		}
		seen[key] = true
		if a.Receipt.Street1 != nil || a.Receipt.City != nil || a.Receipt.ZIP != nil {
			t.Fatal("unneeded private address projected")
		}
		t.Logf("selected ordinal %d: %s / %s", a.Receipt.Ordinal, *a.Receipt.Name, *a.Receipt.Employer)
	}
	again, err := SelectAppearances(ctx, fixtureDir, corpusPin, 3)
	if err != nil || !reflect.DeepEqual(sample, again) {
		t.Fatal("sample not reproducible", err)
	}
	// Removing all company review data must have no influence or be required.
	dir, c := fixtureCopy(t)
	c.Sources = nil
	c.Roles = nil
	for i := range c.Cases {
		c.Cases[i].RoleIDs = nil
		c.Cases[i].Review = "changed"
	}
	changed, err := SelectAppearances(ctx, dir, writeCorpus(t, dir, c), 3)
	if err != nil || !reflect.DeepEqual(sample.Appearances, changed.Appearances) {
		t.Fatal("human annotations entered source selection", err)
	}
	plan, err := sample.DiscoveryPlan(hash([]byte("build")), wikimedia.DiscoveryPolicy)
	if err != nil || len(plan.Appearances) != 3 || len(plan.Searches) > 9 {
		t.Fatal("source-derived plan", err)
	}
}
