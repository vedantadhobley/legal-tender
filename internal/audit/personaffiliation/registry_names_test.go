package personaffiliation

import (
	"context"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
)

func TestEmployerRegistryPlanIsIndependentOfReviewAndWikimedia(t *testing.T) {
	p, err := EmployerRegistryPlan(context.Background(), fixtureDir, corpusPin, hash([]byte("build")))
	if err != nil || len(p.Inputs) != 4 || len(p.Queries) != 3 {
		t.Fatal("plan", err)
	}
	if p.Queries[0].Text != "JC2VENTURES" || len(p.Queries[0].Inputs) != 2 {
		t.Fatal("occurrences/query formatting")
	}
	dir, c := fixtureCopy(t)
	for i := range c.Roles {
		c.Roles[i].Claim.PersonName = "Different review subject"
		c.Roles[i].Claim.OrganizationName = "Different company label"
	}
	changed := writeCorpus(t, dir, c)
	again, err := EmployerRegistryPlan(context.Background(), dir, changed, p.BuildSHA256)
	if err != nil {
		t.Fatal(err)
	}
	again.Selection = p.Selection
	if !reflect.DeepEqual(p, again) {
		t.Fatal("reviewed identities steered registry queries")
	}
}

func TestRetainedRegistryNameCaptureUsesExactFECInputs(t *testing.T) {
	r, err := gleif.ReadNames(fixtureDir+"/gleif-names-v1", "759cc1be8ff0713b1bd57655e081edc8b3820661f1af7049d9ddd62038e25e51")
	if err != nil {
		t.Fatal(err)
	}
	p, err := EmployerRegistryPlan(context.Background(), fixtureDir, corpusPin, r.Manifest.Plan.BuildSHA256)
	if err != nil || !reflect.DeepEqual(p, r.Manifest.Plan) || !r.CaptureUsable {
		t.Fatal("real capture provenance", err)
	}
	for i, want := range []int{0, 3, 0} {
		if len(r.Observations[i].Page.Records) != want {
			t.Fatal("retained page changed", i)
		}
	}
}
