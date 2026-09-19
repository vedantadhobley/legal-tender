package organizationresolution

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestRetainedRegistryCorroboration(t *testing.T) {
	root := filepath.Join("..", "..", "..", "tests", "fixtures", "organization-resolution")
	w, err := wikimedia.Read(filepath.Join(root, "capture-v1"), "6ef200cd16c436544a350c2940e70f677aa35b60ce3da9aacbd522de6e30ade8")
	if err != nil {
		t.Fatal(err)
	}
	g, err := gleif.Read(filepath.Join(root, "gleif-capture-v1"), "da1a319d7a91d87f6e0d2dcbe9ab10443f8e1beee890a0fd31ac966e922bd027")
	if err != nil {
		t.Fatal(err)
	}
	r, err := Corroborate(w, g, wikimedia.Hash([]byte("corpus-build")))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Cases) != 20 || len(g.Observations) != 1 || g.Observations[0].Issue != "" || g.Observations[0].Record.LEI != "300300SRCLQKVTFFOM15" {
		t.Fatal("retained record conservation")
	}
	pairs, missing, observed, unusable := 0, 0, 0, 0
	for _, c := range r.Cases {
		if c.SourceIssue != "" {
			unusable++
		}
		for _, candidate := range c.Candidates {
			pairs++
			switch candidate.State {
			case "no_lei_claim":
				missing++
			case "lei_record_observed_fec_identity_unresolved":
				observed++
			default:
				t.Fatal("unexpected real correspondence", candidate.State)
			}
		}
	}
	if pairs != 25 || missing != 24 || observed != 1 || unusable != 15 {
		t.Fatal(pairs, missing, observed, unusable)
	}
	if r.IdentityPublicationApproved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
		t.Fatal("registry record approved financial identity")
	}
	for _, i := range []int{0, 2} {
		proposed := r.Proposals.Decisions[i].ProposedQID
		found := false
		for _, c := range r.Cases[i].Candidates {
			if c.QID == proposed {
				found = true
				if c.State != "no_lei_claim" {
					t.Fatal("reviewed name proposal invented LEI")
				}
			}
		}
		if !found || proposed == "" {
			t.Fatal("baseline proposal lost")
		}
	}
	again, err := Corroborate(w, g, r.BuildSHA256)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("offline replay differs", err)
	}
}
