package receiptgraph

import "testing"

func TestCommitteeMasterFactsIncludeNonEndpointMastersButNotCandidates(t *testing.T) {
	a, b := "committee-fact", "candidate-fact"
	r := CycleReader{loaded: loaded{masters: map[string]entity{"C00000001": {Kind: "committee", FactID: &a}, "H0AA00001": {Kind: "candidate", FactID: &b}, "C00000002": {Kind: "committee"}}}}
	v := r.CommitteeMasterFacts()
	if len(v) != 1 || v["C00000001"] != a {
		t.Fatal(v)
	}
	v["C00000001"] = "changed"
	if *r.loaded.masters["C00000001"].FactID != a {
		t.Fatal("mutated verified master")
	}
}
