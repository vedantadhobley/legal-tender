package receiptgraph

import (
	"context"
	"testing"
)

func TestOnlyAuthorizedContextsEndCandidatePaths(t *testing.T) {
	r := CycleReader{loaded: loaded{links: []authorization{
		{Key: digest([]byte("authorized")), From: entities + "/C00000001", To: entities + "/H0ZZ00001", State: "authorized"},
		{Key: digest([]byte("other")), From: entities + "/C00000002", To: entities + "/H0ZZ00001", State: "unresolved"},
	}}}
	links := r.AuthorizedLinks()
	if len(links) != 1 || links[0].From != "C00000001" || links[0].To != "H0ZZ00001" {
		t.Fatal(links)
	}
	if _, err := r.AuthorizationEvidence(context.Background(), r.loaded.links[1].Key); err == nil {
		t.Fatal("unresolved context became authorized path")
	}
	if _, err := r.PathEntry(context.Background(), "reconciliation_candidate", 1); err == nil {
		t.Fatal("comparison component became source entry")
	}
}
