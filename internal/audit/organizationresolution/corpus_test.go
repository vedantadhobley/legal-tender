package organizationresolution

import (
	"path/filepath"
	"reflect"
	"testing"

	resolver "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestRetainedRealCorpus(t *testing.T) {
	root := filepath.Join("..", "..", "..", "tests", "fixtures", "organization-resolution")
	o := Options{
		CaptureDirectory: filepath.Join(root, "capture-v1"),
		CaptureSHA256:    "6ef200cd16c436544a350c2940e70f677aa35b60ce3da9aacbd522de6e30ade8",
		CorpusPath:       filepath.Join(root, "corpus-v1.json"),
		CorpusSHA256:     "6475f3beaefc2ab64f4ce72cfb5b4135e6a7084ffd2c0ba7c1ce6cf494415792",
		BuildSHA256:      wikimedia.Hash([]byte("corpus-test")),
	}
	r, err := Run(o)
	if err != nil {
		t.Fatal(err)
	}
	want := Counts{Queries: 20, SourceUsable: 5, SourceUnusable: 15, ReviewedQueries: 2, UnreviewedQueries: 18, ObservedCandidatePairs: 25, ReviewedObservedPairs: 4, UnreviewedObservedPairs: 21, Abstentions: 5, PositiveLabels: 2, PositiveRetrievedNotProposed: 2, NegativeLabels: 2, NegativeNotProposed: 2}
	if r.Counts != want {
		t.Fatalf("real baseline changed: %+v; want %+v", r.Counts, want)
	}
	if r.IdentityPublicationApproved {
		t.Fatal("benchmark approved publication")
	}
	if r.Cases[5].SourceIssue != "http_status_not_ok" || r.Cases[6].SourceIssue != "capture_stopped_or_byte_budget" {
		t.Fatal("rate limit and unattempted inputs collapsed")
	}
	o.ProposalPolicy = resolver.ExpandedPolicy
	expanded, err := Run(o)
	if err != nil {
		t.Fatal(err)
	}
	want.SupportedProposals, want.PositiveProposed = 2, 2
	want.PositiveRetrievedNotProposed, want.Abstentions = 0, 3
	if expanded.Counts != want || expanded.ResolverPolicy != resolver.ExpandedPolicy || expanded.IdentityPublicationApproved {
		t.Fatalf("expanded result changed: %+v; want %+v", expanded.Counts, want)
	}
	for i, old := range r.Cases {
		x := expanded.Cases[i]
		if x.Baseline == nil || x.Baseline.Policy != resolver.Policy || x.Baseline.State != old.ResolverState || x.Baseline.ProposedQID != old.ProposedQID || x.SourceIssue != old.SourceIssue || !reflect.DeepEqual(x.UnreviewedQIDs, old.UnreviewedQIDs) || !reflect.DeepEqual(x.Query, old.Query) {
			t.Fatal("baseline, failures, input or review coverage changed", i)
		}
	}
	for i, expected := range map[int]struct{ qid, rule string }{
		0: {"Q4547697", "digit_letter_boundaries"},
		2: {"Q4596612", "legal_suffix_variant"},
	} {
		x := expanded.Cases[i]
		if x.ProposedQID != expected.qid || len(x.NameCandidates) != 1 || len(x.NameCandidates[0].Matches) == 0 || x.NameCandidates[0].Matches[0].Rule != expected.rule {
			t.Fatal("missing explainable reviewed proposal", i, x)
		}
	}
	again, err := Run(o)
	if err != nil || !reflect.DeepEqual(expanded, again) {
		t.Fatal("expanded replay changed", err)
	}
}
