package fundinggeneration

import (
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

// Contract gate on the actual funding-path API, not a second toy money tracer.
// No person-affiliation publication exists yet. If one is added later it must
// not be smuggled in as a receipt, candidate ending, or monetary path target.
func TestPersonAffiliationsCannotBeFundingPathFamilies(t *testing.T) {
	q := PathQuery{ReceiptOrdinal: 1, EntryFamily: "reported_receipt", Ledger: flow.ScheduleA,
		Target: "H0ZZ00001", Ending: "candidate_authorization_context", MaxHops: 3, Limit: 10, Budget: 1000}
	if err := q.Validate(); err != nil {
		t.Fatal("positive control", err)
	}
	for _, family := range []string{"executive_of", "board_director_of", "controlling_owner_of", "employed_by", "founded", "person_affiliation"} {
		t.Run(family, func(t *testing.T) {
			entry := q
			entry.EntryFamily = family
			if err := entry.Validate(); err == nil {
				t.Fatal("affiliation accepted as receipt entry")
			}
			ending := q
			ending.Ending = family
			if err := ending.Validate(); err == nil {
				t.Fatal("affiliation accepted as candidate funding ending")
			}
		})
	}
	q.Target, q.Ending = "organization:fixture", ""
	if err := q.Validate(); err == nil {
		t.Fatal("corporate affiliation became a monetary path target")
	}
}
