package committeeidentity

import (
	"testing"

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
)

func TestDecisionFromGapPreservesHistoricalEvidenceAndBlocksTerminalIdentity(t *testing.T) {
	calculationSetID := digestParts("calculation")
	gap := fecflowmastergaps.CommitteeGap{
		CommitteeID: "C00123456", State: "found_only_in_historical_master",
		EndpointRoles: []string{"source"},
		HistoricalMasters: []fecflowmastergaps.CommitteeHistoricalAssertion{{
			Cycle: "2018", SourceKind: "official_bulk_archive",
			ArchiveSHA256: digestParts("archive"), SourceRow: 12,
			SourceRowSHA256: digestParts("row"), IssueCodes: []string{}, Name: "EXAMPLE COMMITTEE",
		}},
	}
	decision, err := decisionFromGap(calculationSetID, "2024", gap)
	if err != nil {
		t.Fatal(err)
	}
	if decision.State != StateHistoricalRegistration || decision.TerminalIdentityEligible {
		t.Fatalf("unexpected state or terminal eligibility: %+v", decision)
	}
	if len(decision.HistoricalAssertions) != 1 || decision.HistoricalAssertions[0].Name != "EXAMPLE COMMITTEE" {
		t.Fatalf("historical evidence was not preserved: %+v", decision.HistoricalAssertions)
	}
}

func TestDecisionFromGapKeepsUnresolvedReportedIDExplicit(t *testing.T) {
	decision, err := decisionFromGap(digestParts("calculation"), "2024", fecflowmastergaps.CommitteeGap{
		CommitteeID: "C00987654", State: "absent_from_all_audited_masters", EndpointRoles: []string{"source"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.State != StateUnresolvedReportedID || decision.TerminalIdentityEligible ||
		len(decision.HistoricalAssertions) != 0 || len(decision.SameCycleAssertions) != 0 {
		t.Fatalf("unexpected unresolved decision: %+v", decision)
	}
}

func TestDecisionCountsConserveAllGapStates(t *testing.T) {
	counts := Counts{}
	for _, decision := range []Decision{
		{State: StateHistoricalRegistration, EndpointRoles: []string{"source"}, HistoricalAssertions: make([]fecflowmastergaps.CommitteeHistoricalAssertion, 2)},
		{State: StateAlternateReleaseRegistration, EndpointRoles: []string{"recipient"}, SameCycleAssertions: make([]fecflowmastergaps.CommitteeHistoricalAssertion, 1)},
		{State: StateUnresolvedReportedID, EndpointRoles: []string{"recipient", "source"}},
	} {
		accumulateDecision(&counts, decision)
	}
	if counts.IdentityCoverageDecisions != 3 || decisionCount(counts) != 3 || counts.HistoricalRegistrations != 1 ||
		counts.AlternateReleaseRegistrations != 1 || counts.UnresolvedReportedIDs != 1 ||
		counts.SourceEndpointDecisions != 2 || counts.RecipientEndpointDecisions != 2 || counts.BothEndpointRoleDecisions != 1 ||
		counts.HistoricalRegistrationAssertions != 2 || counts.AlternateRegistrationAssertions != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}
