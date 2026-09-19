package committeeflows

import (
	"testing"

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
	fecidentity "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeidentity"
)

func TestDecisionEntityV2PreservesEvidenceWithoutInventingCanonicalName(t *testing.T) {
	decision := fecidentity.Decision{
		DecisionID: digestParts("decision"), CalculationSetID: digestParts("calculation"),
		CommitteeID: "C00123456", State: fecidentity.StateHistoricalRegistration,
		HistoricalAssertions: []fecflowmastergaps.CommitteeHistoricalAssertion{{Cycle: "2018", Name: "HISTORICAL NAME"}},
	}
	document := decisionEntityV2(decision, "2024")
	if document.SourceState != fecidentity.StateHistoricalRegistration || document.TerminalIdentityEligible {
		t.Fatalf("unexpected identity state: %+v", document)
	}
	if document.Name != "" || len(document.HistoricalAssertions) != 1 || document.HistoricalAssertions[0].Name != "HISTORICAL NAME" {
		t.Fatalf("historical assertion was collapsed into canonical fields: %+v", document)
	}
	if document.IdentityDecisionID == nil || *document.IdentityDecisionID != decision.DecisionID {
		t.Fatalf("identity decision lineage is missing: %+v", document)
	}
}
