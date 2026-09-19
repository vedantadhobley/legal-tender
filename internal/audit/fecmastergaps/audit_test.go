package fecmastergaps

import (
	"strings"
	"testing"

	fecreceipts "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

func TestAnalyzeClassifiesMissingMastersWithoutBackfill(t *testing.T) {
	results := []fecreceipts.Result{
		{
			CandidateID:            "H1CURRENT",
			CommitteeRelationships: []fecreceipts.CommitteeRelationship{{CommitteeID: "C1CURRENT"}},
		},
		{
			CandidateID:            "H2HISTORY",
			MoneyMeasure:           fecreceipts.MoneyMeasure{Amount: fecreceipts.MoneyAmount{State: "point"}},
			IncludedRecords:        fecreceipts.IncludedCounts{Records: 2, Positive: 2},
			CommitteeRelationships: []fecreceipts.CommitteeRelationship{{CommitteeID: "C2HISTORY"}},
			CommitteeSubtotals: []fecreceipts.CommitteeSubtotal{{
				CommitteeID: "C2HISTORY", AmountMinorUnits: "1250",
				IncludedRecords: fecreceipts.IncludedCounts{Records: 2, Positive: 2},
			}},
			SourceSummaries: []fecreceipts.SourceSummary{{Dataset: "all-candidates-summary"}},
		},
		{
			CandidateID:            "H3ABSENT",
			CommitteeRelationships: []fecreceipts.CommitteeRelationship{{CommitteeID: "C3ABSENT"}},
		},
	}
	currentCandidates := map[string]candidateMasterRecord{"H1CURRENT": {}}
	currentCommittees := map[string]committeeMasterRecord{"C1CURRENT": {}}
	candidateHistory := map[string][]CandidateHistoricalAssertion{
		"H2HISTORY": {{Cycle: "2022", Name: "Historical candidate assertion"}},
	}
	committeeHistory := map[string][]CommitteeHistoricalAssertion{
		"C2HISTORY": {{Cycle: "2022", Name: "Historical committee assertion"}},
	}

	candidates, committees, counts, err := analyze(
		results, currentCandidates, currentCommittees,
		map[string][]CandidateHistoricalAssertion{}, map[string][]CommitteeHistoricalAssertion{},
		candidateHistory, committeeHistory,
	)
	if err != nil {
		t.Fatal(err)
	}
	if counts.CandidateReferences != 3 || counts.CandidatesMissingCurrentMaster != 2 || counts.CandidatesFoundOnlyInHistory != 1 || counts.CandidatesAbsentFromAllComparisons != 1 {
		t.Fatalf("unexpected candidate counts: %+v", counts)
	}
	if counts.CommitteeReferences != 3 || counts.CommitteesMissingCurrentMaster != 2 || counts.CommitteesFoundOnlyInHistory != 1 || counts.CommitteesAbsentFromAllComparisons != 1 {
		t.Fatalf("unexpected committee counts: %+v", counts)
	}
	if candidates[0].CandidateID != "H2HISTORY" || candidates[0].State != stateFoundInHistory || len(candidates[0].HistoricalMasters) != 1 {
		t.Fatalf("history candidate was not preserved as a separate assertion: %+v", candidates[0])
	}
	if candidates[1].CandidateID != "H3ABSENT" || candidates[1].State != stateAbsentAll || candidates[1].HistoricalMasters == nil {
		t.Fatalf("absent candidate classification is wrong: %+v", candidates[1])
	}
	if committees[0].CommitteeID != "C2HISTORY" || committees[0].State != stateFoundInHistory || committees[0].AttributedAmountMinorUnits != "1250" {
		t.Fatalf("history committee classification is wrong: %+v", committees[0])
	}
	if committees[1].CommitteeID != "C3ABSENT" || committees[1].State != stateAbsentAll || committees[1].AttributedAmountMinorUnits != "0" {
		t.Fatalf("relationship-only committee classification is wrong: %+v", committees[1])
	}
}

func TestAnalyzeRejectsDuplicateCandidateResults(t *testing.T) {
	_, _, _, err := analyze(
		[]fecreceipts.Result{{CandidateID: "H1DUP"}, {CandidateID: "H1DUP"}},
		map[string]candidateMasterRecord{}, map[string]committeeMasterRecord{},
		map[string][]CandidateHistoricalAssertion{}, map[string][]CommitteeHistoricalAssertion{},
		map[string][]CandidateHistoricalAssertion{}, map[string][]CommitteeHistoricalAssertion{},
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate calculation result") {
		t.Fatalf("expected duplicate-result failure, got %v", err)
	}
}

func TestAnalyzeRejectsInvalidCommitteeSubtotal(t *testing.T) {
	_, _, _, err := analyze(
		[]fecreceipts.Result{{CandidateID: "H1TEST", CommitteeSubtotals: []fecreceipts.CommitteeSubtotal{{CommitteeID: "C1TEST", AmountMinorUnits: "1.25"}}}},
		map[string]candidateMasterRecord{}, map[string]committeeMasterRecord{},
		map[string][]CandidateHistoricalAssertion{}, map[string][]CommitteeHistoricalAssertion{},
		map[string][]CandidateHistoricalAssertion{}, map[string][]CommitteeHistoricalAssertion{},
	)
	if err == nil || !strings.Contains(err.Error(), "invalid subtotal") {
		t.Fatalf("expected invalid-subtotal failure, got %v", err)
	}
}
