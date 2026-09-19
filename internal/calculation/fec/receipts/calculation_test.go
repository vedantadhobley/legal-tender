package receipts

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func TestCanonicalCalculationFixture(t *testing.T) {
	t.Parallel()
	type fixtureReceipt struct {
		FactID                     string  `json:"fact_id"`
		CommitteeID                string  `json:"committee_id"`
		PublisherClassedIndividual *bool   `json:"publisher_classed_individual"`
		MemoedSubtotal             bool    `json:"memoed_subtotal"`
		AmountMinorUnits           *string `json:"amount_minor_units"`
		ReceivedOn                 *string `json:"received_on"`
	}
	type fixtureInput struct {
		CandidateID string           `json:"candidate_id"`
		Cycle       string           `json:"cycle"`
		Linkages    []LinkageFact    `json:"linkages"`
		Receipts    []fixtureReceipt `json:"receipts"`
		Summaries   []SummaryFact    `json:"summaries"`
	}
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate calculation fixture test")
	}
	contractDirectory := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", "..", "contracts", "calculations", "fec", "candidate-itemized-individual-receipts", "v1", "fixtures"))
	inputBytes, err := os.ReadFile(filepath.Join(contractDirectory, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture fixtureInput
	if err := json.Unmarshal(inputBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	input := Input{CandidateID: fixture.CandidateID, Cycle: fixture.Cycle, Linkages: fixture.Linkages, Summaries: fixture.Summaries}
	for _, receipt := range fixture.Receipts {
		input.Receipts = append(input.Receipts, receiptFact(receipt.FactID, receipt.CommitteeID, receipt.PublisherClassedIndividual, receipt.MemoedSubtotal, receipt.AmountMinorUnits, receipt.ReceivedOn))
	}
	result, err := Calculate(input)
	if err != nil {
		t.Fatal(err)
	}
	actualBytes, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	expectedBytes, err := os.ReadFile(filepath.Join(contractDirectory, "resolved-summary-gap.json"))
	if err != nil {
		t.Fatal(err)
	}
	var actual, expected any
	if err := json.Unmarshal(actualBytes, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(expectedBytes, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("calculation output does not match canonical fixture\nactual: %s\nexpected: %s", actualBytes, expectedBytes)
	}
}

func TestCalculatePreservesSignedComponentAndExplainsSummaryGap(t *testing.T) {
	t.Parallel()
	result, err := Calculate(Input{
		CandidateID: "H0AA00001",
		Cycle:       "2024",
		Linkages: []LinkageFact{
			{FactID: "link-a", State: "valid", CandidateID: "H0AA00001", CommitteeID: "C00000001", DesignationCode: "A"},
		},
		Receipts: []fecoccurrence.ScheduleAFact{
			receiptFact("positive", "C00000001", boolPointer(true), false, stringPointer("100"), stringPointer("2023-02-01")),
			receiptFact("negative", "C00000001", boolPointer(true), false, stringPointer("-25"), stringPointer("2024-03-01")),
			receiptFact("zero", "C00000001", boolPointer(true), false, stringPointer("0"), stringPointer("2024-04-01")),
			receiptFact("organization", "C00000001", boolPointer(false), false, stringPointer("900"), stringPointer("2024-05-01")),
			receiptFact("memo", "C00000001", boolPointer(true), true, stringPointer("200"), stringPointer("2024-06-01")),
			receiptFact("unknown-class", "C00000001", nil, false, stringPointer("300"), stringPointer("2024-07-01")),
			receiptFact("unknown-amount", "C00000001", boolPointer(true), false, nil, stringPointer("2024-08-01")),
		},
		Summaries: []SummaryFact{
			{
				FactID: "summary-weball", FactType: "fec.candidate_summary_all.v1", Dataset: "all-candidates-summary",
				CandidateID: "H0AA00001", CoverageThrough: stringPointer("2024-12-31"),
				TotalIndividualContributions: reportedSummary("200"), TotalReceipts: reportedSummary("1000"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "partial" || result.IncludedRecords != (IncludedCounts{Records: 3, Positive: 1, Negative: 1, Zero: 1}) {
		t.Fatalf("unexpected result state/counts: %+v", result)
	}
	if result.MoneyMeasure.Amount.LowerMinorUnits == nil || *result.MoneyMeasure.Amount.LowerMinorUnits != "75" || result.MoneyMeasure.Uncertainty.Coverage != "partial" {
		t.Fatalf("unexpected candidate money measure: %+v", result.MoneyMeasure)
	}
	if result.ExcludedRecords["excluded_non_individual"] != 1 || result.ExcludedRecords["excluded_memo_subtotal"] != 1 {
		t.Fatalf("unexpected exclusions: %+v", result.ExcludedRecords)
	}
	if result.UnresolvedRecords["unresolved_individual_class"] != 1 || result.UnresolvedRecords["unresolved_amount"] != 1 {
		t.Fatalf("unexpected unresolved counts: %+v", result.UnresolvedRecords)
	}
	if len(result.CommitteeSubtotals) != 1 || result.CommitteeSubtotals[0].AmountMinorUnits != "75" {
		t.Fatalf("unexpected committee subtotals: %+v", result.CommitteeSubtotals)
	}
	if len(result.Reconciliations) != 1 {
		t.Fatalf("reconciliations = %d; want 1", len(result.Reconciliations))
	}
	reconciliation := result.Reconciliations[0]
	if reconciliation.CoverageState != "date_bounded" || pointerValue(reconciliation.SummaryMinorUnits) != "200" || pointerValue(reconciliation.ResolvedDetailMinorUnits) != "75" || pointerValue(reconciliation.DifferenceMinorUnits) != "125" {
		t.Fatalf("unexpected reconciliation: %+v", reconciliation)
	}
	if reconciliation.UnresolvedDetailRecords != 2 || len(reconciliation.Issues) != 1 || reconciliation.Issues[0] != "detail_coverage_partial" {
		t.Fatalf("unexpected reconciliation coverage: %+v", reconciliation)
	}
	if len(result.SourceSummaries) != 1 || pointerValue(result.SourceSummaries[0].TotalReceipts.ReportedMinorUnits) != "1000" {
		t.Fatalf("summary context was not retained: %+v", result.SourceSummaries)
	}
}

func TestCalculateBoundsSummaryComparisonWithoutChangingCycleComponent(t *testing.T) {
	t.Parallel()
	result, err := Calculate(Input{
		CandidateID: "H0AA00002", Cycle: "2024",
		Linkages: []LinkageFact{{FactID: "link-p", State: "valid", CandidateID: "H0AA00002", CommitteeID: "C00000002", DesignationCode: "P"}},
		Receipts: []fecoccurrence.ScheduleAFact{
			receiptFact("inside", "C00000002", boolPointer(true), false, stringPointer("100"), stringPointer("2024-05-01")),
			receiptFact("after", "C00000002", boolPointer(true), false, stringPointer("50"), stringPointer("2024-07-01")),
			receiptFact("before", "C00000002", boolPointer(true), false, stringPointer("20"), stringPointer("2022-12-31")),
		},
		Summaries: []SummaryFact{{
			FactID: "summary-webl", FactType: "fec.campaign_summary.v1", Dataset: "current-campaigns-summary",
			CandidateID: "H0AA00002", CoverageThrough: stringPointer("2024-06-30"),
			TotalIndividualContributions: reportedSummary("200"), TotalReceipts: reportedSummary("400"),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pointerValue(result.MoneyMeasure.Amount.LowerMinorUnits) != "170" {
		t.Fatalf("cycle component changed under summary date bound: %+v", result.MoneyMeasure)
	}
	reconciliation := result.Reconciliations[0]
	if pointerValue(reconciliation.ResolvedDetailMinorUnits) != "100" || pointerValue(reconciliation.DifferenceMinorUnits) != "100" || reconciliation.ExcludedOutsideCoverageRecords != 2 {
		t.Fatalf("unexpected date-bounded result: %+v", reconciliation)
	}
}

func TestCalculateBlocksConflictingAuthorizationFromCandidateComponent(t *testing.T) {
	t.Parallel()
	result, err := Calculate(Input{
		CandidateID: "H0AA00003", Cycle: "2024",
		Linkages: []LinkageFact{
			{FactID: "link-a", State: "valid", CandidateID: "H0AA00003", CommitteeID: "C00000003", DesignationCode: "A"},
			{FactID: "link-u", State: "valid", CandidateID: "H0AA00003", CommitteeID: "C00000003", DesignationCode: "U"},
		},
		Receipts: []fecoccurrence.ScheduleAFact{receiptFact("receipt", "C00000003", boolPointer(true), false, stringPointer("100"), stringPointer("2024-01-01"))},
		Summaries: []SummaryFact{{
			FactID: "summary", FactType: "fec.candidate_summary_all.v1", Dataset: "all-candidates-summary",
			CandidateID: "H0AA00003", CoverageThrough: stringPointer("2024-12-31"),
			TotalIndividualContributions: reportedSummary("100"), TotalReceipts: reportedSummary("100"),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "not_comparable" || len(result.CommitteeRelationships) != 1 || result.CommitteeRelationships[0].State != "unresolved" {
		t.Fatalf("conflicting linkage was not isolated: %+v", result)
	}
	if result.UnresolvedRecords["authorization_unresolved"] != 1 || result.MoneyMeasure.Amount.State != "not_applicable" || result.Reconciliations[0].CoverageState != "not_comparable" {
		t.Fatalf("conflicting linkage entered the candidate result: %+v", result)
	}
}

func TestCalculateDoesNotMarkSourceExcludedReceiptAsAuthorizationUnresolved(t *testing.T) {
	t.Parallel()
	result, err := Calculate(Input{
		CandidateID: "H0AA00010", Cycle: "2024",
		Linkages: []LinkageFact{
			{FactID: "link-a", State: "valid", CandidateID: "H0AA00010", CommitteeID: "C00000010", DesignationCode: "A"},
			{FactID: "link-u", State: "valid", CandidateID: "H0AA00010", CommitteeID: "C00000010", DesignationCode: "U"},
		},
		Receipts: []fecoccurrence.ScheduleAFact{
			receiptFact("organization", "C00000010", boolPointer(false), false, stringPointer("100"), stringPointer("2024-01-01")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.UnresolvedRecords["authorization_unresolved"] != 0 {
		t.Fatalf("source-excluded receipt became authorization unresolved: %+v", result)
	}
}

func TestCalculateDoesNotAttributeOneAuthorizedCommitteeToTwoCandidates(t *testing.T) {
	t.Parallel()
	result, err := Calculate(Input{
		CandidateID: "H0AA00006", Cycle: "2024",
		Linkages: []LinkageFact{
			{FactID: "link-first", State: "valid", CandidateID: "H0AA00006", CommitteeID: "C00000006", DesignationCode: "A"},
			{FactID: "link-second", State: "valid", CandidateID: "H0AA00007", CommitteeID: "C00000006", DesignationCode: "P"},
		},
		Receipts: []fecoccurrence.ScheduleAFact{receiptFact("receipt", "C00000006", boolPointer(true), false, stringPointer("100"), stringPointer("2024-01-01"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "not_comparable" || len(result.CommitteeRelationships) != 1 || result.CommitteeRelationships[0].State != "unresolved" {
		t.Fatalf("shared authorized committee was attributed to both candidates: %+v", result)
	}
	if result.UnresolvedRecords["authorization_unresolved"] != 1 || result.IncludedRecords.Records != 0 {
		t.Fatalf("shared committee receipts entered the candidate component: %+v", result)
	}
}

func TestCycleCalculatorRoutesOneReceiptPassToAllCandidates(t *testing.T) {
	t.Parallel()
	calculator, err := NewCycleCalculator("2024", nil, []LinkageFact{
		{FactID: "link-one", State: "valid", CandidateID: "H0AA00008", CommitteeID: "C00000008", DesignationCode: "A"},
		{FactID: "link-two", State: "valid", CandidateID: "H0AA00009", CommitteeID: "C00000009", DesignationCode: "P"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, receipt := range []fecoccurrence.ScheduleAFact{
		receiptFact("one", "C00000008", boolPointer(true), false, stringPointer("100"), stringPointer("2024-01-01")),
		receiptFact("two", "C00000009", boolPointer(true), false, stringPointer("250"), stringPointer("2024-01-02")),
		receiptFact("unrelated", "C00009999", boolPointer(true), false, stringPointer("999"), stringPointer("2024-01-03")),
	} {
		if err := calculator.AddReceipt(receipt); err != nil {
			t.Fatal(err)
		}
	}
	results, err := calculator.Results()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].CandidateID != "H0AA00008" || pointerValue(results[0].MoneyMeasure.Amount.LowerMinorUnits) != "100" || results[1].CandidateID != "H0AA00009" || pointerValue(results[1].MoneyMeasure.Amount.LowerMinorUnits) != "250" {
		t.Fatalf("cycle routing produced unexpected results: %+v", results)
	}
}

func TestCalculateRejectsMinorUnitOverflow(t *testing.T) {
	t.Parallel()
	_, err := Calculate(Input{
		CandidateID: "H0AA00004", Cycle: "2024",
		Linkages: []LinkageFact{{FactID: "link", State: "valid", CandidateID: "H0AA00004", CommitteeID: "C00000004", DesignationCode: "A"}},
		Receipts: []fecoccurrence.ScheduleAFact{
			receiptFact("max", "C00000004", boolPointer(true), false, stringPointer(strconv.FormatInt(math.MaxInt64, 10)), stringPointer("2024-01-01")),
			receiptFact("overflow", "C00000004", boolPointer(true), false, stringPointer("1"), stringPointer("2024-01-02")),
		},
	})
	if err == nil {
		t.Fatal("expected checked minor-unit overflow")
	}
}

func receiptFact(id, committeeID string, individual *bool, memoed bool, minorUnits, receivedOn *string) fecoccurrence.ScheduleAFact {
	observation := fecoccurrence.ScheduleAMoneyObservation{ObservationState: "source_null"}
	if minorUnits != nil {
		observation.ObservationState = "reported_value"
		observation.ReportedMinorUnits = minorUnits
	}
	return fecoccurrence.ScheduleAFact{
		FactID: id, Cycle: "2024",
		TypedFields: fecoccurrence.ScheduleAReceiptTypedFields{
			Recipient:   fecoccurrence.ScheduleARecipientFields{CommitteeID: &committeeID},
			Contributor: fecoccurrence.ScheduleAContributorFields{PublisherClassedIndividual: individual},
			Receipt:     fecoccurrence.ScheduleAReceiptFields{Amount: observation, MemoedSubtotal: memoed, ReceivedOn: receivedOn},
		},
	}
}

func reportedSummary(minorUnits string) SummaryAmount {
	return SummaryAmount{RawValue: minorUnits, ReportedMinorUnits: &minorUnits, ObservationState: "reported_value"}
}

func boolPointer(value bool) *bool       { return &value }
func stringPointer(value string) *string { return &value }

func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
