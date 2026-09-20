package terminalpolicycomparison

import (
	"context"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func money(value int64) fundingbasis.Measures {
	out := fundingbasis.Measures{Rows: 1, Known: 1, Signed: value}
	switch {
	case value > 0:
		out.PositiveRows, out.Positive = 1, value
	case value < 0:
		out.NegativeRows, out.Negative = 1, value
	default:
		out.ZeroRows = 1
	}
	return out
}

func fixturePopulation(parts ...fundingbasis.ReceiptRoleCoverage) fundingbasis.ReceiptPopulation {
	out := fundingbasis.ReceiptPopulation{State: "reported_rows_not_complete_funding", Components: parts}
	for _, part := range parts {
		value := part.Measures
		out.Total.Rows += value.Rows
		out.Total.Known += value.Known
		out.Total.Unknown += value.Unknown
		out.Total.PositiveRows += value.PositiveRows
		out.Total.NegativeRows += value.NegativeRows
		out.Total.ZeroRows += value.ZeroRows
		out.Total.Signed += value.Signed
		out.Total.Positive += value.Positive
		out.Total.Negative += value.Negative
	}
	return out
}

func fixtureInput() Input {
	digest := strings.Repeat("a", 64)
	return Input{
		CandidateID: "H0AA00001", Cycle: "2024", ExecutableSHA256: digest,
		Reference: InputReference{DossierID: digest, DossierSHA256: digest, ReceiptFactSetID: digest, ReceiptManifestSHA: digest, ReceiptSourceRelease: "fec-release"},
		Committees: []CommitteeInput{
			{CommitteeID: "C00000001", Authorization: "authorized", Receipts: fixturePopulation(
				fundingbasis.ReceiptRoleCoverage{Component: "itemized_individual_only", Role: "unresolved", Measures: money(100)},
				fundingbasis.ReceiptRoleCoverage{Component: "itemized_individual_only", Role: "earmarked", Measures: money(50)},
				fundingbasis.ReceiptRoleCoverage{Component: "committee_flow_only", Role: "registered_filer_contribution", Measures: money(30)},
				fundingbasis.ReceiptRoleCoverage{Component: "other_reported_receipt", Role: "unresolved", Measures: money(-10)},
				fundingbasis.ReceiptRoleCoverage{Component: "unknown_amount", Role: "unresolved", Measures: fundingbasis.Measures{Rows: 1, Unknown: 1}},
				fundingbasis.ReceiptRoleCoverage{Component: "memo_subtotal", Role: "semantic_memo", Measures: money(500)},
			)},
			{CommitteeID: "C00000002", Authorization: "unresolved", Receipts: fixturePopulation(
				fundingbasis.ReceiptRoleCoverage{Component: "itemized_individual_only", Role: "unresolved", Measures: money(20)},
			)},
		},
	}
}

func TestCompareConservesEveryScenarioAndKeepsPoliciesUnselected(t *testing.T) {
	result, err := Compare(context.Background(), fixtureInput())
	if err != nil {
		t.Fatal(err)
	}
	if result.TerminalEligible || result.TerminalPolicy != nil || result.AllocationPolicy != nil || len(result.Scenarios) != 6 {
		t.Fatalf("unexpected policy promotion: %#v", result)
	}
	if result.Scope.Included.Rows != 6 || result.Scope.Included.UnknownAmountRows != 1 || result.Scope.Included.SignedMinorUnits != "190" ||
		result.Scope.ExcludedMemo.SignedMinorUnits != "500" {
		t.Fatalf("unexpected scope: %#v", result.Scope)
	}
	for _, scenario := range result.Scenarios {
		if !scenario.Conserved || scenario.Selected {
			t.Fatalf("scenario not conserved or selected: %#v", scenario)
		}
	}
	direct := result.Scenarios[2]
	if direct.Direct.SignedMinorUnits != "100" || direct.Earmarked.SignedMinorUnits != "50" || direct.Unresolved.SignedMinorUnits != "40" || direct.Unresolved.UnknownAmountRows != 1 {
		t.Fatalf("unexpected direct scenario: %#v", direct)
	}
	committeeStop := result.Scenarios[3]
	if committeeStop.Direct.SignedMinorUnits != "130" || committeeStop.Unresolved.SignedMinorUnits != "10" {
		t.Fatalf("unexpected committee-stop scenario: %#v", committeeStop)
	}
	for _, blocked := range result.Scenarios[4:] {
		if blocked.Unresolved.SignedMinorUnits != "190" || blocked.Status != "blocked_not_calculated" {
			t.Fatalf("blocked method invented an allocation: %#v", blocked)
		}
	}
}

func TestCompareRejectsDuplicateCommitteeAndBrokenPopulation(t *testing.T) {
	input := fixtureInput()
	input.Committees[1].CommitteeID = input.Committees[0].CommitteeID
	if _, err := Compare(context.Background(), input); err == nil {
		t.Fatal("duplicate committee accepted")
	}
	input = fixtureInput()
	input.Committees[0].Receipts.Total.Signed++
	if _, err := Compare(context.Background(), input); err == nil {
		t.Fatal("nonconserving population accepted")
	}
}

func TestCompareIdentityIsOrderIndependent(t *testing.T) {
	input := fixtureInput()
	first, err := Compare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input = fixtureInput()
	input.Committees[0], input.Committees[1] = input.Committees[1], input.Committees[0]
	second, err := Compare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ComparisonID != second.ComparisonID {
		t.Fatalf("identity changed with input order: %s != %s", first.ComparisonID, second.ComparisonID)
	}
}
