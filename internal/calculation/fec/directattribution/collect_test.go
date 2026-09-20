package directattribution

import (
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

func pointer[T any](value T) *T { return &value }

func authorizedFixture() []receipts.LinkageFact {
	return []receipts.LinkageFact{
		{FactID: "a", State: "valid", CandidateID: "H00000001", CommitteeID: "C00000001", DesignationCode: "P"},
		{FactID: "b", State: "valid", CandidateID: "H00000001", CommitteeID: "C00000002", DesignationCode: "J"},
		{FactID: "c", State: "invalid", CandidateID: "H00000001", CommitteeID: "C00000003", DesignationCode: "A"},
	}
}

func row(ordinal int64, component string, amount *int64) participants.Row {
	return participants.Row{
		Ordinal: ordinal, Recipient: pointer("C00000001"), Component: component,
		IndividualDecision: "excluded_non_individual", ReceiptRole: committeeflows.RoleUnresolved,
		Amount: amount, AmountState: "reported_value",
	}
}

func TestCollectorConservesAcceptedAttribution(t *testing.T) {
	collector, err := NewCollector(authorizedFixture(), 2)
	if err != nil {
		t.Fatal(err)
	}
	direct := row(1, "itemized_individual_only", pointer(int64(100)))
	direct.IndividualDecision = "included"
	earmarked := row(2, "itemized_individual_only", pointer(int64(50)))
	earmarked.IndividualDecision = "included"
	earmarked.ReceiptRole = committeeflows.RoleEarmarked
	committee := row(3, "committee_flow_only", pointer(int64(40)))
	overlap := row(4, "overlapping_individual_and_committee", pointer(int64(30)))
	unresolvedIndividual := row(5, "unresolved_individual_class", pointer(int64(-10)))
	other := row(6, "other_reported_receipt", pointer(int64(0)))
	unknown := row(7, "unknown_amount", nil)
	unknown.AmountState = "source_null"
	memo := row(8, "memo_subtotal", pointer(int64(100)))
	memo.Memo = true
	outside := row(9, "itemized_individual_only", pointer(int64(20)))
	outside.Recipient = pointer("C99999999")
	outside.IndividualDecision = "included"
	missingRecipient := row(10, "unresolved_recipient", pointer(int64(10)))
	missingRecipient.Recipient = nil
	for index, input := range []participants.Row{direct, earmarked, committee, overlap, unresolvedIndividual, other, unknown, memo, outside, missingRecipient} {
		if err := collector.Observe(index%2, input); err != nil {
			t.Fatalf("observe %d: %v", index, err)
		}
	}
	census, candidates, err := collector.Finish(10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := collector.Authorization(), (AuthorizationCensus{CandidatesWithLinkageFacts: 1, CandidatesWithAuthorization: 1, AuthorizedCommittees: 1, AuthorizedRelationships: 1, UnresolvedRelationships: 1, UnauthorizedRelationships: 1}); !reflect.DeepEqual(got, want) {
		t.Fatalf("authorization = %#v, want %#v", got, want)
	}
	if census.CompleteParticipants.Rows != 10 || census.CompleteParticipants.SignedMinorUnits != "340" || census.OutsideAuthorizationScope.Rows != 2 || census.OutsideAuthorizationScope.SignedMinorUnits != "30" {
		t.Fatalf("unexpected complete census: %#v", census)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	got := candidates[0]
	if got.CandidateID != "H00000001" || !reflect.DeepEqual(got.AuthorizedCommittees, []string{"C00000001"}) || got.AuthorizedScope.Rows != 8 || got.AuthorizedScope.SignedMinorUnits != "310" || got.IncludedNonmemo.Rows != 7 || got.IncludedNonmemo.SignedMinorUnits != "210" {
		t.Fatalf("unexpected candidate scope: %#v", got)
	}
	if got.Direct.Rows != 1 || got.Direct.SignedMinorUnits != "100" || got.Earmarked.Rows != 1 || got.Earmarked.SignedMinorUnits != "50" || got.Unresolved.Rows != 5 || got.Unresolved.SignedMinorUnits != "60" || got.Unresolved.UnknownAmountRows != 1 || got.ExcludedMemo.Rows != 1 || got.ExcludedMemo.SignedMinorUnits != "100" {
		t.Fatalf("unexpected attribution: %#v", got)
	}
	wantReasons := []ReasonMeasures{
		{Reason: "amount_unresolved", Measures: Measures{Rows: 1, UnknownAmountRows: 1, SignedMinorUnits: "0", PositiveMinorUnits: "0", NegativeMinorUnits: "0"}},
		{Reason: "committee_chain_unresolved", Measures: Measures{Rows: 1, KnownAmountRows: 1, PositiveRows: 1, SignedMinorUnits: "40", PositiveMinorUnits: "40", NegativeMinorUnits: "0"}},
		{Reason: "individual_class_unresolved", Measures: Measures{Rows: 1, KnownAmountRows: 1, NegativeRows: 1, SignedMinorUnits: "-10", PositiveMinorUnits: "0", NegativeMinorUnits: "-10"}},
		{Reason: "individual_committee_role_conflict", Measures: Measures{Rows: 1, KnownAmountRows: 1, PositiveRows: 1, SignedMinorUnits: "30", PositiveMinorUnits: "30", NegativeMinorUnits: "0"}},
		{Reason: "other_reported_source_role_unresolved", Measures: Measures{Rows: 1, KnownAmountRows: 1, ZeroRows: 1, SignedMinorUnits: "0", PositiveMinorUnits: "0", NegativeMinorUnits: "0"}},
	}
	if !reflect.DeepEqual(got.UnresolvedReasons, wantReasons) {
		t.Fatalf("reasons = %#v, want %#v", got.UnresolvedReasons, wantReasons)
	}
}

func TestSharedAuthorizationStaysOutsideFinancialRouting(t *testing.T) {
	linkages := []receipts.LinkageFact{
		{FactID: "a", State: "valid", CandidateID: "H00000001", CommitteeID: "C00000001", DesignationCode: "A"},
		{FactID: "b", State: "valid", CandidateID: "S00000002", CommitteeID: "C00000001", DesignationCode: "P"},
	}
	collector, err := NewCollector(linkages, 1)
	if err != nil {
		t.Fatal(err)
	}
	input := row(1, "itemized_individual_only", pointer(int64(25)))
	input.IndividualDecision = "included"
	if err := collector.Observe(0, input); err != nil {
		t.Fatal(err)
	}
	census, candidates, err := collector.Finish(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 || census.OutsideAuthorizationScope.Rows != 1 || census.OutsideAuthorizationScope.SignedMinorUnits != "25" {
		t.Fatal(census, candidates)
	}
	if got := collector.Authorization(); got.CandidatesWithLinkageFacts != 2 || got.CandidatesWithAuthorization != 0 || got.UnresolvedRelationships != 2 {
		t.Fatal(got)
	}
}

func TestCollectorRejectsContradictoryInputs(t *testing.T) {
	if _, err := NewCollector(nil, 1); err == nil {
		t.Fatal("empty linkage population accepted")
	}
	if _, err := NewCollector([]receipts.LinkageFact{{CandidateID: "bad", CommitteeID: "C00000001"}}, 1); err == nil {
		t.Fatal("invalid linkage identity accepted")
	}
	for name, edit := range map[string]func(*participants.Row){
		"memo_component":        func(value *participants.Row) { value.Memo = true },
		"itemized_membership":   func(value *participants.Row) { value.Component = "itemized_individual_only" },
		"unsupported_component": func(value *participants.Row) { value.Component = "new_component" },
	} {
		t.Run(name, func(t *testing.T) {
			collector, err := NewCollector(authorizedFixture(), 1)
			if err != nil {
				t.Fatal(err)
			}
			input := row(1, "committee_flow_only", pointer(int64(1)))
			edit(&input)
			if err := collector.Observe(0, input); err == nil {
				t.Fatal("contradictory row accepted")
			}
		})
	}
	collector, err := NewCollector(authorizedFixture(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := collector.Finish(1); err == nil {
		t.Fatal("incomplete scan accepted")
	}
}

func TestCollectorWorkerCountDoesNotChangeResult(t *testing.T) {
	inputs := []participants.Row{
		row(1, "committee_flow_only", pointer(int64(20))),
		row(2, "other_reported_receipt", pointer(int64(-5))),
		row(3, "unknown_amount", nil),
	}
	var priorCensus Census
	var priorCandidates []CandidateResult
	for workers := 1; workers <= 3; workers++ {
		collector, err := NewCollector(authorizedFixture(), workers)
		if err != nil {
			t.Fatal(err)
		}
		for index, input := range inputs {
			if err := collector.Observe(index%workers, input); err != nil {
				t.Fatal(err)
			}
		}
		census, candidates, err := collector.Finish(uint64(len(inputs)))
		if err != nil {
			t.Fatal(err)
		}
		if workers > 1 && (!reflect.DeepEqual(census, priorCensus) || !reflect.DeepEqual(candidates, priorCandidates)) {
			t.Fatal("worker count changed logical result")
		}
		priorCensus, priorCandidates = census, candidates
	}
}
