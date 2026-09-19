package committeeflows

import "testing"

func TestClassifySourceIdentityRequiresExactAgreement(t *testing.T) {
	one := "C00000001"
	two := "C00000002"
	invalid := "P00000001"
	for name, test := range map[string]struct {
		raw, clean *string
		state      string
		id         string
	}{
		"exact":      {&one, &one, IdentityExactMatching, one},
		"conflict":   {&one, &two, IdentityConflicting, ""},
		"raw only":   {&one, &invalid, IdentityRawOnly, one},
		"clean only": {nil, &two, IdentityCleanOnly, two},
		"none":       {nil, &invalid, IdentityNoValid, ""},
	} {
		t.Run(name, func(t *testing.T) {
			state, id := ClassifySourceIdentity(test.raw, test.clean)
			if state != test.state || id != test.id {
				t.Fatalf("got (%s, %s), want (%s, %s)", state, id, test.state, test.id)
			}
		})
	}
}

func TestClassifyReceiptRoleSeparatesMoneyDirections(t *testing.T) {
	for code, want := range map[string]string{
		"15K": DecisionIncluded,
		"18G": DecisionIncluded,
		"22Z": DecisionIncluded,
		"24K": DecisionExcludedOutbound,
		"18J": DecisionExcludedSemanticMemo,
		"15E": DecisionExcludedEarmarked,
		"10":  DecisionExcludedNoncommittee,
		"15":  DecisionUnresolvedRole,
	} {
		t.Run(code, func(t *testing.T) {
			_, decision := ClassifyReceiptRole(&code)
			if decision != want {
				t.Fatalf("receipt %s decision = %s, want %s", code, decision, want)
			}
		})
	}
}

func TestEvaluateUsesContractDecisionOrder(t *testing.T) {
	source := "C00000001"
	recipient := "C00000002"
	amount := int64(100)
	inbound := "15K"
	input := EvaluationInput{
		NormalizationState: "valid", RecipientCommitteeID: &recipient,
		ContributorID: &source, CleanContributorID: &source,
		AmountObservationState: "reported_value", AmountMinorUnits: &amount,
		ReceiptTypeCode: &inbound,
	}
	if got := Evaluate(input); got.Decision != DecisionIncluded || got.SourceCommitteeID != source || got.ReceiptRole != RoleRegisteredFilerContribution {
		t.Fatalf("included evaluation = %+v", got)
	}
	input.NormalizationState = "invalid"
	input.RecipientCommitteeID = nil
	input.AmountMinorUnits = nil
	if got := Evaluate(input); got.Decision != DecisionInvalidNormalization {
		t.Fatalf("first terminal decision = %s, want %s", got.Decision, DecisionInvalidNormalization)
	}
}
