package independentexpenditures

import (
	"math/big"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const (
	DecisionExcludedMemo     = "excluded_memo"
	DecisionUnresolvedAmount = "unresolved_amount"
	DecisionIncluded         = "included"
)

// FactEvaluation is the reusable, versioned membership decision for one
// Schedule E fact. Downstream identity calculations consume this result so
// they cannot silently implement a different effective-record predicate.
type FactEvaluation struct {
	Decision     string
	Amount       *big.Int
	AmountReason string
	RouteReasons []string
}

// EvaluateFact applies the accepted effective-independent-expenditure
// membership and route policy without grouping or mutating the source fact.
func EvaluateFact(fact fecoccurrence.ScheduleEFact) FactEvaluation {
	memoX := fact.TypedFields.Expenditure.MemoCode != nil && *fact.TypedFields.Expenditure.MemoCode == "X"
	amount, amountReason, amountOK := factAmount(fact)
	if memoX {
		return FactEvaluation{Decision: DecisionExcludedMemo, Amount: amount, AmountReason: amountReason}
	}
	if !amountOK {
		return FactEvaluation{Decision: DecisionUnresolvedAmount, AmountReason: amountReason}
	}
	return FactEvaluation{Decision: DecisionIncluded, Amount: amount, RouteReasons: routeReasons(fact)}
}

func membershipPredicate() MembershipPredicate {
	return MembershipPredicate{
		Version:                PredicateVersion,
		InputFactSchemaVersion: fecoccurrence.ScheduleEFactSchemaVersion,
		MembershipIdentity:     "Schedule E fact-set ID plus fact ID",
		RequiredFields: []string{
			"fact_id",
			"typed_fields.expenditure.memo_code",
			"typed_fields.expenditure.amount.observation_state",
			"typed_fields.expenditure.amount.reported_minor_units",
			"typed_fields.spender.committee_id",
			"typed_fields.candidate.candidate_id",
			"typed_fields.candidate.support_oppose_code",
		},
		DecisionOrder: []PredicateRule{
			{State: DecisionExcludedMemo, All: []string{"memo_code == X"}},
			{State: DecisionUnresolvedAmount, All: []string{"memo_code != X", "reported_minor_units is absent or invalid"}},
			{State: DecisionIncluded, All: []string{"memo_code != X", "reported_minor_units is an exact signed integer"}},
		},
		RouteRequirements: []string{
			"spender committee ID present",
			"candidate ID present",
			"support/oppose code is S or O",
		},
		ExceptionalStates: []string{"unresolved_amount", "included_unattributed"},
	}
}
