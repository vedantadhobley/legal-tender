package receipts

import (
	"strconv"
	"testing"
)

func TestTypedMembershipMatchesExistingReceiptDecision(t *testing.T) {
	yes, no := true, false
	positive, negative, zero := int64(123), int64(-1), int64(0)
	for _, individual := range []*bool{nil, &yes, &no} {
		for _, memo := range []bool{false, true} {
			for _, state := range []string{"reported_value", "source_null", "unknown"} {
				for _, amount := range []*int64{nil, &positive, &negative, &zero} {
					var text *string
					if amount != nil {
						s := strconv.FormatInt(*amount, 10)
						text = &s
					}
					old := decideReceipt(receiptInput{PublisherClassedIndividual: individual, MemoedSubtotal: memo, AmountObservationState: state, AmountMinorUnits: text})
					if got := ItemizedIndividualDecision(individual, memo, state, amount); got != old.State {
						t.Fatalf("got %s want %s", got, old.State)
					}
				}
			}
		}
	}
}
