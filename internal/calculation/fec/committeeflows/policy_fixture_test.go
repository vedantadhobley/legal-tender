package committeeflows

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

type policyFixture struct {
	Facts []struct {
		FactID             string  `json:"fact_id"`
		State              string  `json:"state"`
		Recipient          *string `json:"recipient"`
		ContributorID      *string `json:"contributor_id"`
		CleanContributorID *string `json:"clean_contributor_id"`
		ReceiptType        *string `json:"receipt_type"`
		MemoSubtotal       bool    `json:"memo_subtotal"`
		AmountMinorUnits   *string `json:"amount_minor_units"`
	} `json:"facts"`
	Expected struct {
		DecisionCounts map[string]uint64 `json:"decision_counts"`
		Amounts        map[string]string `json:"amounts"`
		Results        []struct {
			Source           string `json:"source"`
			Recipient        string `json:"recipient"`
			ReceiptRole      string `json:"receipt_role"`
			AmountMinorUnits string `json:"amount_minor_units"`
			ReceiptCount     uint64 `json:"receipt_count"`
		} `json:"results"`
	} `json:"expected"`
}

type fixtureGroupKey struct{ source, recipient, role string }
type fixtureGroupValue struct {
	amount int64
	count  uint64
}

func TestAcceptedPolicyFixture(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "contracts", "calculations", "fec", "receiver-reported-committee-flows", "v1", "fixtures", "policy-cases.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture policyFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}

	counts := map[string]uint64{"source_facts": uint64(len(fixture.Facts))}
	groups := make(map[fixtureGroupKey]fixtureGroupValue)
	var included, excluded, unresolved, known int64
	for _, fact := range fixture.Facts {
		amount, amountState := fixtureAmount(t, fact.AmountMinorUnits)
		if amount != nil {
			known += *amount
		}
		evaluation := Evaluate(EvaluationInput{
			NormalizationState: fact.State, RecipientCommitteeID: fact.Recipient,
			ContributorID: fact.ContributorID, CleanContributorID: fact.CleanContributorID,
			MemoedSubtotal: fact.MemoSubtotal, AmountObservationState: amountState,
			AmountMinorUnits: amount, ReceiptTypeCode: fact.ReceiptType,
		})
		counts[evaluation.Decision]++
		switch evaluation.Decision {
		case DecisionIncluded:
			included += *amount
			key := fixtureGroupKey{evaluation.SourceCommitteeID, *fact.Recipient, evaluation.ReceiptRole}
			value := groups[key]
			value.amount += *amount
			value.count++
			groups[key] = value
		case DecisionExcludedNoSource, DecisionExcludedMemo, DecisionExcludedOutbound,
			DecisionExcludedSemanticMemo, DecisionExcludedEarmarked, DecisionExcludedNoncommittee:
			if amount != nil {
				excluded += *amount
			}
		default:
			if amount != nil {
				unresolved += *amount
			}
		}
	}
	if !reflect.DeepEqual(counts, fixture.Expected.DecisionCounts) {
		t.Fatalf("decision counts = %#v; want %#v", counts, fixture.Expected.DecisionCounts)
	}
	for name, got := range map[string]int64{
		"included_minor_units": included, "excluded_minor_units": excluded,
		"unresolved_minor_units": unresolved, "known_source_minor_units": known,
	} {
		if strconv.FormatInt(got, 10) != fixture.Expected.Amounts[name] {
			t.Fatalf("%s = %d; want %s", name, got, fixture.Expected.Amounts[name])
		}
	}
	if len(groups) != len(fixture.Expected.Results) {
		t.Fatalf("result groups = %d; want %d", len(groups), len(fixture.Expected.Results))
	}
	for _, expected := range fixture.Expected.Results {
		key := fixtureGroupKey{expected.Source, expected.Recipient, expected.ReceiptRole}
		got := groups[key]
		if strconv.FormatInt(got.amount, 10) != expected.AmountMinorUnits || got.count != expected.ReceiptCount {
			t.Fatalf("group %+v = %+v; want amount=%s count=%d", key, got, expected.AmountMinorUnits, expected.ReceiptCount)
		}
	}
}

func fixtureAmount(t *testing.T, value *string) (*int64, string) {
	t.Helper()
	if value == nil {
		return nil, "source_null"
	}
	amount, err := strconv.ParseInt(*value, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return &amount, "reported_value"
}
