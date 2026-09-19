package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// This is a dependency-isolation regression for the shipped readiness review,
// not an implementation of the later qualified-summary financial consumer.
func TestSummaryArithmeticIssuesStayWithinTheirFieldFamily(t *testing.T) {
	for _, equation := range []string{"cash", "individual"} {
		t.Run(equation, func(t *testing.T) {
			r, summary := summaryReviewFixture(t)
			before, err := r.reviewSummary(context.Background(), summary, "C00000001")
			if err != nil {
				t.Fatal(err)
			}
			a := &summary.Committees[0].Assertions[0]
			eq := a.Equations[equation]
			delta, raw := "100", "1.00"
			eq.State, eq.Delta = "different", &delta
			eq.Operands[0].Raw, eq.Operands[0].Value.MinorUnits = raw, &delta
			a.Equations[equation] = eq
			inputBefore, _ := json.Marshal(summary)
			inventoryBefore, _ := json.Marshal(r.result)
			after, err := r.reviewSummary(context.Background(), summary, "C00000001")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before.Receipts, after.Receipts) || !reflect.DeepEqual(before.ReceiptInput, after.ReceiptInput) || !reflect.DeepEqual(before.Assertions[0].Members, after.Assertions[0].Members) {
				t.Fatal("summary issue changed receipt evidence or summary membership")
			}
			if after.ReviewID == before.ReviewID || after.ComparisonReady || after.FundingEligible || after.TerminalEligible {
				t.Fatal("changed evidence lost identity or promoted financial use")
			}
			for i, field := range after.Assertions[0].Fields {
				rule := summaryReviewFields[i]
				if rule.Equation != equation && !reflect.DeepEqual(field, before.Assertions[0].Fields[i]) {
					t.Fatal("unrelated reported field changed", field.Field)
				}
				if rule.Equation == equation {
					want := "summary_" + equation + "_arithmetic_different"
					found := false
					for _, reason := range field.Blockers {
						found = found || reason == want
					}
					if !found {
						t.Fatal("affected arithmetic issue not retained", field.Field)
					}
				}
				if field.Field == eq.Operands[0].Field && (field.Raw != raw || field.Value.MinorUnits == nil || *field.Value.MinorUnits != delta) {
					t.Fatal("reported scalar corrected or discarded")
				}
			}
			inputAfter, _ := json.Marshal(summary)
			inventoryAfter, _ := json.Marshal(r.result)
			if !bytes.Equal(inputBefore, inputAfter) || !bytes.Equal(inventoryBefore, inventoryAfter) {
				t.Fatal("review mutated an input")
			}
		})
	}
}

func TestSummaryFieldConflictDoesNotBecomeAGlobalReceiptBlock(t *testing.T) {
	r, summary := summaryReviewFixture(t)
	baseline, err := r.reviewSummary(context.Background(), summary, "C00000001")
	if err != nil {
		t.Fatal(err)
	}
	summary.Committees[0].State = "conflicting_assertions"
	summary.Committees[0].ConflictFields = []string{"TTL_RECEIPTS", "CMTE_NM"}
	// The synthetic second variant changes only contact metadata and one field.
	second := summary.Committees[0].Assertions[0]
	second.ID = strings.Repeat("c", 64)
	second.Equations = nil
	raw, _ := json.Marshal(summary.Committees[0].Assertions[0].Equations)
	if err := json.Unmarshal(raw, &second.Equations); err != nil {
		t.Fatal(err)
	}
	eq := second.Equations["cash"]
	delta := "100"
	for i := range eq.Operands {
		if eq.Operands[i].Field == "TTL_RECEIPTS" {
			eq.Operands[i].Raw = "1.00"
			eq.Operands[i].Value.MinorUnits = &delta
		}
	}
	eq.State, eq.Delta = "different", &delta
	second.Equations["cash"] = eq
	summary.Committees[0].Assertions = append(summary.Committees[0].Assertions, second)
	result, err := r.reviewSummary(context.Background(), summary, "C00000001")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assertions) != 2 || !reflect.DeepEqual(result.Receipts, baseline.Receipts) || !reflect.DeepEqual(result.ComparisonBlocks, baseline.ComparisonBlocks) {
		t.Fatal("conflict collapsed variants or changed independent receipt population")
	}
	for _, variant := range result.Assertions {
		if len(variant.ScopeBlockers) != 0 {
			t.Fatal("contact or scalar conflict became a reporting-scope conflict")
		}
		for _, field := range variant.Fields {
			for _, reason := range field.Blockers {
				if reason == "conflicting_reported_field" && field.Field != "TTL_RECEIPTS" {
					t.Fatal("field conflict propagated outside its scalar")
				}
			}
		}
	}
}
