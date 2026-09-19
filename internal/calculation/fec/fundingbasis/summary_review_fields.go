package fundingbasis

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

// This focused consumer covers nine fields already present in verified summary
// diagnostics. Other preserved source fields are not assessed by this policy.
type summaryFieldRule struct {
	Field        string `json:"field"`
	Equation     string `json:"equation"`
	Relationship string `json:"receipt_relationship"`
	Blocker      string `json:"blocker"`
}

var summaryReviewFields = []summaryFieldRule{
	{"INDV_ITEM_CONTB", "individual", "candidate_individual_predicate", "individual_predicate_form_scope_unverified"},
	{"INDV_UNITEM_CONTB", "individual", "no_itemized_inventory_counterpart", "unitemized_not_observed_in_itemized_inventory"},
	{"INDV_CONTB", "individual", "incomplete_individual_population", "unitemized_component_not_in_inventory"},
	{"TTL_RECEIPTS", "cash", "unverified_receipt_family_population", "complete_receipt_family_scope_unverified"},
	{"TTL_FED_RECEIPTS", "cash_federal_columns", "unverified_federal_receipt_population", "federal_receipt_family_scope_unverified"},
	{"COH_BOP", "cash", "no_receipt_inventory_balance", "cash_balance_not_observed_in_receipt_inventory"},
	{"COH_COP", "cash", "no_receipt_inventory_balance", "cash_balance_not_observed_in_receipt_inventory"},
	{"TTL_DISB", "cash", "no_receipt_inventory_disbursements", "disbursements_not_observed_in_receipt_inventory"},
	{"TTL_FED_DISB", "cash_federal_columns", "no_receipt_inventory_disbursements", "disbursements_not_observed_in_receipt_inventory"},
}

func reviewVariant(cycle string, conflicts []string, a summaryassertion.Assertion) (SummaryVariantReview, error) {
	v := SummaryVariantReview{
		AssertionID: a.ID, RepresentativeFact: a.RepresentativeFactID, CommitteeType: a.CommitteeType, Designation: a.Designation,
		Members: a.Members, CoverageStart: a.CoverageStart, CoverageEnd: a.CoverageEnd,
		ScopeBlockers: []string{}, Fields: []SummaryFieldReview{}, Diagnostics: map[string]SummaryDiagnostic{},
	}
	for name, eq := range a.Equations {
		v.Diagnostics[name] = SummaryDiagnostic{eq.State, eq.Delta}
	}
	if hasAny(conflicts, "CMTE_TP", "CMTE_DSGN", "CVG_START_DT", "CVG_END_DT") {
		v.ScopeBlockers = append(v.ScopeBlockers, "conflicting_summary_scope")
	}
	if a.CoverageStart.State != committeesummary.Valid || a.CoverageEnd.State != committeesummary.Valid {
		v.ScopeBlockers = append(v.ScopeBlockers, "summary_coverage_unavailable")
	} else {
		year, err := strconv.Atoi(cycle)
		if err != nil || a.CoverageStart.Value == nil || a.CoverageEnd.Value == nil {
			return SummaryVariantReview{}, fmt.Errorf("invalid verified summary coverage")
		}
		start, end := *a.CoverageStart.Value, *a.CoverageEnd.Value
		if start > end {
			v.ScopeBlockers = append(v.ScopeBlockers, "summary_coverage_reversed")
		}
		if start < fmt.Sprintf("%04d-01-01", year-1) || end > cycle+"-12-31" || end < fmt.Sprintf("%04d-01-01", year-1) || start > cycle+"-12-31" {
			v.ScopeBlockers = append(v.ScopeBlockers, "summary_coverage_outside_source_cycle")
		}
	}
	for _, rule := range summaryReviewFields {
		eq, ok := a.Equations[rule.Equation]
		if !ok {
			return SummaryVariantReview{}, fmt.Errorf("missing verified summary diagnostic %s", rule.Equation)
		}
		var operand *summaryassertion.Operand
		for i := range eq.Operands {
			if eq.Operands[i].Field == rule.Field {
				operand = &eq.Operands[i]
			}
		}
		if operand == nil {
			return SummaryVariantReview{}, fmt.Errorf("missing verified summary field %s", rule.Field)
		}
		f := SummaryFieldReview{Field: rule.Field, Raw: operand.Raw, Value: operand.Value, Relationship: rule.Relationship, Blockers: []string{rule.Blocker}}
		if operand.Value.State != committeesummary.Valid {
			f.Blockers = append(f.Blockers, "summary_value_"+operand.Value.State)
		}
		if slices.Contains(conflicts, rule.Field) {
			f.Blockers = append(f.Blockers, "conflicting_reported_field")
		}
		// Federal-column sensitivity is not an accepted cash identity. Its state
		// remains visible but neither fails nor rescues a financial comparison.
		if rule.Equation != "cash_federal_columns" && eq.State != "equal" {
			f.Blockers = append(f.Blockers, "summary_"+rule.Equation+"_arithmetic_"+eq.State)
		}
		v.Fields = append(v.Fields, f)
	}
	return v, nil
}
