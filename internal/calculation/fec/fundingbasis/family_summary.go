package fundingbasis

import (
	"context"
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const FamilySummaryVersion = "legal-tender.fec.receipt-family-summary.v1"

type FamilySummaryField struct {
	FamilyIndex int                                    `json:"family_index"`
	Summary     summaryassertion.FieldValue            `json:"summary"`
	Reported    summaryassertion.WindowFieldComparison `json:"versus_reported_window"`
	Comparison  summaryassertion.WindowFieldComparison `json:"versus_qualified_detail_window"`
}

type FamilySummaryAssertion struct {
	AssertionID          string               `json:"assertion_id"`
	RepresentativeFactID string               `json:"representative_fact_id"`
	Fields               []FamilySummaryField `json:"fields"`
}

type FamilySummaryComparison struct {
	Version                    string                   `json:"version"`
	Window                     FamilyWindowComparison   `json:"family_window"`
	Assertions                 []FamilySummaryAssertion `json:"assertions"`
	Blockers                   []string                 `json:"blockers"`
	SourceAlignment            string                   `json:"source_alignment"`
	SameReportMembershipProven bool                     `json:"same_report_membership_proven"`
	FinancialUseEligible       bool                     `json:"financial_use_eligible"`
	TerminalEligible           bool                     `json:"terminal_attribution_eligible"`
}

// CompareReceiptFamilySummary is additive and read-only. Raw summary fields are
// reverified against the nested comparison's exact immutable input. No saved
// readiness decisions, summary variants, field names or member sets are accepted.
func CompareReceiptFamilySummary(ctx context.Context, request ReceiptWindowRequest) (FamilySummaryComparison, error) {
	w, err := CompareReceiptFamilyWindow(ctx, request)
	if err != nil {
		return FamilySummaryComparison{}, err
	}
	fields, err := summaryassertion.ReadAssertionFields(ctx, request.Summary.StorageRoot, request.Summary.SummaryManifest, w.Reviewed.Compared.Reported, []string{"PTY_CMTE_CONTB", "OTH_CMTE_CONTB", "TRANF_FROM_OTHER_AUTH_CMTE", "CAND_LOAN", "OTH_LOANS"})
	if err != nil {
		return FamilySummaryComparison{}, err
	}
	return compareReceiptFamilySummary(ctx, w, fields)
}

// Exact form/line/field mappings, tied to the reviewed source contract by tests.
// Neither TTL_LOANS nor a loan balance can substitute for these reported fields.
func familySummaryField(spec reportscope.ReceiptFamilyField) string {
	for _, accepted := range reportscope.ReceiptFamilyFields(spec.Form) {
		if accepted != spec {
			continue
		}
		switch spec.ID {
		case "party_contributions":
			return "PTY_CMTE_CONTB"
		case "other_committee_contributions":
			return "OTH_CMTE_CONTB"
		case "authorized_transfers", "affiliated_or_party_transfers":
			return "TRANF_FROM_OTHER_AUTH_CMTE"
		case "candidate_made_or_guaranteed_loans":
			return "CAND_LOAN"
		case "other_loans", "loans_received":
			return "OTH_LOANS"
		}
	}
	return ""
}

func compareReceiptFamilySummary(ctx context.Context, w FamilyWindowComparison, fields []summaryassertion.AssertionFields) (FamilySummaryComparison, error) {
	r := FamilySummaryComparison{Version: FamilySummaryVersion, Window: w, Assertions: []FamilySummaryAssertion{}, Blockers: []string{}, SourceAlignment: "independent_snapshots"}
	s := w.Reviewed.Compared.Reported
	if s.Summary == nil {
		if len(fields) != 0 {
			return r, fmt.Errorf("unexpected summary fields without committee")
		}
		r.Blockers = append(r.Blockers, "summary_committee_absent")
		return r, ctx.Err()
	}
	if len(fields) != len(s.Summary.Assertions) {
		return r, fmt.Errorf("summary assertion field conservation failed")
	}
	for ai, a := range s.Summary.Assertions {
		if fields[ai].AssertionID != a.ID || fields[ai].RepresentativeFactID != a.RepresentativeFactID {
			return r, fmt.Errorf("summary assertion field identity mismatch")
		}
		assertion := FamilySummaryAssertion{AssertionID: a.ID, RepresentativeFactID: a.RepresentativeFactID, Fields: []FamilySummaryField{}}
		for fi, family := range w.Families {
			if err := ctx.Err(); err != nil {
				return r, err
			}
			name := familySummaryField(family.Field)
			var operand *summaryassertion.FieldValue
			for _, f := range fields[ai].Fields {
				if f.Field == name && name != "" {
					if operand != nil {
						return r, fmt.Errorf("repeated verified family summary field")
					}
					operand = &f
				}
			}
			if operand == nil {
				return r, fmt.Errorf("unsupported or missing verified family summary field")
			}
			pair := FamilySummaryField{FamilyIndex: fi, Summary: *operand}
			for _, detail := range []bool{false, true} {
				field := reportperiod.WindowField{Name: family.Field.MetadataField, ReportedWindowReady: family.ReportedWindowReady, WindowValueMinorUnits: family.ReportedWindowMinorUnits, MemberBindingIndexes: []int{}}
				if detail {
					field.ReportedWindowReady, field.WindowValueMinorUnits = family.ComparisonWindowReady, family.ComparisonWindowMinorUnits
				}
				for _, member := range family.Members {
					if member.ReportIndex == nil {
						continue // missing member already blocks its full family window
					}
					ri := *member.ReportIndex
					if ri < 0 || ri >= len(w.Reviewed.Compared.Reports) {
						return r, fmt.Errorf("invalid verified family report index")
					}
					field.MemberBindingIndexes = append(field.MemberBindingIndexes, w.Reviewed.Compared.Reports[ri].BindingIndex)
				}
				// The family report binding order was verified against the nested
				// reported window by sameFamilyReportEvidence. Use that unchanged
				// scope and its original observation references.
				value, err := summaryassertion.CompareReportedFlowField(a, s.Summary.ConflictFields, s.Window, name, operand.Value, field)
				if err != nil {
					return r, err
				}
				if detail {
					pair.Comparison = value
				} else {
					pair.Reported = value
				}
			}
			assertion.Fields = append(assertion.Fields, pair)
		}
		r.Assertions = append(r.Assertions, assertion)
	}
	return r, ctx.Err()
}
