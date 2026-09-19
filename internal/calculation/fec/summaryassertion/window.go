package summaryassertion

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

const WindowComparisonVersion = "legal-tender.fec.summary-report-window.v1"

type WindowComparisonRequest struct {
	StorageRoot, SummaryManifest string
	Window                       reportperiod.WindowRequest
}

type WindowComparison struct {
	Version                     string                      `json:"version"`
	Cycle                       string                      `json:"cycle"`
	CommitteeID                 string                      `json:"committee_id"`
	SummaryInput                Input                       `json:"summary_input"`
	SummaryCalculationID        string                      `json:"summary_calculation_id"`
	SummaryCounts               Counts                      `json:"summary_counts"`
	Summary                     *Committee                  `json:"summary"`
	Window                      reportperiod.WindowReview   `json:"window"`
	Comparisons                 []AssertionWindowComparison `json:"comparisons"`
	Blockers                    []string                    `json:"blockers"`
	SourceAlignment             string                      `json:"source_alignment"`
	SameReportMembershipProven  bool                        `json:"same_report_membership_proven"`
	FinancialUseEligible        bool                        `json:"financial_use_eligible"`
	TerminalAttributionEligible bool                        `json:"terminal_attribution_eligible"`
}

type AssertionWindowComparison struct {
	AssertionID string                  `json:"assertion_id"`
	Fields      []WindowFieldComparison `json:"fields"`
	CycleSpan   *SummaryCycleSpan       `json:"cycle_span,omitempty"` // v2 only; not financial coverage
}

type WindowFieldComparison struct {
	SummaryField            string   `json:"summary_field"`
	WindowField             string   `json:"window_field"`
	ScopeBasis              string   `json:"scope_basis"`
	SummaryMinorUnits       *string  `json:"summary_minor_units"`
	WindowMinorUnits        *string  `json:"window_minor_units"`
	DeltaMinorUnits         *string  `json:"delta_minor_units"` // summary minus window; never a correction
	State                   string   `json:"state"`             // blocked, equal, different
	ReportedComparisonReady bool     `json:"reported_comparison_ready"`
	Blockers                []string `json:"blockers"`
}

type windowRule struct{ summary, window, equation, scope string }

var windowRules = []windowRule{
	{"INDV_ITEM_CONTB", "individual_itemized_contributions_period", "individual", "cycle_start_through_summary_end"},
	{"INDV_UNITEM_CONTB", "individual_unitemized_contributions_period", "individual", "cycle_start_through_summary_end"},
	{"INDV_CONTB", "total_individual_contributions_period", "individual", "cycle_start_through_summary_end"},
	{"TTL_RECEIPTS", "total_receipts_period", "cash", "cycle_start_through_summary_end"},
	{"TTL_DISB", "total_disbursements_period", "cash", "cycle_start_through_summary_end"},
	{"COH_BOP", "cash_on_hand_beginning_period", "cash", "cycle_start_stock"},
	{"COH_COP", "cash_on_hand_end_period", "cash", "summary_end_stock"},
}

// CompareWindow re-verifies both inputs. Neither saved results nor a caller's
// committee/cycle/readiness flags can establish compatibility. This is a bounded
// diagnostic, not a recurring per-committee scan or synchronized reconciliation.
func CompareWindow(ctx context.Context, request WindowComparisonRequest) (WindowComparison, error) {
	return compareWindowRequest(ctx, request, WindowComparisonVersion)
}

func compareWindowRequest(ctx context.Context, request WindowComparisonRequest, version string) (WindowComparison, error) {
	w, err := reportperiod.ReviewWindow(ctx, request.Window)
	if err != nil {
		return WindowComparison{}, err
	}
	s, err := Run(ctx, request.StorageRoot, request.SummaryManifest, strconv.Itoa(w.Membership.Evidence.Query.Cycle))
	if err != nil {
		return WindowComparison{}, err
	}
	return compareWindowVersion(s, w, version)
}

func compareWindow(s Result, w reportperiod.WindowReview) (WindowComparison, error) {
	return compareWindowVersion(s, w, WindowComparisonVersion)
}

func compareWindowVersion(s Result, w reportperiod.WindowReview, version string) (WindowComparison, error) {
	if version != WindowComparisonVersion && version != WindowComparisonV2 {
		return WindowComparison{}, fmt.Errorf("unsupported summary/window comparison version")
	}
	q := w.Membership.Evidence.Query
	if s.Cycle != strconv.Itoa(q.Cycle) {
		return WindowComparison{}, fmt.Errorf("summary/report cycle mismatch")
	}
	r := WindowComparison{Version: version, Cycle: s.Cycle, CommitteeID: q.CommitteeID,
		SummaryInput: s.Input, SummaryCalculationID: s.CalculationID, SummaryCounts: s.Counts,
		Window: w, Comparisons: []AssertionWindowComparison{}, Blockers: []string{}, SourceAlignment: "independent_snapshots"}
	for i := range s.Committees {
		if s.Committees[i].CommitteeID == q.CommitteeID {
			r.Summary = &s.Committees[i]
			break
		}
	}
	if r.Summary == nil {
		r.Blockers = append(r.Blockers, "summary_committee_absent")
		return r, nil
	}
	types := metadataCommitteeTypes(w)
	cycleStart := fmt.Sprintf("%04d-01-01", q.Cycle-1)
	for _, a := range r.Summary.Assertions {
		v := AssertionWindowComparison{AssertionID: a.ID, Fields: []WindowFieldComparison{}}
		if version == WindowComparisonV2 {
			v.CycleSpan = summaryCycleSpan(a, r.Summary.ConflictFields, q.Cycle)
		}
		for _, rule := range windowRules {
			if version == WindowComparisonV2 && rule.scope == "cycle_start_through_summary_end" {
				rule.scope = "reported_summary_coverage"
			}
			f, err := compareWindowField(a, r.Summary.ConflictFields, w, types, cycleStart, s.Cycle+"-12-31", rule)
			if err != nil {
				return WindowComparison{}, err
			}
			v.Fields = append(v.Fields, f)
		}
		r.Comparisons = append(r.Comparisons, v)
	}
	return r, nil
}

func metadataCommitteeTypes(w reportperiod.WindowReview) []string {
	types := []string{}
	for _, p := range w.Membership.Evidence.Pages {
		for _, record := range p.Records {
			var raw struct {
				Type string `json:"committee_type"`
			}
			// Missing/null/type-invalid values cannot qualify a mapping.
			if err := json.Unmarshal(record.Raw, &raw); err != nil {
				raw.Type = ""
			}
			types = append(types, raw.Type)
		}
	}
	return types
}

func compareWindowField(a Assertion, conflicts []string, w reportperiod.WindowReview, types []string, cycleStart, cycleEnd string, rule windowRule) (WindowFieldComparison, error) {
	for _, o := range a.Equations[rule.equation].Operands {
		if o.Field == rule.summary {
			return compareWindowValue(a, conflicts, w, types, cycleStart, cycleEnd, rule, o.Value)
		}
	}
	return WindowFieldComparison{}, fmt.Errorf("missing verified summary field %s", rule.summary)
}

// CompareReportedFlowField applies the existing v2 scope checks to a separately
// verified scalar. Callers own the reviewed form-specific field mapping; this
// helper neither selects a summary assertion nor constructs window membership.
func CompareReportedFlowField(a Assertion, conflicts []string, w reportperiod.WindowReview, summaryField string, value committeesummary.MoneyValue, field reportperiod.WindowField) (WindowFieldComparison, error) {
	w.Fields = []reportperiod.WindowField{field}
	cycle := w.Membership.Evidence.Query.Cycle
	return compareWindowValue(a, conflicts, w, metadataCommitteeTypes(w), fmt.Sprintf("%04d-01-01", cycle-1), fmt.Sprintf("%04d-12-31", cycle), windowRule{summary: summaryField, window: field.Name, scope: "reported_summary_coverage"}, value)
}

func compareWindowValue(a Assertion, conflicts []string, w reportperiod.WindowReview, types []string, cycleStart, cycleEnd string, rule windowRule, value committeesummary.MoneyValue) (WindowFieldComparison, error) {
	f := WindowFieldComparison{SummaryField: rule.summary, WindowField: rule.window, ScopeBasis: rule.scope, State: "blocked", Blockers: []string{}}
	for _, name := range []string{"CMTE_TP", "CMTE_DSGN", "CVG_START_DT", "CVG_END_DT"} {
		if slices.Contains(conflicts, name) {
			f.Blockers = append(f.Blockers, "conflicting_summary_scope")
			break
		}
	}
	if !slices.Contains([]string{"A", "J", "P", "U", "B", "D"}, a.Designation) {
		f.Blockers = append(f.Blockers, "unqualified_summary_designation")
	}
	start, end := "", ""
	if a.CoverageStart.State != committeesummary.Valid || a.CoverageEnd.State != committeesummary.Valid || a.CoverageStart.Value == nil || a.CoverageEnd.Value == nil {
		f.Blockers = append(f.Blockers, "summary_coverage_unavailable")
	} else {
		start, end = *a.CoverageStart.Value, *a.CoverageEnd.Value
		if start > end || start < cycleStart || end > cycleEnd {
			f.Blockers = append(f.Blockers, "summary_coverage_outside_ordered_cycle")
		}
	}
	window := w.Membership.Window
	switch rule.scope {
	case "reported_summary_coverage":
		if window.Start != start {
			f.Blockers = append(f.Blockers, "summary_start_mismatch")
		}
		if window.End != end {
			f.Blockers = append(f.Blockers, "summary_end_mismatch")
		}
	case "cycle_start_through_summary_end":
		if window.Start != cycleStart || start != cycleStart {
			f.Blockers = append(f.Blockers, "cycle_prefix_unqualified")
		}
		if window.End != end {
			f.Blockers = append(f.Blockers, "summary_end_mismatch")
		}
	case "cycle_start_stock":
		if window.Start != cycleStart || start != cycleStart {
			f.Blockers = append(f.Blockers, "cycle_opening_boundary_unqualified")
		}
	case "summary_end_stock":
		if window.End != end {
			f.Blockers = append(f.Blockers, "summary_end_mismatch")
		}
	}
	f.SummaryMinorUnits = value.MinorUnits
	if value.State != committeesummary.Valid || value.MinorUnits == nil {
		f.Blockers = append(f.Blockers, "summary_value_"+value.State)
	}
	if slices.Contains(conflicts, rule.summary) {
		f.Blockers = append(f.Blockers, "conflicting_reported_field")
	}
	var field *reportperiod.WindowField
	for _, value := range w.Fields {
		if value.Name == rule.window {
			field = &value
			break
		}
	}
	if field == nil {
		return f, fmt.Errorf("missing verified window field %s", rule.window)
	}
	f.WindowMinorUnits = field.WindowValueMinorUnits
	if !field.ReportedWindowReady || field.WindowValueMinorUnits == nil {
		f.Blockers = append(f.Blockers, "reported_window_field_unready")
	}
	form := ""
	switch a.CommitteeType {
	case "H", "S":
		form = "Form 3"
	case "N", "Q", "O", "U", "V", "W", "X", "Y":
		form = "Form 3X"
	}
	if form == "" {
		f.Blockers = append(f.Blockers, "unqualified_summary_committee_type")
	}
	for _, i := range field.MemberBindingIndexes {
		if i < 0 || i >= len(w.Bindings) {
			return f, fmt.Errorf("invalid verified binding index")
		}
		b := w.Bindings[i]
		if b.ObservationIndex == nil {
			return f, fmt.Errorf("bound field missing observation")
		}
		o := *b.ObservationIndex
		if o < 0 || o >= len(types) || o >= len(w.Membership.Observations) || types[o] != a.CommitteeType || w.Membership.Observations[o].ReportForm != form {
			f.Blockers = append(f.Blockers, "report_summary_type_or_form_mismatch")
			break
		}
	}
	// Arithmetic contradictions remain in the two original evidence objects.
	// They do not erase independently scoped reported scalar comparisons.
	if len(f.Blockers) == 0 {
		x, ok := new(big.Int).SetString(*f.SummaryMinorUnits, 10)
		y, valid := new(big.Int).SetString(*f.WindowMinorUnits, 10)
		if !ok || !valid {
			return f, fmt.Errorf("invalid verified comparison operand")
		}
		delta := x.Sub(x, y).String()
		f.DeltaMinorUnits, f.ReportedComparisonReady, f.State = &delta, true, "different"
		if delta == "0" {
			f.State = "equal"
		}
	}
	return f, nil
}
