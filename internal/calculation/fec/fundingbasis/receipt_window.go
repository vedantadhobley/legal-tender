package fundingbasis

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

const ReceiptWindowVersion = "legal-tender.fec.receipt-reported-window.v1"
const itemizedPeriodField = "individual_itemized_contributions_period"

type ReceiptWindowRequest struct {
	Summary                    summaryassertion.WindowComparisonRequest
	ProfilePath, ProfileSHA256 string
}

type ReceiptWindowReport struct {
	BindingIndex       int                      `json:"binding_index"`
	FileNumber         string                   `json:"file_number"`
	Groups             []ReportLineProfileGroup `json:"groups"`
	Total              Measures                 `json:"total_occurrences"`
	Line               Measures                 `json:"reviewed_nonmemo_line_occurrences"`
	ValueBasis         string                   `json:"value_basis"`
	DetailMinorUnits   *string                  `json:"detail_minor_units"`
	ReportedMinorUnits *string                  `json:"reported_minor_units"`
	DeltaMinorUnits    *string                  `json:"delta_minor_units"` // reported minus detail
	State              string                   `json:"state"`
	Blockers           []string                 `json:"blockers"`
}

type ReceiptSummaryComparison struct {
	AssertionID string `json:"assertion_id"`
	summaryassertion.WindowFieldComparison
}

type ReceiptWindowComparison struct {
	Version                           string                            `json:"version"`
	LinePolicy                        string                            `json:"line_policy"`
	Reported                          summaryassertion.WindowComparison `json:"reported"`
	Profile                           reportmetadata.Artifact           `json:"profile"`
	ProfileID                         string                            `json:"profile_id"`
	Source                            fecrelease.StagedOutput           `json:"schedule_a_source"`
	Reports                           []ReceiptWindowReport             `json:"reports"`
	Line                              Measures                          `json:"reviewed_nonmemo_line_occurrences"`
	DetailMinorUnits                  *string                           `json:"detail_minor_units"`
	ReportedMinorUnits                *string                           `json:"reported_minor_units"`
	DeltaMinorUnits                   *string                           `json:"delta_minor_units"` // reported minus detail
	State                             string                            `json:"state"`
	SummaryComparisons                []ReceiptSummaryComparison        `json:"summary_comparisons"`
	Blockers                          []string                          `json:"blockers"`
	OccurrenceComparisonReady         bool                              `json:"occurrence_comparison_ready"`
	SourceBodyRescanned               bool                              `json:"source_body_rescanned"`
	UniqueTransactionMembershipProven bool                              `json:"unique_transaction_membership_proven"`
	FinancialUseEligible              bool                              `json:"financial_use_eligible"`
	TerminalEligible                  bool                              `json:"terminal_attribution_eligible"`
}

// CompareReceiptWindow compares occurrence subtotals, not effective transactions.
// Summary/cover/metadata inputs are reverified. The pinned complete profile is
// revalidated and must belong to that exact summary release; no bulk scan runs.
func CompareReceiptWindow(ctx context.Context, request ReceiptWindowRequest) (ReceiptWindowComparison, error) {
	s, err := summaryassertion.CompareWindowV2(ctx, request.Summary)
	if err != nil {
		return ReceiptWindowComparison{}, err
	}
	p, a, err := readLineProfile(ctx, request.ProfilePath, request.ProfileSHA256, request.Summary.StorageRoot, s)
	if err != nil {
		return ReceiptWindowComparison{}, err
	}
	r, err := compareReceiptWindow(p, s)
	r.Profile = a
	return r, err
}

func compareReceiptWindow(p ReportLineProfile, s summaryassertion.WindowComparison) (ReceiptWindowComparison, error) {
	r := ReceiptWindowComparison{Version: ReceiptWindowVersion, LinePolicy: ReportLinePolicy, Reported: s, ProfileID: p.ProfileID, Source: p.Source,
		Reports: []ReceiptWindowReport{}, SummaryComparisons: []ReceiptSummaryComparison{}, State: "blocked", Blockers: []string{}}
	var field *reportperiod.WindowField
	for _, f := range s.Window.Fields {
		if f.Name == itemizedPeriodField {
			field = &f
			break
		}
	}
	if field == nil {
		return r, fmt.Errorf("missing verified itemized window field")
	}
	r.ReportedMinorUnits = field.WindowValueMinorUnits
	if !field.ReportedWindowReady || field.WindowValueMinorUnits == nil {
		r.Blockers = append(r.Blockers, "reported_itemized_window_unready")
	}
	byFile := map[string][]ReportLineProfileGroup{}
	for _, g := range p.Reports {
		if g.Key.Committee == (Cell{true, s.CommitteeID}) && g.Key.File.Present {
			byFile[g.Key.File.Value] = append(byFile[g.Key.File.Value], g)
		}
	}
	for _, j := range field.MemberBindingIndexes {
		b := s.Window.Bindings[j]
		d, err := compareReceiptReport(s.Window.Membership.Observations[*b.ObservationIndex], b, byFile[b.Document.FileNumber])
		if err != nil {
			return r, err
		}
		d.BindingIndex = j
		r.Reports = append(r.Reports, d)
		if err := r.Line.merge(d.Line); err != nil {
			return r, err
		}
		if d.State == "blocked" {
			r.Blockers = append(r.Blockers, "unqualified_report_detail")
		}
	}
	if len(r.Reports) == 0 {
		r.Blockers = append(r.Blockers, "no_bound_report_detail")
	}
	slices.Sort(r.Blockers)
	r.Blockers = slices.Compact(r.Blockers)
	if len(r.Blockers) == 0 {
		x := strconv.FormatInt(r.Line.Signed, 10)
		r.DetailMinorUnits, r.OccurrenceComparisonReady = &x, true
		var err error
		r.DeltaMinorUnits, r.State, err = receiptDifference(r.ReportedMinorUnits, &x)
		if err != nil {
			return r, err
		}
	}
	for _, a := range s.Comparisons {
		for _, f := range a.Fields {
			if f.SummaryField != "INDV_ITEM_CONTB" {
				continue
			}
			f.Blockers = slices.Clone(f.Blockers)
			f.WindowField, f.WindowMinorUnits = "schedule_a_reported_nonmemo_line_occurrences", r.DetailMinorUnits
			f.DeltaMinorUnits, f.State, f.ReportedComparisonReady = nil, "blocked", false
			if !r.OccurrenceComparisonReady {
				f.Blockers = append(f.Blockers, "receipt_occurrence_window_unready")
			}
			if len(f.Blockers) == 0 {
				var err error
				f.DeltaMinorUnits, f.State, err = receiptDifference(f.SummaryMinorUnits, f.WindowMinorUnits)
				if err != nil {
					return r, err
				}
				f.ReportedComparisonReady = true
			}
			r.SummaryComparisons = append(r.SummaryComparisons, ReceiptSummaryComparison{a.AssertionID, f})
		}
	}
	return r, nil
}

func compareReceiptReport(o reportperiod.Observation, b reportperiod.WindowBinding, groups []ReportLineProfileGroup) (ReceiptWindowReport, error) {
	r := ReceiptWindowReport{FileNumber: b.Document.FileNumber, Groups: slices.Clone(groups), State: "blocked", ValueBasis: "unqualified", Blockers: []string{}}
	if r.Groups == nil {
		r.Groups = []ReportLineProfileGroup{}
	}
	for _, f := range b.Fields {
		if f.Name == itemizedPeriodField && f.ReportedValueBound {
			r.ReportedMinorUnits = f.Metadata.MinorUnits
		}
	}
	if !b.ScopeBound || r.ReportedMinorUnits == nil {
		r.Blockers = append(r.Blockers, "report_itemized_field_unbound")
	}
	form := map[string]string{"Form 3": "F3", "Form 3X": "F3X"}[o.ReportForm]
	for _, g := range groups {
		if err := r.Total.merge(g.Measures); err != nil {
			return r, err
		}
		if form == "" || g.Key.Form != (Cell{true, form}) || g.Key.ReportType != (Cell{true, o.ReportType}) || g.Key.ReportYear != (Cell{true, strconv.Itoa(o.ReportYear)}) {
			r.Blockers = append(r.Blockers, "profile_report_scope_mismatch")
		}
		switch g.Key.Disposition {
		case "reviewed_nonmemo_line":
			if err := r.Line.merge(g.Measures); err != nil {
				return r, err
			}
		case "unresolved_memo_code", "unresolved_line_amount":
			r.Blockers = append(r.Blockers, g.Key.Disposition)
		}
	}
	if r.Line.Rows > 0 {
		r.ValueBasis = "profile_occurrence_subtotal"
	} else if len(groups) == 0 && r.ReportedMinorUnits != nil && *r.ReportedMinorUnits == "0" && originalHasNoScheduleA(b) {
		// A full original with no SA records and an explicit bound zero cover
		// corroborates this empty profile case. Absence by itself never does.
		r.ValueBasis = "empty_profile_with_original_and_explicit_reported_zero"
	} else {
		r.Blockers = append(r.Blockers, "no_qualified_detail_value")
	}
	slices.Sort(r.Blockers)
	r.Blockers = slices.Compact(r.Blockers)
	if len(r.Blockers) == 0 {
		v := strconv.FormatInt(r.Line.Signed, 10)
		r.DetailMinorUnits = &v
		var err error
		r.DeltaMinorUnits, r.State, err = receiptDifference(r.ReportedMinorUnits, &v)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}

func originalHasNoScheduleA(b reportperiod.WindowBinding) bool {
	d := b.Document
	if !b.ScopeBound || d.CaptureExtent != "complete_response" || d.Representation != "electronic_8.4" || d.Disposition != "electronic_cover_parsed" || len(d.Records) < 2 {
		return false
	}
	for _, r := range d.Records {
		if !r.Complete {
			return false
		}
		tag := bytes.SplitN(r.Raw, []byte{0x1c}, 2)[0]
		if strings.HasPrefix(string(tag), "SA") {
			return false
		}
	}
	return true
}

func receiptDifference(reported, detail *string) (*string, string, error) {
	if reported == nil || detail == nil {
		return nil, "blocked", fmt.Errorf("missing verified receipt comparison operand")
	}
	x, ok := new(big.Int).SetString(*reported, 10)
	y, valid := new(big.Int).SetString(*detail, 10)
	if !ok || !valid {
		return nil, "blocked", fmt.Errorf("invalid verified receipt comparison operand")
	}
	v := x.Sub(x, y).String()
	state := "different"
	if v == "0" {
		state = "equal"
	}
	return &v, state, nil
}
