package fundingbasis

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const ReceiptFamiliesVersion = "legal-tender.fec.receipt-family-comparison.v1"
const ReceiptFamiliesVersionV2 = "legal-tender.fec.receipt-family-comparison.v2"

type ReceiptFamilyComparison struct {
	Field              reportscope.ReceiptFamilyField `json:"field"`
	Binding            *reportperiod.FieldBinding     `json:"binding"`
	GroupIndexes       []int                          `json:"group_indexes"`
	Nonmemo            Measures                       `json:"nonmemo_occurrences"`
	DetailMinorUnits   *string                        `json:"detail_minor_units"`
	ReportedMinorUnits *string                        `json:"reported_minor_units"`
	DeltaMinorUnits    *string                        `json:"delta_minor_units"` // reported minus detail
	State              string                         `json:"state"`
	Blockers           []string                       `json:"blockers"`
}

type ReceiptFamilyReport struct {
	BindingIndex        int                       `json:"binding_index"`
	FileNumber          string                    `json:"file_number"`
	Groups              []ReportLineProfileGroup  `json:"groups"`
	OutsideGroupIndexes []int                     `json:"outside_family_group_indexes"`
	Total               Measures                  `json:"total_occurrences"`
	Families            []ReceiptFamilyComparison `json:"families"`
	Blockers            []string                  `json:"blockers"`
}

type ReceiptFamiliesComparison struct {
	Version                           string                            `json:"version"`
	MapVersion                        string                            `json:"map_version"`
	Reported                          summaryassertion.WindowComparison `json:"reported"`
	FamilyReports                     reportperiod.ReceiptFamilyReports `json:"family_reports"`
	Profile                           reportmetadata.Artifact           `json:"profile"`
	ProfileID                         string                            `json:"profile_id"`
	Source                            fecrelease.StagedOutput           `json:"schedule_a_source"`
	Reports                           []ReceiptFamilyReport             `json:"reports"`
	SourceBodyRescanned               bool                              `json:"source_body_rescanned"`
	FamilyWindowComparisonReady       bool                              `json:"family_window_comparison_ready"`
	UniqueTransactionMembershipProven bool                              `json:"unique_transaction_membership_proven"`
	FinancialUseEligible              bool                              `json:"financial_use_eligible"`
	TerminalEligible                  bool                              `json:"terminal_attribution_eligible"`
}

// CompareReceiptFamilies adds per-report occurrence comparisons. It reuses the
// pinned complete profile, never the old 11AI dispositions as a family filter.
// Existing seven-field summary/window evidence remains unchanged and separate.
func CompareReceiptFamilies(ctx context.Context, request ReceiptWindowRequest) (ReceiptFamiliesComparison, error) {
	return compareReceiptFamilies(ctx, request, ReceiptFamiliesVersion, reportperiod.ReviewReceiptFamilyReports, reportscope.ReceiptFamilyFields)
}

// CompareReceiptFamiliesV2 distinguishes comparable required-itemized fields
// from thresholded detail components. It does not change the v1 window policy.
func CompareReceiptFamiliesV2(ctx context.Context, request ReceiptWindowRequest) (ReceiptFamiliesComparison, error) {
	return compareReceiptFamilies(ctx, request, ReceiptFamiliesVersionV2, reportperiod.ReviewReceiptFamilyReportsV2, reportscope.ReceiptFamilyFieldsV2)
}

func compareReceiptFamilies(ctx context.Context, request ReceiptWindowRequest, version string, review func(context.Context, reportperiod.WindowRequest) (reportperiod.ReceiptFamilyReports, error), fields func(string) []reportscope.ReceiptFamilyField) (ReceiptFamiliesComparison, error) {
	s, err := summaryassertion.CompareWindowV2(ctx, request.Summary)
	if err != nil {
		return ReceiptFamiliesComparison{}, err
	}
	f, err := review(ctx, request.Summary.Window)
	if err != nil {
		return ReceiptFamiliesComparison{}, err
	}
	if !sameFamilyReportEvidence(s.Window, f) {
		return ReceiptFamiliesComparison{}, fmt.Errorf("report evidence changed between comparison reads")
	}
	p, a, err := readLineProfile(ctx, request.ProfilePath, request.ProfileSHA256, request.Summary.StorageRoot, s)
	if err != nil {
		return ReceiptFamiliesComparison{}, err
	}
	r := ReceiptFamiliesComparison{Version: version, MapVersion: reportscope.ReceiptFamilyMapVersion, Reported: s, FamilyReports: f, Profile: a, ProfileID: p.ProfileID, Source: p.Source, Reports: []ReceiptFamilyReport{}}
	byFile := map[string][]ReportLineProfileGroup{}
	for _, g := range p.Reports {
		if g.Key.Committee == (Cell{true, s.CommitteeID}) && g.Key.File.Present {
			byFile[g.Key.File.Value] = append(byFile[g.Key.File.Value], g)
		}
	}
	for j, b := range f.Bindings {
		if err := ctx.Err(); err != nil {
			return ReceiptFamiliesComparison{}, err
		}
		var o reportperiod.Observation
		if b.ObservationIndex != nil {
			o = f.Membership.Observations[*b.ObservationIndex]
		}
		d, err := compareReceiptFamilyReportFields(o, b, byFile[b.Document.FileNumber], fields)
		if err != nil {
			return ReceiptFamiliesComparison{}, err
		}
		d.BindingIndex = j
		r.Reports = append(r.Reports, d)
	}
	return r, nil
}

func sameFamilyReportEvidence(w reportperiod.WindowReview, f reportperiod.ReceiptFamilyReports) bool {
	if !reflect.DeepEqual(w.Membership, f.Membership) || w.DocumentSet != f.DocumentSet || len(w.Bindings) != len(f.Bindings) {
		return false
	}
	for i, b := range w.Bindings {
		d := f.Bindings[i].Document
		if b.Document.Body != d.Body || b.Document.Headers != d.Headers || b.Document.FileNumber != d.FileNumber {
			return false
		}
	}
	return true
}

func compareReceiptFamilyReport(o reportperiod.Observation, b reportperiod.WindowBinding, groups []ReportLineProfileGroup) (ReceiptFamilyReport, error) {
	return compareReceiptFamilyReportFields(o, b, groups, reportscope.ReceiptFamilyFields)
}

func compareReceiptFamilyReportFields(o reportperiod.Observation, b reportperiod.WindowBinding, groups []ReportLineProfileGroup, fields func(string) []reportscope.ReceiptFamilyField) (ReceiptFamilyReport, error) {
	r := ReceiptFamilyReport{FileNumber: b.Document.FileNumber, Groups: slices.Clone(groups), OutsideGroupIndexes: []int{}, Families: []ReceiptFamilyComparison{}, Blockers: slices.Clone(b.ScopeBlockers)}
	if r.Groups == nil {
		r.Groups = []ReportLineProfileGroup{}
	}
	if !b.ScopeBound {
		r.Blockers = append(r.Blockers, "report_scope_unbound")
	}
	form := map[string]string{"Form 3": "F3", "Form 3X": "F3X"}[o.ReportForm]
	for _, g := range groups {
		if err := r.Total.merge(g.Measures); err != nil {
			return r, err
		}
		if form == "" || g.Key.Form != (Cell{true, form}) || g.Key.ReportType != (Cell{true, o.ReportType}) || g.Key.ReportYear != (Cell{true, strconv.Itoa(o.ReportYear)}) {
			r.Blockers = append(r.Blockers, "profile_report_scope_mismatch")
		}
	}
	if form == "" {
		r.Blockers = append(r.Blockers, "unsupported_report_form")
	}
	slices.Sort(r.Blockers)
	r.Blockers = slices.Compact(r.Blockers)
	assigned := make([]bool, len(groups))
	for _, spec := range fields(form) {
		f := ReceiptFamilyComparison{Field: spec, GroupIndexes: []int{}, State: "blocked", Blockers: slices.Clone(r.Blockers)}
		for i := range b.Fields {
			if b.Fields[i].Name == spec.MetadataField {
				f.Binding = &b.Fields[i]
				f.Blockers = append(f.Blockers, b.Fields[i].Blockers...)
				if b.Fields[i].ReportedValueBound {
					f.ReportedMinorUnits = b.Fields[i].Metadata.MinorUnits
				}
			}
		}
		if f.ReportedMinorUnits == nil {
			f.Blockers = append(f.Blockers, "report_family_field_unbound")
		}
		for i, g := range groups {
			if g.Key.Form != (Cell{true, spec.Form}) || g.Key.Schedule != (Cell{true, "SA"}) || g.Key.Line != (Cell{true, spec.Line}) {
				continue
			}
			assigned[i] = true
			f.GroupIndexes = append(f.GroupIndexes, i)
			// Same raw memo convention as the existing occurrence comparison;
			// no new interpretation of Y, no individual/date/transaction filter.
			if g.Key.Memo == (Cell{true, "X"}) {
				continue
			}
			if g.Key.Memo.Present && g.Key.Memo.Value != "" {
				f.Blockers = append(f.Blockers, "unresolved_memo_code")
				continue
			}
			if err := f.Nonmemo.merge(g.Measures); err != nil {
				return r, err
			}
			if g.Measures.Unknown > 0 {
				f.Blockers = append(f.Blockers, "unresolved_line_amount")
			}
		}
		// Even an explicit zero cover does not supply missing detail here.
		if f.Nonmemo.Rows == 0 {
			f.Blockers = append(f.Blockers, "no_nonmemo_family_detail")
		}
		if spec.DetailRelation != "" && spec.DetailRelation != reportscope.RequiredItemized && spec.DetailRelation != reportscope.ThresholdedDetail {
			f.Blockers = append(f.Blockers, "unsupported_detail_relation")
		}
		slices.Sort(f.Blockers)
		f.Blockers = slices.Compact(f.Blockers)
		if len(f.Blockers) == 0 {
			v := strconv.FormatInt(f.Nonmemo.Signed, 10)
			f.DetailMinorUnits = &v
			if spec.DetailRelation == reportscope.ThresholdedDetail {
				// Equal-looking values do not prove equal populations. Neither a
				// residual unitemized amount nor a numeric bound is inferred.
				f.State = "component_not_comparable"
			} else {
				var err error
				f.DeltaMinorUnits, f.State, err = receiptDifference(f.ReportedMinorUnits, &v)
				if err != nil {
					return r, err
				}
			}
		}
		r.Families = append(r.Families, f)
	}
	for i, ok := range assigned {
		if !ok {
			r.OutsideGroupIndexes = append(r.OutsideGroupIndexes, i)
		}
	}
	return r, nil
}
