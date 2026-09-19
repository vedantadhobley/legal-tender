package fundingbasis

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const FamilyAbsenceVersion = "legal-tender.fec.receipt-family-absence.v1"

type FamilyAbsence struct {
	FamilyID               string   `json:"family_id"`
	Line                   string   `json:"line"`
	ProfileRows            uint64   `json:"profile_rows"`
	OriginalObservedRows   uint64   `json:"original_observed_rows"`
	State                  string   `json:"state"`
	ReportedZeroMinorUnits *string  `json:"qualified_reported_zero_minor_units"`
	Blockers               []string `json:"blockers"`
}

type ReportFamilyAbsence struct {
	ReportIndex     int                    `json:"report_index"`
	Census          reportscope.LineCensus `json:"original_census"`
	LineCountsMatch bool                   `json:"original_profile_line_counts_match"`
	Blockers        []string               `json:"blockers"`
	Families        []FamilyAbsence        `json:"families"`
}

type FamilyAbsenceReview struct {
	Version              string                    `json:"version"`
	Compared             ReceiptFamiliesComparison `json:"compared"`
	Reports              []ReportFamilyAbsence     `json:"reports"`
	FamilyWindowReady    bool                      `json:"family_window_comparison_ready"`
	FinancialUseEligible bool                      `json:"financial_use_eligible"`
	TerminalEligible     bool                      `json:"terminal_attribution_eligible"`
}

// ReviewFamilyAbsence adds a narrow reported-zero qualification. It leaves all
// earlier detail/comparison fields intact, and produces no synthetic receipt.
func ReviewFamilyAbsence(ctx context.Context, request ReceiptWindowRequest) (FamilyAbsenceReview, error) {
	prior, err := CompareReceiptFamilies(ctx, request)
	if err != nil {
		return FamilyAbsenceReview{}, err
	}
	r := FamilyAbsenceReview{Version: FamilyAbsenceVersion, Compared: prior, Reports: []ReportFamilyAbsence{}}
	for i, report := range prior.Reports {
		d := prior.FamilyReports.Bindings[report.BindingIndex].Document
		c, err := reportscope.InventoryScheduleALines(ctx, reportscope.Request{SourceURL: d.SourceURL, BodyPath: d.Body.Path, BodySHA256: d.Body.SHA256, HeadersPath: d.Headers.Path, HeadersSHA256: d.Headers.SHA256})
		if err != nil {
			return FamilyAbsenceReview{}, err
		}
		if c.Body != d.Body || c.Headers != d.Headers {
			return FamilyAbsenceReview{}, fmt.Errorf("original changed during absence review")
		}
		v := qualifyFamilyAbsence(report, c)
		v.ReportIndex = i
		r.Reports = append(r.Reports, v)
	}
	return r, nil
}

func qualifyFamilyAbsence(report ReceiptFamilyReport, c reportscope.LineCensus) ReportFamilyAbsence {
	r := ReportFamilyAbsence{Census: c, Blockers: append([]string{}, report.Blockers...), Families: []FamilyAbsence{}}
	if !c.Complete {
		r.Blockers = append(r.Blockers, "original_line_census_incomplete")
	}
	profile, original := map[string]uint64{}, map[string]uint64{}
	profileScopeKnown := true
	for _, g := range report.Groups {
		k := g.Key
		if k.Form != (Cell{true, c.Form}) || k.Schedule != (Cell{true, "SA"}) || !k.Line.Present || !slices.Contains(reportscope.ScheduleALines(c.Form), k.Line.Value) {
			r.Blockers = append(r.Blockers, "unreviewed_profile_line_scope")
			profileScopeKnown = false
			continue
		}
		profile[k.Line.Value] += g.Measures.Rows // Complete profile counts were revalidated first.
	}
	for _, line := range c.Lines {
		original[strings.TrimPrefix(line.Tag, "SA")] = uint64(len(line.RecordOrdinals))
	}
	r.LineCountsMatch = c.Complete && profileScopeKnown && len(profile) == len(original)
	for line, count := range original {
		if profile[line] != count {
			r.LineCountsMatch = false
		}
	}
	if !r.LineCountsMatch {
		r.Blockers = append(r.Blockers, "profile_original_line_counts_differ_or_unknown")
	}
	slices.Sort(r.Blockers)
	r.Blockers = slices.Compact(r.Blockers)
	for _, f := range report.Families {
		v := FamilyAbsence{FamilyID: f.Field.ID, Line: f.Field.Line, ProfileRows: profile[f.Field.Line], OriginalObservedRows: original[f.Field.Line], State: "blocked", Blockers: append([]string{}, r.Blockers...)}
		if !slices.Contains(reportscope.ReceiptFamilyFields(c.Form), f.Field) {
			v.Blockers = append(v.Blockers, "unreviewed_absence_family")
		}
		if f.Binding == nil || !f.Binding.ReportedValueBound || f.ReportedMinorUnits == nil {
			v.Blockers = append(v.Blockers, "reported_family_field_unbound")
		} else if *f.ReportedMinorUnits != "0" {
			v.Blockers = append(v.Blockers, "reported_family_field_not_zero")
		}
		if v.ProfileRows != 0 {
			v.Blockers = append(v.Blockers, "family_has_profile_occurrences")
		}
		if v.OriginalObservedRows != 0 {
			v.Blockers = append(v.Blockers, "family_has_original_occurrences")
		}
		slices.Sort(v.Blockers)
		v.Blockers = slices.Compact(v.Blockers)
		if len(v.Blockers) == 0 {
			zero := "0"
			v.ReportedZeroMinorUnits = &zero
			v.State = "qualified_reported_zero"
		}
		r.Families = append(r.Families, v)
	}
	return r
}
