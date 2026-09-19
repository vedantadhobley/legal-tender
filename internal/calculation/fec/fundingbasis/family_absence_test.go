package fundingbasis

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func absenceFixture(t *testing.T) (ReceiptFamilyReport, reportscope.LineCensus) {
	t.Helper()
	o, b, g := receiptReportFixture()
	zero := "0"
	b.Fields[0].Name = "political_party_committee_contributions_period"
	b.Fields[0].Metadata.MinorUnits = &zero
	r, err := compareReceiptFamilyReport(o, b, g)
	if err != nil {
		t.Fatal(err)
	}
	return r, reportscope.LineCensus{Form: "F3", Complete: true, Lines: []reportscope.CensusLine{{Tag: "SA11AI", RecordOrdinals: []int{3}}}}
}

func TestFamilyAbsenceQualification(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		mutate        func(*ReceiptFamilyReport, *reportscope.LineCensus)
	}{
		{"other_lines_present", "", func(_ *ReceiptFamilyReport, _ *reportscope.LineCensus) {}},
		{"empty_report", "", func(r *ReceiptFamilyReport, c *reportscope.LineCensus) { r.Groups = nil; c.Lines = nil }},
		{"positive_cover", "reported_family_field_not_zero", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) {
			v := "100"
			r.Families[0].ReportedMinorUnits = &v
		}},
		{"negative_cover", "reported_family_field_not_zero", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) {
			v := "-100"
			r.Families[0].ReportedMinorUnits = &v
		}},
		{"blank_cover", "reported_family_field_unbound", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Families[0].ReportedMinorUnits = nil }},
		{"unbound_cover", "reported_family_field_unbound", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) {
			r.Families[0].Binding.ReportedValueBound = false
		}},
		{"no_binding", "reported_family_field_unbound", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Families[0].Binding = nil }},
		{"scope", "report_scope_unbound", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) {
			r.Blockers = append(r.Blockers, "report_scope_unbound")
		}},
		{"partial_original", "original_line_census_incomplete", func(_ *ReceiptFamilyReport, c *reportscope.LineCensus) { c.Complete = false }},
		{"missing_profile", "profile_original_line_counts_differ_or_unknown", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Groups = nil }},
		{"missing_original_line", "profile_original_line_counts_differ_or_unknown", func(_ *ReceiptFamilyReport, c *reportscope.LineCensus) { c.Lines = nil }},
		{"wrong_count", "profile_original_line_counts_differ_or_unknown", func(_ *ReceiptFamilyReport, c *reportscope.LineCensus) {
			c.Lines[0].RecordOrdinals = append(c.Lines[0].RecordOrdinals, 4)
		}},
		{"wrong_form", "unreviewed_profile_line_scope", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Groups[0].Key.Form.Value = "F3X" }},
		{"wrong_schedule", "unreviewed_profile_line_scope", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Groups[0].Key.Schedule.Value = "SL" }},
		{"unknown_line", "unreviewed_profile_line_scope", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Groups[0].Key.Line.Value = "19A" }},
		{"unknown_family", "unreviewed_absence_family", func(r *ReceiptFamilyReport, _ *reportscope.LineCensus) { r.Families[0].Field.Line = "15" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c := absenceFixture(t)
			tc.mutate(&r, &c)
			before, _ := json.Marshal(r)
			a := qualifyFamilyAbsence(r, c)
			f := a.Families[0]
			if tc.blocker == "" {
				if f.State != "qualified_reported_zero" || f.ReportedZeroMinorUnits == nil || *f.ReportedZeroMinorUnits != "0" || len(f.Blockers) != 0 {
					t.Fatal(f)
				}
			} else if f.State != "blocked" || f.ReportedZeroMinorUnits != nil || !slices.Contains(f.Blockers, tc.blocker) {
				t.Fatal(f)
			}
			after, _ := json.Marshal(r)
			if string(before) != string(after) || r.Families[0].DetailMinorUnits != nil {
				t.Fatal("mutated prior detail")
			}
		})
	}
}

func TestZeroNetAndMemoPopulationsAreNotAbsent(t *testing.T) {
	for _, memo := range []Cell{{true, "X"}, {true, "Y"}, {true, ""}, {false, ""}} {
		for _, measure := range []Measures{{Rows: 1, Unknown: 1}, {Rows: 1, Known: 1, ZeroRows: 1}, {Rows: 2, Known: 2, PositiveRows: 1, NegativeRows: 1, Positive: 100, Negative: -100}} {
			r, c := absenceFixture(t)
			g := r.Groups[0]
			g.Key.Line.Value = "11B"
			g.Key.Memo = memo
			g.Measures = measure
			r.Groups = append(r.Groups, g)
			ordinals := []int{4}
			if measure.Rows == 2 {
				ordinals = append(ordinals, 5)
			}
			c.Lines = append(c.Lines, reportscope.CensusLine{Tag: "SA11B", RecordOrdinals: ordinals})
			a := qualifyFamilyAbsence(r, c)
			f := a.Families[0]
			if !a.LineCountsMatch || f.State != "blocked" || f.ReportedZeroMinorUnits != nil || !slices.Contains(f.Blockers, "family_has_profile_occurrences") || !slices.Contains(f.Blockers, "family_has_original_occurrences") {
				t.Fatal(f)
			}
		}
	}
}

func TestFamilyAbsencePolicySubset(t *testing.T) {
	body, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-family-absence/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Version  string
		Families map[string][]string
		Guards   map[string]bool
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Version != FamilyAbsenceVersion {
		t.Fatal("policy version mismatch")
	}
	for _, v := range p.Guards {
		if v {
			t.Fatal("eligibility promoted")
		}
	}
	for _, form := range []string{"F3", "F3X"} {
		ids := []string{}
		for _, f := range reportscope.ReceiptFamilyFields(form) {
			ids = append(ids, f.ID)
		}
		if !slices.Equal(ids, p.Families[form]) {
			t.Fatal("changed subset", form)
		}
	}
}
