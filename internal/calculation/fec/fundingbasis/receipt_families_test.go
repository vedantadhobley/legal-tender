package fundingbasis

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func TestFamilyReportComparison(t *testing.T) {
	for _, tc := range []struct {
		name, state, blocker string
		mutate               func(*reportperiod.Observation, *reportperiod.WindowBinding, *[]ReportLineProfileGroup)
	}{
		{"exact", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {}},
		{"dates_not_clipped", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Dates.First = "1975-01-01"
			(*g)[0].Dates.Last = "2029-01-01"
		}},
		{"individual_not_filter", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Key.Individual = "false"
			(*g)[0].Key.Decision = "excluded"
		}},
		{"duplicates_not_removed", "different", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Measures = Measures{Rows: 2, Known: 2, PositiveRows: 2, Positive: 200, Signed: 200}
		}},
		{"negative", "different", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Measures = Measures{Rows: 1, Known: 1, NegativeRows: 1, Negative: -100, Signed: -100}
		}},
		{"memo_preserved", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			memo := (*g)[0]
			memo.Key.Memo = Cell{true, "X"}
			*g = append(*g, memo)
		}},
		{"unknown_memo", "blocked", "unresolved_memo_code", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			memo := (*g)[0]
			memo.Key.Memo = Cell{true, "Y"}
			*g = append(*g, memo)
		}},
		{"unknown_amount", "blocked", "unresolved_line_amount", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Measures = Measures{Rows: 1, Unknown: 1}
		}},
		{"no_detail", "blocked", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
		}},
		{"zero_cover_not_detail", "blocked", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
			zero := "0"
			b.Fields[0].Metadata.MinorUnits = &zero
		}},
		{"memo_only", "blocked", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Key.Memo = Cell{true, "X"}
		}},
		{"form", "blocked", "profile_report_scope_mismatch", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			o.ReportForm = "Form 3X"
		}},
		{"year", "blocked", "profile_report_scope_mismatch", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			o.ReportYear++
		}},
		{"type", "blocked", "profile_report_scope_mismatch", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			o.ReportType = "YE"
		}},
		{"unbound", "blocked", "report_family_field_unbound", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			b.Fields[0].ReportedValueBound = false
		}},
		{"partial", "blocked", "report_scope_unbound", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			b.ScopeBound = false
		}},
		{"line_alias", "blocked", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Key.Line.Value = "11b"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, b, g := receiptReportFixture()
			b.Fields[0].Name = "political_party_committee_contributions_period"
			g[0].Key.Line = Cell{true, "11B"}
			g[0].Key.Disposition = "outside_reviewed_form_line"
			tc.mutate(&o, &b, &g)
			before, _ := json.Marshal(g)
			r, err := compareReceiptFamilyReport(o, b, g)
			if err != nil {
				t.Fatal(err)
			}
			f := r.Families[0]
			if f.State != tc.state || tc.blocker != "" && !slices.Contains(f.Blockers, tc.blocker) {
				t.Fatal(f)
			}
			if f.State == "blocked" && (f.DetailMinorUnits != nil || f.DeltaMinorUnits != nil) {
				t.Fatal("invented detail")
			}
			after, _ := json.Marshal(g)
			if string(before) != string(after) {
				t.Fatal("mutated old profile")
			}
			seen := make([]int, len(g))
			for _, f := range r.Families {
				for _, i := range f.GroupIndexes {
					seen[i]++
				}
			}
			for _, i := range r.OutsideGroupIndexes {
				seen[i]++
			}
			for _, n := range seen {
				if n != 1 {
					t.Fatal("lost or duplicated group")
				}
			}
		})
	}
}

func TestEveryFamilyHasIndependentComparison(t *testing.T) {
	for _, form := range []string{"F3", "F3X"} {
		for _, spec := range reportscope.ReceiptFamilyFields(form) {
			o, b, groups := receiptReportFixture()
			o.ReportForm = map[string]string{"F3": "Form 3", "F3X": "Form 3X"}[form]
			b.Fields[0].Name = spec.MetadataField
			groups[0].Key.Form, groups[0].Key.Line = Cell{true, form}, Cell{true, spec.Line}
			r, err := compareReceiptFamilyReport(o, b, groups)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range r.Families {
				want := "blocked"
				if f.Field.ID == spec.ID {
					want = "equal"
				}
				if f.State != want {
					t.Fatal(form, spec, f)
				}
			}
		}
	}
}

func TestFamilyReportEvidenceCannotChangeBetweenReads(t *testing.T) {
	_, b, _ := receiptReportFixture()
	w := reportperiod.WindowReview{Bindings: []reportperiod.WindowBinding{b}}
	f := reportperiod.ReceiptFamilyReports{Bindings: []reportperiod.WindowBinding{b}}
	if !sameFamilyReportEvidence(w, f) {
		t.Fatal("identical sources rejected")
	}
	f.Bindings[0].Document.Body.SHA256 = "changed"
	if sameFamilyReportEvidence(w, f) {
		t.Fatal("accepted changed original")
	}
	f.Bindings[0] = b
	f.DocumentSet.SHA256 = "changed"
	if sameFamilyReportEvidence(w, f) {
		t.Fatal("accepted changed descriptor")
	}
	f.DocumentSet = w.DocumentSet
	f.Membership.Version = "changed"
	if sameFamilyReportEvidence(w, f) {
		t.Fatal("accepted changed membership")
	}
}
