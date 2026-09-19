package fundingbasis

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func TestReceiptFamiliesV2RelationsAndInvariance(t *testing.T) {
	for _, form := range []string{"F3", "F3X"} {
		for _, spec := range reportscope.ReceiptFamilyFieldsV2(form) {
			for _, tc := range []struct {
				name, blocker string
				mutate        func(*reportperiod.Observation, *reportperiod.WindowBinding, *[]ReportLineProfileGroup)
			}{
				{"equal_values", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {}},
				{"different_values", "", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
					v := "999"
					b.Fields[0].Metadata.MinorUnits = &v
				}},
				{"renamed_file_committee", "", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					b.Document.FileNumber = "987654"
					(*g)[0].Key.File = Cell{true, "987654"}
					(*g)[0].Key.Committee = Cell{true, "C87654321"}
				}},
				{"other_year", "", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					o.ReportYear = 2030
					(*g)[0].Key.ReportYear = Cell{true, "2030"}
				}},
				{"no_date_or_individual_filter", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					(*g)[0].Dates.First = "1975-01-01"
					(*g)[0].Key.Individual = "false"
					(*g)[0].Key.Decision = "excluded"
				}},
				{"signed_detail", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					(*g)[0].Measures = Measures{Rows: 1, Known: 1, NegativeRows: 1, Negative: -100, Signed: -100}
				}},
				{"memo_preserved", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					m := (*g)[0]
					m.Key.Memo = Cell{true, "X"}
					*g = append(*g, m)
				}},
				{"missing_detail", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					*g = nil
				}},
				{"zero_is_not_detail", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					*g = nil
					v := "0"
					b.Fields[0].Metadata.MinorUnits = &v
				}},
				{"unknown_amount", "unresolved_line_amount", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					(*g)[0].Measures = Measures{Rows: 1, Unknown: 1}
				}},
				{"unknown_memo", "unresolved_memo_code", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					m := (*g)[0]
					m.Key.Memo = Cell{true, "Y"}
					*g = append(*g, m)
				}},
				{"unbound_field", "report_family_field_unbound", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
					b.Fields[0].ReportedValueBound = false
				}},
				{"unbound_scope", "report_scope_unbound", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
					b.ScopeBound = false
				}},
				{"line_suffix", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					(*g)[0].Key.Line.Value += "A"
				}},
				{"wrong_schedule", "no_nonmemo_family_detail", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
					(*g)[0].Key.Schedule.Value = "SB"
				}},
			} {
				t.Run(form+"/"+spec.ID+"/"+tc.name, func(t *testing.T) {
					o, b, groups := receiptReportFixture()
					o.ReportForm = map[string]string{"F3": "Form 3", "F3X": "Form 3X"}[form]
					b.Fields[0].Name = spec.MetadataField
					groups[0].Key.Form, groups[0].Key.Line = Cell{true, form}, Cell{true, spec.Line}
					tc.mutate(&o, &b, &groups)
					before, _ := json.Marshal(groups)
					r, err := compareReceiptFamilyReportFields(o, b, groups, reportscope.ReceiptFamilyFieldsV2)
					if err != nil {
						t.Fatal(err)
					}
					seen := slices.Clone(r.OutsideGroupIndexes)
					for _, f := range r.Families {
						seen = append(seen, f.GroupIndexes...)
						if f.Field.ID != spec.ID {
							continue
						}
						if tc.blocker != "" {
							if f.State != "blocked" || !slices.Contains(f.Blockers, tc.blocker) || f.DetailMinorUnits != nil || f.DeltaMinorUnits != nil {
								t.Fatal(f)
							}
						} else if spec.DetailRelation == reportscope.ThresholdedDetail {
							if f.State != "component_not_comparable" || f.DetailMinorUnits == nil || f.ReportedMinorUnits == nil || f.DeltaMinorUnits != nil || len(f.Blockers) != 0 {
								t.Fatal("partial detail promoted or erased", f)
							}
						} else if f.DeltaMinorUnits == nil || (f.State != "equal" && f.State != "different") {
							t.Fatal(f)
						}
					}
					slices.Sort(seen)
					if len(seen) != len(groups) {
						t.Fatal("lost group")
					}
					for i, v := range seen {
						if i != v {
							t.Fatal("duplicated group")
						}
					}
					after, _ := json.Marshal(groups)
					if string(before) != string(after) {
						t.Fatal("mutated raw profile")
					}
				})
			}
		}
	}
}

func TestReceiptFamiliesV2UnknownRelationAndFormDoNotFallback(t *testing.T) {
	o, b, g := receiptReportFixture()
	fields := func(string) []reportscope.ReceiptFamilyField {
		s := reportscope.ReceiptFamilyFieldsV2("F3")[0]
		s.DetailRelation = "future_relation"
		return []reportscope.ReceiptFamilyField{s}
	}
	b.Fields[0].Name = fields("")[0].MetadataField
	g[0].Key.Line = Cell{true, "11B"}
	r, err := compareReceiptFamilyReportFields(o, b, g, fields)
	if err != nil || !slices.Contains(r.Families[0].Blockers, "unsupported_detail_relation") || r.Families[0].DeltaMinorUnits != nil {
		t.Fatal(r, err)
	}
	o.ReportForm = "Form 3P"
	r, err = compareReceiptFamilyReportFields(o, b, g, reportscope.ReceiptFamilyFieldsV2)
	if err != nil || len(r.Families) != 0 || len(r.OutsideGroupIndexes) != len(g) {
		t.Fatal(r, err)
	}
	o.ReportForm = "Form 3X"
	g[0].Key.Form, g[0].Key.Line = Cell{true, "F3X"}, Cell{true, "11D"}
	r, err = compareReceiptFamilyReportFields(o, b, g, reportscope.ReceiptFamilyFieldsV2)
	if err != nil || len(r.OutsideGroupIndexes) != 1 {
		t.Fatal("F3X subtotal treated as candidate receipts", r, err)
	}
}
