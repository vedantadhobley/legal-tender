package reportperiod

import (
	"context"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func TestReceiptFamilyBindings(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		mutate        func([]string, []string, map[string]any)
		prefix        bool
	}{
		{"equal", "", nil, false},
		{"signed", "reported_value_mismatch", func(_, f []string, _ map[string]any) { f[32] = "-0.01" }, false},
		{"blank", "cover_field:blank", func(_, f []string, _ map[string]any) { f[32] = "" }, false},
		{"invalid", "cover_field:invalid", func(_, f []string, _ map[string]any) { f[32] = "0.001" }, false},
		{"null", "metadata_field:source_null", func(_, _ []string, b map[string]any) { b["political_party_committee_contributions_period"] = nil }, false},
		{"missing", "metadata_field:source_null", func(_, _ []string, b map[string]any) { delete(b, "political_party_committee_contributions_period") }, false},
		{"string", "metadata_field:invalid", func(_, _ []string, b map[string]any) { b["political_party_committee_contributions_period"] = "0.00" }, false},
		{"superseded", "not_observed_chain_candidate", func(_, _ []string, b map[string]any) { b["is_amended"] = true }, false},
		{"prefix", "partial_document_capture", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := bindingFixture(t, func(h, f []string, b map[string]any) {
				for _, field := range reportscope.ReceiptFamilyFields("F3X") {
					f[field.Sequence-1] = "0.00"
					b[field.MetadataField] = 0
				}
				if tc.mutate != nil {
					tc.mutate(h, f, b)
				}
			}, tc.prefix)
			m, err := Inspect(context.Background(), req.Membership)
			if tc.name == "string" {
				if err == nil {
					t.Fatal("accepted metadata schema drift")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := bindReceiptFamilyFields(context.Background(), m, req.Document)
			if err != nil || len(r.Fields) != 4 {
				t.Fatal(r, err)
			}
			if r.Version != ReceiptFamilyBindingVersion || r.Document.Version != reportscope.ReceiptFamilyCoverVersion || r.CashBasisReady || r.CycleTotalReady || r.TerminalAttributionEligible {
				t.Fatal("wrong contract")
			}
			if tc.blocker == "" {
				for _, f := range r.Fields {
					if !f.ReportedValueBound || *f.DeltaMinorUnits != "0" {
						t.Fatal(f)
					}
				}
			} else {
				if r.Fields[0].ReportedValueBound || !slices.Contains(r.Fields[0].Blockers, tc.blocker) {
					t.Fatal(r.Fields[0])
				}
				if r.ScopeBound && !r.Fields[1].ReportedValueBound {
					t.Fatal("field-local failure leaked")
				}
			}
			old, err := BindFields(context.Background(), req)
			if err != nil || len(old.Fields) != 7 || old.Version != BindingVersion || old.Document.Version != reportscope.ElectronicVersion {
				t.Fatal("changed old projection", err)
			}
		})
	}
}
