package reportperiod

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func TestReceiptFamilyV2BindingsStayFieldLocal(t *testing.T) {
	for _, target := range reportscope.ReceiptFamilyFieldsV2("F3X") {
		for _, tc := range []struct {
			name, blocker string
			cover         string
			metadata      any
		}{
			{"exact", "", "1.01", 1.01},
			{"mismatch", "reported_value_mismatch", "-0.01", 1.01},
			{"blank", "cover_field:blank", "", 1.01},
			{"invalid", "cover_field:invalid", "0.001", 1.01},
			{"null", "metadata_field:source_null", "1.01", nil},
		} {
			t.Run(target.ID+"/"+tc.name, func(t *testing.T) {
				req := bindingFixture(t, func(_, cover []string, record map[string]any) {
					for _, spec := range reportscope.ReceiptFamilyFieldsV2("F3X") {
						cover[spec.Sequence-1], record[spec.MetadataField] = "1.01", 1.01
					}
					cover[target.Sequence-1], record[target.MetadataField] = tc.cover, tc.metadata
				}, false)
				m, err := Inspect(context.Background(), req.Membership)
				if err != nil {
					t.Fatal(err)
				}
				r, err := bindReceiptFamilyFieldsV2(context.Background(), m, req.Document)
				if err != nil || len(r.Fields) != 8 || !r.ScopeBound || r.Version != ReceiptFamilyBindingVersionV2 || r.CashBasisReady || r.TerminalAttributionEligible {
					t.Fatal(r, err)
				}
				for _, f := range r.Fields {
					if f.Name == target.MetadataField && tc.blocker != "" {
						if f.ReportedValueBound || !slices.Contains(f.Blockers, tc.blocker) {
							t.Fatal(f)
						}
					} else if !f.ReportedValueBound || *f.Metadata.MinorUnits != "101" {
						t.Fatal("unrelated field blocked", f)
					}
				}
			})
		}
	}
	for _, value := range []string{"1.01", "", "0.001"} {
		req := bindingFixture(t, func(_, cover []string, record map[string]any) {
			cover[38] = "1.01"
			record["offsets_to_operating_expenditures_period"] = value
		}, false)
		m, err := Inspect(context.Background(), req.Membership)
		if err != nil {
			t.Fatal(err)
		}
		r, err := bindReceiptFamilyFieldsV2(context.Background(), m, req.Document)
		if err != nil {
			t.Fatal(err)
		}
		f := r.Fields[5]
		if f.ReportedValueBound != (value == "1.01") || string(f.Metadata.Raw) != string(marshal(value)) {
			t.Fatal("string coercion or lost raw value", f)
		}
	}
}

func TestBindingStringFieldsMatchPinnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../../contracts/sources/fec/report-metadata/v1/record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]struct {
			Properties map[string]struct{ Type []string }
		} `json:"$defs"`
	}
	if err := json.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	for form, endpoint := range map[string]string{"F3": "HouseSenate", "F3X": "PacParty"} {
		for _, spec := range reportscope.ReceiptFamilyFieldsV2(form) {
			if bindingAllowsString(spec.MetadataField) != slices.Contains(schema.Defs[endpoint].Properties[spec.MetadataField].Type, "string") {
				t.Fatal("string acceptance drift", spec)
			}
		}
	}
	if bindingAllowsString("unknown_individual_period") || bindingAllowsString("other_offsets_to_operating_expenditures_period") {
		t.Fatal("substring alias accepted")
	}
}
