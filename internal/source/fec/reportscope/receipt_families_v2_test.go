package reportscope

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"testing"
)

func TestReceiptFamilyV2SourceMapAndProjection(t *testing.T) {
	var mapping struct {
		Forms map[string]struct {
			Leaves []struct {
				ID       string
				Line     string
				Sequence int
				Schedule string `json:"detail_schedule"`
				Relation string `json:"detail_relation"`
			}
		}
	}
	var policy struct{ Families map[string][]string }
	var schema struct {
		Defs map[string]struct {
			Properties map[string]struct{ Type []string }
		} `json:"$defs"`
	}
	for path, dest := range map[string]any{
		"../../../../contracts/calculations/fec/receipt-families/v1/contract.json":        &mapping,
		"../../../../contracts/calculations/fec/receipt-family-comparison/v2/policy.json": &policy,
		"../../../../contracts/sources/fec/report-metadata/v1/record.schema.json":         &schema,
	} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, dest); err != nil {
			t.Fatal(err)
		}
	}
	for _, form := range []string{"F3", "F3X"} {
		fields := ReceiptFamilyFieldsV2(form)
		ids := []string{}
		lines := map[string]bool{}
		rows := electronicFixture(form == "F3")
		for i, spec := range fields {
			ids = append(ids, spec.ID)
			if lines[spec.Line] {
				t.Fatal("overlapping leaf", spec)
			}
			lines[spec.Line] = true
			found := false
			for _, leaf := range mapping.Forms[form].Leaves {
				if leaf.ID == spec.ID {
					found = leaf.Line == spec.Line && leaf.Sequence == spec.Sequence && leaf.Schedule == "SA" && leaf.Relation == spec.DetailRelation
				}
			}
			if !found {
				t.Fatal("unreviewed field relationship", spec)
			}
			endpoint := map[string]string{"F3": "HouseSenate", "F3X": "PacParty"}[form]
			if !slices.Contains(schema.Defs[endpoint].Properties[spec.MetadataField].Type, "number") {
				t.Fatal("unreviewed metadata field", spec)
			}
			setField(rows, 1, spec.Sequence, strconv.Itoa(i+1)+".01")
			if spec.CorroboratingSequence != 0 {
				setField(rows, 1, spec.CorroboratingSequence, "-0.01")
			}
		}
		if !slices.Equal(ids, policy.Families[form]) {
			t.Fatal("changed accepted subset", ids)
		}
		// Every mapped non-individual SA leaf must be represented, not selected
		// according to whether a retained committee happened to report it.
		for _, leaf := range mapping.Forms[form].Leaves {
			if leaf.Schedule == "SA" && leaf.Relation != "itemized_component" && !slices.Contains(ids, leaf.ID) {
				t.Fatal("omitted mapped SA leaf", leaf)
			}
		}
		r := electronicRequest(t, rows, false)
		a, err := AssessReceiptFamiliesV2(context.Background(), r)
		if err != nil || a.Version != ReceiptFamilyCoverVersionV2 || len(a.PeriodFields) != len(fields) {
			t.Fatal(a, err)
		}
		for i, spec := range fields {
			f := a.PeriodFields[i]
			if f.Name != spec.MetadataField {
				t.Fatal(f)
			}
			want := 1
			if spec.CorroboratingSequence != 0 {
				want = 2
			}
			if len(f.Amounts) != want {
				t.Fatal("lost duplicate-position evidence", f)
			}
			for _, v := range f.Amounts {
				if v.Sequence == spec.Sequence && v.MinorUnits != strconv.Itoa((i+1)*100+1) {
					t.Fatal("wrong period position", f)
				}
				if v.Sequence == spec.CorroboratingSequence && v.MinorUnits != "-1" {
					t.Fatal("conflict overwritten", f)
				}
			}
		}
		old, err := AssessReceiptFamilies(context.Background(), r)
		if err != nil || len(old.PeriodFields) != len(ReceiptFamilyFields(form)) || old.Version != ReceiptFamilyCoverVersion {
			t.Fatal("v1 widened", err)
		}
	}
	for _, form := range []string{"F3P", "F3N", "f3", "Form 3", ""} {
		if len(ReceiptFamilyFieldsV2(form)) != 0 {
			t.Fatal("accepted alias", form)
		}
	}
}
