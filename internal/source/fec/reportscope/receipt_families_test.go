package reportscope

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
)

func TestReceiptFamilySourceMap(t *testing.T) {
	body, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-families/v1/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Version string
		Forms   map[string]struct {
			Leaves []struct {
				ID       string
				Line     string
				Sequence int
				Schedule string `json:"detail_schedule"`
				Relation string `json:"detail_relation"`
			}
		}
	}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatal(err)
	}
	if c.Version != ReceiptFamilyMapVersion {
		t.Fatal("changed reviewed map")
	}
	metadataBody, err := os.ReadFile("../../../../contracts/sources/fec/report-metadata/v1/record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var metadata struct {
		Defs map[string]struct {
			Properties map[string]struct {
				Types []string `json:"type"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(metadataBody, &metadata); err != nil {
		t.Fatal(err)
	}
	policyBody, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-family-comparison/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct{ Families map[string][]string }
	if err := json.Unmarshal(policyBody, &policy); err != nil {
		t.Fatal(err)
	}
	for _, form := range []string{"F3", "F3X"} {
		ids := []string{}
		for _, spec := range ReceiptFamilyFields(form) {
			ids = append(ids, spec.ID)
			endpoint := map[string]string{"F3": "HouseSenate", "F3X": "PacParty"}[form]
			if !slices.Equal(metadata.Defs[endpoint].Properties[spec.MetadataField].Types, []string{"number", "null"}) {
				t.Fatal("unreviewed metadata field/type", spec)
			}
			found := false
			for _, leaf := range c.Forms[form].Leaves {
				if leaf.ID == spec.ID {
					found = leaf.Line == spec.Line && leaf.Sequence == spec.Sequence && leaf.Schedule == "SA" && leaf.Relation == "all_required_itemized"
				}
			}
			if !found {
				t.Fatal("unreviewed family mapping", spec)
			}
		}
		if !slices.Equal(ids, policy.Families[form]) {
			t.Fatal("changed runtime subset", form)
		}
	}
	for _, form := range []string{"F3P", "f3", "F3N", "", "Form 3"} {
		if len(ReceiptFamilyFields(form)) != 0 {
			t.Fatal("accepted alias", form)
		}
	}
	for _, form := range []string{"F3PN", "F3P", "f3n", "F3", "F3XZ"} {
		if ReceiptFamilyCoverForm(form) != "" {
			t.Fatal("accepted unsupported cover", form)
		}
	}
	a := ReceiptFamilyFields("F3")
	a[0].Line = "WRONG"
	if ReceiptFamilyFields("F3")[0].Line != "11B" {
		t.Fatal("shared mutable mapping")
	}
}
