package reportmetadata

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestCompiledShapesMatchReviewedContract(t *testing.T) {
	b, err := os.ReadFile("../../../../contracts/sources/fec/report-metadata/v1/record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]struct {
			Required   []string `json:"required"`
			Properties map[string]struct {
				Type  []string `json:"type"`
				Items struct {
					Type json.RawMessage `json:"type"`
				} `json:"items"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatal(err)
	}
	for name, endpoint := range map[string]string{"Filings": "/v1/filings/", "HouseSenate": "/v1/reports/house-senate/", "PacParty": "/v1/reports/pac-party/"} {
		shape := shapes[endpoint]
		def := schema.Defs[name]
		if len(def.Properties) != len(shape) || len(def.Required) != len(shape) {
			t.Fatal("field count drift", name)
		}
		for field, kind := range shape {
			p, ok := def.Properties[field]
			if !ok || !slices.Contains(def.Required, field) || !slices.Contains(p.Type, "null") {
				t.Fatal("field contract drift", field)
			}
			if strings.HasSuffix(kind, "[]") {
				if !slices.Equal(p.Type, []string{"array", "null"}) {
					t.Fatal(field)
				}
				var types []string
				if len(p.Items.Type) > 0 && p.Items.Type[0] == '"' {
					var single string
					if err := json.Unmarshal(p.Items.Type, &single); err != nil {
						t.Fatal(err)
					}
					types = []string{single}
				} else if err := json.Unmarshal(p.Items.Type, &types); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(types, strings.Split(strings.TrimSuffix(kind, "[]"), "|")) {
					t.Fatal("array type drift", field)
				}
			} else if !slices.Equal(p.Type, append(strings.Split(kind, "|"), "null")) {
				t.Fatal("scalar type drift", field)
			}
		}
	}
}
