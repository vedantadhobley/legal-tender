package wikimedia

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func roleItem(p, id string) map[string]any {
	return map[string]any{"snaktype": "value", "property": p, "datatype": "wikibase-item", "datavalue": map[string]any{"type": "wikibase-entityid", "value": map[string]any{"id": id, "entity-type": "item"}}}
}

func roleRow(subject, p, target string) map[string]any {
	return map[string]any{"id": subject + "$fixture", "type": "statement", "rank": "normal", "mainsnak": roleItem(p, target)}
}

func roleTimeFixture(p, date string, precision int) map[string]any {
	return map[string]any{"snaktype": "value", "property": p, "datatype": "time", "datavalue": map[string]any{"type": "time", "value": map[string]any{"time": date, "precision": precision, "timezone": 0, "before": 0, "after": 0, "calendarmodel": "http://www.wikidata.org/entity/Q1985727"}}}
}

func roleBody(claims map[string]any) []byte {
	entity := func(id, label string, claims map[string]any) map[string]any {
		return map[string]any{"id": id, "type": "item", "pageid": 1, "ns": 0, "title": id, "lastrevid": 10, "modified": "2026-09-15T00:00:00Z", "labels": map[string]any{"en": map[string]any{"language": "en", "value": label}}, "claims": claims}
	}
	human := map[string]any{"P31": []any{roleRow("Q2", "P31", "Q5")}}
	b, _ := json.Marshal(map[string]any{"success": 1, "entities": map[string]any{"Q1": entity("Q1", "Example company", claims), "Q2": entity("Q2", "Example person", human)}})
	return b
}

func TestRolePropertyDirectionsAndConservation(t *testing.T) {
	claims := map[string]any{}
	for _, p := range []string{"P108", "P112", "P169", "P3320", "P127", "P1830", "P488", "P39"} {
		r := roleRow("Q1", p, "Q2")
		r["id"] = "Q1$" + p
		claims[p] = []any{r}
	}
	claims["P999"] = []any{roleRow("Q1", "P999", "Q2")}
	b := roleBody(claims)
	r, err := ExtractRoles(b, Hash(b), []string{"Q2", "Q1"})
	if err != nil {
		t.Fatal(err)
	}
	if r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution || len(r.Entities) != 2 || len(r.Entities[0].Statements) != 8 || !slices.Equal(r.ExpectedIDs, []string{"Q1", "Q2"}) {
		t.Fatal("conservation or approval")
	}
	if len(r.Entities[0].Uninterpreted) != 1 || r.Entities[0].Uninterpreted[0] != (PropertyCount{"P999", 1}) {
		t.Fatal("uninterpreted property lost")
	}
	for _, s := range r.Entities[0].Statements {
		if s.RawSHA256 != Hash(s.Raw) || s.SubjectID != "wikidata:Q1" || s.ObjectID != "wikidata:Q2" {
			t.Fatal("source direction lost")
		}
		wantHolder := "wikidata:Q1"
		if slices.Contains([]string{"P112", "P169", "P3320", "P127", "P488"}, s.Property) {
			wantHolder = "wikidata:Q2"
		}
		if s.HolderID != wantHolder || s.HolderHumanStatementObserved != (wantHolder == "wikidata:Q2") {
			t.Fatal("holder orientation/type", s.Property)
		}
		if s.Role == "controlling_owner" {
			t.Fatal("ordinary ownership promoted to control")
		}
		if (s.Property == "P39" || s.Property == "P488") && s.Role != "unknown" {
			t.Fatal("position/chair promoted")
		}
	}
	again, err := ExtractRoles(b, Hash(b), []string{"Q1", "Q2"})
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("unstable replay")
	}
}

func TestRoleRanksQualifiersAndReferences(t *testing.T) {
	row := roleRow("Q1", "P169", "Q2")
	row["rank"] = "deprecated"
	row["qualifiers"] = map[string]any{
		"P580":  []any{roleTimeFixture("P580", "+2015-00-00T00:00:00Z", 9), roleTimeFixture("P580", "+2016-05-00T00:00:00Z", 10)},
		"P582":  []any{roleTimeFixture("P582", "+2020-01-02T00:00:00Z", 11)},
		"P585":  []any{roleTimeFixture("P585", "+2017-01-01T00:00:00Z", 9)},
		"P3831": []any{roleItem("P3831", "Q3")},
	}
	row["references"] = []any{map[string]any{"hash": "source", "snaks": map[string]any{"P813": []any{roleTimeFixture("P813", "+2026-09-15T00:00:00Z", 11)}}}}
	b := roleBody(map[string]any{"P169": []any{row, row}})
	r, err := ExtractRoles(b, Hash(b), []string{"Q1", "Q2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range r.Entities[0].Statements {
		if len(s.Times) != 4 || s.ReferenceCount != 1 || s.Times[0].Text != "2015" || s.Times[1].Text != "2016-05" || s.Times[2].Text != "2020-01-02" || s.Times[3].Text != "2017" {
			t.Fatal("precision or reference/validity boundary")
		}
		for _, issue := range []string{"duplicate_statement_id", "deprecated_statement", "multiple_time_qualifiers:P580", "uninterpreted_qualifier:P3831"} {
			if !slices.Contains(s.Issues, issue) {
				t.Fatal("missing issue", issue)
			}
		}
	}
	if r.Entities[0].Statements[0].Locator == r.Entities[0].Statements[1].Locator {
		t.Fatal("duplicate occurrences collapsed")
	}
}

func TestRoleMalformedStatementsStayVisible(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"unknown key", func(r map[string]any) { r["future"] = true }},
		{"wrong subject", func(r map[string]any) { r["id"] = "Q3$elsewhere" }},
		{"unknown rank", func(r map[string]any) { r["rank"] = "current" }},
		{"null main", func(r map[string]any) { r["mainsnak"] = nil }},
		{"wrong property", func(r map[string]any) { r["mainsnak"] = roleItem("P108", "Q2") }},
		{"wrong datatype", func(r map[string]any) { r["mainsnak"].(map[string]any)["datatype"] = "string" }},
		{"numeric identity conflict", func(r map[string]any) {
			r["mainsnak"].(map[string]any)["datavalue"].(map[string]any)["value"].(map[string]any)["numeric-id"] = 3
		}},
		{"null qualifiers", func(r map[string]any) { r["qualifiers"] = nil }},
		{"invalid reference", func(r map[string]any) { r["references"] = []any{42} }},
		{"bad qualifier order", func(r map[string]any) { r["qualifiers-order"] = []any{"P580"} }},
		{"null temporal qualifier", func(r map[string]any) { r["qualifiers"] = map[string]any{"P580": []any{nil}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := roleRow("Q1", "P169", "Q2")
			tc.change(row)
			b := roleBody(map[string]any{"P169": []any{row}})
			r, e := ExtractRoles(b, Hash(b), []string{"Q1", "Q2"})
			if e != nil {
				t.Fatal(e)
			}
			if len(r.Entities[0].Statements) != 1 || len(r.Entities[0].Statements[0].Issues) < 1 {
				t.Fatal("lost invalid occurrence")
			}
		})
	}
	for _, kind := range []string{"somevalue", "novalue"} {
		row := roleRow("Q1", "P169", "Q2")
		row["mainsnak"] = map[string]any{"property": "P169", "datatype": "wikibase-item", "snaktype": kind}
		b := roleBody(map[string]any{"P169": []any{row}})
		r, e := ExtractRoles(b, Hash(b), []string{"Q1", "Q2"})
		if e != nil {
			t.Fatal(e)
		}
		s := r.Entities[0].Statements[0]
		if s.ObjectID != "" || s.SnakType != kind || !slices.Contains(s.Issues, "source_"+kind) {
			t.Fatal("source unknown/no-value lost")
		}
	}
}

func TestRoleTimePreservesUnsupportedAndUnknown(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		value     any
	}{
		{"Julian", "calendarmodel", "http://www.wikidata.org/entity/Q1985786"},
		{"before uncertainty", "before", 1}, {"after uncertainty", "after", 1}, {"timezone", "timezone", 60},
		{"century", "precision", 7}, {"missing precision", "precision", nil}, {"invalid day", "time", "+2023-02-30T00:00:00Z"},
		{"BCE", "time", "-0001-01-01T00:00:00Z"}, {"timestamp", "time", "+2023-01-01T12:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := roleTimeFixture("P580", "+2023-01-01T00:00:00Z", 11)
			v["datavalue"].(map[string]any)["value"].(map[string]any)[tc.key] = tc.value
			b, _ := json.Marshal(v)
			r := extractRoleTime("P580", 0, b)
			if !strings.HasPrefix(r.State, "unsupported_") || r.Text != "" || !reflect.DeepEqual([]byte(r.Raw), b) {
				t.Fatal("time silently repaired", r.State)
			}
		})
	}
	for _, kind := range []string{"somevalue", "novalue"} {
		b, _ := json.Marshal(map[string]any{"property": "P580", "datatype": "time", "snaktype": kind})
		if r := extractRoleTime("P580", 0, b); r.State != "source_"+kind {
			t.Fatal("time unknown/no-value collapsed")
		}
	}
}

func TestRoleInputFailures(t *testing.T) {
	b := roleBody(map[string]any{})
	for _, ids := range [][]string{nil, {"Q1"}, {"Q1", "Q1"}, {"Q1", "not-an-id"}} {
		if _, e := ExtractRoles(b, Hash(b), ids); e == nil {
			t.Fatal("accepted wrong expected item set")
		}
	}
	if _, e := ExtractRoles(b, strings.Repeat("a", 64), []string{"Q1", "Q2"}); e == nil {
		t.Fatal("accepted bad pin")
	}
	for _, body := range [][]byte{[]byte(`{"error":{"code":"maxlag"},"servedby":"test"}`), []byte(`{"entities":{},"success":1,"success":1}`), []byte(`{"entities":{},"success":1,"future":true}`)} {
		if _, e := ExtractRoles(body, Hash(body), []string{"Q1"}); e == nil {
			t.Fatal("accepted failed source")
		}
	}
}

func TestRoleEndpointAndHumanEvidenceStates(t *testing.T) {
	for _, tc := range []struct {
		name                string
		missing, deprecated bool
		target              string
		want                string
	}{
		{"unloaded", false, false, "Q3", "holder_entity_not_loaded"},
		{"missing", true, false, "Q2", "holder_source_entity_missing"},
		{"deprecated human assertion", false, true, "Q2", "holder_human_statement_not_observed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := roleBody(map[string]any{"P169": []any{roleRow("Q1", "P169", tc.target)}})
			var root map[string]any
			_ = json.Unmarshal(b, &root)
			entities := root["entities"].(map[string]any)
			if tc.missing {
				entities["Q2"] = map[string]any{"id": "Q2", "missing": true}
			}
			if tc.deprecated {
				entities["Q2"].(map[string]any)["claims"].(map[string]any)["P31"].([]any)[0].(map[string]any)["rank"] = "deprecated"
			}
			b, _ = json.Marshal(root)
			r, e := ExtractRoles(b, Hash(b), []string{"Q1", "Q2"})
			if e != nil {
				t.Fatal(e)
			}
			s := r.Entities[0].Statements[0]
			if s.HolderHumanStatementObserved || !slices.Contains(s.Issues, tc.want) {
				t.Fatal("endpoint evidence promoted or gap hidden")
			}
			if tc.missing && r.Entities[1].State != "source_entity_missing" {
				t.Fatal("missing item became no roles")
			}
		})
	}
}

func TestRetainedRoleCorpusBackoffIsNotAbsence(t *testing.T) {
	for name, pin := range map[string]string{
		"wikidata-maxlag-1.json": "c476a4cac300b5467c8773ee95f75f1d43bb0e280421ec702273d507bad2f1e9",
		"wikidata-maxlag-2.json": "6e1da6cb47ea6082868dc6fbc87022578210b7d515682f15a6dd6f92fcf54b5b",
		"wikidata-maxlag-3.json": "a6ffbabde4577b4fa8a98fb5b5399a235b84a169563af4ee3a685b24859af956",
	} {
		b, e := os.ReadFile(filepath.Join("../../../tests/fixtures/person-affiliation", name))
		if e != nil {
			t.Fatal(e)
		}
		if Hash(b) != pin {
			t.Fatal("failed observation bytes changed")
		}
		if _, e := ExtractRoles(b, pin, []string{"Q1393271", "Q173395", "Q8034666"}); e == nil {
			t.Fatal("live maxlag returned empty evidence")
		}
	}
}

func TestRetainedOrganizationBodiesRoleExtraction(t *testing.T) {
	dir := "../../../tests/fixtures/organization-resolution/capture-v1"
	counts := map[string]int{
		"1488cb69bdbdbff17f4540429f1c9155d78ae3254974d7b4da27d75cee9d5a43.body": 45,
		"170c8d6855097ff0cf4f7fb613f0367151c58f18afedb465d8e8d1b53b45f209.body": 3,
		"a107a21854241e9e5f0d54df09a22aafdea4cfcabb27c969c6d033a07802d109.body": 5,
	}
	for file, want := range counts {
		t.Run(file[:8], func(t *testing.T) {
			b, e := os.ReadFile(filepath.Join(dir, file))
			if e != nil {
				t.Fatal(e)
			}
			var envelope struct{ Entities map[string]json.RawMessage }
			if e = json.Unmarshal(b, &envelope); e != nil {
				t.Fatal(e)
			}
			ids := []string{}
			for id := range envelope.Entities {
				ids = append(ids, id)
			}
			r, e := ExtractRoles(b, strings.TrimSuffix(file, ".body"), ids)
			if e != nil {
				t.Fatal(e)
			}
			n := 0
			for _, entity := range r.Entities {
				n += len(entity.Statements)
				for _, s := range entity.Statements {
					if s.RawSHA256 != Hash(s.Raw) {
						t.Fatal("raw identity mismatch")
					}
					if (s.Property == "P39" || s.Property == "P488") && s.Role != "unknown" {
						t.Fatal("general office or chair promoted to corporate authority")
					}
				}
			}
			if n != want {
				t.Fatalf("selected count: got %d want %d", n, want)
			}
		})
	}
}
