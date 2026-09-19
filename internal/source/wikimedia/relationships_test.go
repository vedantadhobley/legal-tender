package wikimedia

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func relationshipQuery(t *testing.T, raw []byte, ids []string, id string) RelationshipQuery {
	t.Helper()
	r, err := QueryRelationships(raw, Hash(raw), ids, id, "")
	if err != nil {
		t.Fatal(err)
	}
	if r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution || r.Scope != "one_hop_in_supplied_response" {
		t.Fatal("query approved identity, graph or finance")
	}
	return r
}

func TestRelationshipQueryInverseHierarchyAndUnknownEndpoint(t *testing.T) {
	b := roleBody(map[string]any{"P749": []any{roleRow("Q1", "P749", "Q2"), roleRow("Q1", "P749", "Q3")}})
	var body map[string]any
	if err := json.Unmarshal(b, &body); err != nil {
		t.Fatal(err)
	}
	entities := body["entities"].(map[string]any)
	entities["Q2"].(map[string]any)["claims"].(map[string]any)["P355"] = []any{roleRow("Q2", "P355", "Q1")}
	b, _ = json.Marshal(body)
	r := relationshipQuery(t, b, []string{"Q2", "Q1"}, "wikidata:Q1")
	if len(r.Matches) != 3 || r.SelectedStatementsScanned != 3 {
		t.Fatal("multiple parents/inverse occurrences lost")
	}
	parents := map[string]int{}
	for _, m := range r.Matches {
		if m.Hierarchy == nil || m.Hierarchy.ChildID != "wikidata:Q1" || m.Statement.HolderID != "" || m.Statement.Role != "not_applicable" {
			t.Fatal("hierarchy promoted to personal role or wrong orientation")
		}
		parents[m.Hierarchy.ParentID]++
		if m.Statement.Property == "P355" && (m.Statement.SubjectID != "wikidata:Q2" || m.Statement.ObjectID != "wikidata:Q1") {
			t.Fatal("source direction changed")
		}
		if m.Statement.ObjectID == "wikidata:Q3" && (m.Object.State != "item_not_loaded" || m.Object.Label != "") {
			t.Fatal("invented endpoint profile")
		}
		if len(m.Statement.Times) != 0 {
			t.Fatal("invented hierarchy dates")
		}
	}
	if parents["wikidata:Q2"] != 2 || parents["wikidata:Q3"] != 1 {
		t.Fatal("inverse claims deduplicated or parent selected")
	}
	unloaded := relationshipQuery(t, b, []string{"Q1", "Q2"}, "wikidata:Q3")
	if unloaded.Entity.State != "item_not_loaded" || len(unloaded.Matches) != 1 {
		t.Fatal("unloaded target cannot be queried")
	}
	empty := relationshipQuery(t, b, []string{"Q1", "Q2"}, "wikidata:Q999")
	if empty.Entity.State != "item_not_loaded" || len(empty.Matches) != 0 || empty.Matches == nil {
		t.Fatal("empty query incorrectly represented")
	}
	legacy, err := ExtractRoles(b, Hash(b), []string{"Q1", "Q2"})
	if err != nil || len(legacy.Entities[0].Statements) != 0 || len(legacy.Entities[1].Statements) != 0 || legacy.Contract != RoleContract {
		t.Fatal("old role scope changed", err)
	}
}

func TestRelationshipQueryPreservesProblematicOccurrences(t *testing.T) {
	unknown := roleRow("Q1", "P355", "Q2")
	unknown["mainsnak"] = map[string]any{"snaktype": "somevalue", "property": "P355", "datatype": "wikibase-item"}
	noValue := roleRow("Q1", "P355", "Q2")
	noValue["mainsnak"] = map[string]any{"snaktype": "novalue", "property": "P355", "datatype": "wikibase-item"}
	bad := roleRow("Q1", "P355", "Q2")
	bad["unreviewed_field"] = true
	self := roleRow("Q1", "P749", "Q1")
	self["rank"] = "deprecated"
	self["qualifiers"] = map[string]any{
		"P580":  []any{roleTimeFixture("P580", "+1995-01-01T00:00:00Z", 9)},
		"P582":  []any{roleTimeFixture("P582", "+2015-00-00T00:00:00Z", 9)},
		"P1107": []any{map[string]any{"raw_quantity": "not interpreted by this view"}},
	}
	b := roleBody(map[string]any{"P355": []any{unknown, noValue, bad}, "P749": []any{self, self}})
	r := relationshipQuery(t, b, []string{"Q1", "Q2"}, "wikidata:Q1")
	if len(r.Matches) != 5 {
		t.Fatal("malformed, unknown, duplicate or self statement dropped")
	}
	for _, m := range r.Matches {
		if m.Statement.RawSHA256 != Hash(m.Statement.Raw) {
			t.Fatal("raw occurrence lost")
		}
		if m.Statement.Property == "P355" && (m.Object != nil || m.Hierarchy != nil) {
			t.Fatal("invented unknown or unsupported target")
		}
		if m.Statement.Property == "P749" {
			for _, issue := range []string{"self_relation", "deprecated_statement", "duplicate_statement_id", "uninterpreted_qualifier:P1107"} {
				if !slices.Contains(m.Statement.Issues, issue) {
					t.Fatal("missing issue", issue)
				}
			}
			if m.Statement.Times[0].Text != "1995" || m.Statement.Times[1].Text != "2015" {
				t.Fatal("year precision changed")
			}
		}
	}
	if r.ObservedAt != "" || r.ObservationTimeBasis != "not_supplied" {
		t.Fatal("revision time substituted for observation")
	}
	withTime, err := QueryRelationships(b, Hash(b), []string{"Q1", "Q2"}, "wikidata:Q1", "2026-09-16T08:00:00-04:00")
	if err != nil || withTime.ObservationTimeBasis != "caller_supplied" || withTime.ObservedAt != "2026-09-16T08:00:00-04:00" || !reflect.DeepEqual(r.Matches, withTime.Matches) {
		t.Fatal("observation time changed relationships", err)
	}
}

func TestRelationshipQuerySourceMissingIsNotUnloaded(t *testing.T) {
	b := roleBody(map[string]any{"P355": []any{roleRow("Q1", "P355", "Q2")}})
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	body["entities"].(map[string]any)["Q2"] = map[string]any{"id": "Q2", "missing": true}
	b, _ = json.Marshal(body)
	r := relationshipQuery(t, b, []string{"Q1", "Q2"}, "wikidata:Q2")
	if r.Entity.State != "source_entity_missing" || len(r.Matches) != 1 || r.Matches[0].Object.State != "source_entity_missing" {
		t.Fatal("source-missing endpoint lost")
	}
}

func TestRelationshipQueryRejectsInvalidInputs(t *testing.T) {
	b := roleBody(map[string]any{})
	for _, id := range []string{"Q1", "wikidata:Q0", "fec:candidate:H0", "John Chambers", "wikidata:Q1 ", "Wikidata:Q1"} {
		if _, err := QueryRelationships(b, Hash(b), []string{"Q1", "Q2"}, id, ""); err == nil {
			t.Fatal("accepted unqualified identity", id)
		}
	}
	for _, ids := range [][]string{nil, {"Q1"}, {"Q1", "Q1"}, {"Q1", "Q2", "Q3"}} {
		if _, err := QueryRelationships(b, Hash(b), ids, "wikidata:Q1", ""); err == nil {
			t.Fatal("accepted wrong expected IDs", ids)
		}
	}
	if _, err := QueryRelationships(b, strings.Repeat("a", 64), []string{"Q1", "Q2"}, "wikidata:Q1", ""); err == nil {
		t.Fatal("accepted wrong pin")
	}
	if _, err := QueryRelationships(b, Hash(b), []string{"Q1", "Q2"}, "wikidata:Q1", "2026-09-16"); err == nil {
		t.Fatal("accepted non-timestamp")
	}
	for _, b := range [][]byte{[]byte(`{"error":{"code":"maxlag"}}`), bytes.Repeat([]byte(" "), MaxBody+1)} {
		if _, err := QueryRelationships(b, Hash(b), []string{"Q1"}, "wikidata:Q1", ""); err == nil {
			t.Fatal("source failure became empty success")
		}
	}
}

func TestRelationshipQueryRetainedExamples(t *testing.T) {
	for _, tc := range []struct {
		file, pin string
	}{
		{"../../../tests/fixtures/person-affiliation/wikidata-interactive-1.json", "5683ad0692e124dcd9d87bcdc81b7448def720f632a2e4e4b66b8aae4f490076"},
		{"../../../tests/fixtures/person-affiliation/discovery-v2/70550b265085b63444770915cf00c183292bc32856da77e411186b1f3b6a4f3f.body", "70550b265085b63444770915cf00c183292bc32856da77e411186b1f3b6a4f3f"},
	} {
		t.Run(tc.pin[:8], func(t *testing.T) {
			b, err := os.ReadFile(tc.file)
			if err != nil || Hash(b) != tc.pin {
				t.Fatal("retained fixture identity", err)
			}
			var source struct {
				Entities map[string]struct {
					Claims map[string][]json.RawMessage `json:"claims"`
				} `json:"entities"`
			}
			if err := json.Unmarshal(b, &source); err != nil {
				t.Fatal(err)
			}
			ids := slices.Sorted(maps.Keys(source.Entities))
			for _, id := range ids {
				r := relationshipQuery(t, b, ids, "wikidata:"+id)
				// Independently enumerate original source arrays and both endpoints;
				// exact occurrence bytes must survive, not just expected totals.
				want := map[string][]byte{}
				for subject, e := range source.Entities {
					for p, rows := range e.Claims {
						if !slices.Contains([]string{"P108", "P112", "P169", "P3320", "P127", "P1830", "P488", "P39", "P355", "P749"}, p) {
							continue
						}
						for n, raw := range rows {
							var s struct {
								Main struct {
									Value struct {
										Value struct {
											ID string `json:"id"`
										} `json:"value"`
									} `json:"datavalue"`
								} `json:"mainsnak"`
							}
							if err := json.Unmarshal(raw, &s); err != nil {
								t.Fatal(err)
							}
							if subject == id || s.Main.Value.Value.ID == id {
								want[fmt.Sprintf("/entities/%s/claims/%s/%d", subject, p, n)] = raw
							}
						}
					}
				}
				if len(r.Matches) != len(want) {
					t.Fatalf("%s occurrences: got %d want %d", id, len(r.Matches), len(want))
				}
				for _, m := range r.Matches {
					if !bytes.Equal(m.Statement.Raw, want[m.Statement.Locator]) {
						t.Fatal("occurrence bytes lost")
					}
					delete(want, m.Statement.Locator)
				}
				if len(want) != 0 {
					t.Fatal("occurrences missing")
				}
				again := relationshipQuery(t, b, reversed(ids), "wikidata:"+id)
				if !reflect.DeepEqual(r, again) {
					t.Fatal("input order changed query")
				}
			}
			if strings.Contains(tc.file, "interactive") {
				checkRelationshipTimeline(t, b, ids)
			} else {
				r := relationshipQuery(t, b, ids, "wikidata:Q1626564")
				parents := map[string]int{}
				for _, m := range r.Matches {
					if m.Hierarchy != nil && m.Hierarchy.ChildID == "wikidata:Q1626564" {
						parents[m.Hierarchy.ParentID]++
					}
				}
				if parents["wikidata:Q4744011"] != 2 || parents["wikidata:Q9584"] != 1 {
					t.Fatal("retained parent/inverse assertions lost", parents)
				}
			}
		})
	}
}

func checkRelationshipTimeline(t *testing.T, b []byte, ids []string) {
	t.Helper()
	r := relationshipQuery(t, b, ids, "wikidata:Q173395")
	terms := map[string][]string{}
	for _, m := range r.Matches {
		if m.Statement.Property == "P169" || m.Statement.Property == "P3320" {
			for _, tm := range m.Statement.Times {
				terms[m.Statement.Property+":"+m.Statement.ObjectID] = append(terms[m.Statement.Property+":"+m.Statement.ObjectID], tm.Text)
			}
		}
	}
	for key, dates := range map[string][]string{
		"P169:wikidata:Q1393271":    {"1995", "2015"},
		"P3320:wikidata:Q113119124": {"2017-03", "2023-02"},
		"P3320:wikidata:Q5214356":   {"2023-10", "2026-05"},
	} {
		if !slices.Equal(terms[key], dates) {
			t.Fatal("coarse terms changed", key, terms[key])
		}
	}
	workday := relationshipQuery(t, b, ids, "wikidata:Q8034666")
	founders := 0
	for _, m := range workday.Matches {
		if m.Statement.Property == "P112" {
			founders++
		}
	}
	if founders != 2 {
		t.Fatal("multiple founders lost")
	}
}

func reversed(ids []string) []string {
	result := slices.Clone(ids)
	slices.Reverse(result)
	return result
}
