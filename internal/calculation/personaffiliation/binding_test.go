package personaffiliation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

// Invented source bodies go through the real parser; typed role annotations do
// not supply the expected interpretation to the binding diagnostic.
func bindingEntity(id, name string, claims map[string]any) map[string]any {
	return map[string]any{"id": id, "type": "item", "pageid": 1, "ns": 0, "title": id,
		"lastrevid": 1, "modified": "2026-09-16T00:00:00Z",
		"labels": map[string]any{"en": map[string]any{"language": "en", "value": name}}, "claims": claims}
}

func bindingRole(subject, property, object string) map[string]any {
	return map[string]any{"id": subject + "$" + property + "-" + object, "type": "statement", "rank": "normal",
		"mainsnak": map[string]any{"property": property, "snaktype": "value", "datatype": "wikibase-item",
			"datavalue": map[string]any{"type": "wikibase-entityid", "value": map[string]any{"id": object, "entity-type": "item"}}}}
}

func bindingTime(property, value string, precision int) map[string]any {
	return map[string]any{"property": property, "snaktype": "value", "datatype": "time",
		"datavalue": map[string]any{"type": "time", "value": map[string]any{"time": value, "precision": precision,
			"timezone": 0, "before": 0, "after": 0, "calendarmodel": "http://www.wikidata.org/entity/Q1985727"}}}
}

func bindingEvidence(t *testing.T, entities map[string]any) wikimedia.RoleEvidence {
	t.Helper()
	b, err := json.Marshal(map[string]any{"success": 1, "entities": entities})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for id := range entities {
		ids = append(ids, id)
	}
	e, err := wikimedia.ExtractRoles(b, wikimedia.Hash(b), ids)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func bindingAssessment(t *testing.T, a Appearance, e wikimedia.RoleEvidence) BindingResult {
	t.Helper()
	r, err := AssessBinding(a, e)
	if err != nil {
		t.Fatal(err)
	}
	if r.IdentityResolved || r.GraphPublicationApproved || r.FinancialAttribution || !reflect.DeepEqual(r.Appearance, a) || r.BodySHA256 != e.BodySHA256 {
		t.Fatal("appearance mutated or correspondence became identity/finance approval")
	}
	return r
}

func TestBindingSeparatesNamesEmployerAndRoleTime(t *testing.T) {
	a, _ := fixture()
	current := bindingRole("Q3", "P169", "Q1")
	current["qualifiers"] = map[string]any{
		"P580": []any{bindingTime("P580", "+2020-00-00T00:00:00Z", 9)},
		"P582": []any{bindingTime("P582", "+2025-00-00T00:00:00Z", 9)},
	}
	old := bindingRole("Q3", "P169", "Q2")
	old["rank"] = "deprecated"
	old["qualifiers"] = map[string]any{"P582": []any{bindingTime("P582", "+2015-00-00T00:00:00Z", 9)}}
	other := bindingRole("Q4", "P3320", "Q1")
	e := bindingEvidence(t, map[string]any{
		"Q1": bindingEntity("Q1", "Alex Example", map[string]any{}),
		"Q2": bindingEntity("Q2", "Alex Example", map[string]any{}),
		"Q3": bindingEntity("Q3", "Example Corp", map[string]any{"P169": []any{current, current, old}}),
		"Q4": bindingEntity("Q4", "Another Organization", map[string]any{"P3320": []any{other}}),
	})
	r := bindingAssessment(t, a, e)
	if r.NameState != "multiple_name_candidates_identity_unresolved" || len(r.Candidates) != 2 {
		t.Fatal("expired/deprecated rival erased", r.NameState)
	}
	if len(r.Candidates[0].Roles) != 3 || len(r.Candidates[1].Roles) != 1 {
		t.Fatal("duplicate or multiple affiliations dropped")
	}
	for _, c := range r.Candidates {
		if c.EmployerState != "employer_name_correspondence_not_identity" {
			t.Fatal("existing explained employer normalization not used")
		}
	}
	if r.Candidates[0].Roles[0].TemporalState != "within_reported_bounds" || r.Candidates[1].Roles[0].TemporalState != "after_reported_end_precision" {
		t.Fatal("time comparison did not stay separate")
	}
	if r.Candidates[0].Roles[2].TemporalState != "role_time_unknown" || r.Candidates[0].Roles[2].EndpointState != "employer_name_not_corresponding" {
		t.Fatal("second organization inherited anchor employer or dates")
	}
	if !slices.Contains(r.Candidates[1].Roles[0].Statement.Issues, "deprecated_statement") {
		t.Fatal("deprecated rank issue lost")
	}
	before, _ := json.Marshal(e)
	slices.Reverse(e.Entities)
	for i := range e.Entities {
		slices.Reverse(e.Entities[i].Statements)
	}
	again := bindingAssessment(t, a, e)
	if !reflect.DeepEqual(r, again) {
		t.Fatal("input order changed assessment")
	}
	// Restore the test's reordering, then verify the function did not mutate input.
	slices.Reverse(e.Entities)
	for i := range e.Entities {
		slices.Reverse(e.Entities[i].Statements)
	}
	after, _ := json.Marshal(e)
	if string(before) != string(after) {
		t.Fatal("source reader output mutated")
	}
}

func TestBindingNameRulesDoNotRepairPeopleOrMergeOrganizations(t *testing.T) {
	a, _ := fixture()
	for _, tc := range []struct {
		name, employer string
		aliases        bool
		wantName       bool
		wantEmployer   string
	}{
		{"Alex Example", "Example Corporation", false, true, "employer_name_correspondence_not_identity"},
		{"ALEX, EXAMPLE.", "Example Corp", false, true, "employer_name_correspondence_not_identity"},
		{"Alex Example", "Example Bank", false, true, "no_corresponding_employer_endpoint_observed"},
		{"Alex Example", "Example Holdings", false, true, "no_corresponding_employer_endpoint_observed"},
		{"Alex Q Example", "Example Corporation", false, false, ""},
		{"A Example", "Example Corporation", false, false, ""},
		{"Alex Exampel", "Example Corporation", false, false, ""},
		{"Alexander Example", "Example Corporation", false, false, ""},
		{"Alexander Example", "Example Corporation", true, true, "employer_name_correspondence_not_identity"},
	} {
		t.Run(tc.name+tc.employer+fmt.Sprint(tc.aliases), func(t *testing.T) {
			person := bindingEntity("Q1", tc.name, map[string]any{})
			if tc.aliases {
				person["aliases"] = map[string]any{"en": []any{map[string]any{"language": "en", "value": "Alex Example"}}}
			}
			e := bindingEvidence(t, map[string]any{
				"Q1": person,
				"Q2": bindingEntity("Q2", tc.employer, map[string]any{"P169": []any{bindingRole("Q2", "P169", "Q1")}}),
			})
			r := bindingAssessment(t, a, e)
			if (len(r.Candidates) == 1) != tc.wantName {
				t.Fatal("invented or lost source name correspondence")
			}
			if tc.wantName && r.Candidates[0].EmployerState != tc.wantEmployer {
				t.Fatal("organization semantic words collapsed")
			}
			if tc.aliases && (len(r.Candidates[0].Names) != 1 || r.Candidates[0].Names[0].Source != "english_alias:0") {
				t.Fatal("alias correspondence lost its source")
			}
		})
	}
}

func TestBindingKeepsNoRoleCandidatesAndUnloadedOrganizations(t *testing.T) {
	a, _ := fixture()
	e := bindingEvidence(t, map[string]any{
		"Q1": bindingEntity("Q1", "Alex Example", map[string]any{"P108": []any{bindingRole("Q1", "P108", "Q99")}}),
		"Q2": bindingEntity("Q2", "Alex Example", map[string]any{}),
	})
	r := bindingAssessment(t, a, e)
	if len(r.Candidates) != 2 || len(r.Candidates[1].Roles) != 0 || r.Candidates[0].Roles[0].EndpointState != "endpoint_not_loaded" {
		t.Fatal("no-role rival or missing endpoint erased")
	}
	if r.Candidates[0].Roles[0].Statement.Role != "employee" {
		t.Fatal("reported CEO occupation promoted employment to executive role")
	}
	a.Receipt.Cycle = 1980
	s := "ENGINEER"
	a.Receipt.Occupation = &s
	again := bindingAssessment(t, a, e)
	if !reflect.DeepEqual(r.Candidates, again.Candidates) {
		t.Fatal("cycle or occupation changed identity/role interpretation")
	}
	a.Receipt.Name = nil
	if r := bindingAssessment(t, a, e); len(r.Candidates) != 0 || r.NameState != "reported_name_unavailable" {
		t.Fatal("unknown name created identity")
	}
	a.Source.SHA256 = "bad"
	if _, err := AssessBinding(a, e); err == nil {
		t.Fatal("unbound appearance accepted")
	}
	a, _ = fixture()
	e.Contract = "invented"
	if _, err := AssessBinding(a, e); err == nil {
		t.Fatal("wrong evidence contract accepted")
	}
	if _, err := AssessBinding(a, wikimedia.RoleEvidence{}); err == nil {
		t.Fatal("missing source became empty search")
	}
}

func TestBindingRoleDatePrecision(t *testing.T) {
	for _, tc := range []struct {
		name, day, start, end, asOf, want string
	}{
		{"interior_years", "2024-06-01", "2020", "2025", "", "within_reported_bounds"},
		{"start_year", "2020-12-31", "2020", "2025", "", "within_boundary_precision_unconfirmed"},
		{"end_year", "2025-01-01", "2020", "2025", "", "within_boundary_precision_unconfirmed"},
		{"before_start", "2019-12-31", "2020", "2025", "", "before_reported_start_precision"},
		{"after_end", "2026-01-01", "2020", "2025", "", "after_reported_end_precision"},
		{"start_month", "2024-03-31", "2024-03", "2024-12", "", "within_boundary_precision_unconfirmed"},
		{"end_month", "2024-12-01", "2024-03", "2024-12", "", "within_boundary_precision_unconfirmed"},
		{"interior_months", "2024-04-01", "2024-03", "2024-12", "", "within_reported_bounds"},
		{"exact_start", "2024-03-01", "2024-03-01", "2024-12-31", "", "within_reported_bounds"},
		{"exact_end", "2024-12-31", "2024-03-01", "2024-12-31", "", "within_reported_bounds"},
		{"mixed_precision", "2024-04-01", "2024", "2024-12-31", "", "within_boundary_precision_unconfirmed"},
		{"open_future", "2024-06-01", "2025", "", "", "before_reported_start_precision"},
		{"open_past", "2024-06-01", "", "2015", "", "after_reported_end_precision"},
		{"open_end", "2024-06-01", "2020", "", "", "open_period_unconfirmed"},
		{"open_start", "2024-06-01", "", "2025", "", "open_period_unconfirmed"},
		{"missing", "2024-06-01", "", "", "", "role_time_unknown"},
		{"no_receipt_day", "", "2020", "2025", "", "receipt_date_unknown"},
		{"bad_receipt_day", "2024-02-30", "2020", "2025", "", "receipt_date_unusable"},
		{"timestamp", "2024-06-01 12:45:00.000", "2020", "2025", "", "within_reported_bounds"},
		{"as_of_day", "2024-06-01", "", "", "2024-06-01", "on_reported_as_of_day"},
		{"as_of_year", "2024-06-01", "", "", "2024", "within_as_of_precision_unconfirmed"},
		{"other_as_of", "2023-06-01", "", "", "2024", "different_as_of_period_not_role_exclusion"},
		{"mixed_as_of", "2024-06-01", "2020", "2025", "2024", "mixed_as_of_and_period_unassessed"},
		{"inverted", "2024-06-01", "2025", "2020", "", "inconsistent_reported_bounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := bindingRole("Q2", "P169", "Q1")
			q := map[string]any{}
			for p, text := range map[string]string{"P580": tc.start, "P582": tc.end, "P585": tc.asOf} {
				if text == "" {
					continue
				}
				precision := map[int]int{4: 9, 7: 10, 10: 11}[len(text)]
				value := text
				for len(value) < 10 {
					value += "-00"
				}
				q[p] = []any{bindingTime(p, "+"+value+"T00:00:00Z", precision)}
			}
			row["qualifiers"] = q
			e := bindingEvidence(t, map[string]any{
				"Q1": bindingEntity("Q1", "Alex Example", map[string]any{}),
				"Q2": bindingEntity("Q2", "Example Corporation", map[string]any{"P169": []any{row}}),
			})
			a, _ := fixture()
			a.Receipt.ReceiptDate = &tc.day
			r := bindingAssessment(t, a, e)
			if got := r.Candidates[0].Roles[0].TemporalState; got != tc.want {
				t.Fatal(got, "want", tc.want)
			}
		})
	}
	// Multiple constraints and unsupported source time profiles never choose a
	// favorable value or become a fabricated interval.
	for _, tc := range []struct {
		qualifiers map[string]any
		want       string
	}{
		{map[string]any{"P582": []any{bindingTime("P582", "+2015-00-00T00:00:00Z", 9), bindingTime("P582", "+2025-00-00T00:00:00Z", 9)}}, "multiple_time_values_unassessed"},
		{map[string]any{"P582": []any{bindingTime("P582", "+2025-00-00T00:00:00Z", 8)}}, "unsupported_or_unknown_time_value"},
	} {
		row := bindingRole("Q2", "P169", "Q1")
		row["qualifiers"] = tc.qualifiers
		e := bindingEvidence(t, map[string]any{
			"Q1": bindingEntity("Q1", "Alex Example", map[string]any{}),
			"Q2": bindingEntity("Q2", "Example Corporation", map[string]any{"P169": []any{row}}),
		})
		a, _ := fixture()
		if got := bindingAssessment(t, a, e).Candidates[0].Roles[0].TemporalState; got != tc.want {
			t.Fatal(got, tc.want)
		}
	}
	if got := compareRoleDate(nil, wikimedia.RoleStatement{}); got != "receipt_date_unknown" {
		t.Fatal(got)
	}
	if got := compareRoleDate(strptr("2024-01-01"), wikimedia.RoleStatement{Times: []wikimedia.RoleTime{{Property: "P580", State: "year_precision", Text: strings.Repeat("1", 100)}}}); got != "unsupported_or_unknown_time_value" {
		t.Fatal("invalid typed time profile", got)
	}
}

func strptr(s string) *string { return &s }
