package personaffiliation

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func relevanceFixture() wikimedia.DiscoveryResult {
	name, employer := "Alex Example", "Route7 Inc"
	person := wikimedia.RoleEntity{ID: "Q1", Label: "Alex Q. Example", Aliases: []string{"Alex Example"}, Revision: 1}
	company := wikimedia.RoleEntity{ID: "Q2", Label: "Route 7", Revision: 2, Statements: []wikimedia.RoleStatement{
		{HolderID: "wikidata:Q1", RelatedEntityID: "wikidata:Q2", Property: "P169", Role: "executive", Rank: "deprecated", Issues: []string{"deprecated_rank"}, Locator: "/entities/Q2/claims/P169/0", Raw: []byte(`{"rank":"deprecated"}`)},
	}}
	person.Statements = []wikimedia.RoleStatement{{HolderID: "wikidata:Q1", RelatedEntityID: "wikidata:Q3", Property: "P108", Role: "employee"}}
	return wikimedia.DiscoveryResult{CaptureUsable: true, Plan: wikimedia.DiscoveryPlan{Appearances: []wikimedia.DiscoveryAppearance{{Name: &name, Employer: &employer}}}, Observations: []wikimedia.DiscoveryObservation{{
		Search: wikimedia.DiscoverySearch{Appearances: []int{0}}, State: "retrieved_page_candidates_not_identities", Candidates: []wikimedia.DiscoveryCandidate{
			{QID: "Q1", PageID: 1, State: "type_unverified", HumanStatementObserved: true},
			{QID: "Q2", PageID: 2, State: "type_unverified"},
		}, Roles: &wikimedia.RoleEvidence{BodySHA256: wikimedia.Hash([]byte("body")), Entities: []wikimedia.RoleEntity{person, company}},
	}}}
}

func TestDiscoveryRelevanceDoesNotApproveOrUseSearchContext(t *testing.T) {
	r := relevanceFixture()
	before, _ := json.Marshal(r)
	got := AssessDiscovery(r, "build")
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("source mutated")
	}
	if got.IdentityApproved || got.EmploymentVerified || got.GraphPublicationApproved || got.FinancialAttribution || got.ExhaustiveDiscovery {
		t.Fatal("unexpected approval")
	}
	c := got.Observations[0].Candidates[0]
	if c.PersonNameState != "name_corresponds_human_item" || len(c.PersonNames) != 1 || c.PersonNames[0].Source != "english_alias:0" || len(c.Roles) != 2 {
		t.Fatal("name/role evidence lost", c)
	}
	if c.Roles[0].EndpointState != "endpoint_not_loaded" || c.Roles[1].EndpointState != "employer_name_corresponds_type_unverified" || c.Roles[1].EmployerNames[0].Rule != "digit_letter_boundaries_and_legal_suffix_variant" {
		t.Fatal("endpoint evidence")
	}
	if c.Roles[1].TemporalState != "not_assessed" || !reflect.DeepEqual(c.Roles[1].Statement, r.Observations[0].Roles.Entities[1].Statements[0]) {
		t.Fatal("rank/time/issues/raw lost")
	}
	// An exact name/employer search with a sole person hit is still not role evidence.
	r.Observations[0].Roles.Entities = r.Observations[0].Roles.Entities[:1]
	r.Observations[0].Roles.Entities[0].Statements = nil
	r.Observations[0].Search.Kind, r.Observations[0].Search.Text = "person_name_employer", `"Alex" "Example" "Route7" "Inc"`
	c = AssessDiscovery(r, "build").Observations[0].Candidates[0]
	if len(c.Roles) != 0 || len(c.EmployerNames) != 0 {
		t.Fatal("query terms became evidence")
	}
}

func TestDiscoveryRelevanceNamesFailuresAndResponseBoundaries(t *testing.T) {
	for _, name := range []string{"Alex Q. Example", "A. Example", "Example, Alex", "Alexander Example", "Alex Exampel"} {
		r := relevanceFixture()
		r.Observations[0].Roles.Entities[0].Aliases = nil
		r.Observations[0].Roles.Entities[0].Label = name
		if c := AssessDiscovery(r, "build").Observations[0].Candidates[0]; len(c.PersonNames) != 0 {
			t.Fatal("invented person variant", name)
		}
	}
	for _, state := range []string{"disambiguation_page", "source_entity_missing", "entity_response_unusable", "page_has_no_qid"} {
		r := relevanceFixture()
		r.Observations[0].Candidates[0].State = state
		if c := AssessDiscovery(r, "build").Observations[0].Candidates[0]; c.PersonNameState != "source_unusable" || len(c.PersonNames)+len(c.Roles) != 0 {
			t.Fatal("unusable candidate used", state)
		}
	}
	r := relevanceFixture()
	r.CaptureUsable = false
	r.Observations[0].Issue = "maxlag"
	got := AssessDiscovery(r, "build")
	if got.CaptureUsable || got.Observations[0].Candidates[0].PersonNameState != "source_unusable" {
		t.Fatal("failure became absence")
	}
	r = relevanceFixture()
	other := r.Observations[0]
	other.Roles = &wikimedia.RoleEvidence{Entities: []wikimedia.RoleEntity{{ID: "Q3", Label: "Route7 Inc"}}}
	r.Observations = append(r.Observations, other)
	if got := AssessDiscovery(r, "build"); got.Observations[0].Candidates[0].Roles[0].EndpointState != "endpoint_not_loaded" {
		t.Fatal("cross-response endpoint merge")
	}
	r = relevanceFixture()
	r.Observations[0].Roles.Entities = append(r.Observations[0].Roles.Entities, wikimedia.RoleEntity{ID: "Q3", State: "source_entity_missing"})
	if got := AssessDiscovery(r, "build"); got.Observations[0].Candidates[0].Roles[0].EndpointState != "endpoint_missing" {
		t.Fatal("missing endpoint used")
	}
	// Rival name matches stay separate, never become votes or a winner.
	r = relevanceFixture()
	rival := r.Observations[0].Roles.Entities[0]
	rival.ID = "Q4"
	r.Observations[0].Roles.Entities = append(r.Observations[0].Roles.Entities, rival)
	r.Observations[0].Candidates = append(r.Observations[0].Candidates, wikimedia.DiscoveryCandidate{QID: "Q4", PageID: 4, State: "type_unverified", HumanStatementObserved: true})
	got = AssessDiscovery(r, "build")
	if len(got.Observations[0].Candidates) != 3 || got.Observations[0].Candidates[2].PersonNameState != "name_corresponds_human_item" || got.IdentityApproved {
		t.Fatal("rival lost")
	}
}

func TestDiscoveryRelevanceMissingInputsAndUnverifiedTypes(t *testing.T) {
	r := relevanceFixture()
	r.Observations[0].Candidates[0].HumanStatementObserved = false
	if c := AssessDiscovery(r, "build").Observations[0].Candidates[0]; c.PersonNameState != "name_corresponds_type_unverified" {
		t.Fatal("name became human type proof")
	}
	blank := "  "
	for _, value := range []*string{nil, &blank} {
		r.Plan.Appearances[0].Name, r.Plan.Appearances[0].Employer = value, value
		c := AssessDiscovery(r, "build").Observations[0].Candidates[0]
		if c.PersonNameState != "reported_name_unavailable" || c.Roles[1].EndpointState != "reported_employer_unavailable" {
			t.Fatal("missing inputs became mismatches")
		}
	}
	r.Observations = nil
	r.Plan.InputStates = []string{"no_searchable_text"}
	got := AssessDiscovery(r, "build")
	if len(got.Appearances) != 1 || len(got.Observations) != 0 || got.InputStates[0] != "no_searchable_text" {
		t.Fatal("unqueried appearance lost")
	}
}

func TestRetainedDiscoveryRelevance(t *testing.T) {
	r, err := wikimedia.ReadDiscovery("../../../tests/fixtures/person-affiliation/discovery-v1", "c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c")
	if err != nil {
		t.Fatal(err)
	}
	got := AssessDiscovery(r, "build")
	if !reflect.DeepEqual(got, AssessDiscovery(r, "build")) || !got.CaptureUsable || len(got.Observations) != 12 {
		t.Fatal("replay/membership")
	}
	matches := map[string]int{}
	for _, o := range got.Observations {
		for _, c := range o.Candidates {
			if c.PersonNameState == "name_corresponds_human_item" {
				matches[c.QID]++
			}
			for _, s := range c.Roles {
				if len(s.EmployerNames) > 0 {
					t.Fatal("baseline unexpectedly supports employer", c.QID)
				}
			}
		}
	}
	if matches["Q5233095"] != 3 || matches["Q1393271"] != 2 || matches["Q93784"] != 2 {
		t.Fatal("retained name correspondence changed", matches)
	}
}

func TestRetainedVariantsPreserveNamesakeAndNonCompanyRivals(t *testing.T) {
	r, err := wikimedia.ReadDiscovery("../../../tests/fixtures/person-affiliation/discovery-v2", "5ff30d1c86bd6e4b2b7746f88628057f30d03220a6b27a6853fc8124e04a9c81")
	if err != nil {
		t.Fatal(err)
	}
	got := AssessDiscovery(r, "build")
	if len(got.Appearances) != 4 || len(got.Observations) != 20 || !got.CaptureUsable || got.IdentityApproved || got.EmploymentVerified || got.FinancialAttribution {
		t.Fatal("conservation/approval")
	}
	personIDs := map[string]bool{}
	vehicleNameMatch := false
	for _, o := range got.Observations {
		for _, c := range o.Candidates {
			if c.PersonNameState == "name_corresponds_human_item" {
				personIDs[c.QID] = true
			}
			if c.QID == "Q1626665" && len(c.EmployerNames) == 1 && c.EmployerNames[0].Name == "Ridgeline" {
				vehicleNameMatch = true
			}
			for _, check := range c.Roles {
				if len(check.EmployerNames) != 0 {
					t.Fatal("new employer evidence unexpectedly appears")
				}
			}
		}
	}
	if !reflect.DeepEqual(personIDs, map[string]bool{"Q5233095": true, "Q1393271": true, "Q93784": true}) || !vehicleNameMatch {
		t.Fatal("namesake/type counterexamples lost")
	}
	if !reflect.DeepEqual(got, AssessDiscovery(r, "build")) {
		t.Fatal("variant replay changed")
	}
}
