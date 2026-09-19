package personaffiliation

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func enrichmentFixture(t *testing.T) (Appearance, wikimedia.DiscoveryResult, string) {
	t.Helper()
	a, _ := fixture()
	build := wikimedia.Hash([]byte("test build"))
	person := bindingEntity("Q1", "Alex Q. Example", map[string]any{})
	person["aliases"] = map[string]any{"en": []any{map[string]any{"language": "en", "value": "Alex Example"}}}
	role := bindingRole("Q3", "P169", "Q1")
	role["qualifiers"] = map[string]any{
		"P580": []any{bindingTime("P580", "+2020-00-00T00:00:00Z", 9)},
		"P582": []any{bindingTime("P582", "+2025-00-00T00:00:00Z", 9)},
	}
	evidence := bindingEvidence(t, map[string]any{
		"Q1": person,
		"Q2": bindingEntity("Q2", "Alex Example", map[string]any{}),
		"Q3": bindingEntity("Q3", "Example Corp", map[string]any{"P169": []any{role}}),
	})
	plan, err := wikimedia.PlanDiscovery([]wikimedia.DiscoveryAppearance{{SHA256: a.Source.SHA256, Locator: a.Source.Locator, Name: a.Receipt.Name, Employer: a.Receipt.Employer}}, "synthetic source inputs", build)
	if err != nil {
		t.Fatal(err)
	}
	d := wikimedia.DiscoveryResult{Plan: plan, CaptureSHA256: wikimedia.Hash([]byte("capture")), CaptureUsable: true}
	for _, search := range plan.Searches {
		d.Observations = append(d.Observations, wikimedia.DiscoveryObservation{Search: search, SearchSHA256: wikimedia.Hash([]byte(search.Text)), State: "retrieved_page_candidates_not_identities", Roles: &evidence,
			Candidates: []wikimedia.DiscoveryCandidate{{PageID: 1, QID: "Q1", State: "type_unverified"}, {PageID: 2, QID: "Q2", State: "type_unverified"}, {PageID: 3, QID: "Q3", State: "type_unverified"}}})
	}
	return a, d, build
}

func TestEnrichmentSourceAliasesDatesAndMissingEmployerRival(t *testing.T) {
	a, d, build := enrichmentFixture(t)
	before, _ := json.Marshal(d)
	r, err := EnrichAppearances([]Appearance{a}, d, build)
	if err != nil {
		t.Fatal(err)
	}
	if r.IdentityResolved || r.GraphPublicationApproved || r.FinancialAttribution || r.Policy != EnrichmentPolicy {
		t.Fatal("approval boundary")
	}
	p := r.Appearances[0]
	if p.NameState != "multiple_name_candidates_identity_unresolved" || !reflect.DeepEqual(p.MatchingSourceIDs, []string{"wikidata:Q1", "wikidata:Q2"}) || len(p.EmployerContextIDs) != 1 || !reflect.DeepEqual(a, p.Appearance) {
		t.Fatal(p.NameState, p.MatchingSourceIDs, p.EmployerContextIDs)
	}
	for _, s := range p.Searches {
		if len(s.Candidates) != 3 {
			t.Fatal("retrieved candidate lost")
		}
		one, two := s.Candidates[0], s.Candidates[1]
		if one.Names.Label != "Alex Q. Example" || len(one.Names.Aliases) != 1 || one.Relevance.PersonNames[0].Source != "english_alias:0" || one.Names.Source.SHA256 != d.Observations[0].Roles.BodySHA256 {
			t.Fatal("source name evidence lost", one)
		}
		if len(one.Roles) != 1 || one.Roles[0].TemporalState != "within_reported_bounds" || one.Roles[0].Endpoint.Label != "Example Corp" || one.Roles[0].Statement.Times[0].Text != "2020" {
			t.Fatal("coarse role or endpoint lost", one.Roles)
		}
		if len(two.Roles) != 0 || len(two.Relevance.PersonNames) == 0 {
			t.Fatal("no-employer rival discarded")
		}
	}
	after, _ := json.Marshal(d)
	if string(before) != string(after) {
		t.Fatal("capture mutated")
	}
	again, err := EnrichAppearances([]Appearance{a}, d, build)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("replay changed", err)
	}
	// A supplied search result without an exact name remains visible. A single
	// exact alias is never a claim that the other retrieved people are excluded.
	for i := range d.Observations[0].Roles.Entities {
		if d.Observations[0].Roles.Entities[i].ID == "Q2" {
			d.Observations[0].Roles.Entities[i].Label = "Alexander Example"
		}
	}
	r, err = EnrichAppearances([]Appearance{a}, d, build)
	if err != nil || r.Appearances[0].NameState != "one_name_candidate_other_results_unassessed_identity_unresolved" || len(r.Appearances[0].Searches[0].Candidates) != 3 {
		t.Fatal("unmatched rival hidden", err)
	}
}

func TestEnrichmentBindingsFailuresAndResponseIsolation(t *testing.T) {
	for _, change := range []func(*Appearance, *wikimedia.DiscoveryResult){
		func(a *Appearance, _ *wikimedia.DiscoveryResult) { a.Receipt.Name = strptr("Other person") },
		func(a *Appearance, _ *wikimedia.DiscoveryResult) { a.Receipt.Employer = nil },
		func(a *Appearance, _ *wikimedia.DiscoveryResult) { a.Source.Locator = "other" },
		func(_ *Appearance, d *wikimedia.DiscoveryResult) { d.Observations = d.Observations[:1] },
		func(_ *Appearance, d *wikimedia.DiscoveryResult) { d.Observations[0].Search.Text = "unplanned" },
	} {
		a, d, build := enrichmentFixture(t)
		change(&a, &d)
		if _, err := EnrichAppearances([]Appearance{a}, d, build); err == nil {
			t.Fatal("mismatched source accepted")
		}
	}
	a, d, build := enrichmentFixture(t)
	// A different response supplies the endpoint. It cannot repair the missing
	// endpoint in this response or silently lend its revision to that claim.
	first := *d.Observations[0].Roles
	first.Entities = slices.Clone(first.Entities)
	first.Entities = append(first.Entities[:0], wikimedia.RoleEntity{ID: "Q1", Label: "Alex Example", Statements: []wikimedia.RoleStatement{{HolderID: "wikidata:Q1", RelatedEntityID: "wikidata:Q3", Role: "employee", Property: "P108"}}})
	d.Observations[0].Roles = &first
	r, err := EnrichAppearances([]Appearance{a}, d, build)
	if err != nil || r.Appearances[0].Searches[0].Candidates[0].Roles[0].EndpointState != "endpoint_not_loaded" {
		t.Fatal("cross-response join", err)
	}
	d.Observations[1].Issue = "maxlag"
	d.Observations[1].State = "source_unusable_or_unattempted"
	d.Observations[1].Roles = nil
	d.CaptureUsable = false
	r, err = EnrichAppearances([]Appearance{a}, d, build)
	if err != nil || r.CaptureUsable || r.Appearances[0].RetrievalState != "source_unusable_or_unattempted" || r.Appearances[0].Searches[1].Issue != "maxlag" {
		t.Fatal("failure hidden", err)
	}
}

func TestEnrichmentKeepsRoleWithoutCandidateAndEmptySourceText(t *testing.T) {
	a, d, build := enrichmentFixture(t)
	// No loaded page candidate names the holder, but the publisher's company
	// statement still survives in the same response with its exact locator.
	d.Observations[0].Candidates = d.Observations[0].Candidates[1:]
	r, err := EnrichAppearances([]Appearance{a}, d, build)
	if err != nil || len(r.Appearances[0].Searches[0].UnassignedRoles) != 1 || r.Appearances[0].Searches[0].UnassignedRoles[0].HolderID != "wikidata:Q1" {
		t.Fatal("source relationship lost without a candidate", err)
	}
	a.Receipt.Name, a.Receipt.Employer = nil, nil
	plan, err := wikimedia.PlanDiscovery([]wikimedia.DiscoveryAppearance{{SHA256: a.Source.SHA256, Locator: a.Source.Locator}}, "empty input", build)
	if err != nil {
		t.Fatal(err)
	}
	d.Plan, d.Observations = plan, nil
	r, err = EnrichAppearances([]Appearance{a}, d, build)
	if err != nil || len(r.Appearances) != 1 || r.Appearances[0].RetrievalState != "no_search_planned" || len(r.Appearances[0].Searches) != 0 {
		t.Fatal("unsearchable appearance disappeared", err)
	}
}
