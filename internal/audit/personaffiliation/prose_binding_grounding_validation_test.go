package personaffiliation

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

// Independent annotations, never a conversion or repair of a model response.
func groundingUnitInput(t *testing.T) (modelCase, []byte, groundedRoleInput) {
	t.Helper()
	c, names, old := bindingUnitInput(t)
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	in := groundedRoleInput{Selections: append([]citationSelection{}, old.Selections...), Interpretations: []groundedRoleProposal{}, Links: old.Links}
	selectText := func(id string, entry int, text string) string {
		o, err := cat.literalOptions(entry, text)
		if err != nil || len(o.Matches) != 1 || o.Matches[0].Range == nil {
			t.Fatal("unit literal", id, err)
		}
		r := *o.Matches[0].Range
		in.Selections = append(in.Selections, citationSelection{id, &r.Entry, &r.First, &r.Last})
		return id
	}
	for i, p := range old.Interpretations {
		entry := -1
		for _, s := range old.Selections {
			if s.ID == p.Clause {
				entry = *s.Entry
			}
		}
		person, org := "", ""
		if i == 0 {
			person = selectText("surface_person", entry, "Zoë Test")
			org = selectText("surface_org", entry, "A&B")
		}
		binding := p.Binding
		if i != 0 && binding == "direct" {
			binding = "coreference"
		}
		in.Interpretations = append(in.Interpretations, groundedRoleProposal{ID: p.ID, Entry: &entry, Focus: p.Clause, Kind: p.Kind, Normalized: p.Normalized, Role: p.Role, Subjects: p.Subjects, Organizations: p.Organizations, SubjectSurface: &person, OrganizationSurface: &org, AdditionalEvidence: p.AdditionalEvidence, Binding: binding, Status: p.Status})
	}
	return c, names, in
}

func groundingJSON(t *testing.T, in groundedRoleInput) []byte {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestProseGroundingWholeContextAndSurfaces(t *testing.T) {
	c, names, in := groundingUnitInput(t)
	out, err := inspectGroundedRoles(c, names, groundingJSON(t, in), "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out.Proposal, in) || out.Surfaces[0].Subject != "exact_surface" || out.Surfaces[0].Organization != "exact_surface" {
		t.Fatal("proposal or literal surface changed")
	}
	r := out.Review.Derived.Review.Review
	if !reflect.DeepEqual(r.Context, c.Entries) || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution || r.Interpretations[0].State != "context_blocked" || r.Interpretations[2].State != "unverified_role_interpretation" {
		t.Fatal("context/acceptance changed")
	}
	for _, p := range out.Review.Proposal.Interpretations {
		var text string
		for _, m := range r.Mentions {
			if m.ID == p.Clause {
				text = m.Evidence.Matched.Text
			}
		}
		found := false
		for _, e := range c.Entries {
			if text == e.Text {
				found = true
			}
		}
		if !found {
			t.Fatal("context was not a complete source entry")
		}
	}
	// Source-valid exact surfaces still do not prove a role or relation.
	in.Links = []contextProposal{}
	in.Interpretations = in.Interpretations[:1]
	in.Interpretations[0].Normalized = "unsupported interpretation"
	out, err = inspectGroundedRoles(c, names, groundingJSON(t, in), "synthetic_test")
	if err != nil || out.Review.Derived.Review.Review.Interpretations[0].State != "unverified_role_interpretation" {
		t.Fatal("structural checks claimed meaning", err)
	}
	// An explicit anonymous role is retained without manufacturing an endpoint.
	in.Interpretations[0].Subjects = []string{}
	empty := ""
	in.Interpretations[0].SubjectSurface = &empty
	in.Interpretations[0].Binding = "unresolved"
	out, err = inspectGroundedRoles(c, names, groundingJSON(t, in), "synthetic_test")
	if err != nil || out.Surfaces[0].Subject != "no_candidate" || out.Review.Derived.Review.Review.Interpretations[0].State != "binding_unresolved" {
		t.Fatal("anonymous evidence lost", err)
	}
}

func TestProseGroundingRejectsInvalidEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*groundedRoleInput){
		"missing entry":            func(v *groundedRoleInput) { v.Interpretations[0].Entry = nil },
		"wrong entry":              func(v *groundedRoleInput) { n := 999; v.Interpretations[0].Entry = &n },
		"missing surface":          func(v *groundedRoleInput) { v.Interpretations[0].SubjectSurface = nil },
		"direct without surface":   func(v *groundedRoleInput) { s := ""; v.Interpretations[0].SubjectSurface = &s },
		"different direct surface": func(v *groundedRoleInput) { s := "surface_org"; v.Interpretations[0].SubjectSurface = &s },
		"surface outside entry":    func(v *groundedRoleInput) { s := "denial"; v.Interpretations[0].SubjectSurface = &s },
		"name as surface":          func(v *groundedRoleInput) { s := "n0"; v.Interpretations[0].SubjectSurface = &s },
		"unknown focus":            func(v *groundedRoleInput) { v.Interpretations[0].Focus = "missing" },
		"unknown extra":            func(v *groundedRoleInput) { v.Interpretations[0].AdditionalEvidence = []string{"ctx_0"} },
		"reserved ID":              func(v *groundedRoleInput) { v.Selections[0].ID = "ctx_0" },
		"duplicate selection":      func(v *groundedRoleInput) { v.Selections = append(v.Selections, v.Selections[0]) },
		"null array":               func(v *groundedRoleInput) { v.Selections = nil },
		"wrong endpoint type":      func(v *groundedRoleInput) { v.Interpretations[0].Subjects = []string{"n1"} },
	} {
		t.Run(name, func(t *testing.T) {
			c, names, in := groundingUnitInput(t)
			mutate(&in)
			out, err := inspectGroundedRoles(c, names, groundingJSON(t, in), "synthetic_test")
			if err == nil || !reflect.DeepEqual(out, groundedRoleReport{}) {
				t.Fatal("bad input returned evidence")
			}
		})
	}
	c, names, in := groundingUnitInput(t)
	raw := groundingJSON(t, in)
	for _, bad := range [][]byte{append(append([]byte{}, raw...), raw...), []byte(`{"identity_approved":true,` + string(raw[1:])), []byte(string(raw) + strings.Repeat(" ", 128<<10))} {
		if _, err := inspectGroundedRoles(c, names, bad, "synthetic_test"); err == nil {
			t.Fatal("forgery/budget/trailing JSON accepted")
		}
	}
}

func TestProseGroundingRequestContract(t *testing.T) {
	c, names, _ := groundingUnitInput(t)
	p := modelProfile{"gpt-oss-120b", "medium", 4096}
	raw, err := proseGroundingRequest(c, names, p)
	if err != nil {
		t.Fatal(err)
	}
	old, err := proseBindingModelRequest(c, names, p)
	if err != nil {
		t.Fatal(err)
	}
	var request, baseline map[string]any
	if json.Unmarshal(raw, &request) != nil || json.Unmarshal(old, &baseline) != nil {
		t.Fatal("bad request JSON")
	}
	for _, key := range []string{"model", "max_tokens", "reasoning_effort", "seed", "stream"} {
		if !reflect.DeepEqual(request[key], baseline[key]) {
			t.Fatal("controls changed", key)
		}
	}
	if !reflect.DeepEqual(request["messages"].([]any)[1], baseline["messages"].([]any)[1]) || request["messages"].([]any)[0].(map[string]any)["content"] != proseGroundingPrompt {
		t.Fatal("names/source or prompt changed")
	}
	again, err := proseGroundingRequest(c, names, p)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("nondeterministic request", err)
	}
	if len(proseInputCases(t, groundingFixture)) != 5 {
		t.Fatal("fresh sample changed")
	}
}

func TestProseGroundingPronounsRemainInferred(t *testing.T) {
	body := []byte(`<p>Rémi Vale was CEO of North Studio from 2014 to 2017.</p><p>He was the company's treasurer in 2018, not CFO.</p>`)
	source := companypage.Source{URL: "https://example.org/grounding-unit", ObservedOn: "2026-09-17", SHA256: hash(body)}
	e, err := companypage.Extract(context.Background(), body, source)
	if err != nil {
		t.Fatal(err)
	}
	c := modelCase{ID: "grounding-unit", Source: source}
	for i, entry := range e.Entries {
		c.Entries = append(c.Entries, modelEntry{i, entry.Kind, entry.Text, entry.Span})
	}
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	selectText := func(id string, entry int, text string) citationSelection {
		o, err := cat.literalOptions(entry, text)
		if err != nil || len(o.Matches) != 1 || o.Matches[0].Range == nil {
			t.Fatal("unit literal", id, err)
		}
		r := *o.Matches[0].Range
		return citationSelection{id, &r.Entry, &r.First, &r.Last}
	}
	names, err := json.Marshal(nameSelectionInput{Names: []nameSelection{
		{citationSelection: selectText("person", 0, "Rémi Vale"), Kind: "person"},
		{citationSelection: selectText("company", 0, "North Studio"), Kind: "organization"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	entry, subject, org := 1, "pronoun", "possessive"
	in := groundedRoleInput{Selections: []citationSelection{
		selectText("title", 1, "treasurer"), selectText(subject, 1, "He"), selectText(org, 1, "the company's"),
	}, Interpretations: []groundedRoleProposal{{ID: "role", Entry: &entry, Focus: "title", Kind: "role", Normalized: "treasurer", Role: "unknown", Subjects: []string{"n0"}, Organizations: []string{"n1"}, SubjectSurface: &subject, OrganizationSurface: &org, AdditionalEvidence: []string{}, Binding: "coreference", Status: "asserted"}}, Links: []contextProposal{}}
	out, err := inspectGroundedRoles(c, names, groundingJSON(t, in), "synthetic_test")
	if err != nil || out.Surfaces[0].Subject != "different_surface" || out.Surfaces[0].Organization != "different_surface" {
		t.Fatal("pronoun evidence changed", err)
	}
	r := out.Review.Derived.Review.Review
	if r.Interpretations[0].State != "unverified_role_interpretation" || r.Interpretations[0].Proposal.Binding != "coreference" || r.IdentityApproved {
		t.Fatal("coreference approved")
	}
	var clause string
	for _, m := range r.Mentions {
		if m.ID == "ctx_1" {
			clause = m.Evidence.Matched.Text
		}
	}
	if clause != c.Entries[1].Text {
		t.Fatal("date or negative qualification dropped")
	}
	in.Interpretations[0].Binding = "direct"
	if _, err := inspectGroundedRoles(c, names, groundingJSON(t, in), "synthetic_test"); err == nil {
		t.Fatal("pronouns accepted as exact direct names")
	}
	// Unknown antecedents remain unresolved even though a surface occurs in text.
	in.Interpretations[0].Binding = "unresolved"
	in.Interpretations[0].Subjects = []string{}
	in.Interpretations[0].Organizations = []string{}
	out, err = inspectGroundedRoles(c, []byte(`{"names":[]}`), groundingJSON(t, in), "synthetic_test")
	if err != nil || out.Review.Derived.Review.Review.Interpretations[0].State != "binding_unresolved" {
		t.Fatal("missing names repaired", err)
	}
}
