package personaffiliation

import (
	"encoding/json"
	"fmt"
	"strings"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// A literal role focus is not its complete evidence. Go attaches the whole
// statement entry. Surface references and candidate endpoints remain separate.
type groundedRoleProposal struct {
	ID                  string      `json:"id"`
	Entry               *int        `json:"statement_entry"`
	Focus               string      `json:"focus"`
	Kind                string      `json:"kind"`
	Normalized          string      `json:"normalized"`
	Role                screen.Role `json:"role_kind"`
	Subjects            []string    `json:"subjects"`
	Organizations       []string    `json:"organizations"`
	SubjectSurface      *string     `json:"subject_surface"`
	OrganizationSurface *string     `json:"organization_surface"`
	AdditionalEvidence  []string    `json:"additional_evidence"`
	Binding             string      `json:"binding"`
	Status              string      `json:"status"`
}
type groundedRoleInput struct {
	Selections      []citationSelection    `json:"selections"`
	Interpretations []groundedRoleProposal `json:"interpretations"`
	Links           []contextProposal      `json:"context_links"`
}
type endpointSurfaceCheck struct {
	Interpretation string `json:"interpretation"`
	Subject        string `json:"subject"`
	Organization   string `json:"organization"`
}
type groundedRoleReport struct {
	Method   string                 `json:"method"`
	Proposal groundedRoleInput      `json:"supplied_proposal"`
	Surfaces []endpointSurfaceCheck `json:"surface_checks"`
	Review   bindingSelectionReport `json:"derived_review"`
}

func inspectGroundedRoles(c modelCase, names, raw []byte, origin string) (groundedRoleReport, error) {
	fail := func(msg string) (groundedRoleReport, error) {
		return groundedRoleReport{}, fmt.Errorf("role grounding: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	var in groundedRoleInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return groundedRoleReport{}, err
	}
	if in.Selections == nil || in.Interpretations == nil || in.Links == nil || len(in.Selections) > 64 || len(in.Interpretations) > 32 || len(in.Links) > 32 {
		return fail("missing arrays or scope budget")
	}
	_, candidates, err := bindingCandidates(c, names, origin)
	if err != nil {
		return groundedRoleReport{}, err
	}
	cat, err := newCitationCatalog(c)
	if err != nil {
		return groundedRoleReport{}, err
	}
	byName := map[string]bindingCandidate{}
	for _, n := range candidates {
		byName[n.ID] = n
	}
	selected := map[string]screen.ProseCitation{}
	for _, s := range in.Selections {
		if s.ID == "" || strings.HasPrefix(s.ID, "ctx_") || byName[s.ID].ID != "" || selected[s.ID].Matched.Text != "" || s.Entry == nil || s.First == nil || s.Last == nil {
			return fail("invalid selection or reserved ID")
		}
		v, err := cat.selectRange(citationRange{*s.Entry, *s.First, *s.Last})
		if err != nil {
			return groundedRoleReport{}, err
		}
		selected[s.ID] = v
	}
	derived := derivedReferenceInput{Selections: append([]citationSelection{}, in.Selections...), Interpretations: []referenceInterpretationProposal{}, Links: in.Links}
	contexts := map[int]string{}
	checks := []endpointSurfaceCheck{}
	for _, p := range in.Interpretations {
		if p.Entry == nil || p.SubjectSurface == nil || p.OrganizationSurface == nil || p.AdditionalEvidence == nil || len(p.AdditionalEvidence) > 64 {
			return fail("missing entry, surfaces or extra evidence")
		}
		focus, ok := selected[p.Focus]
		if !ok || focus.Entry != *p.Entry {
			return fail("focus is not in statement entry")
		}
		clause := contexts[*p.Entry]
		if clause == "" {
			for _, e := range cat.Entries {
				if e.Entry != *p.Entry {
					continue
				}
				clause = fmt.Sprintf("ctx_%d", e.Entry)
				first, last, entry := 0, len(e.Tokens)-1, e.Entry
				derived.Selections = append(derived.Selections, citationSelection{clause, &entry, &first, &last})
				contexts[entry] = clause
			}
		}
		check := func(surface string, endpoints []string, kind string) (string, error) {
			for _, id := range endpoints {
				if byName[id].Kind != kind {
					return "", fmt.Errorf("wrong candidate type")
				}
			}
			var literal string
			if surface != "" {
				s, ok := selected[surface]
				if !ok || s.Entry != *p.Entry {
					return "", fmt.Errorf("surface is not in statement entry")
				}
				literal = s.Matched.Text
			}
			if len(endpoints) == 0 {
				return "no_candidate", nil
			}
			if len(endpoints) > 1 {
				return "alternative_candidates", nil
			}
			if literal == "" {
				return "unstated_surface", nil
			}
			if literal == byName[endpoints[0]].Text {
				return "exact_surface", nil
			}
			return "different_surface", nil
		}
		subject, err := check(*p.SubjectSurface, p.Subjects, "person")
		if err != nil {
			return fail(err.Error())
		}
		org, err := check(*p.OrganizationSurface, p.Organizations, "organization")
		if err != nil {
			return fail(err.Error())
		}
		if p.Binding == "direct" && (subject != "exact_surface" || org != "exact_surface") {
			return fail("direct binding lacks exact endpoint surfaces")
		}
		extra := append([]string{p.Focus}, p.AdditionalEvidence...)
		for _, s := range []string{*p.SubjectSurface, *p.OrganizationSurface} {
			if s != "" {
				extra = append(extra, s)
			}
		}
		// Extra references must be supplied, not guessed generated context handles.
		for _, id := range p.AdditionalEvidence {
			if _, ok := selected[id]; !ok && byName[id].ID == "" {
				return fail("unknown additional evidence")
			}
		}
		derived.Interpretations = append(derived.Interpretations, referenceInterpretationProposal{ID: p.ID, Clause: clause, Kind: p.Kind, Normalized: p.Normalized, Role: p.Role, Subjects: p.Subjects, Organizations: p.Organizations, AdditionalEvidence: extra, Binding: p.Binding, Status: p.Status})
		checks = append(checks, endpointSurfaceCheck{p.ID, subject, org})
	}
	encoded, err := json.Marshal(derived)
	if err != nil {
		return groundedRoleReport{}, err
	}
	review, err := inspectBindingSelections(c, names, encoded, origin)
	if err != nil {
		return groundedRoleReport{}, err
	}
	return groundedRoleReport{Method: "whole-entry-role-grounding.v1", Proposal: in, Surfaces: checks, Review: review}, nil
}
