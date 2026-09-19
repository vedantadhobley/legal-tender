package personaffiliation

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func referentJSON(t *testing.T, in referentInput) []byte {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Independently annotated unit source, not a repaired model response.
func referentUnitInput(t *testing.T) (modelCase, []byte, referentInput) {
	c, names, grounding := groundingUnitInput(t)
	grounding.Links = []contextProposal{{From: "positive", To: "negative", Kind: "contradicts"}}
	in := referentInput{Grounding: grounding, Referents: []roleReferents{}, Corrections: []correctionTargets{{Correction: "notice", State: "proposed", Targets: []string{"positive"}}}}
	for _, p := range grounding.Interpretations {
		if p.Kind == "role" {
			in.Referents = append(in.Referents, roleReferents{Interpretation: p.ID, Subject: &referentChoice{State: "unassessed", Candidates: []string{}}, Organization: &referentChoice{State: "unassessed", Candidates: []string{}}})
		}
	}
	return c, names, in
}

func TestProseReferentSeparationAndCorrection(t *testing.T) {
	c, names, in := referentUnitInput(t)
	out, err := inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out.Proposal, in) || out.Derived.Surfaces[0].Subject != "exact_surface" || out.Referents[0].Subject != "unverified_unassessed" {
		t.Fatal("literal match was promoted to referent")
	}
	r := out.Derived.Review.Derived.Review.Review
	if !reflect.DeepEqual(r.Context, c.Entries) || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution || r.Interpretations[0].State != "context_blocked" || len(r.Interpretations[0].ContextFlags) != 2 || len(r.Interpretations[2].ContextFlags) != 0 || len(r.Interpretations[3].ContextFlags) != 1 {
		t.Fatal("correction scope, conflict, source or approval changed")
	}
	if r.Interpretations[0].Proposal.Status != "asserted" || len(out.Proposal.Grounding.Links) != 1 || len(out.Derived.Proposal.Links) != 2 {
		t.Fatal("original claim/links overwritten")
	}
	in.Referents[0].Subject = &referentChoice{State: "proposed", Candidates: []string{"n0"}}
	in.Referents[0].Organization = &referentChoice{State: "ambiguous", Candidates: []string{"n1", "n2"}}
	in.Corrections[0] = correctionTargets{Correction: "notice", State: "ambiguous", Targets: []string{"positive", "other-role"}}
	out, err = inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil || out.Referents[0].Subject != "unverified_proposed" || out.Referents[0].Organization != "unverified_ambiguous" || len(out.Derived.Proposal.Links) != 1 {
		t.Fatal("alternatives collapsed or retracted", err)
	}
	in.Grounding.Links = []contextProposal{}
	in.Corrections[0] = correctionTargets{Correction: "notice", State: "unresolved", Targets: []string{}}
	out, err = inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil || len(out.Derived.Proposal.Links) != 0 {
		t.Fatal("unresolved target invented links", err)
	}
	in.Referents[0].Subject = &referentChoice{State: "unresolved", Candidates: []string{}}
	out, err = inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil || out.Referents[0].Subject != "unverified_unresolved" || out.Derived.Surfaces[0].Subject != "exact_surface" {
		t.Fatal("literal equality overrode unresolved referent", err)
	}
	// Two explicit targets are a collective proposal, unlike alternatives.
	in.Corrections[0] = correctionTargets{Correction: "notice", State: "proposed", Targets: []string{"positive", "other-role"}}
	out, err = inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil || len(out.Derived.Proposal.Links) != 2 {
		t.Fatal("explicit multiple targets lost", err)
	}
	// Valid references still cannot prove that these targets are semantically right.
	if out.Derived.Review.Derived.Review.Review.IdentityApproved {
		t.Fatal("proposal approved identity")
	}
	again, err := inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("nondeterministic result", err)
	}
}

func TestProseReferentContractRejectsOmissionsAndForgery(t *testing.T) {
	for name, mutate := range map[string]func(*referentInput){
		"missing role":           func(v *referentInput) { v.Referents = v.Referents[1:] },
		"duplicate role":         func(v *referentInput) { v.Referents = append(v.Referents, v.Referents[0]) },
		"missing endpoint state": func(v *referentInput) { v.Referents[0].Subject = nil },
		"null candidate array":   func(v *referentInput) { v.Referents[0].Subject.Candidates = nil },
		"resolved state forbidden": func(v *referentInput) {
			v.Referents[0].Subject = &referentChoice{State: "resolved", Candidates: []string{"n0"}}
		},
		"wrong candidate type": func(v *referentInput) {
			v.Referents[0].Subject = &referentChoice{State: "proposed", Candidates: []string{"n1"}}
		},
		"unknown candidate": func(v *referentInput) {
			v.Referents[0].Subject = &referentChoice{State: "proposed", Candidates: []string{"missing"}}
		},
		"duplicate alternative": func(v *referentInput) {
			v.Referents[0].Subject = &referentChoice{State: "ambiguous", Candidates: []string{"n0", "n0"}}
		},
		"singleton ambiguity": func(v *referentInput) {
			v.Referents[0].Subject = &referentChoice{State: "ambiguous", Candidates: []string{"n0"}}
		},
		"unresolved with selected person": func(v *referentInput) {
			v.Referents[0].Subject = &referentChoice{State: "unresolved", Candidates: []string{"n0"}}
		},
		"missing correction":         func(v *referentInput) { v.Corrections = []correctionTargets{} },
		"null corrections":           func(v *referentInput) { v.Corrections = nil },
		"duplicate correction":       func(v *referentInput) { v.Corrections = append(v.Corrections, v.Corrections[0]) },
		"unknown correction":         func(v *referentInput) { v.Corrections[0].Correction = "missing" },
		"correction not role":        func(v *referentInput) { v.Corrections[0].Targets = []string{"notice"} },
		"unknown target":             func(v *referentInput) { v.Corrections[0].Targets = []string{"missing"} },
		"duplicate target":           func(v *referentInput) { v.Corrections[0].Targets = []string{"positive", "positive"} },
		"null targets":               func(v *referentInput) { v.Corrections[0].Targets = nil },
		"empty proposed targets":     func(v *referentInput) { v.Corrections[0].Targets = []string{} },
		"unresolved with target":     func(v *referentInput) { v.Corrections[0].State = "unresolved" },
		"singleton target ambiguity": func(v *referentInput) { v.Corrections[0].State = "ambiguous" },
		"competing retraction authority": func(v *referentInput) {
			v.Grounding.Links = append(v.Grounding.Links, contextProposal{From: "notice", To: "positive", Kind: "retracts"})
		},
		"scope budget": func(v *referentInput) { v.Corrections = make([]correctionTargets, 33) },
	} {
		t.Run(name, func(t *testing.T) {
			c, names, in := referentUnitInput(t)
			mutate(&in)
			out, err := inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
			if err == nil || !reflect.DeepEqual(out, referentReport{}) {
				t.Fatal("invalid input produced partial evidence")
			}
		})
	}
	c, names, in := referentUnitInput(t)
	raw := string(referentJSON(t, in))
	for _, bad := range []string{raw + raw, raw + strings.Repeat(" ", 128<<10), `{"identity_approved":true,` + raw[1:], `{"assessment_origin":"reviewed_fixture",` + raw[1:], `{"interpretation_origin":"reviewed_fixture",` + raw[1:]} {
		if _, err := inspectReferentContract(c, names, []byte(bad), "synthetic_test", "synthetic_test"); err == nil {
			t.Fatal("forged or unbounded input accepted")
		}
	}
	if _, err := inspectReferentContract(c, names, []byte(raw), "synthetic_test", "approved"); err == nil {
		t.Fatal("unsupported assessment origin accepted")
	}
}

func TestProseReferentEmptyCase(t *testing.T) {
	c, names, _ := referentUnitInput(t)
	in := referentInput{Grounding: groundedRoleInput{Selections: []citationSelection{}, Interpretations: []groundedRoleProposal{}, Links: []contextProposal{}}, Referents: []roleReferents{}, Corrections: []correctionTargets{}}
	out, err := inspectReferentContract(c, names, referentJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil || len(out.Referents) != 0 || len(out.Derived.Proposal.Links) != 0 {
		t.Fatal("empty proposals invented an assessment", err)
	}
}
