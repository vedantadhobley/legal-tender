package personaffiliation

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Independent synthetic annotations only. Never convert a recorded model answer.
func derivedReferenceUnitInput(t *testing.T) (citationCatalog, derivedReferenceInput) {
	t.Helper()
	c, annotated := mentionUnitInput(t)
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	selected := reviewedCitationSelections(t, cat, annotated)
	in := derivedReferenceInput{Selections: selected.Selections, Interpretations: []referenceInterpretationProposal{}, Links: selected.Links}
	for _, p := range annotated.Interpretations {
		in.Interpretations = append(in.Interpretations, referenceInterpretationProposal{
			ID: p.ID, Clause: p.Clause, Kind: p.Kind, Normalized: p.Normalized, Role: p.Role,
			Subjects: p.Subjects, Organizations: p.Organizations, AdditionalEvidence: []string{}, Binding: p.Binding, Status: p.Status,
		})
	}
	return cat, in
}

func encodeReferenceInput(t *testing.T, in derivedReferenceInput) []byte {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestProseDerivedReferencesUnionAndContext(t *testing.T) {
	cat, in := derivedReferenceUnitInput(t)
	// Repeated extras and overlap with required anchors are harmless bookkeeping;
	// preserve them in the supplied proposal, deduplicate only the derived list.
	in.Interpretations[0].AdditionalEvidence = []string{"denial", "company", "correction", "denial"}
	in.Links = append(in.Links, contextProposal{From: "negative", To: "positive", Kind: "contradicts"})
	raw := encodeReferenceInput(t, in)
	out, err := inspectDerivedCitationReferences(cat, raw, "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	if out.Method != "source-token-derived-references.v1" || !reflect.DeepEqual(out.Proposal, in) {
		t.Fatal("original proposal lost or rewritten")
	}
	r := out.Review.Review
	if !slices.Equal(r.Interpretations[0].Proposal.Evidence, []string{"ceo", "name", "company", "denial", "correction"}) {
		t.Fatal("required reference order or deduplication changed")
	}
	if !slices.Equal(r.Interpretations[1].Proposal.Evidence, []string{"correction"}) {
		t.Fatal("clause-only interpretation changed")
	}
	if !reflect.DeepEqual(out.Review.Contexts[0].Entries, []modelEntry{cat.Case.Entries[0], cat.Case.Entries[1], cat.Case.Entries[3]}) || !reflect.DeepEqual(r.Context, cat.Case.Entries) {
		t.Fatal("extra context dropped, duplicated or out of source order")
	}
	if !slices.Equal(r.Interpretations[0].ContextFlags, []string{"conflicting_supplied_interpretations", "retracted_by_supplied_interpretation"}) || r.Interpretations[0].State != "context_blocked" || r.Interpretations[2].State != "unverified_role_interpretation" || r.Interpretations[3].State != "context_blocked" {
		t.Fatal("context rules or unrelated interpretation changed")
	}
	if r.Interpretations[0].Proposal.Status != "asserted" || r.Interpretations[3].Proposal.Status != "denied" || r.Source != cat.Case.Source || r.Origin != "synthetic_test" || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
		t.Fatal("source, status or approval boundary changed")
	}
	again, err := inspectDerivedCitationReferences(cat, raw, "synthetic_test")
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("reference derivation is not deterministic", err)
	}
	// Derived output is not an alias of the supplied proposal's slices.
	out.Review.Review.Interpretations[0].Proposal.Subjects[0] = "changed"
	if out.Proposal.Interpretations[0].Subjects[0] != "name" {
		t.Fatal("derived output overwrote the supplied proposal")
	}
}

func TestProseDerivedReferencesDoNotInferMeaning(t *testing.T) {
	cat, in := derivedReferenceUnitInput(t)
	in.Links = []contextProposal{}
	in.Interpretations[0].AdditionalEvidence = []string{"correction"}
	// Both are valid source IDs, but 'company' is not a person. Retain the
	// alternatives; a reference checker cannot classify or resolve them.
	in.Interpretations[0].Subjects = []string{"name", "company"}
	in.Interpretations[0].Binding = "ambiguous"
	in.Interpretations[2].Subjects = []string{}
	in.Interpretations[2].Organizations = []string{}
	in.Interpretations[2].Binding = "unresolved"
	out, err := inspectDerivedCitationReferences(cat, encodeReferenceInput(t, in), "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	r := out.Review.Review
	if !slices.Equal(r.Interpretations[0].Proposal.Subjects, []string{"name", "company"}) || !slices.Equal(r.Interpretations[0].Proposal.Evidence, []string{"ceo", "name", "company", "correction"}) || r.Interpretations[0].State != "binding_ambiguous" || r.Interpretations[2].State != "binding_unresolved" || len(r.Interpretations[2].Proposal.Subjects) != 0 || r.Interpretations[3].State != "nonpositive_denied" {
		t.Fatal("ambiguity, absent endpoints or denial changed")
	}
	if len(r.Interpretations[0].ContextFlags) != 0 {
		t.Fatal("additional context invented a retraction without a supplied link")
	}
	in.Interpretations[0].Clause = "name" // A name alone does not support the role.
	in.Interpretations[0].Subjects = []string{"company"}
	in.Interpretations[0].Binding = "direct"
	out, err = inspectDerivedCitationReferences(cat, encodeReferenceInput(t, in), "synthetic_test")
	if err != nil || out.Review.Review.Interpretations[0].State != "unverified_role_interpretation" || out.Review.Review.IdentityApproved || out.Review.Review.GraphPublicationApproved || out.Review.Review.FinancialAttribution {
		t.Fatal("valid references claimed meaning or approval", err)
	}
}

func TestProseDerivedReferencesRejectInvalidInput(t *testing.T) {
	for name, mutate := range map[string]func(*derivedReferenceInput){
		"null selections":           func(in *derivedReferenceInput) { in.Selections = nil },
		"null interpretations":      func(in *derivedReferenceInput) { in.Interpretations = nil },
		"null links":                func(in *derivedReferenceInput) { in.Links = nil },
		"null additional evidence":  func(in *derivedReferenceInput) { in.Interpretations[0].AdditionalEvidence = nil },
		"unknown extra":             func(in *derivedReferenceInput) { in.Interpretations[0].AdditionalEvidence = []string{"missing"} },
		"empty extra ID":            func(in *derivedReferenceInput) { in.Interpretations[0].AdditionalEvidence = []string{""} },
		"extra budget":              func(in *derivedReferenceInput) { in.Interpretations[0].AdditionalEvidence = make([]string, 65) },
		"unknown clause":            func(in *derivedReferenceInput) { in.Interpretations[0].Clause = "missing" },
		"empty clause":              func(in *derivedReferenceInput) { in.Interpretations[0].Clause = "" },
		"unknown subject":           func(in *derivedReferenceInput) { in.Interpretations[0].Subjects = []string{"missing"} },
		"unknown organization":      func(in *derivedReferenceInput) { in.Interpretations[0].Organizations = []string{"missing"} },
		"null subjects":             func(in *derivedReferenceInput) { in.Interpretations[0].Subjects = nil },
		"null organizations":        func(in *derivedReferenceInput) { in.Interpretations[0].Organizations = nil },
		"missing required endpoint": func(in *derivedReferenceInput) { in.Interpretations[0].Subjects = []string{} },
		"duplicate endpoint":        func(in *derivedReferenceInput) { in.Interpretations[0].Subjects = []string{"name", "name"} },
		"multiple direct endpoints": func(in *derivedReferenceInput) { in.Interpretations[0].Subjects = []string{"name", "company"} },
		"null coordinate":           func(in *derivedReferenceInput) { in.Selections[0].First = nil },
		"range bounds":              func(in *derivedReferenceInput) { *in.Selections[0].Last = 10000 },
		"duplicate selection":       func(in *derivedReferenceInput) { in.Selections = append(in.Selections, in.Selections[0]) },
		"unknown context target":    func(in *derivedReferenceInput) { in.Links[0].To = "missing" },
	} {
		t.Run(name, func(t *testing.T) {
			cat, in := derivedReferenceUnitInput(t)
			mutate(&in)
			out, err := inspectDerivedCitationReferences(cat, encodeReferenceInput(t, in), "synthetic_test")
			if err == nil || !reflect.DeepEqual(out, derivedReferenceReport{}) {
				t.Fatal("invalid input returned a partial report")
			}
		})
	}
	cat, in := derivedReferenceUnitInput(t)
	base := string(encodeReferenceInput(t, in))
	for name, raw := range map[string]string{
		"old evidence field": strings.Replace(base, `"additional_evidence":[]`, `"evidence":[]`, 1),
		"missing extras":     strings.Replace(base, `"additional_evidence":[],`, "", 1),
		"missing subjects":   strings.Replace(base, `"subjects":["name"],`, "", 1),
		"missing clause":     strings.Replace(base, `"clause":"ceo",`, "", 1),
		"null clause":        strings.Replace(base, `"clause":"ceo"`, `"clause":null`, 1),
		"null extra ID":      strings.Replace(base, `"additional_evidence":[]`, `"additional_evidence":[null]`, 1),
		"duplicate JSON key": strings.Replace(base, `"clause":"ceo"`, `"clause":"ceo","clause":"ceo"`, 1),
		"forged approval":    strings.Replace(base, `"clause":"ceo"`, `"clause":"ceo","identity_approved":true`, 1),
		"forged origin":      `{"interpretation_origin":"reviewed_fixture",` + base[1:],
		"forged quote":       strings.Replace(base, `"id":"name"`, `"id":"name","text":"invented"`, 1),
		"trailing value":     base + base,
		"input budget":       base + strings.Repeat(" ", 128<<10),
	} {
		t.Run(name, func(t *testing.T) {
			if raw == base {
				t.Fatal("test mutation did not apply")
			}
			out, err := inspectDerivedCitationReferences(cat, []byte(raw), "synthetic_test")
			if err == nil || !reflect.DeepEqual(out, derivedReferenceReport{}) {
				t.Fatal("invalid JSON contract returned a partial report")
			}
		})
	}
	// Empty, explicit arrays mean no proposals, not missing data or acceptance.
	empty := []byte(`{"selections":[],"interpretations":[],"context_links":[]}`)
	out, err := inspectDerivedCitationReferences(cat, empty, "synthetic_test")
	if err != nil || len(out.Review.Review.Interpretations) != 0 || out.Review.Review.GraphPublicationApproved {
		t.Fatal("empty proposal failed", err)
	}
	if _, err := inspectDerivedCitationReferences(cat, empty, "accepted"); err == nil {
		t.Fatal("unsupported origin accepted")
	}
}
