package personaffiliation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Synthetic annotations only, not edited model captures.
func bindingUnitInput(t *testing.T) (modelCase, []byte, derivedReferenceInput) {
	t.Helper()
	cat, in := derivedReferenceUnitInput(t)
	names := nameSelectionInput{Names: []nameSelection{}}
	selections := []citationSelection{}
	ids := map[string]string{"name": "n0", "company": "n1", "civic": "n2"}
	for _, s := range in.Selections {
		if ids[s.ID] == "" {
			selections = append(selections, s)
			continue
		}
		kind := "organization"
		if s.ID == "name" {
			kind = "person"
		}
		names.Names = append(names.Names, nameSelection{citationSelection: s, Kind: kind})
	}
	in.Selections = selections
	for i := range in.Interpretations {
		p := &in.Interpretations[i]
		for j, id := range p.Subjects {
			p.Subjects[j] = ids[id]
		}
		for j, id := range p.Organizations {
			p.Organizations[j] = ids[id]
		}
	}
	raw, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	return cat.Case, raw, in
}

func TestProseBindingContractContext(t *testing.T) {
	c, names, in := bindingUnitInput(t)
	in.Links = append(in.Links, contextProposal{From: "negative", To: "positive", Kind: "contradicts"})
	out, err := inspectBindingSelections(c, names, encodeReferenceInput(t, in), "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	r := out.Derived.Review.Review
	if !reflect.DeepEqual(out.Proposal, in) || out.Names.Supplied[0].ID != "name" || out.Candidates[0].ID != "n0" || out.Candidates[0].OriginalID != "name" || out.Candidates[0].Text != "Zoë Test" || !reflect.DeepEqual(r.Context, c.Entries) || r.Source != c.Source {
		t.Fatal("source, name input or supplied proposal changed")
	}
	if !slices.Equal(r.Interpretations[0].Proposal.Evidence, []string{"ceo", "n0", "n1"}) || len(r.Interpretations[0].ContextFlags) != 2 || r.Interpretations[0].State != "context_blocked" || r.Interpretations[2].State != "unverified_role_interpretation" || r.Interpretations[3].Proposal.Status != "denied" || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
		t.Fatal("reference/context rules or approvals changed")
	}
	// Correctly typed IDs still do not prove the intended person/company binding.
	in.Links = []contextProposal{}
	in.Interpretations[0].Organizations = []string{"n2"}
	out, err = inspectBindingSelections(c, names, encodeReferenceInput(t, in), "synthetic_test")
	if err != nil || out.Derived.Review.Review.Interpretations[0].State != "unverified_role_interpretation" || out.Derived.Review.Review.IdentityApproved {
		t.Fatal("shape validation claimed semantic truth", err)
	}
	// No name candidates: keep the supported clause with unresolved empty endpoints.
	in.Interpretations = in.Interpretations[:1]
	in.Interpretations[0].Subjects = []string{}
	in.Interpretations[0].Organizations = []string{}
	in.Interpretations[0].Binding = "unresolved"
	out, err = inspectBindingSelections(c, []byte(`{"names":[]}`), encodeReferenceInput(t, in), "synthetic_test")
	if err != nil || len(out.Candidates) != 0 || out.Derived.Review.Review.Interpretations[0].State != "binding_unresolved" {
		t.Fatal("missing candidates invented an endpoint", err)
	}
}

func TestProseBindingContractRejectsSubstitution(t *testing.T) {
	for name, mutate := range map[string]func(*derivedReferenceInput){
		"new person endpoint":          func(v *derivedReferenceInput) { v.Interpretations[0].Subjects = []string{"ceo"} },
		"new organization endpoint":    func(v *derivedReferenceInput) { v.Interpretations[0].Organizations = []string{"ceo"} },
		"person typed as organization": func(v *derivedReferenceInput) { v.Interpretations[0].Organizations = []string{"n0"} },
		"organization typed as person": func(v *derivedReferenceInput) { v.Interpretations[0].Subjects = []string{"n1"} },
		"unknown candidate":            func(v *derivedReferenceInput) { v.Interpretations[0].Subjects = []string{"n99"} },
		"name used as clause":          func(v *derivedReferenceInput) { v.Interpretations[0].Clause = "n0" },
		"override name coordinates":    func(v *derivedReferenceInput) { v.Selections[0].ID = "n0" },
		"null selections":              func(v *derivedReferenceInput) { v.Selections = nil },
		"null interpretations":         func(v *derivedReferenceInput) { v.Interpretations = nil },
		"null context":                 func(v *derivedReferenceInput) { v.Links = nil },
		"unknown context target":       func(v *derivedReferenceInput) { v.Links[0].To = "missing" },
		"range bounds":                 func(v *derivedReferenceInput) { *v.Selections[0].Last = 9999 },
		"scope budget":                 func(v *derivedReferenceInput) { v.Selections = make([]citationSelection, 64) },
		"activity out of task":         func(v *derivedReferenceInput) { v.Interpretations[0].Kind = "activity" },
	} {
		t.Run(name, func(t *testing.T) {
			c, names, in := bindingUnitInput(t)
			mutate(&in)
			out, err := inspectBindingSelections(c, names, encodeReferenceInput(t, in), "synthetic_test")
			if err == nil || !reflect.DeepEqual(out, bindingSelectionReport{}) {
				t.Fatal("invalid binding returned a partial report")
			}
		})
	}
	c, names, in := bindingUnitInput(t)
	raw := string(encodeReferenceInput(t, in))
	for _, field := range []string{`"names":[]`, `"identity_approved":true`, `"interpretation_origin":"reviewed_fixture"`} {
		if _, err := inspectBindingSelections(c, names, []byte("{"+field+","+raw[1:]), "synthetic_test"); err == nil {
			t.Fatal("producer overrode caller-owned field")
		}
	}
	if _, err := inspectBindingSelections(c, names, []byte(raw+raw), "synthetic_test"); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if _, err := inspectBindingSelections(c, names, []byte(raw+strings.Repeat(" ", 128<<10)), "synthetic_test"); err == nil {
		t.Fatal("input budget accepted")
	}
}

func TestProseBindingRequestContract(t *testing.T) {
	c, names, _ := bindingUnitInput(t)
	profile := modelProfile{"gpt-oss-120b", "medium", 4096}
	for _, nameInput := range [][]byte{names, []byte(`{"names":[]}`)} {
		raw, err := proseBindingModelRequest(c, nameInput, profile)
		if err != nil {
			t.Fatal(err)
		}
		var request struct {
			Model           string
			ReasoningEffort string `json:"reasoning_effort"`
			MaxTokens       int    `json:"max_tokens"`
			Messages        []struct{ Role, Content string }
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		_, expected, _ := bindingCandidates(c, nameInput, "model_proposal")
		var input struct {
			Entries []citationTokenEntry
			Names   []bindingCandidate
		}
		if err := json.Unmarshal([]byte(request.Messages[1].Content), &input); err != nil {
			t.Fatal(err)
		}
		cat, _ := newCitationCatalog(c)
		encoded, _ := json.Marshal(cat.Entries)
		actual, _ := json.Marshal(input.Entries)
		if request.Model != profile.Model || request.ReasoningEffort != profile.ReasoningEffort || request.MaxTokens != profile.MaxTokens || request.Messages[0].Content != proseBindingModelPrompt || !reflect.DeepEqual(input.Names, expected) || !bytes.Equal(encoded, actual) {
			t.Fatal("source/candidates changed or review labels entered request")
		}
		again, err := proseBindingModelRequest(c, nameInput, profile)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("nondeterministic binding request", err)
		}
	}
}
