package personaffiliation

import (
	"encoding/json"
	"fmt"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// New offline proposal contract, not a repair path for saved model responses.
// Required references follow explicit selections; meaning remains caller-supplied.
type referenceInterpretationProposal struct {
	ID                 string      `json:"id"`
	Clause             string      `json:"clause"`
	Kind               string      `json:"kind"`
	Normalized         string      `json:"normalized"`
	Role               screen.Role `json:"role_kind,omitempty"`
	Subjects           []string    `json:"subjects"`
	Organizations      []string    `json:"organizations"`
	AdditionalEvidence []string    `json:"additional_evidence"`
	Binding            string      `json:"binding"`
	Status             string      `json:"status"`
}
type derivedReferenceInput struct {
	Selections      []citationSelection               `json:"selections"`
	Interpretations []referenceInterpretationProposal `json:"interpretations"`
	Links           []contextProposal                 `json:"context_links"`
}
type derivedReferenceReport struct {
	Method   string                  `json:"method"`
	Proposal derivedReferenceInput   `json:"supplied_proposal"`
	Review   citationSelectionReport `json:"derived_review"`
}

func inspectDerivedCitationReferences(c citationCatalog, raw []byte, origin string) (derivedReferenceReport, error) {
	fail := func(msg string) (derivedReferenceReport, error) {
		return derivedReferenceReport{}, fmt.Errorf("derived citation references: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	var in derivedReferenceInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return derivedReferenceReport{}, err
	}
	if in.Interpretations == nil || len(in.Interpretations) > 32 {
		return fail("missing or unbounded interpretations")
	}
	derived := citationSelectionInput{Selections: in.Selections, Interpretations: []interpretationProposal{}, Links: in.Links}
	for _, p := range in.Interpretations {
		if p.AdditionalEvidence == nil || len(p.AdditionalEvidence) > 64 || len(p.Subjects) > 64 || len(p.Organizations) > 64 {
			return fail("missing additional evidence or reference budget")
		}
		// Stable union: clause, subjects, organizations, then explicit extras.
		// Do not collapse endpoint alternatives or invent missing references.
		refs := []string{}
		seen := map[string]bool{}
		for _, group := range [][]string{{p.Clause}, p.Subjects, p.Organizations, p.AdditionalEvidence} {
			for _, id := range group {
				if !seen[id] {
					refs = append(refs, id)
					seen[id] = true
				}
			}
		}
		derived.Interpretations = append(derived.Interpretations, interpretationProposal{
			ID: p.ID, Clause: p.Clause, Kind: p.Kind, Normalized: p.Normalized, Role: p.Role,
			Subjects: p.Subjects, Organizations: p.Organizations, Evidence: refs, Binding: p.Binding, Status: p.Status,
		})
	}
	encoded, err := json.Marshal(derived)
	if err != nil {
		return derivedReferenceReport{}, err
	}
	review, err := inspectCitationSelections(c, encoded, origin)
	if err != nil {
		return derivedReferenceReport{}, err // No partial report or relaxed v1 validation.
	}
	return derivedReferenceReport{Method: "source-token-derived-references.v1", Proposal: in, Review: review}, nil
}
