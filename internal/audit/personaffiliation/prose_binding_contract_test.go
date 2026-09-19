package personaffiliation

import (
	"encoding/json"
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type bindingCandidate struct {
	ID         string `json:"id"`
	OriginalID string `json:"original_id"`
	Kind       string `json:"kind"`
	Text       string `json:"text"`
	Entry      int    `json:"entry"`
}
type bindingSelectionReport struct {
	Method     string                 `json:"method"`
	Names      nameSelectionReport    `json:"first_stage_names"`
	Candidates []bindingCandidate     `json:"name_candidates"`
	Proposal   derivedReferenceInput  `json:"supplied_proposal"`
	Derived    derivedReferenceReport `json:"derived_review"`
}

func bindingCandidates(c modelCase, names []byte, origin string) (nameSelectionReport, []bindingCandidate, error) {
	out, err := inspectNameSelections(c, names, origin)
	if err != nil {
		return nameSelectionReport{}, nil, err
	}
	candidates := []bindingCandidate{}
	for i, n := range out.Supplied {
		evidence := out.Citations.Review.Mentions[i].Evidence
		candidates = append(candidates, bindingCandidate{
			ID: fmt.Sprintf("n%d", i), OriginalID: n.ID, Kind: n.Kind, Text: evidence.Matched.Text, Entry: evidence.Entry,
		})
	}
	return out, candidates, nil
}

// Endpoints must come from the uncorrected first-stage list. New source ranges
// can support a claim but cannot create replacement people or organizations.
func inspectBindingSelections(c modelCase, names, raw []byte, origin string) (bindingSelectionReport, error) {
	fail := func(msg string) (bindingSelectionReport, error) {
		return bindingSelectionReport{}, fmt.Errorf("role binding: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	nameReport, candidates, err := bindingCandidates(c, names, origin)
	if err != nil {
		return bindingSelectionReport{}, err
	}
	var in derivedReferenceInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return bindingSelectionReport{}, err
	}
	if in.Selections == nil || in.Interpretations == nil || in.Links == nil || len(in.Selections)+len(candidates) > 64 || len(in.Interpretations) > 32 || len(in.Links) > 32 {
		return fail("missing arrays or scope budget")
	}
	kinds := map[string]string{}
	combined := derivedReferenceInput{Selections: []citationSelection{}, Interpretations: in.Interpretations, Links: in.Links}
	for i, n := range candidates {
		kinds[n.ID] = n.Kind
		s := nameReport.Supplied[i].citationSelection
		s.ID = n.ID // Local handle only; original IDs/ranges remain in nameReport.
		combined.Selections = append(combined.Selections, s)
	}
	clauses := map[string]bool{}
	for _, s := range in.Selections {
		if kinds[s.ID] != "" {
			return fail("evidence selection redefines a name candidate")
		}
		clauses[s.ID] = true
		combined.Selections = append(combined.Selections, s)
	}
	for _, p := range in.Interpretations {
		if (p.Kind != "role" && p.Kind != "correction" && p.Kind != "unknown") || !clauses[p.Clause] {
			return fail("unsupported interpretation or missing clause selection")
		}
		for _, id := range p.Subjects {
			if kinds[id] != "person" {
				return fail("subject is not a supplied person candidate")
			}
		}
		for _, id := range p.Organizations {
			if kinds[id] != "organization" {
				return fail("organization is not a supplied organization candidate")
			}
		}
	}
	cat, err := newCitationCatalog(c)
	if err != nil {
		return bindingSelectionReport{}, err
	}
	encoded, err := json.Marshal(combined)
	if err != nil {
		return bindingSelectionReport{}, err
	}
	review, err := inspectDerivedCitationReferences(cat, encoded, origin)
	if err != nil {
		return bindingSelectionReport{}, err
	}
	return bindingSelectionReport{Method: "roles-from-name-candidates.v1", Names: nameReport, Candidates: candidates, Proposal: in, Derived: review}, nil
}
