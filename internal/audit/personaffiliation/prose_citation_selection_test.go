package personaffiliation

import (
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Pointer fields distinguish missing coordinates from the valid token ID zero.
// There are deliberately no caller-supplied quote, offset or approval fields.
type citationSelection struct {
	ID    string `json:"id"`
	Entry *int   `json:"entry"`
	First *int   `json:"first_token"`
	Last  *int   `json:"last_token"`
}
type citationSelectionInput struct {
	Selections      []citationSelection      `json:"selections"`
	Interpretations []interpretationProposal `json:"interpretations"`
	Links           []contextProposal        `json:"context_links"`
}
type selectedEntryContext struct {
	Interpretation string       `json:"interpretation"`
	Entries        []modelEntry `json:"selected_entries"`
}
type citationSelectionReport struct {
	Method   string                      `json:"method"`
	Review   mentionInterpretationReport `json:"review"`
	Contexts []selectedEntryContext      `json:"interpretation_context"`
}

// Origin is caller-owned provenance, not something the proposal can claim.
// Reviewed tests and the opt-in model adapter supply their own distinct origins.
func inspectCitationSelections(c citationCatalog, raw []byte, origin string) (citationSelectionReport, error) {
	fail := func(msg string) (citationSelectionReport, error) {
		return citationSelectionReport{}, fmt.Errorf("citation selection: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	var in citationSelectionInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return citationSelectionReport{}, err
	}
	if in.Selections == nil || len(in.Selections) > 64 {
		return fail("missing or unbounded selections")
	}
	input := mentionInterpretationInput{Origin: origin, Mentions: []mentionProposal{}, Interpretations: in.Interpretations, Links: in.Links}
	entries := map[string]int{}
	for _, s := range in.Selections {
		if s.Entry == nil || s.First == nil || s.Last == nil {
			return fail("missing token reference")
		}
		quote, err := c.selectRange(citationRange{Entry: *s.Entry, First: *s.First, Last: *s.Last})
		if err != nil {
			return citationSelectionReport{}, err
		}
		input.Mentions = append(input.Mentions, mentionProposal{ID: s.ID, Entry: quote.Entry, Text: quote.Matched.Text, Start: &quote.Matched.Start})
		entries[s.ID] = quote.Entry
	}
	review, err := inspectMentionInterpretations(c.Case, encodeMentionInput(input))
	if err != nil {
		return citationSelectionReport{}, err // Keep existing semantic-reference and context rules unchanged.
	}
	out := citationSelectionReport{Method: "source-token-citation.v1", Review: review, Contexts: []selectedEntryContext{}}
	for _, p := range in.Interpretations {
		selected := map[int]bool{}
		for _, ref := range p.Evidence {
			selected[entries[ref]] = true
		}
		context := selectedEntryContext{Interpretation: p.ID, Entries: []modelEntry{}}
		for _, entry := range c.Case.Entries {
			if selected[entry.ID] {
				context.Entries = append(context.Entries, entry)
			}
		}
		out.Contexts = append(out.Contexts, context)
	}
	return out, nil
}
