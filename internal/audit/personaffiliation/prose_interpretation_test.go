package personaffiliation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Test-only contract proof, NOT a prose interpreter. Callers propose semantics;
// Go validates literal anchors/references and applies explicit context links.
type mentionProposal struct {
	ID    string `json:"id"`
	Entry int    `json:"entry"`
	Text  string `json:"text"`
	Start *int   `json:"text_start,omitempty"`
}
type interpretationProposal struct {
	ID            string      `json:"id"`
	Clause        string      `json:"clause"`
	Kind          string      `json:"kind"`
	Normalized    string      `json:"normalized"`
	Role          screen.Role `json:"role_kind,omitempty"`
	Subjects      []string    `json:"subjects"`
	Organizations []string    `json:"organizations"`
	Evidence      []string    `json:"evidence"`
	Binding       string      `json:"binding"`
	Status        string      `json:"status"`
}
type contextProposal struct {
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"`
}
type mentionInterpretationInput struct {
	Origin          string                   `json:"interpretation_origin"`
	Mentions        []mentionProposal        `json:"mentions"`
	Interpretations []interpretationProposal `json:"interpretations"`
	Links           []contextProposal        `json:"context_links"`
}
type boundMention struct {
	ID       string               `json:"id"`
	Evidence screen.ProseCitation `json:"evidence"`
}
type interpretationReview struct {
	Proposal     interpretationProposal `json:"proposal"`
	ContextFlags []string               `json:"context_flags"`
	State        string                 `json:"review_state"`
}
type mentionInterpretationReport struct {
	Method                   string                 `json:"method"`
	Origin                   string                 `json:"interpretation_origin"`
	Source                   companypage.Source     `json:"source"`
	Context                  []modelEntry           `json:"context"`
	Mentions                 []boundMention         `json:"literal_mentions"`
	Interpretations          []interpretationReview `json:"interpretations"`
	Links                    []contextProposal      `json:"context_links"`
	Limitations              []string               `json:"limitations"`
	IdentityApproved         bool                   `json:"identity_approved"`
	GraphPublicationApproved bool                   `json:"graph_publication_approved"`
	FinancialAttribution     bool                   `json:"financial_attribution"`
}

func inspectMentionInterpretations(c modelCase, raw []byte) (mentionInterpretationReport, error) {
	var in mentionInterpretationInput
	fail := func(msg string) (mentionInterpretationReport, error) {
		return mentionInterpretationReport{}, fmt.Errorf("mention interpretation: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	if err := strictjson.Decode(raw, &in); err != nil {
		return mentionInterpretationReport{}, err
	}
	if err := c.Source.Validate(); err != nil {
		return mentionInterpretationReport{}, err
	}
	if in.Origin != "reviewed_fixture" && in.Origin != "synthetic_test" && in.Origin != "model_proposal" {
		return fail("unsupported annotation origin")
	}
	if in.Mentions == nil || in.Interpretations == nil || in.Links == nil || len(in.Mentions) > 64 || len(in.Interpretations) > 32 || len(in.Links) > 32 || len(c.Entries) == 0 || len(c.Entries) > 32 {
		return fail("missing arrays or scope budget")
	}
	entries := map[int]modelEntry{}
	for _, e := range c.Entries {
		if _, ok := entries[e.ID]; ok || e.ID < 0 || e.Text == "" || !utf8.ValidString(e.Text) {
			return fail("invalid source entries")
		}
		entries[e.ID] = e
	}
	out := mentionInterpretationReport{Method: "mention-interpretation-boundary.v1", Origin: in.Origin, Source: c.Source,
		Context: slices.Clone(c.Entries), Mentions: []boundMention{}, Interpretations: []interpretationReview{}, Links: slices.Clone(in.Links),
		Limitations: []string{"reader_verified_source_required", "annotations_supplied_not_automatically_extracted", "literal_anchors_not_semantic_validation", "context_links_are_supplied_interpretations_not_verified_corrections", "no_canonical_identity_or_date_interval", "no_automatic_affiliation_acceptance"}}
	if in.Origin == "model_proposal" {
		out.Limitations[1] = "model_supplied_interpretations_not_semantically_verified"
	}
	mentions := map[string]boundMention{}
	for _, m := range in.Mentions {
		e, ok := entries[m.Entry]
		if !ok || m.ID == "" || len(m.ID) > 64 || m.Text == "" || !utf8.ValidString(m.Text) {
			return fail("invalid mention")
		}
		if _, ok := mentions[m.ID]; ok {
			return fail("duplicate mention ID")
		}
		start := strings.Index(e.Text, m.Text)
		if m.Start != nil {
			start = *m.Start
		} else if start >= 0 && strings.Contains(e.Text[start+1:], m.Text) {
			return fail("repeated text requires an explicit occurrence offset")
		}
		if start < 0 || start > len(e.Text) || len(m.Text) > len(e.Text)-start || e.Text[start:start+len(m.Text)] != m.Text || !utf8.ValidString(e.Text[:start]) {
			return fail("nonliteral mention or invalid text byte offset")
		}
		v := boundMention{ID: m.ID, Evidence: screen.ProseCitation{Entry: m.Entry, HTML: e.Span, Matched: screen.ProseText{Text: m.Text, Start: start, End: start + len(m.Text)}}}
		mentions[m.ID] = v
		out.Mentions = append(out.Mentions, v)
	}
	refsValid := func(ids []string) bool {
		seen := map[string]bool{}
		for _, id := range ids {
			if _, ok := mentions[id]; !ok || seen[id] {
				return false
			}
			seen[id] = true
		}
		return true
	}
	items := map[string]interpretationProposal{}
	for _, p := range in.Interpretations {
		if _, exists := items[p.ID]; exists || p.ID == "" || len(p.ID) > 64 || len(p.Normalized) > 256 {
			return fail("interpretation ID or label")
		}
		if !slices.Contains([]string{"role", "activity", "name_form", "correction", "unknown"}, p.Kind) ||
			!slices.Contains([]string{"direct", "coreference", "ambiguous", "unresolved"}, p.Binding) ||
			!slices.Contains([]string{"asserted", "denied", "hypothetical", "unclear"}, p.Status) {
			return fail("unsupported interpretation vocabulary")
		}
		if p.Subjects == nil || p.Organizations == nil || len(p.Evidence) == 0 || !refsValid(p.Evidence) || !refsValid(p.Subjects) || !refsValid(p.Organizations) || !slices.Contains(p.Evidence, p.Clause) {
			return fail("missing mention reference")
		}
		for _, id := range append(slices.Clone(p.Subjects), p.Organizations...) {
			if !slices.Contains(p.Evidence, id) {
				return fail("binding anchor not in evidence")
			}
		}
		if p.Kind == "role" {
			if !slices.Contains([]screen.Role{screen.Executive, screen.BoardDirector, screen.Employee, screen.Founder, screen.Owner, screen.ControllingOwner, screen.UnknownRole}, p.Role) {
				return fail("role classification required")
			}
			if p.Binding != "unresolved" && (len(p.Subjects) == 0 || len(p.Organizations) == 0) {
				return fail("role endpoints missing")
			}
		} else if p.Role != "" {
			return fail("non-role interpretation carries a role")
		}
		if p.Binding == "direct" || p.Binding == "coreference" {
			if len(p.Subjects) > 1 || len(p.Organizations) > 1 {
				return fail("multiple alternatives require ambiguity")
			}
		}
		items[p.ID] = p
	}
	flags := map[string][]string{}
	seenLinks := map[contextProposal]bool{}
	for _, link := range in.Links {
		from, a := items[link.From]
		to, b := items[link.To]
		if !a || !b || link.From == link.To || seenLinks[link] {
			return fail("invalid context reference")
		}
		seenLinks[link] = true
		switch link.Kind {
		case "retracts":
			if from.Kind != "correction" || to.Kind != "role" {
				return fail("retraction requires correction and role target")
			}
			flags[link.To] = append(flags[link.To], "retracted_by_supplied_interpretation")
		case "contradicts":
			if from.Kind != "role" || to.Kind != "role" {
				return fail("conflict requires two role interpretations")
			}
			flags[link.From] = append(flags[link.From], "conflicting_supplied_interpretations")
			flags[link.To] = append(flags[link.To], "conflicting_supplied_interpretations")
		default:
			return fail("unsupported context link")
		}
	}
	for _, p := range in.Interpretations {
		f := append([]string{}, flags[p.ID]...)
		slices.Sort(f)
		f = slices.Compact(f)
		state := "unverified_" + p.Kind + "_interpretation"
		switch {
		case len(f) != 0:
			state = "context_blocked"
		case p.Kind == "activity":
			state = "incidental_activity_not_role"
		case p.Kind != "role": // Name forms and corrections never become roles.
		case p.Status != "asserted":
			state = "nonpositive_" + p.Status
		case p.Binding == "ambiguous" || p.Binding == "unresolved":
			state = "binding_" + p.Binding
		}
		out.Interpretations = append(out.Interpretations, interpretationReview{Proposal: p, ContextFlags: f, State: state})
	}
	return out, nil
}

func encodeMentionInput(in mentionInterpretationInput) []byte {
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	} // Only fixed JSON-safe test structs reach this helper.
	return b
}
