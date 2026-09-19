package organizationresolution

import (
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const ExpandedPolicy = "organization-name-proposals.v2"

type ProposalBaseline struct {
	Policy      string `json:"policy"`
	State       string `json:"state"`
	ProposedQID string `json:"proposed_qid,omitempty"`
}

// NameMatch explains a candidate proposal, never identity or confidence. Label,
// alias and page-title matches remain separate source observations, not votes.
type NameMatch struct {
	Source    string   `json:"source"`
	Name      string   `json:"name"`
	PageID    int64    `json:"page_id,omitempty"`
	Rule      string   `json:"rule"`
	Query     NameForm `json:"query_form"`
	Candidate NameForm `json:"candidate_form"`
}

type NameForm struct {
	Normalized            string `json:"normalized"`
	Comparison            string `json:"comparison"`
	DigitLetterBoundaries bool   `json:"digit_letter_boundaries,omitempty"`
	RemovedLegalSuffix    string `json:"removed_legal_suffix,omitempty"`
}

func ValidPolicy(policy string) bool {
	return policy == "" || policy == Policy || policy == ExpandedPolicy
}

// ResolveWithPolicy defaults to the frozen v1 matcher. V2 is opt-in and retains
// the v1 outcome for each observation, including failures and abstentions.
func ResolveWithPolicy(replay wikimedia.Replay, build, policy string) (Result, error) {
	if !ValidPolicy(policy) {
		return Result{}, fmt.Errorf("unsupported organization proposal policy %q", policy)
	}
	r := Resolve(replay, build)
	if policy != ExpandedPolicy {
		return r, nil
	}
	r.Policy = ExpandedPolicy
	for i := range r.Decisions {
		d := &r.Decisions[i]
		d.Baseline = &ProposalBaseline{Policy: Policy, State: d.State, ProposedQID: d.ProposedQID}
		if d.Evidence.Issue != "" {
			continue
		}
		incomplete := false
		for _, page := range d.Evidence.Pages {
			if page.Props["wikibase_item"] == "" {
				incomplete = true
			}
		}
		var matched []string
		for j := range d.Candidates {
			c := &d.Candidates[j]
			e, ok := d.Evidence.Entities[c.QID]
			if !ok || e.ID != c.QID || e.Missing != nil || e.Type != "item" {
				c.State = "entity_evidence_incomplete"
				incomplete = true
				continue
			}
			// Reuse v1's direct-human and disambiguation guards, not its winner.
			if c.State != "name_not_exact" && c.State != "exact_name_type_unverified" {
				continue
			}
			c.Matches = candidateMatches(d.Evidence, e)
			c.State = "name_not_matched"
			if len(c.Matches) > 0 {
				c.State = "name_candidate_type_unverified"
				matched = append(matched, c.QID)
			}
		}
		d.ProposedQID = ""
		d.State = "no_name_candidate"
		switch {
		case len(matched) > 1:
			d.State = "ambiguous_name_candidates"
		case incomplete:
			d.State = "candidate_evidence_incomplete"
		case len(matched) == 1:
			d.State = "single_name_candidate_in_search_window"
			d.ProposedQID = matched[0]
		case len(d.Evidence.Pages) == 0:
			d.State = "no_search_results"
		}
	}
	return r, nil
}

func candidateMatches(o wikimedia.Observation, e wikimedia.Entity) []NameMatch {
	seen := map[NameMatch]bool{}
	add := func(source, name string, pageID int64) {
		m, ok := matchName(o.Query.Text, name)
		if ok {
			m.Source, m.Name, m.PageID = source, name, pageID
			seen[m] = true
		}
	}
	if label, ok := e.Labels["en"]; ok {
		add("wikidata_label_en", label.Value, 0)
	}
	for _, alias := range e.Aliases["en"] {
		add("wikidata_alias_en", alias.Value, 0)
	}
	for _, p := range o.Pages {
		_, disambiguation := p.Props["disambiguation"]
		if p.Props["wikibase_item"] == e.ID && !disambiguation {
			add("wikipedia_title", p.Title, p.ID)
		}
	}
	matches := make([]NameMatch, 0, len(seen))
	for m := range seen {
		matches = append(matches, m)
	}
	sort.Slice(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.PageID < b.PageID
	})
	return matches
}
