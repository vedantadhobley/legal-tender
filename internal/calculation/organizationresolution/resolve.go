package organizationresolution

import (
	"encoding/json"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/nameform"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const Policy = "organization-name-proposals.v1"

type Candidate struct {
	QID        string      `json:"qid"`
	ExactNames []string    `json:"exact_names"`
	State      string      `json:"state"`
	Matches    []NameMatch `json:"name_matches,omitempty"`
}

type Decision struct {
	Evidence    wikimedia.Observation `json:"evidence"`
	State       string                `json:"state"`
	Candidates  []Candidate           `json:"candidates"`
	ProposedQID string                `json:"proposed_qid,omitempty"`
	Baseline    *ProposalBaseline     `json:"baseline,omitempty"`
}

type Result struct {
	Policy               string     `json:"policy"`
	BuildSHA256          string     `json:"build_sha256"`
	CaptureSHA256        string     `json:"capture_sha256"`
	Complete             bool       `json:"capture_usable"`
	IdentityResolved     bool       `json:"identity_resolved"`
	EmploymentVerified   bool       `json:"employment_verified"`
	OwnershipVerified    bool       `json:"ownership_verified"`
	FinancialAttribution bool       `json:"financial_attribution"`
	Decisions            []Decision `json:"decisions"`
}

// Normalize preserves word order, accents and legal suffixes. It intentionally
// does not fix spelling, remove "Inc", expand abbreviations or merge tokens.
func Normalize(s string) string {
	return nameform.Normalize(s)
}

func Resolve(replay wikimedia.Replay, build string) Result {
	r := Result{Policy: Policy, BuildSHA256: build, CaptureSHA256: replay.CaptureSHA256, Complete: true, Decisions: []Decision{}}
	for _, o := range replay.Observations {
		d := Decision{Evidence: o, State: "no_exact_name_candidate", Candidates: []Candidate{}}
		if o.Issue != "" {
			d.State = "source_incomplete"
			r.Complete = false
			r.Decisions = append(r.Decisions, d)
			continue
		}
		ids := wikimedia.PageIDs(o.Pages)
		var exact []string
		incomplete := false
		for _, p := range o.Pages {
			if p.Props["wikibase_item"] == "" {
				incomplete = true
			}
		}
		for _, id := range ids {
			e := o.Entities[id]
			c := Candidate{QID: id, State: "name_not_exact", ExactNames: []string{}}
			if e.Missing != nil {
				c.State = "entity_missing"
				incomplete = true
			} else if human(e) {
				c.State = "human_type_conflict"
			} else {
				names := map[string]bool{}
				if label, ok := e.Labels["en"]; ok {
					names[label.Value] = true
				}
				for _, a := range e.Aliases["en"] {
					names[a.Value] = true
				}
				usablePage := false
				for _, p := range o.Pages {
					if p.Props["wikibase_item"] == id {
						if _, disambig := p.Props["disambiguation"]; !disambig {
							names[p.Title] = true
							usablePage = true
						}
					}
				}
				if !usablePage {
					c.State = "disambiguation_page"
				} else {
					for n := range names {
						if Normalize(n) != "" && Normalize(n) == Normalize(o.Query.Text) {
							c.ExactNames = append(c.ExactNames, n)
						}
					}
					sort.Strings(c.ExactNames)
					if len(c.ExactNames) > 0 {
						c.State = "exact_name_type_unverified"
						exact = append(exact, id)
					}
				}
			}
			d.Candidates = append(d.Candidates, c)
		}
		switch {
		case len(exact) > 1:
			d.State = "ambiguous_exact_name_candidates"
		case incomplete:
			d.State = "candidate_evidence_incomplete"
		case len(exact) == 1:
			d.State = "single_exact_name_candidate_in_search_window"
			d.ProposedQID = exact[0]
		case len(o.Pages) == 0:
			d.State = "no_search_results"
		}
		r.Decisions = append(r.Decisions, d)
	}
	return r
}

// A direct nondeprecated P31=Q5 assertion is a conflict for an organization
// query. Other P31 values are NOT treated as proof of corporate identity. All
// statements, including deprecated and qualified ones, remain in Evidence.
func human(e wikimedia.Entity) bool {
	var claims []struct {
		Rank string `json:"rank"`
		Main struct {
			Property string `json:"property"`
			SnakType string `json:"snaktype"`
			Value    struct {
				Type  string `json:"type"`
				Value struct {
					ID string `json:"id"`
				} `json:"value"`
			} `json:"datavalue"`
		} `json:"mainsnak"`
	}
	if json.Unmarshal(e.Claims["P31"], &claims) != nil {
		return false
	}
	for _, c := range claims {
		if (c.Rank == "normal" || c.Rank == "preferred") && c.Main.Property == "P31" && c.Main.SnakType == "value" && c.Main.Value.Type == "wikibase-entityid" && c.Main.Value.Value.ID == "Q5" {
			return true
		}
	}
	return false
}
