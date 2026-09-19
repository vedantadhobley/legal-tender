package personaffiliation

import (
	"fmt"
	"slices"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/nameform"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const DiscoveryRelevancePolicy = "affiliation-candidate-relevance.v1"

type DiscoveryRelevance struct {
	Policy                   string                          `json:"policy"`
	BuildSHA256              string                          `json:"build_sha256"`
	CaptureSHA256            string                          `json:"capture_sha256"`
	CaptureUsable            bool                            `json:"capture_usable"`
	Appearances              []wikimedia.DiscoveryAppearance `json:"appearances"`
	InputStates              []string                        `json:"input_states"`
	Observations             []AppearanceSearch              `json:"observations"`
	ExhaustiveDiscovery      bool                            `json:"exhaustive_discovery"`
	IdentityApproved         bool                            `json:"identity_approved"`
	EmploymentVerified       bool                            `json:"employment_verified"`
	GraphPublicationApproved bool                            `json:"graph_publication_approved"`
	FinancialAttribution     bool                            `json:"financial_attribution"`
}

type AppearanceSearch struct {
	Appearance       int                  `json:"appearance_index"`
	Observation      int                  `json:"observation_index"`
	State            string               `json:"source_state"`
	Issue            string               `json:"source_issue,omitempty"`
	SearchSHA256     string               `json:"search_sha256"`
	EntityBodySHA256 string               `json:"entity_body_sha256,omitempty"`
	Candidates       []CandidateRelevance `json:"candidates"`
}

type CandidateRelevance struct {
	PageID                 int64                              `json:"page_id"`
	QID                    string                             `json:"qid,omitempty"`
	Revision               int64                              `json:"entity_revision,omitempty"`
	SourceState            string                             `json:"source_state"`
	HumanStatementObserved bool                               `json:"human_statement_observed"`
	PersonNameState        string                             `json:"person_name_state"`
	PersonNames            []organizationresolution.NameMatch `json:"person_name_correspondences"`
	EmployerNames          []organizationresolution.NameMatch `json:"employer_name_correspondences"`
	Roles                  []CandidateRole                    `json:"reported_role_checks"`
}

type CandidateRole struct {
	Statement        wikimedia.RoleStatement            `json:"statement"`
	EndpointState    string                             `json:"related_endpoint_state"`
	EndpointRevision int64                              `json:"related_endpoint_revision,omitempty"`
	EmployerNames    []organizationresolution.NameMatch `json:"employer_name_correspondences"`
	TemporalState    string                             `json:"temporal_state"`
}

// AssessDiscovery consumes verified ReadDiscovery output. It describes names
// and statement endpoints within each response; it neither joins revisions nor
// picks identities. Query context, rank and repeated hits are not corroboration.
func AssessDiscovery(r wikimedia.DiscoveryResult, build string) DiscoveryRelevance {
	out := DiscoveryRelevance{Policy: DiscoveryRelevancePolicy, BuildSHA256: build,
		CaptureSHA256: r.CaptureSHA256, CaptureUsable: r.CaptureUsable, Appearances: slices.Clone(r.Plan.Appearances), InputStates: slices.Clone(r.Plan.InputStates), Observations: []AppearanceSearch{}}
	for ai, appearance := range r.Plan.Appearances {
		for oi, o := range r.Observations {
			if !slices.Contains(o.Search.Appearances, ai) {
				continue
			}
			a := AppearanceSearch{Appearance: ai, Observation: oi, State: o.State,
				Issue: o.Issue, SearchSHA256: o.SearchSHA256, Candidates: []CandidateRelevance{}}
			entities := map[string]wikimedia.RoleEntity{}
			if o.Roles != nil {
				a.EntityBodySHA256 = o.Roles.BodySHA256
				for _, e := range o.Roles.Entities {
					entities[e.ID] = e
				}
			}
			for _, c := range o.Candidates {
				a.Candidates = append(a.Candidates, assessDiscoveryCandidate(appearance, o, c, entities))
			}
			out.Observations = append(out.Observations, a)
		}
	}
	return out
}

func assessDiscoveryCandidate(a wikimedia.DiscoveryAppearance, o wikimedia.DiscoveryObservation, c wikimedia.DiscoveryCandidate, entities map[string]wikimedia.RoleEntity) CandidateRelevance {
	d := CandidateRelevance{PageID: c.PageID, QID: c.QID, SourceState: c.State,
		HumanStatementObserved: c.HumanStatementObserved, PersonNameState: "source_unusable",
		PersonNames: []organizationresolution.NameMatch{}, EmployerNames: []organizationresolution.NameMatch{}, Roles: []CandidateRole{}}
	e, loaded := entities[c.QID]
	if o.Issue != "" || c.State != "type_unverified" || !loaded || o.Roles == nil {
		return d
	}
	d.Revision = e.Revision
	d.PersonNames = discoveryNameMatches(a.Name, e, o.Pages, true)
	d.EmployerNames = discoveryNameMatches(a.Employer, e, o.Pages, false)
	d.PersonNameState = "name_not_corresponding"
	if a.Name == nil || nameform.Normalize(*a.Name) == "" {
		d.PersonNameState = "reported_name_unavailable"
	} else if len(d.PersonNames) > 0 {
		d.PersonNameState = "name_corresponds_type_unverified"
		if c.HumanStatementObserved {
			d.PersonNameState = "name_corresponds_human_item"
		}
	}
	// Include every statement occurrence naming this holder, even if its name
	// differs or its rank/qualifiers make it unusable. There is no screening here.
	for _, subject := range o.Roles.Entities {
		for _, s := range subject.Statements {
			if s.HolderID == "wikidata:"+c.QID {
				d.Roles = append(d.Roles, discoveryRoleCheck(a.Employer, s, o.Pages, entities))
			}
		}
	}
	return d
}

func discoveryRoleCheck(employer *string, s wikimedia.RoleStatement, pages []wikimedia.Page, entities map[string]wikimedia.RoleEntity) CandidateRole {
	c := CandidateRole{Statement: s, EndpointState: "endpoint_not_loaded", EmployerNames: []organizationresolution.NameMatch{}, TemporalState: "not_assessed"}
	if s.RelatedEntityID == "" {
		c.EndpointState = "endpoint_unspecified"
		return c
	}
	e, found := entities[strings.TrimPrefix(s.RelatedEntityID, "wikidata:")]
	if !found {
		return c
	}
	c.EndpointRevision = e.Revision
	if e.State == "source_entity_missing" {
		c.EndpointState = "endpoint_missing"
		return c
	}
	c.EmployerNames = discoveryNameMatches(employer, e, pages, false)
	c.EndpointState = "employer_name_not_corresponding"
	if employer == nil || nameform.Normalize(*employer) == "" {
		c.EndpointState = "reported_employer_unavailable"
	} else if len(c.EmployerNames) > 0 {
		c.EndpointState = "employer_name_corresponds_type_unverified"
	}
	return c
}

func discoveryNameMatches(reported *string, e wikimedia.RoleEntity, pages []wikimedia.Page, person bool) []organizationresolution.NameMatch {
	matches := []organizationresolution.NameMatch{}
	if reported == nil {
		return matches
	}
	add := func(source, text string, pageID int64) {
		var m organizationresolution.NameMatch
		var ok bool
		if person {
			q, n := nameform.Normalize(*reported), nameform.Normalize(text)
			if q != "" && q == n {
				m = organizationresolution.NameMatch{Rule: "normalized_exact", Query: organizationresolution.NameForm{Normalized: q, Comparison: q}, Candidate: organizationresolution.NameForm{Normalized: n, Comparison: n}}
				ok = true
			}
		} else {
			m, ok = organizationresolution.MatchName(*reported, text)
		}
		if ok {
			m.Source, m.Name, m.PageID = source, text, pageID
			matches = append(matches, m)
		}
	}
	add("english_label", e.Label, 0)
	for i, alias := range e.Aliases {
		add(fmt.Sprintf("english_alias:%d", i), alias, 0)
	}
	for _, p := range pages {
		if _, disambiguation := p.Props["disambiguation"]; !disambiguation && p.Props["wikibase_item"] == e.ID {
			add("wikipedia_title", p.Title, p.ID)
		}
	}
	return matches
}
