package personaffiliation

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const EnrichmentPolicy = "fec-appearance-affiliation-evidence.v1"

// SourceNames are publisher vocabulary, not parsed given/family names. Keeping
// them intact lets labels and attested aliases work without guessing components.
type SourceNames struct {
	Source   Reference `json:"source"`
	ID       string    `json:"source_entity_id"`
	Revision int64     `json:"revision"`
	Label    string    `json:"english_label"`
	Aliases  []string  `json:"english_aliases"`
}

type EnrichedRole struct {
	CandidateRole
	Endpoint *SourceNames `json:"endpoint_names,omitempty"`
}

type EnrichedCandidate struct {
	// Relevance keeps every retrieved page, even a mismatch, untyped entity,
	// missing item or disambiguation page. Names and roles never select a winner.
	Relevance CandidateRelevance `json:"relevance"`
	Names     *SourceNames       `json:"source_names,omitempty"`
	Roles     []EnrichedRole     `json:"roles"`
}

type EnrichmentSearch struct {
	Observation      int                       `json:"observation_index"`
	Search           wikimedia.DiscoverySearch `json:"search"`
	State            string                    `json:"state"`
	Issue            string                    `json:"issue,omitempty"`
	SearchSHA256     string                    `json:"search_sha256"`
	EntityBodySHA256 string                    `json:"entity_body_sha256,omitempty"`
	Pages            []wikimedia.Page          `json:"pages"`
	Candidates       []EnrichedCandidate       `json:"candidates"`
	UnassignedRoles  []wikimedia.RoleStatement `json:"roles_without_usable_candidate"`
}

type AppearanceEnrichment struct {
	Appearance         Appearance         `json:"appearance"`
	InputState         string             `json:"input_state"`
	RetrievalState     string             `json:"retrieval_state"`
	NameState          string             `json:"name_state"`
	MatchingSourceIDs  []string           `json:"matching_source_ids"`
	EmployerContextIDs []string           `json:"employer_context_source_ids"`
	Searches           []EnrichmentSearch `json:"searches"`
}

type EnrichmentResult struct {
	Policy                   string                 `json:"policy"`
	BuildSHA256              string                 `json:"build_sha256"`
	CaptureSHA256            string                 `json:"capture_sha256"`
	Selection                string                 `json:"selection"`
	CaptureUsable            bool                   `json:"capture_usable"`
	Appearances              []AppearanceEnrichment `json:"appearances"`
	Limitations              []string               `json:"limitations"`
	IdentityResolved         bool                   `json:"identity_resolved"`
	GraphPublicationApproved bool                   `json:"graph_publication_approved"`
	FinancialAttribution     bool                   `json:"financial_attribution"`
}

// EnrichAppearances joins verified FEC appearances to ReadDiscovery's verified
// capture. Exact references AND source text must agree. Each source response
// remains its own evidence scope; no endpoint, revision or role is filled from
// another response. Repeated QIDs in the summary are an inventory, not votes.
func EnrichAppearances(appearances []Appearance, discovery wikimedia.DiscoveryResult, build string) (EnrichmentResult, error) {
	if !wikimedia.Digest(build) || !wikimedia.Digest(discovery.CaptureSHA256) || len(appearances) == 0 || len(appearances) > wikimedia.MaxQueries || len(appearances) != len(discovery.Plan.Appearances) || len(discovery.Plan.InputStates) != len(appearances) || len(discovery.Observations) != len(discovery.Plan.Searches) {
		return EnrichmentResult{}, fmt.Errorf("enrichment requires bounded verified appearances and discovery")
	}
	inputs := make(map[Reference]Appearance, len(appearances))
	for _, a := range appearances {
		if !validReference(a.Source) || a.Receipt.Ordinal < 1 {
			return EnrichmentResult{}, fmt.Errorf("invalid enrichment appearance")
		}
		if _, duplicate := inputs[a.Source]; duplicate {
			return EnrichmentResult{}, fmt.Errorf("duplicate enrichment appearance")
		}
		inputs[a.Source] = a
	}
	r := EnrichmentResult{Policy: EnrichmentPolicy, BuildSHA256: build, CaptureSHA256: discovery.CaptureSHA256,
		Selection: discovery.Plan.Selection, CaptureUsable: discovery.CaptureUsable, Appearances: []AppearanceEnrichment{},
		Limitations: []string{
			"bounded_search_windows_not_exhaustive_discovery",
			"all_retrieved_candidates_retained_including_unassessed_name_variants",
			"name_alias_and_employer_correspondence_not_identity_or_legal_entity_resolution",
			"community_statements_not_independent_corroboration",
			"repeated_source_ids_are_inventory_not_independent_support",
			"no_cross_response_endpoint_or_revision_merge",
			"source_role_time_precision_preserved_no_continuity_inference",
			"occupation_retained_without_invented_role_interpretation",
			"no_identity_winner_or_financial_attribution",
		}}
	seen := map[Reference]bool{}
	for i, input := range discovery.Plan.Appearances {
		ref := Reference{SHA256: input.SHA256, Locator: input.Locator}
		a, found := inputs[ref]
		if !found || seen[ref] || !reflect.DeepEqual(a.Receipt.Name, input.Name) || !reflect.DeepEqual(a.Receipt.Employer, input.Employer) {
			return EnrichmentResult{}, fmt.Errorf("discovery does not match exact FEC appearances")
		}
		seen[ref] = true
		out := AppearanceEnrichment{Appearance: a, InputState: discovery.Plan.InputStates[i], RetrievalState: "no_search_planned",
			NameState: "no_name_correspondence_in_search_scope", MatchingSourceIDs: []string{}, EmployerContextIDs: []string{}, Searches: []EnrichmentSearch{}}
		names, employers := map[string]bool{}, map[string]bool{}
		for oi, observation := range discovery.Observations {
			if !reflect.DeepEqual(observation.Search, discovery.Plan.Searches[oi]) {
				return EnrichmentResult{}, fmt.Errorf("discovery observation differs from planned search")
			}
			if !slices.Contains(observation.Search.Appearances, i) {
				continue
			}
			if out.RetrievalState == "no_search_planned" {
				out.RetrievalState = "planned_responses_parsed_search_scope_limited"
			}
			if observation.Issue != "" {
				out.RetrievalState = "source_unusable_or_unattempted"
			}
			search := enrichSearch(a, input, oi, observation)
			for _, candidate := range search.Candidates {
				c := candidate.Relevance
				if len(c.PersonNames) == 0 {
					continue
				}
				names["wikidata:"+c.QID] = true
				for _, role := range candidate.Roles {
					if len(role.EmployerNames) > 0 {
						employers["wikidata:"+c.QID] = true
					}
				}
			}
			out.Searches = append(out.Searches, search)
		}
		out.MatchingSourceIDs, out.EmployerContextIDs = stringSet(names), stringSet(employers)
		if len(names) > 1 {
			out.NameState = "multiple_name_candidates_identity_unresolved"
		} else if len(names) == 1 {
			out.NameState = "one_name_candidate_other_results_unassessed_identity_unresolved"
		}
		r.Appearances = append(r.Appearances, out)
	}
	sort.Slice(r.Appearances, func(i, j int) bool {
		return referenceLess(r.Appearances[i].Appearance.Source, r.Appearances[j].Appearance.Source)
	})
	return r, nil
}

func enrichSearch(a Appearance, input wikimedia.DiscoveryAppearance, index int, o wikimedia.DiscoveryObservation) EnrichmentSearch {
	r := EnrichmentSearch{Observation: index, Search: o.Search, State: o.State, Issue: o.Issue, SearchSHA256: o.SearchSHA256, Pages: slices.Clone(o.Pages), Candidates: []EnrichedCandidate{}, UnassignedRoles: []wikimedia.RoleStatement{}}
	entities := map[string]wikimedia.RoleEntity{}
	assigned := map[string]bool{}
	if o.Roles != nil {
		r.EntityBodySHA256 = o.Roles.BodySHA256
		for _, e := range o.Roles.Entities {
			entities[e.ID] = e
		}
	}
	for _, c := range o.Candidates {
		relevance := assessDiscoveryCandidate(input, o, c, entities)
		out := EnrichedCandidate{Relevance: relevance, Roles: []EnrichedRole{}}
		if e, ok := entities[c.QID]; ok {
			out.Names = sourceNames(r.EntityBodySHA256, e)
		}
		for _, role := range relevance.Roles {
			assigned[role.Statement.Locator] = true
			role.TemporalState = compareRoleDate(a.Receipt.ReceiptDate, role.Statement)
			d := EnrichedRole{CandidateRole: role}
			if endpoint, found := entities[strings.TrimPrefix(role.Statement.RelatedEntityID, "wikidata:")]; found {
				d.Endpoint = sourceNames(r.EntityBodySHA256, endpoint)
			}
			out.Roles = append(out.Roles, d)
		}
		// Role details occur once in the report, with actual receipt-date checks.
		out.Relevance.Roles = nil
		r.Candidates = append(r.Candidates, out)
	}
	if o.Roles != nil {
		for _, e := range o.Roles.Entities {
			for _, s := range e.Statements {
				if !assigned[s.Locator] {
					r.UnassignedRoles = append(r.UnassignedRoles, s)
				}
			}
		}
	}
	return r
}

func sourceNames(body string, e wikimedia.RoleEntity) *SourceNames {
	return &SourceNames{Source: Reference{SHA256: body, Locator: "/entities/" + e.ID}, ID: "wikidata:" + e.ID,
		Revision: e.Revision, Label: e.Label, Aliases: slices.Clone(e.Aliases)}
}
