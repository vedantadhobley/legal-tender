package personaffiliation

import (
	"fmt"
	"slices"
	"sort"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const BindingPolicy = "person-affiliation-binding-diagnostic.v1"

type BindingCandidate struct {
	ID            string          `json:"source_entity_id"`
	Revision      int64           `json:"entity_revision"`
	Names         []org.NameMatch `json:"person_name_correspondences"`
	EmployerState string          `json:"reported_employer_context"`
	Roles         []CandidateRole `json:"reported_role_checks"`
}

type BindingResult struct {
	Policy                   string             `json:"policy"`
	Appearance               Appearance         `json:"appearance"`
	BodySHA256               string             `json:"body_sha256"`
	ExpectedIDs              []string           `json:"expected_ids"`
	NameState                string             `json:"name_candidate_state"`
	Candidates               []BindingCandidate `json:"candidates"`
	Limitations              []string           `json:"limitations"`
	IdentityResolved         bool               `json:"identity_resolved"`
	GraphPublicationApproved bool               `json:"graph_publication_approved"`
	FinancialAttribution     bool               `json:"financial_attribution"`
}

// AssessBinding compares one verified appearance with one ExtractRoles response.
// Like AssessDiscovery, it consumes the source reader's output, not untrusted
// serialized claims. The caller verifies source bytes. No review labels, candidate
// QIDs, receipt amounts or cycle boundaries are inputs to the matching rules.
// This diagnoses evidence gaps; even a unique name/employer/date correspondence
// is not an identity decision or verified transaction-time affiliation.
func AssessBinding(a Appearance, evidence wikimedia.RoleEvidence) (BindingResult, error) {
	if !validReference(a.Source) || a.Receipt.Ordinal < 1 || evidence.Contract != wikimedia.RoleContract || !wikimedia.Digest(evidence.BodySHA256) || len(evidence.Entities) == 0 || len(evidence.Entities) > wikimedia.MaxRoleEntities {
		return BindingResult{}, fmt.Errorf("binding requires a source appearance and extracted role evidence")
	}
	entities := make(map[string]wikimedia.RoleEntity, len(evidence.Entities))
	ordered := slices.Clone(evidence.Entities)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	count := 0
	for _, e := range ordered {
		if _, duplicate := entities[e.ID]; duplicate || e.ID == "" {
			return BindingResult{}, fmt.Errorf("duplicate or missing binding entity ID")
		}
		entities[e.ID] = e
		count += len(e.Statements)
		if count > maxClaims {
			return BindingResult{}, fmt.Errorf("binding statement budget")
		}
	}
	r := BindingResult{
		Policy: BindingPolicy, Appearance: a, BodySHA256: evidence.BodySHA256,
		ExpectedIDs: slices.Clone(evidence.ExpectedIDs), NameState: "no_name_candidate_in_supplied_response",
		Candidates: []BindingCandidate{}, Limitations: []string{
			"caller_verified_source_inputs_not_authenticated_here",
			"supplied_snapshot_not_exhaustive_person_search",
			"name_alias_and_employer_correspondence_not_identity_or_entity_type",
			"role_time_compares_reported_qualifiers_not_verified_historical_truth",
			"source_issues_and_other_qualifiers_remain_unassessed_constraints",
			"reported_occupation_not_interpreted_or_cross_checked",
			"no_cross_response_identity_or_relationship_join",
			"no_identity_winner_primary_company_or_money_allocation",
		},
	}
	sort.Strings(r.ExpectedIDs)
	if a.Receipt.Name == nil || org.Normalize(*a.Receipt.Name) == "" {
		r.NameState = "reported_name_unavailable"
	}
	for _, e := range ordered {
		if e.State == "source_entity_missing" {
			continue
		}
		names := discoveryNameMatches(a.Receipt.Name, e, nil, true)
		if len(names) == 0 {
			continue
		}
		c := BindingCandidate{ID: "wikidata:" + e.ID, Revision: e.Revision, Names: names,
			EmployerState: "no_corresponding_employer_endpoint_observed", Roles: []CandidateRole{}}
		if a.Receipt.Employer == nil || org.Normalize(*a.Receipt.Employer) == "" {
			c.EmployerState = "reported_employer_unavailable"
		}
		for _, subject := range ordered {
			for _, s := range subject.Statements {
				if s.HolderID != c.ID {
					continue
				}
				d := discoveryRoleCheck(a.Receipt.Employer, s, nil, entities)
				d.TemporalState = compareRoleDate(a.Receipt.ReceiptDate, s)
				c.Roles = append(c.Roles, d)
				if len(d.EmployerNames) > 0 {
					// Expiry, rank and source issues cannot erase a rival's name or
					// employer correspondence and manufacture a unique identity.
					c.EmployerState = "employer_name_correspondence_not_identity"
				}
			}
		}
		sort.Slice(c.Roles, func(i, j int) bool { return c.Roles[i].Statement.Locator < c.Roles[j].Statement.Locator })
		r.Candidates = append(r.Candidates, c)
	}
	if len(r.Candidates) == 1 {
		r.NameState = "one_name_candidate_identity_unresolved"
	} else if len(r.Candidates) > 1 {
		r.NameState = "multiple_name_candidates_identity_unresolved"
	}
	return r, nil
}
