package personaffiliation

import (
	"fmt"
	"sort"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
)

const EvidencePolicy = "person-affiliation-evidence.v1"

// RoleObservation is a caller-authenticated source assertion. Polarity is
// "asserted" or "denied"; denial must be explicit, never inferred from a missing
// role. Origin identifies the underlying observation, shared by republications.
// An absent origin is unknown provenance, not an independent observation.
// Claim dates describe role validity, never document or acquisition dates.
// RoleID names a specific role, not just the broad Claim.Role classification;
// leaving it empty preserves the assertion but prevents continuity inference.
type RoleObservation struct {
	Claim    Claim      `json:"claim"`
	RoleID   string     `json:"source_role_id,omitempty"`
	Polarity string     `json:"polarity"`
	Origin   *Reference `json:"origin,omitempty"`
}

type ObservationDecision struct {
	Observation   RoleObservation `json:"observation"`
	NameMatch     bool            `json:"name_correspondence"`
	EmployerMatch *org.NameMatch  `json:"employer_correspondence,omitempty"`
	RoleState     string          `json:"role_state"`
	TimeState     string          `json:"time_state"`
}

type RoleTimeline struct {
	PersonID              string      `json:"source_person_id"`
	OrganizationID        string      `json:"source_organization_id"`
	Role                  Role        `json:"role"`
	RoleID                string      `json:"source_role_id,omitempty"`
	State                 string      `json:"state"`
	Members               []Reference `json:"members"`
	Support               []Reference `json:"source_support_at_day"`
	Contrary              []Reference `json:"source_denial_at_day"`
	HasUnassessedEvidence bool        `json:"has_unassessed_evidence"`
	Continuity            *Continuity `json:"continuity,omitempty"`
}

type Continuity struct {
	Before     string      `json:"before"`
	After      string      `json:"after"`
	GapDays    int64       `json:"gap_days"`
	References []Reference `json:"bracketing_observations"`
}

type EvidenceAssessment struct {
	Policy                   string                `json:"policy"`
	Appearance               Appearance            `json:"appearance"`
	CandidateState           string                `json:"name_employer_candidate_state"`
	CandidatePersonIDs       []string              `json:"candidate_person_ids"`
	Decisions                []ObservationDecision `json:"decisions"`
	Timelines                []RoleTimeline        `json:"timelines"`
	InferContinuity          bool                  `json:"infer_continuity"`
	Limitations              []string              `json:"limitations"`
	IdentityResolved         bool                  `json:"identity_resolved"`
	GraphPublicationApproved bool                  `json:"graph_publication_approved"`
	FinancialAttribution     bool                  `json:"financial_attribution"`
}

// AssessEvidence evaluates supplied claims without accepting donor identities.
// It does not authenticate source bytes, extract prose, reconcile entity IDs, or
// discover omitted evidence. Origins and role meanings require a trusted adapter.
// Continuity is opt-in and remains a hypothesis about source-qualified endpoints.
func AssessEvidence(a Appearance, observations []RoleObservation, inferContinuity bool) (EvidenceAssessment, error) {
	if !validReference(a.Source) || a.Receipt.Ordinal < 1 || len(observations) > maxClaims {
		return EvidenceAssessment{}, fmt.Errorf("affiliation evidence appearance or budget")
	}
	r := EvidenceAssessment{Policy: EvidencePolicy, Appearance: a, InferContinuity: inferContinuity,
		CandidateState: "no_name_employer_candidate", CandidatePersonIDs: []string{},
		Decisions: []ObservationDecision{}, Timelines: []RoleTimeline{}, Limitations: []string{
			"caller_authenticates_source_semantics_origins_and_role_dates",
			"supplied_evidence_not_exhaustive_discovery",
			"name_employer_correspondence_not_identity_or_legal_entity_resolution",
			"identity_corroboration_rule_not_implemented",
			"source_endpoint_equality_not_cross_source_identity_join",
			"day_precision_only_coarse_dates_require_explicit_issue",
			"continuity_is_hypothesis_not_uninterrupted_service_proof",
			"no_financial_or_terminal_policy",
		}}
	seen, candidates := map[Reference]bool{}, map[string]bool{}
	type groupKey struct {
		person, organization string
		role                 Role
		roleID               string
	}
	groups := map[groupKey][]RoleObservation{}
	unknownRoles := map[[2]string]bool{}
	for _, o := range observations {
		c := o.Claim
		if !validReference(c.Source) || seen[c.Source] || !qualifiedID(c.PersonID) || !qualifiedID(c.OrganizationID) || !validPeriod(c) || roleState(c.Role) == "unsupported_role" || (o.RoleID != "" && !qualifiedID(o.RoleID)) || (o.Polarity != "asserted" && o.Polarity != "denied") || (o.Origin != nil && !validReference(*o.Origin)) {
			return EvidenceAssessment{}, fmt.Errorf("affiliation observation reference, role, polarity or dates")
		}
		seen[c.Source] = true
		d := ObservationDecision{Observation: o, NameMatch: nameMatch(a.Receipt.Name, c.PersonName), RoleState: roleState(c.Role), TimeState: roleTime(a.Receipt.ReceiptDate, c)}
		if a.Receipt.Employer != nil {
			if match, ok := org.MatchName(*a.Receipt.Employer, c.OrganizationName); ok {
				d.EmployerMatch = &match
			}
		}
		// Negative assertions and expired/issue-bearing roles still preserve a
		// named rival. Correspondence does not mean employment was affirmed.
		if d.NameMatch && d.EmployerMatch != nil {
			candidates[c.PersonID] = true
		}
		r.Decisions = append(r.Decisions, d)
		key := groupKey{c.PersonID, c.OrganizationID, c.Role, o.RoleID}
		groups[key] = append(groups[key], o)
		if c.Role == UnknownRole {
			unknownRoles[[2]string{c.PersonID, c.OrganizationID}] = true
		}
	}
	sort.Slice(r.Decisions, func(i, j int) bool {
		return referenceLess(r.Decisions[i].Observation.Claim.Source, r.Decisions[j].Observation.Claim.Source)
	})
	for id := range candidates {
		r.CandidatePersonIDs = append(r.CandidatePersonIDs, id)
	}
	sort.Strings(r.CandidatePersonIDs)
	if len(candidates) == 1 {
		r.CandidateState = "single_name_employer_candidate_identity_unresolved"
	} else if len(candidates) > 1 {
		r.CandidateState = "ambiguous_name_employer_candidates"
	}
	for key, members := range groups {
		timeline := assessTimeline(a.Receipt.ReceiptDate, members, inferContinuity)
		timeline.PersonID, timeline.OrganizationID, timeline.Role = key.person, key.organization, key.role
		timeline.RoleID = key.roleID
		// An unscoped assertion for this role category, or an unmapped role,
		// might constrain the specific role. Keep it in its own group but do not
		// skip it to manufacture a bridge in another group.
		if unknownRoles[[2]string{key.person, key.organization}] || (key.roleID != "" && len(groups[groupKey{key.person, key.organization, key.role, ""}]) > 0) {
			timeline.HasUnassessedEvidence = true
			if timeline.Continuity != nil {
				timeline.State, timeline.Continuity = "continuity_unscoped_role_evidence", nil
			}
		}
		r.Timelines = append(r.Timelines, timeline)
	}
	sort.Slice(r.Timelines, func(i, j int) bool {
		a, b := r.Timelines[i], r.Timelines[j]
		if a.PersonID != b.PersonID {
			return a.PersonID < b.PersonID
		}
		if a.OrganizationID != b.OrganizationID {
			return a.OrganizationID < b.OrganizationID
		}
		if a.Role != b.Role {
			return a.Role < b.Role
		}
		return a.RoleID < b.RoleID
	})
	return r, nil
}

func referenceLess(a, b Reference) bool {
	if a.SHA256 != b.SHA256 {
		return a.SHA256 < b.SHA256
	}
	return a.Locator < b.Locator
}
