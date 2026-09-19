package personaffiliation

import (
	"fmt"
	"sort"
	"strings"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
)

const IdentityEvidencePolicy = "person-identity-evidence.v2"

// PersonNameComponents preserves source-provided fields. It does not split a
// display name or expand initials and nicknames. Issue marks an extraction or
// meaning limit that prevents the components from supporting correspondence.
type PersonNameComponents struct {
	Prefix string `json:"prefix,omitempty"`
	First  string `json:"first,omitempty"`
	Middle string `json:"middle,omitempty"`
	Last   string `json:"last,omitempty"`
	Suffix string `json:"suffix,omitempty"`
	Issue  string `json:"issue,omitempty"`
}

// IdentityObservation binds structured name fields to the same exact source
// occurrence as a role observation. The role, its meaning and its dates remain
// separate from the name and organization correspondence.
type IdentityObservation struct {
	Role RoleObservation      `json:"role_observation"`
	Name PersonNameComponents `json:"source_name_components"`
}

// ReportedRoleMeaning is a reviewed/source-adapter interpretation of the exact
// FEC occupation text. This evaluator deliberately contains no occupation-title
// dictionary. An issue keeps the interpretation visible but unassessed.
type ReportedRoleMeaning struct {
	Source Reference `json:"source"`
	Raw    string    `json:"reported_occupation"`
	Role   Role      `json:"role"`
	RoleID string    `json:"source_role_id,omitempty"`
	Issue  string    `json:"issue,omitempty"`
}

type LocalityContext string

const (
	PersonalLocality LocalityContext = "personal"
	BusinessLocality LocalityContext = "business"
	IssuerLocality   LocalityContext = "issuer"
	CareOfLocality   LocalityContext = "care_of"
	MailingLocality  LocalityContext = "mailing"
	UnknownLocality  LocalityContext = "unknown"
)

// LocalityObservation is optional typed evidence. It is never an identity gate.
// AsOf describes the locality observation, not the document retrieval date.
type LocalityObservation struct {
	Source   Reference       `json:"source"`
	PersonID string          `json:"source_person_id"`
	City     string          `json:"city,omitempty"`
	State    string          `json:"state,omitempty"`
	Context  LocalityContext `json:"context"`
	AsOf     string          `json:"as_of,omitempty"`
	Issue    string          `json:"issue,omitempty"`
}

type ComponentCorrespondence struct {
	Reported string `json:"reported,omitempty"`
	Source   string `json:"source,omitempty"`
	State    string `json:"state"`
}

type PersonNameCorrespondence struct {
	State  string                  `json:"state"`
	Prefix ComponentCorrespondence `json:"prefix"`
	First  ComponentCorrespondence `json:"first"`
	Middle ComponentCorrespondence `json:"middle"`
	Last   ComponentCorrespondence `json:"last"`
	Suffix ComponentCorrespondence `json:"suffix"`
}

type IdentityObservationDecision struct {
	Observation        IdentityObservation      `json:"observation"`
	Name               PersonNameCorrespondence `json:"name_correspondence"`
	Organization       *org.NameMatch           `json:"organization_correspondence,omitempty"`
	CandidateState     string                   `json:"candidate_state"`
	RoleCorrespondence string                   `json:"role_correspondence"`
	RoleState          string                   `json:"source_role_state"`
	TimeState          string                   `json:"role_time_state"`
}

type LocalityDecision struct {
	Observation    LocalityObservation `json:"observation"`
	Correspondence string              `json:"text_correspondence"`
	TimeState      string              `json:"time_state"`
	UseState       string              `json:"identity_use_state"`
}

type IdentityCandidate struct {
	PersonID         string      `json:"source_person_id"`
	State            string      `json:"state"`
	EvidenceState    string      `json:"evidence_state"`
	References       []Reference `json:"references"`
	NameStates       []string    `json:"name_states"`
	RoleStates       []string    `json:"role_correspondence_states"`
	TimeStates       []string    `json:"role_time_states"`
	TimelineStates   []string    `json:"relationship_timeline_states"`
	LocalityStates   []string    `json:"locality_states"`
	BlocksUniqueness bool        `json:"blocks_unique_proposal"`
}

type IdentityEvidenceAssessment struct {
	Policy                   string                        `json:"policy"`
	Appearance               Appearance                    `json:"appearance"`
	State                    string                        `json:"state"`
	ReportedRole             *ReportedRoleMeaning          `json:"reported_role_meaning,omitempty"`
	Decisions                []IdentityObservationDecision `json:"decisions"`
	Candidates               []IdentityCandidate           `json:"candidates"`
	Localities               []LocalityDecision            `json:"localities"`
	RoleTimelines            []RoleTimeline                `json:"role_timelines"`
	Limitations              []string                      `json:"limitations"`
	IdentityResolved         bool                          `json:"identity_resolved"`
	GraphPublicationApproved bool                          `json:"graph_publication_approved"`
	FinancialAttribution     bool                          `json:"financial_attribution"`
}

// AssessIdentityEvidence classifies supplied evidence without selecting a
// person. A supported candidate or an unassessed same-employer rival blocks a
// unique proposal; neither becomes an accepted identity. Role correspondence,
// role timing and locality are descriptive outputs and never identity gates.
func AssessIdentityEvidence(a Appearance, reported *ReportedRoleMeaning, observations []IdentityObservation, localities []LocalityObservation, inferContinuity bool) (IdentityEvidenceAssessment, error) {
	if len(observations) > maxClaims || len(localities) > maxClaims {
		return IdentityEvidenceAssessment{}, fmt.Errorf("identity evidence budget")
	}
	if err := validateReportedRole(a, reported); err != nil {
		return IdentityEvidenceAssessment{}, err
	}
	roles := make([]RoleObservation, len(observations))
	for i, observation := range observations {
		roles[i] = observation.Role
	}
	base, err := AssessEvidence(a, roles, inferContinuity)
	if err != nil {
		return IdentityEvidenceAssessment{}, err
	}
	r := IdentityEvidenceAssessment{
		Policy: IdentityEvidencePolicy, Appearance: a, State: "abstain_no_candidate",
		ReportedRole: reported, Decisions: []IdentityObservationDecision{}, Candidates: []IdentityCandidate{},
		Localities: []LocalityDecision{}, RoleTimelines: base.Timelines, Limitations: []string{
			"supplied_candidate_and_evidence_scope_not_exhaustive",
			"structured_name_and_role_meanings_are_caller_authenticated",
			"name_and_organization_correspondence_not_person_or_legal_entity_identity",
			"missing_first_name_variant_evidence_remains_unassessed",
			"role_meaning_role_time_and_locality_do_not_decide_person_identity",
			"distinct_origins_do_not_establish_source_independence",
			"locality_context_and_time_must_not_be_reinterpreted_as_residence",
			"no_financial_or_terminal_policy",
		},
	}
	type aggregate struct {
		supported           bool
		refs                map[Reference]bool
		origins             map[Reference]bool
		originUnknown       bool
		organizations       map[string]bool
		names, roles, times map[string]bool
		timelines           map[string]bool
		localities          map[string]bool
	}
	people := map[string]*aggregate{}
	baseDecisions := map[Reference]ObservationDecision{}
	for _, decision := range base.Decisions {
		baseDecisions[decision.Observation.Claim.Source] = decision
	}
	for _, observation := range observations {
		name := comparePersonName(a, observation.Name)
		baseDecision := baseDecisions[observation.Role.Claim.Source]
		decision := IdentityObservationDecision{
			Observation: observation, Name: name, CandidateState: "not_a_candidate",
			RoleCorrespondence: compareRole(reported, observation.Role),
			RoleState:          baseDecision.RoleState, TimeState: baseDecision.TimeState,
		}
		if a.Receipt.Employer != nil {
			if match, ok := matchEmployer(*a.Receipt.Employer, observation.Role.Claim.OrganizationName); ok {
				decision.Organization = &match
			}
		}
		plausibleName := name.State == "exact_components" || name.State == "compatible_missing_optional_components"
		unresolvedName := name.State == "unassessed_name_components"
		switch {
		case name.State == "exact_components" && decision.Organization != nil:
			decision.CandidateState = "supported_name_organization_correspondence"
		case name.State == "compatible_missing_optional_components" && decision.Organization != nil:
			decision.CandidateState = "unassessed_missing_optional_name_component"
		case unresolvedName && decision.Organization != nil:
			decision.CandidateState = "unassessed_name_components_same_organization"
		case name.State == "first_name_variant_unassessed" && decision.Organization != nil:
			decision.CandidateState = "unresolved_name_variant_not_candidate"
		case plausibleName:
			decision.CandidateState = "organization_correspondence_unassessed"
		}
		blockingCandidate := decision.CandidateState == "supported_name_organization_correspondence" || decision.CandidateState == "unassessed_missing_optional_name_component" || decision.CandidateState == "unassessed_name_components_same_organization"
		if blockingCandidate {
			id := observation.Role.Claim.PersonID
			p := people[id]
			if p == nil {
				p = &aggregate{refs: map[Reference]bool{}, origins: map[Reference]bool{}, organizations: map[string]bool{}, names: map[string]bool{}, roles: map[string]bool{}, times: map[string]bool{}, timelines: map[string]bool{}, localities: map[string]bool{}}
				people[id] = p
			}
			p.organizations[observation.Role.Claim.OrganizationID] = true
			p.refs[observation.Role.Claim.Source], p.names[name.State], p.roles[decision.RoleCorrespondence], p.times[decision.TimeState] = true, true, true, true
			if decision.CandidateState == "supported_name_organization_correspondence" {
				p.supported = true
			}
			if observation.Role.Origin == nil {
				p.originUnknown = true
			} else {
				p.origins[*observation.Role.Origin] = true
			}
		}
		r.Decisions = append(r.Decisions, decision)
	}
	for _, timeline := range r.RoleTimelines {
		if p := people[timeline.PersonID]; p != nil && p.organizations[timeline.OrganizationID] {
			p.timelines[timeline.State] = true
		}
	}
	sort.Slice(r.Decisions, func(i, j int) bool {
		return referenceLess(r.Decisions[i].Observation.Role.Claim.Source, r.Decisions[j].Observation.Role.Claim.Source)
	})
	seenLocalities := map[Reference]bool{}
	for _, locality := range localities {
		if seenLocalities[locality.Source] {
			return IdentityEvidenceAssessment{}, fmt.Errorf("duplicate locality observation")
		}
		seenLocalities[locality.Source] = true
		decision, err := assessLocality(a, locality)
		if err != nil {
			return IdentityEvidenceAssessment{}, err
		}
		r.Localities = append(r.Localities, decision)
		if p := people[locality.PersonID]; p != nil {
			p.localities[decision.Correspondence+":"+decision.TimeState+":"+decision.UseState] = true
		}
	}
	sort.Slice(r.Localities, func(i, j int) bool {
		return referenceLess(r.Localities[i].Observation.Source, r.Localities[j].Observation.Source)
	})
	supported, blocking := 0, 0
	for id, p := range people {
		candidate := IdentityCandidate{PersonID: id, State: "unassessed_candidate", EvidenceState: provenanceState(p.origins, p.originUnknown, p.supported), References: referenceSet(p.refs), NameStates: stringSet(p.names), RoleStates: stringSet(p.roles), TimeStates: stringSet(p.times), TimelineStates: stringSet(p.timelines), LocalityStates: stringSet(p.localities), BlocksUniqueness: true}
		if p.supported {
			candidate.State = "supported_candidate_identity_unresolved"
			supported++
		}
		r.Candidates = append(r.Candidates, candidate)
		blocking++
	}
	sort.Slice(r.Candidates, func(i, j int) bool { return r.Candidates[i].PersonID < r.Candidates[j].PersonID })
	switch {
	case supported > 1:
		r.State = "abstain_multiple_supported_candidates"
	case supported == 1 && blocking > 1:
		r.State = "supported_candidate_with_unresolved_rivals"
	case supported == 1:
		r.State = "single_supported_candidate_in_supplied_scope_identity_unresolved"
	case blocking > 0:
		r.State = "abstain_unassessed_candidates"
	}
	return r, nil
}

// matchEmployer extends the shared bounded matcher only for the explicit
// CO/COMPANY legal-designator family. It does not remove arbitrary words or
// claim that corresponding text identifies one legal entity.
func matchEmployer(reported, source string) (org.NameMatch, bool) {
	if match, ok := org.MatchName(reported, source); ok {
		return match, true
	}
	q, s := org.Normalize(reported), org.Normalize(source)
	qStem, qSuffix := splitCompanySuffix(q)
	sStem, sSuffix := splitCompanySuffix(s)
	if qStem == "" || qStem != sStem || (qSuffix == "" && sSuffix == "") {
		return org.NameMatch{}, false
	}
	return org.NameMatch{
		Rule:      "company_legal_suffix_variant",
		Query:     org.NameForm{Normalized: q, Comparison: qStem, RemovedLegalSuffix: qSuffix},
		Candidate: org.NameForm{Normalized: s, Comparison: sStem, RemovedLegalSuffix: sSuffix},
	}, true
}

func splitCompanySuffix(value string) (string, string) {
	for _, suffix := range []string{" COMPANY", " CO"} {
		if strings.HasSuffix(value, suffix) {
			return strings.TrimSuffix(value, suffix), strings.TrimSpace(suffix)
		}
	}
	return value, ""
}

func validateReportedRole(a Appearance, reported *ReportedRoleMeaning) error {
	if reported == nil {
		return nil
	}
	if !validReference(reported.Source) || reported.Source != a.Source || roleState(reported.Role) == "unsupported_role" || (reported.RoleID != "" && !qualifiedID(reported.RoleID)) || a.Receipt.Occupation == nil || reported.Raw != *a.Receipt.Occupation {
		return fmt.Errorf("reported role meaning is not bound to the appearance")
	}
	return nil
}

func comparePersonName(a Appearance, source PersonNameComponents) PersonNameCorrespondence {
	r := PersonNameCorrespondence{
		Prefix: compareComponent(pointerText(a.Receipt.Prefix), source.Prefix),
		First:  compareComponent(pointerText(a.Receipt.First), source.First),
		Middle: compareComponent(pointerText(a.Receipt.Middle), source.Middle),
		Last:   compareComponent(pointerText(a.Receipt.Last), source.Last),
		Suffix: compareComponent(pointerText(a.Receipt.Suffix), source.Suffix),
	}
	if source.Issue != "" || r.First.State == "both_missing" || r.First.State == "reported_missing" || r.First.State == "source_missing" || r.Last.State == "both_missing" || r.Last.State == "reported_missing" || r.Last.State == "source_missing" {
		r.State = "unassessed_name_components"
		return r
	}
	if r.Last.State != "exact" {
		r.State = "distinguishing_component_conflict"
		return r
	}
	if r.First.State != "exact" {
		r.State = "first_name_variant_unassessed"
		return r
	}
	if r.Middle.State == "conflict" || r.Suffix.State == "conflict" {
		r.State = "distinguishing_component_conflict"
		return r
	}
	if r.Middle.State == "reported_missing" || r.Middle.State == "source_missing" || r.Suffix.State == "reported_missing" || r.Suffix.State == "source_missing" {
		r.State = "compatible_missing_optional_components"
		return r
	}
	r.State = "exact_components"
	return r
}

func compareComponent(reported, source string) ComponentCorrespondence {
	r := ComponentCorrespondence{Reported: reported, Source: source, State: "conflict"}
	q, s := org.Normalize(reported), org.Normalize(source)
	switch {
	case q == "" && s == "":
		r.State = "both_missing"
	case q == "":
		r.State = "reported_missing"
	case s == "":
		r.State = "source_missing"
	case q == s:
		r.State = "exact"
	}
	return r
}

func compareRole(reported *ReportedRoleMeaning, observation RoleObservation) string {
	if reported == nil {
		return "reported_role_semantics_unassessed"
	}
	if reported.Issue != "" || observation.Claim.Issue != "" || reported.Role == UnknownRole || observation.Claim.Role == UnknownRole {
		return "role_semantics_unassessed"
	}
	if observation.Polarity == "denied" {
		return "source_denies_role"
	}
	if reported.Role != observation.Claim.Role {
		return "broad_role_mismatch"
	}
	if reported.RoleID == "" || observation.RoleID == "" {
		return "broad_role_correspondence_specific_role_unassessed"
	}
	if reported.RoleID != observation.RoleID {
		return "specific_role_mismatch"
	}
	return "specific_role_correspondence"
}

func assessLocality(a Appearance, observation LocalityObservation) (LocalityDecision, error) {
	if !validReference(observation.Source) || !qualifiedID(observation.PersonID) || (strings.TrimSpace(observation.City) == "" && strings.TrimSpace(observation.State) == "") || (observation.AsOf != "" && !validDay(observation.AsOf)) {
		return LocalityDecision{}, fmt.Errorf("invalid locality observation")
	}
	switch observation.Context {
	case PersonalLocality, BusinessLocality, IssuerLocality, CareOfLocality, MailingLocality, UnknownLocality:
	default:
		return LocalityDecision{}, fmt.Errorf("invalid locality context")
	}
	d := LocalityDecision{Observation: observation, Correspondence: "reported_locality_unavailable", TimeState: "source_time_unknown", UseState: "optional_context_not_identity_gate"}
	if observation.Issue != "" {
		d.UseState = "unassessed_source_qualifier"
	}
	city, state := pointerText(a.Receipt.City), pointerText(a.Receipt.State)
	if org.Normalize(city) != "" || org.Normalize(state) != "" {
		cityMatch := org.Normalize(city) != "" && org.Normalize(city) == org.Normalize(observation.City)
		stateMatch := org.Normalize(state) != "" && org.Normalize(state) == org.Normalize(observation.State)
		switch {
		case cityMatch && stateMatch:
			d.Correspondence = "city_state_text_correspondence"
		case stateMatch:
			d.Correspondence = "state_text_correspondence_city_unresolved"
		default:
			d.Correspondence = "locality_text_not_corresponding"
		}
	}
	day, ok := "", false
	if a.Receipt.ReceiptDate != nil {
		day, ok = receiptDay(*a.Receipt.ReceiptDate)
	}
	if !ok {
		if a.Receipt.ReceiptDate == nil || *a.Receipt.ReceiptDate == "" {
			d.TimeState = "receipt_date_unknown"
		} else {
			d.TimeState = "receipt_date_unusable"
		}
	} else if observation.AsOf != "" {
		switch {
		case observation.AsOf == day:
			d.TimeState = "observed_on_receipt_day"
		case observation.AsOf < day:
			d.TimeState = "observed_before_receipt_day"
		default:
			d.TimeState = "observed_after_receipt_day"
		}
	}
	if observation.Context != PersonalLocality {
		d.UseState = "nonpersonal_or_unknown_context_not_residence_evidence"
	}
	if observation.Issue != "" {
		d.UseState = "unassessed_source_qualifier"
	}
	return d, nil
}

func provenanceState(origins map[Reference]bool, unknown, supported bool) string {
	switch {
	case unknown:
		if supported {
			return "dependency_unknown_support"
		}
		return "dependency_unknown_unassessed"
	case len(origins) == 1:
		if supported {
			return "one_origin_support"
		}
		return "one_origin_unassessed"
	case len(origins) > 1:
		return "multiple_origins_independence_unassessed"
	default:
		return "dependency_unknown_support"
	}
}

func pointerText(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func referenceSet(set map[Reference]bool) []Reference {
	out := make([]Reference, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return referenceLess(out[i], out[j]) })
	return out
}

func stringSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
