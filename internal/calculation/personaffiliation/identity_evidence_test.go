package personaffiliation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func identityFixture() (Appearance, ReportedRoleMeaning, IdentityObservation) {
	a, claim := fixture()
	a.Receipt.First, a.Receipt.Last = strptr("Alex"), strptr("Example")
	a.Receipt.Middle, a.Receipt.Prefix, a.Receipt.Suffix = strptr(""), strptr(""), strptr("")
	a.Receipt.City, a.Receipt.State = strptr("Boston"), strptr("MA")
	origin := Reference{SHA256: fmt.Sprintf("%064x", 90), Locator: "origin/company-biography"}
	reported := ReportedRoleMeaning{Source: a.Source, Raw: "CEO", Role: Executive, RoleID: "fixture:chief-executive"}
	observation := IdentityObservation{
		Role: RoleObservation{Claim: claim, RoleID: reported.RoleID, Polarity: "asserted", Origin: &origin},
		Name: PersonNameComponents{First: "Alex", Last: "Example"},
	}
	return a, reported, observation
}

func assessIdentity(t *testing.T, a Appearance, reported *ReportedRoleMeaning, observations []IdentityObservation, localities []LocalityObservation) IdentityEvidenceAssessment {
	t.Helper()
	r, err := AssessIdentityEvidence(a, reported, observations, localities, false)
	if err != nil {
		t.Fatal(err)
	}
	if r.Policy != IdentityEvidencePolicy || r.IdentityResolved || r.GraphPublicationApproved || r.FinancialAttribution || !reflect.DeepEqual(r.Appearance, a) || len(r.Decisions) != len(observations) {
		t.Fatal("evidence loss or acceptance boundary")
	}
	return r
}

func TestIdentityEvidenceKeepsIdentityRoleTimeAndRivalsSeparate(t *testing.T) {
	a, reported, main := identityFixture()
	rival := main
	rival.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 3), Locator: "roles/rival"}
	rival.Role.Claim.PersonID = "fixture:person-b"
	rival.Role.Claim.Role = UnknownRole
	rival.Role.RoleID = "fixture:head-of-growth"
	rival.Name.Middle = "Q"
	unrelated := main
	unrelated.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 4), Locator: "roles/other-company"}
	unrelated.Role.Claim.PersonID, unrelated.Role.Claim.OrganizationID = "fixture:person-c", "fixture:org-c"
	unrelated.Role.Claim.OrganizationName = "Other Company"

	r := assessIdentity(t, a, &reported, []IdentityObservation{unrelated, rival, main}, nil)
	if r.State != "supported_candidate_with_unresolved_rivals" || len(r.Candidates) != 2 {
		t.Fatal(r.State, r.Candidates)
	}
	if r.Candidates[0].PersonID != "fixture:person-a" || r.Candidates[0].State != "supported_candidate_identity_unresolved" || r.Candidates[0].EvidenceState != "one_origin_support" || !r.Candidates[0].BlocksUniqueness {
		t.Fatal(r.Candidates[0])
	}
	if r.Candidates[1].PersonID != "fixture:person-b" || r.Candidates[1].State != "unassessed_candidate" || !slices.Contains(r.Candidates[1].NameStates, "compatible_missing_optional_components") {
		t.Fatal(r.Candidates[1])
	}
	states := map[string]IdentityObservationDecision{}
	for _, decision := range r.Decisions {
		states[decision.Observation.Role.Claim.PersonID] = decision
	}
	if states["fixture:person-a"].RoleCorrespondence != "specific_role_correspondence" || states["fixture:person-a"].TimeState != "within_reported_period" {
		t.Fatal(states["fixture:person-a"])
	}
	if states["fixture:person-b"].RoleCorrespondence != "role_semantics_unassessed" || states["fixture:person-b"].CandidateState != "unassessed_missing_optional_name_component" {
		t.Fatal(states["fixture:person-b"])
	}
	if states["fixture:person-c"].CandidateState != "organization_correspondence_unassessed" || states["fixture:person-c"].Organization != nil {
		t.Fatal(states["fixture:person-c"])
	}
	// Changing role coverage changes only role-time evidence, not candidacy.
	outside := main
	outside.Role.Claim.ValidFrom, outside.Role.Claim.ValidThrough = "2020-01-01", "2020-12-31"
	timed := assessIdentity(t, a, &reported, []IdentityObservation{outside, rival}, nil)
	if timed.State != r.State || timed.Decisions[0].TimeState == states["fixture:person-a"].TimeState {
		t.Fatal("role time changed identity classification", timed.State, timed.Decisions)
	}
}

func TestIdentityEvidenceOnlySameDistinguishingEvidenceBlocks(t *testing.T) {
	a, reported, main := identityFixture()
	a.Receipt.Middle = strptr("A")
	main.Name.Middle = "A"
	suffixRival := main
	suffixRival.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 31), Locator: "roles/suffix-rival"}
	suffixRival.Role.Claim.PersonID = "fixture:person-b"
	suffixRival.Name.Suffix = "Jr"
	middleConflict := main
	middleConflict.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 32), Locator: "roles/middle-conflict"}
	middleConflict.Role.Claim.PersonID = "fixture:person-c"
	middleConflict.Name.Middle = "B"

	r := assessIdentity(t, a, &reported, []IdentityObservation{main, suffixRival, middleConflict}, nil)
	if r.State != "supported_candidate_with_unresolved_rivals" || len(r.Candidates) != 2 {
		t.Fatal(r.State, r.Candidates)
	}
	decisions := map[string]IdentityObservationDecision{}
	for _, decision := range r.Decisions {
		decisions[decision.Observation.Role.Claim.PersonID] = decision
	}
	if decisions["fixture:person-b"].Name.State != "compatible_missing_optional_components" || decisions["fixture:person-b"].CandidateState != "unassessed_missing_optional_name_component" {
		t.Fatal(decisions["fixture:person-b"])
	}
	if decisions["fixture:person-c"].Name.State != "distinguishing_component_conflict" || decisions["fixture:person-c"].CandidateState != "not_a_candidate" {
		t.Fatal("distinguishable rival blocked uniqueness", decisions["fixture:person-c"])
	}
}

func TestIdentityEvidencePreservesRelationshipContradiction(t *testing.T) {
	a, reported, positive := identityFixture()
	denial := positive
	denial.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 33), Locator: "roles/denial"}
	denial.Role.Polarity = "denied"
	r := assessIdentity(t, a, &reported, []IdentityObservation{positive, denial}, nil)
	if len(r.Candidates) != 1 || !slices.Contains(r.Candidates[0].RoleStates, "source_denies_role") || !slices.Contains(r.Candidates[0].TimelineStates, "conflicting_source_assertions") {
		t.Fatal("relationship contradiction collapsed into identity", r.Candidates)
	}
}

func TestIdentityEvidenceVariantsOrganizationRulesAndProvenance(t *testing.T) {
	a, reported, observation := identityFixture()
	observation.Name.First = "Al"
	observation.Role.Claim.PersonName = "Al Example"
	observation.Role.Claim.OrganizationName = "Example Corp"
	r := assessIdentity(t, a, &reported, []IdentityObservation{observation}, nil)
	if r.State != "abstain_no_candidate" || len(r.Candidates) != 0 || r.Decisions[0].Name.State != "first_name_variant_unassessed" || r.Decisions[0].CandidateState != "unresolved_name_variant_not_candidate" || r.Decisions[0].Organization.Rule != "legal_suffix_variant" {
		t.Fatal(r)
	}
	observation.Name.First = "Jordan"
	observation.Name.Middle = "Q"
	if got := assessIdentity(t, a, &reported, []IdentityObservation{observation}, nil); len(got.Candidates) != 0 || got.Decisions[0].Name.State != "first_name_variant_unassessed" {
		t.Fatal("unattested first-name variant acquired support", got)
	}
	// CO/Company is an explicit legal-designator rule local to this contract.
	a.Receipt.Employer = strptr("United Refining Co")
	observation.Name = PersonNameComponents{First: "Alex", Last: "Example"}
	observation.Role.Claim.OrganizationName = "United Refining Company"
	if got := assessIdentity(t, a, &reported, []IdentityObservation{observation}, nil); got.Decisions[0].Organization == nil || got.Decisions[0].Organization.Rule != "company_legal_suffix_variant" {
		t.Fatal("company suffix correspondence lost", got.Decisions)
	}

	a, reported, observation = identityFixture()
	observation.Role.Origin = nil
	if got := assessIdentity(t, a, &reported, []IdentityObservation{observation}, nil); got.Candidates[0].EvidenceState != "dependency_unknown_support" {
		t.Fatal(got.Candidates[0])
	}
	second := observation
	second.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 5), Locator: "roles/second"}
	secondOrigin := Reference{SHA256: fmt.Sprintf("%064x", 91), Locator: "origin/second"}
	observation.Role.Origin = &secondOrigin
	thirdOrigin := Reference{SHA256: fmt.Sprintf("%064x", 92), Locator: "origin/third"}
	second.Role.Origin = &thirdOrigin
	if got := assessIdentity(t, a, &reported, []IdentityObservation{observation, second}, nil); got.Candidates[0].EvidenceState != "multiple_origins_independence_unassessed" {
		t.Fatal("distinct origins became corroboration", got.Candidates[0])
	}
}

func TestIdentityEvidenceLocalityIsTypedOptionalContext(t *testing.T) {
	a, reported, observation := identityFixture()
	personal := LocalityObservation{Source: Reference{SHA256: fmt.Sprintf("%064x", 6), Locator: "locality/personal"}, PersonID: observation.Role.Claim.PersonID, City: "Boston", State: "MA", Context: PersonalLocality, AsOf: "2024-05-01"}
	issuer := personal
	issuer.Source.Locator, issuer.Context, issuer.AsOf = "locality/issuer", IssuerLocality, "2024-06-01"
	r := assessIdentity(t, a, &reported, []IdentityObservation{observation}, []LocalityObservation{issuer, personal})
	if r.State != "single_supported_candidate_in_supplied_scope_identity_unresolved" || len(r.Localities) != 2 || len(r.Candidates[0].LocalityStates) != 2 {
		t.Fatal(r.State, r.Localities, r.Candidates)
	}
	states := map[LocalityContext]LocalityDecision{}
	for _, decision := range r.Localities {
		states[decision.Observation.Context] = decision
	}
	if states[PersonalLocality].Correspondence != "city_state_text_correspondence" || states[PersonalLocality].TimeState != "observed_before_receipt_day" || states[PersonalLocality].UseState != "optional_context_not_identity_gate" {
		t.Fatal(states[PersonalLocality])
	}
	if states[IssuerLocality].UseState != "nonpersonal_or_unknown_context_not_residence_evidence" || states[IssuerLocality].TimeState != "observed_on_receipt_day" {
		t.Fatal(states[IssuerLocality])
	}
}

func TestIdentityEvidenceValidationAndDeterministicReplay(t *testing.T) {
	a, reported, observation := identityFixture()
	other := observation
	other.Role.Claim.Source = Reference{SHA256: fmt.Sprintf("%064x", 7), Locator: "roles/other"}
	other.Role.Claim.PersonID = "fixture:person-b"
	other.Name.Middle = "B"
	input := []IdentityObservation{observation, other}
	before, _ := json.Marshal(input)
	r := assessIdentity(t, a, &reported, input, nil)
	after, _ := json.Marshal(input)
	if string(before) != string(after) {
		t.Fatal("input mutated")
	}
	slices.Reverse(input)
	if again := assessIdentity(t, a, &reported, input, nil); !reflect.DeepEqual(r, again) {
		t.Fatal("input order changed output")
	}
	for _, change := range []func(*Appearance, *ReportedRoleMeaning){
		func(_ *Appearance, role *ReportedRoleMeaning) { role.Source.Locator = "other" },
		func(_ *Appearance, role *ReportedRoleMeaning) { role.Raw = "OTHER" },
		func(_ *Appearance, role *ReportedRoleMeaning) { role.Role = "invented" },
		func(_ *Appearance, role *ReportedRoleMeaning) { role.RoleID = "bare" },
		func(a *Appearance, _ *ReportedRoleMeaning) { a.Receipt.Occupation = nil },
	} {
		badA, badRole, _ := identityFixture()
		change(&badA, &badRole)
		if _, err := AssessIdentityEvidence(badA, &badRole, nil, nil, false); err == nil {
			t.Fatal("unbound reported-role interpretation accepted")
		}
	}
	badLocality := LocalityObservation{Source: Reference{SHA256: fmt.Sprintf("%064x", 8), Locator: "locality"}, PersonID: "fixture:person-a", City: "Boston", Context: "residence_guess"}
	if _, err := AssessIdentityEvidence(a, &reported, []IdentityObservation{observation}, []LocalityObservation{badLocality}, false); err == nil {
		t.Fatal("invented locality context accepted")
	}
	locality := LocalityObservation{Source: Reference{SHA256: fmt.Sprintf("%064x", 9), Locator: "locality"}, PersonID: "fixture:person-a", City: "Boston", Context: PersonalLocality}
	if _, err := AssessIdentityEvidence(a, &reported, []IdentityObservation{observation}, []LocalityObservation{locality, locality}, false); err == nil {
		t.Fatal("duplicate locality occurrence accepted")
	}
}
