package personaffiliation

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/identityassertions"
)

// Invented people and organizations. These test rule behavior, not real matches.
func fixture() (Appearance, Claim) {
	str := func(s string) *string { return &s }
	a := Appearance{Source: Reference{SHA256: fmt.Sprintf("%064x", 1), Locator: "receipt/1"}, Receipt: identityassertions.Receipt{
		Ordinal: 1, Cycle: 2024, Name: str("Alex Example"), Employer: str("Example Corporation"), Occupation: str("CEO"), ReceiptDate: str("2024-06-01"),
	}}
	c := Claim{Source: Reference{SHA256: fmt.Sprintf("%064x", 2), Locator: "roles/1"}, PersonID: "fixture:person-a", OrganizationID: "fixture:org-a",
		PersonName: "Alex Example", OrganizationName: "Example Corporation", Role: Executive, ValidFrom: "2024-01-01", ValidThrough: "2024-12-31"}
	return a, c
}

func assess(t *testing.T, a Appearance, claims ...Claim) Result {
	t.Helper()
	r, err := Assess(a, claims)
	if err != nil {
		t.Fatal(err)
	}
	if r.Policy != Policy || r.IdentityResolved || r.GraphPublicationApproved || r.TerminalEligible || r.FinancialAttribution {
		t.Fatal("screening became identity, graph or money approval")
	}
	if !reflect.DeepEqual(r.Appearance, a) {
		t.Fatal("source appearance changed")
	}
	if len(r.Decisions) != len(claims) {
		t.Fatal("source role occurrences lost")
	}
	return r
}

func TestScreeningRequiresMoreThanPersonNameOrOccupation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*Appearance, *Claim)
		identity string
		match    bool
	}{
		{"name_employer_and_dated_role", nil, "single_name_employer_candidate", true},
		{"format_only", func(a *Appearance, _ *Claim) { s := " ALEX, EXAMPLE. "; a.Receipt.Name = &s }, "single_name_employer_candidate", true},
		{"different_person", func(_ *Appearance, c *Claim) { c.PersonName = "Jordan Example" }, "no_name_employer_candidate", false},
		{"typo_is_not_repaired", func(_ *Appearance, c *Claim) { c.PersonName = "Alex Exampel" }, "no_name_employer_candidate", false},
		{"initial_is_not_full_name", func(_ *Appearance, c *Claim) { c.PersonName = "A Example" }, "no_name_employer_candidate", false},
		{"unrelated_employer", func(_ *Appearance, c *Claim) { c.OrganizationName = "Other Corporation" }, "no_name_employer_candidate", false},
		{"bank_not_parent", func(_ *Appearance, c *Claim) { c.OrganizationName = "Example Bank" }, "no_name_employer_candidate", false},
		{"no_suffix_invention", func(_ *Appearance, c *Claim) { c.OrganizationName = "Example Corp" }, "no_name_employer_candidate", false},
		{"missing_name", func(a *Appearance, _ *Claim) { a.Receipt.Name = nil }, "no_name_employer_candidate", false},
		{"missing_employer", func(a *Appearance, _ *Claim) { a.Receipt.Employer = nil }, "no_name_employer_candidate", false},
		{"missing_occupation_does_not_erase_role_evidence", func(a *Appearance, _ *Claim) { a.Receipt.Occupation = nil }, "single_name_employer_candidate", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, c := fixture()
			if tc.change != nil {
				tc.change(&a, &c)
			}
			r := assess(t, a, c)
			if r.IdentityState != tc.identity || r.Decisions[0].ScreeningMatch != tc.match {
				t.Fatal(r.IdentityState, r.Decisions)
			}
		})
	}
	a, _ := fixture()
	r := assess(t, a)
	if r.IdentityState != "no_name_employer_candidate" {
		t.Fatal("reported CEO occupation created an external identity")
	}
}

func TestRoleEvidenceSeparatesLeadershipFromEmployeesAndUnknowns(t *testing.T) {
	for _, tc := range []struct {
		role  Role
		want  string
		match bool
	}{
		{Executive, "leadership_or_control", true}, {BoardDirector, "leadership_or_control", true}, {ControllingOwner, "leadership_or_control", true},
		{Employee, "reported_employment_only", false}, {Founder, "founder_not_current_authority", false},
		{Owner, "ownership_without_control_evidence", false}, {UnknownRole, "role_unknown", false}, {Role("vice president"), "unsupported_role", false},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			a, c := fixture()
			c.Role = tc.role
			r := assess(t, a, c)
			if d := r.Decisions[0]; d.RoleState != tc.want || d.ScreeningMatch != tc.match {
				t.Fatal(d)
			}
		})
	}
}

func TestRoleDatesDoNotBecomeCycleWideOrLifetimeEmployment(t *testing.T) {
	for _, tc := range []struct {
		name, date, start, end, asof, want string
		match                              bool
	}{
		{"start_inclusive", "2024-01-01", "2024-01-01", "2024-12-31", "", "within_reported_period", true},
		{"end_inclusive", "2024-12-31", "2024-01-01", "2024-12-31", "", "within_reported_period", true},
		{"before_start", "2023-12-31", "2024-01-01", "2024-12-31", "", "outside_reported_period", false},
		{"after_end", "2025-01-01", "2024-01-01", "2024-12-31", "", "outside_reported_period", false},
		{"undated_role", "2024-06-01", "", "", "", "unknown_time", false},
		{"unknown_receipt", "", "2024-01-01", "2024-12-31", "", "unknown_time", false},
		{"invalid_receipt", "2024-02-30", "2024-01-01", "2024-12-31", "", "unusable_reported_date", false},
		{"year_only_receipt", "2024", "2024-01-01", "2024-12-31", "", "unusable_reported_date", false},
		{"fec_timestamp", "2024-06-01 00:00:00", "2024-01-01", "2024-12-31", "", "within_reported_period", true},
		{"fec_fractional_timestamp", "2024-06-01 13:15:16.123000", "2024-01-01", "2024-12-31", "", "within_reported_period", true},
		{"no_timezone_guess", "2024-06-01T00:00:00Z", "2024-01-01", "2024-12-31", "", "unusable_reported_date", false},
		{"bad_time", "2024-06-01 25:00:00", "2024-01-01", "2024-12-31", "", "unusable_reported_date", false},
		{"no_timestamp_prefix_truncation", "2024-06-01 00:00:00junk", "2024-01-01", "2024-12-31", "", "unusable_reported_date", false},
		{"asof_exact_day", "2024-06-01", "", "", "2024-06-01", "on_reported_as_of_date", true},
		{"asof_not_backfill", "2023-06-01", "", "", "2024-06-01", "unknown_time", false},
		{"asof_not_extend", "2025-06-01", "", "", "2024-06-01", "unknown_time", false},
		{"unknown_end", "2024-06-01", "2024-01-01", "", "", "unknown_time", false},
		{"known_future_start", "2023-06-01", "2024-01-01", "", "", "outside_reported_period", false},
		{"unknown_start", "2024-06-01", "", "2024-12-31", "", "unknown_time", false},
		{"known_past_end", "2025-06-01", "", "2024-12-31", "", "outside_reported_period", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, c := fixture()
			a.Receipt.ReceiptDate = &tc.date
			c.ValidFrom, c.ValidThrough, c.AsOf = tc.start, tc.end, tc.asof
			r := assess(t, a, c)
			if d := r.Decisions[0]; d.TimeState != tc.want || d.ScreeningMatch != tc.match {
				t.Fatal(d)
			}
			a.Receipt.Cycle = 2028
			other := assess(t, a, c)
			if !reflect.DeepEqual(r.Decisions, other.Decisions) {
				t.Fatal("source cycle changed role validity")
			}
		})
	}
}

func TestNamesakesAndRepeatedSourceRecordsCannotManufactureIdentity(t *testing.T) {
	a, c := fixture()
	other := c
	other.Source.Locator = "roles/2"
	other.PersonID = "fixture:person-b"
	r := assess(t, a, c, other)
	if r.IdentityState != "ambiguous_name_employer_candidates" {
		t.Fatal("namesakes merged")
	}
	for _, d := range r.Decisions {
		if d.ScreeningMatch {
			t.Fatal("ambiguous candidate selected")
		}
	}
	// A historical role is not proof that a namesake cannot be the donor.
	other.ValidFrom, other.ValidThrough = "2020-01-01", "2020-12-31"
	if r := assess(t, a, c, other); r.IdentityState != "ambiguous_name_employer_candidates" {
		t.Fatal("role dates manufactured unique person")
	}
	other.PersonID = c.PersonID
	r = assess(t, a, c, other)
	if r.IdentityState != "single_name_employer_candidate" || len(r.CandidatePersonIDs) != 1 || len(r.Decisions) != 2 {
		t.Fatal("repeated source records became people or were discarded")
	}
	other.PersonID = "different-namespace:person-a"
	if r := assess(t, a, c, other); r.IdentityState != "ambiguous_name_employer_candidates" {
		t.Fatal("unrelated identifier namespaces merged")
	}
	// Same name elsewhere is preserved but does not outweigh employer context.
	other.PersonID, other.OrganizationName = "fixture:person-b", "Other Corporation"
	r = assess(t, a, c, other)
	if !r.Decisions[0].ScreeningMatch || r.Decisions[1].ScreeningMatch {
		t.Fatal("unrelated namesake leaked into affiliations")
	}
}

func TestJobChangesMultipleAffiliationsAndDeterministicReplay(t *testing.T) {
	a, former := fixture()
	former.ValidThrough = "2024-05-31"
	current := former
	current.Source.Locator = "roles/2"
	current.OrganizationID, current.OrganizationName = "fixture:org-b", "Other Corporation"
	current.ValidFrom, current.ValidThrough = "2024-06-01", "2025-12-31"
	board := current
	board.Source.Locator = "roles/3"
	board.OrganizationID, board.OrganizationName, board.Role = "fixture:org-c", "Third Corporation", BoardDirector
	// Employer B identifies a candidate, while another dated role on the same
	// external person ID remains a separate affiliation proposal, not a new donor.
	a.Receipt.Employer = &current.OrganizationName
	claims := []Claim{former, current, board}
	r := assess(t, a, claims...)
	if r.Decisions[0].ScreeningMatch || !r.Decisions[1].ScreeningMatch || !r.Decisions[2].ScreeningMatch {
		t.Fatal("job change/parallel affiliations", r.Decisions)
	}
	if r.Decisions[2].EmployerMatch || !r.Decisions[2].SameCandidatePerson {
		t.Fatal("secondary affiliation disguised as reported employer")
	}
	earlier := a
	day := "2024-05-31"
	earlier.Receipt.ReceiptDate = &day
	earlier.Receipt.Employer = &former.OrganizationName
	prior := assess(t, earlier, claims...)
	if !prior.Decisions[0].ScreeningMatch || prior.Decisions[1].ScreeningMatch || prior.Decisions[2].ScreeningMatch {
		t.Fatal("later employer backfilled into earlier receipt")
	}
	original, _ := json.Marshal(claims)
	want, _ := json.Marshal(r)
	rng := rand.New(rand.NewSource(5))
	for range 25 {
		shuffled := append([]Claim(nil), claims...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		again, _ := json.Marshal(assess(t, a, shuffled...))
		if string(want) != string(again) {
			t.Fatal("input order affected replay")
		}
	}
	after, _ := json.Marshal(claims)
	if string(original) != string(after) {
		t.Fatal("caller evidence mutated")
	}
	// One known person on two appearances remains two source-qualified decisions.
	b := a
	b.Source.Locator = "receipt/2"
	b.Receipt.Ordinal = 2
	if reflect.DeepEqual(r.Appearance.Source, assess(t, b, claims...).Appearance.Source) {
		t.Fatal("appearance grain lost")
	}
}

func TestRawReportedFieldsRemainEvidenceNotDecisionOverrides(t *testing.T) {
	a, c := fixture()
	for _, occupation := range []string{"CEO", "teacher", "retired", "", " ", "NOT PROVIDED"} {
		copy := a
		copy.Receipt.Occupation = &occupation
		r := assess(t, copy, c)
		if *r.Appearance.Receipt.Occupation != occupation || !reflect.DeepEqual(r.Decisions, assess(t, a, c).Decisions) {
			t.Fatal("raw occupation overwritten or used as identity/role proof")
		}
	}
	for _, employer := range []*string{nil, new(string), func() *string { s := " "; return &s }()} {
		copy := a
		copy.Receipt.Employer = employer
		r := assess(t, copy, c)
		if !reflect.DeepEqual(r.Appearance.Receipt.Employer, employer) || r.Decisions[0].ScreeningMatch {
			t.Fatal("missing employer state collapsed or resolved")
		}
	}
	// Identity/role screening has no financial input. This reflection guard
	// protects the API boundary, not a claim that dollar aggregation exists.
	for _, typ := range []reflect.Type{reflect.TypeFor[Appearance](), reflect.TypeFor[Claim](), reflect.TypeFor[Decision](), reflect.TypeFor[identityassertions.Receipt]()} {
		for _, field := range []string{"Amount", "AmountCents", "Total", "Weight", "PrimaryCompany"} {
			if _, found := typ.FieldByName(field); found {
				t.Fatalf("%s acquired %s", typ.Name(), field)
			}
		}
	}
}

func TestInvalidEvidenceFailsAndUncertainEvidenceStaysExplicit(t *testing.T) {
	for _, mutate := range []func(*Claim){
		func(c *Claim) { c.Source.SHA256 = "bad" }, func(c *Claim) { c.Source.Locator = "" },
		func(c *Claim) { c.PersonID = "" }, func(c *Claim) { c.OrganizationID = "" },
		func(c *Claim) { c.ValidFrom = "2024-02-30" }, func(c *Claim) { c.ValidFrom = "2025-01-01" },
		func(c *Claim) { c.AsOf = "2024-06-01" }, // mixed as-of and interval semantics
	} {
		a, c := fixture()
		mutate(&c)
		if _, err := Assess(a, []Claim{c}); err == nil {
			t.Fatal("invalid evidence accepted")
		}
	}
	a, c := fixture()
	if _, err := Assess(a, []Claim{c, c}); err == nil {
		t.Fatal("same exact occurrence supplied twice")
	}
	c.Issue = "source_identity_or_role_conflict"
	r := assess(t, a, c)
	if r.Decisions[0].ScreeningMatch || r.Decisions[0].Claim.Issue != c.Issue {
		t.Fatal("source issue became usable role")
	}
	secondary := c
	secondary.Source.Locator = "roles/secondary"
	secondary.Issue = ""
	secondary.OrganizationName = "Other Corporation"
	secondary.OrganizationID = "fixture:org-b"
	for _, d := range assess(t, a, c, secondary).Decisions {
		if d.ScreeningMatch {
			t.Fatal("failed employer anchor authorized another affiliation")
		}
	}
	// An unresolved rival must not disappear solely because its role is unusable.
	_, good := fixture()
	c.Source.Locator = "roles/2"
	c.PersonID = "fixture:person-b"
	if r := assess(t, a, good, c); r.IdentityState != "ambiguous_name_employer_candidates" {
		t.Fatal("failed rival erased")
	}
	for _, invalid := range []Appearance{{}, {Source: Reference{SHA256: "bad", Locator: "receipt/1"}}} {
		if _, err := Assess(invalid, []Claim{good}); err == nil {
			t.Fatal("unbound appearance")
		}
	}
}
