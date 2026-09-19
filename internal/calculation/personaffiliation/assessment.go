// Package personaffiliation screens supplied role evidence without resolving
// donor identities, publishing graph edges, or attributing money. A match is a
// proposal inside the supplied candidate set, never a claim of global uniqueness.
package personaffiliation

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/identityassertions"
	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
)

const Policy = "person-affiliation-screening.v1"
const maxClaims = 10000

type Role string

// Source adapters must justify these meanings. Occupation text, a founder
// statement or a shareholding percentage must not silently create these roles.
const (
	Executive        Role = "executive"
	BoardDirector    Role = "board_director"
	ControllingOwner Role = "controlling_owner"
	Employee         Role = "employee"
	Founder          Role = "founder"
	Owner            Role = "owner"
	UnknownRole      Role = "unknown"
)

// A reference names retained evidence; this pure evaluator checks its shape,
// not source availability, authenticity or the adapter's semantic extraction.
type Reference struct {
	SHA256  string `json:"sha256"`
	Locator string `json:"locator"`
}

type Appearance struct {
	Source  Reference                  `json:"source"`
	Receipt identityassertions.Receipt `json:"receipt"`
}

// PersonID and OrganizationID are source-qualified identities supplied by a
// future adapter, not keys derived from normalized names. Different source ID
// namespaces are never reconciled here. Dates assert role validity, not retrieval
// time. An as-of observation must not be supplied as an open-ended interval.
type Claim struct {
	Source           Reference `json:"source"`
	PersonID         string    `json:"person_id"`
	OrganizationID   string    `json:"organization_id"`
	PersonName       string    `json:"person_name"`
	OrganizationName string    `json:"organization_name"`
	Role             Role      `json:"role"`
	ValidFrom        string    `json:"valid_from,omitempty"`
	ValidThrough     string    `json:"valid_through,omitempty"`
	AsOf             string    `json:"as_of,omitempty"`
	Issue            string    `json:"issue,omitempty"`
}

type Decision struct {
	Claim               Claim  `json:"claim"`
	NameMatch           bool   `json:"name_match"`
	EmployerMatch       bool   `json:"reported_employer_match"`
	SameCandidatePerson bool   `json:"same_external_candidate_person"`
	RoleState           string `json:"role_state"`
	TimeState           string `json:"time_state"`
	ScreeningMatch      bool   `json:"screening_match_not_identity_approval"`
}

type Result struct {
	Policy                   string     `json:"policy"`
	Appearance               Appearance `json:"appearance"`
	IdentityState            string     `json:"identity_state"`
	CandidatePersonIDs       []string   `json:"candidate_person_ids"`
	Decisions                []Decision `json:"decisions"`
	Limitations              []string   `json:"limitations"`
	IdentityResolved         bool       `json:"identity_resolved"`
	GraphPublicationApproved bool       `json:"graph_publication_approved"`
	TerminalEligible         bool       `json:"terminal_attribution_eligible"`
	FinancialAttribution     bool       `json:"financial_attribution"`
}

// Assess retains one result per source appearance and one decision per supplied
// role occurrence. No amounts or election-cycle thresholds participate. The
// caller must supply the complete intended candidate set, including rivals and
// source issues; this function cannot detect omitted discovery evidence.
func Assess(a Appearance, claims []Claim) (Result, error) {
	if !validReference(a.Source) || a.Receipt.Ordinal < 1 || len(claims) > maxClaims {
		return Result{}, fmt.Errorf("affiliation appearance reference or claim budget")
	}
	ordered := append([]Claim(nil), claims...)
	seen := map[Reference]bool{}
	for _, c := range ordered {
		if !validReference(c.Source) || seen[c.Source] || !qualifiedID(c.PersonID) || !qualifiedID(c.OrganizationID) || !validPeriod(c) {
			return Result{}, fmt.Errorf("affiliation claim reference, identity scope or period")
		}
		seen[c.Source] = true
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i].Source, ordered[j].Source
		if a.SHA256 != b.SHA256 {
			return a.SHA256 < b.SHA256
		}
		return a.Locator < b.Locator
	})
	r := Result{Policy: Policy, Appearance: a, IdentityState: "no_name_employer_candidate", CandidatePersonIDs: []string{}, Decisions: []Decision{}, Limitations: []string{
		"supplied_candidate_set_not_exhaustive_identity_search",
		"name_and_employer_correspondence_not_person_identity",
		"source_role_semantics_and_entity_ids_supplied_not_verified_here",
		"reported_occupation_retained_not_interpreted_or_cross_checked",
		"role_dates_are_not_receipt_cycle_or_capture_dates",
		"affiliations_are_overlapping_context_not_additive_money",
	}}
	people := map[string]bool{}
	usableAnchors := map[string]bool{}
	for _, c := range ordered {
		d := Decision{Claim: c, NameMatch: nameMatch(a.Receipt.Name, c.PersonName), EmployerMatch: nameMatch(a.Receipt.Employer, c.OrganizationName), RoleState: roleState(c.Role), TimeState: roleTime(a.Receipt.ReceiptDate, c)}
		// Dates and role/status cannot erase a competing personal identity. An
		// expired/failed role does not prove that person could not be the donor.
		if d.NameMatch && d.EmployerMatch {
			people[c.PersonID] = true
			if c.Issue == "" {
				usableAnchors[c.PersonID] = true
			}
		}
		r.Decisions = append(r.Decisions, d)
	}
	for id := range people {
		r.CandidatePersonIDs = append(r.CandidatePersonIDs, id)
	}
	sort.Strings(r.CandidatePersonIDs)
	if len(people) > 1 {
		r.IdentityState = "ambiguous_name_employer_candidates"
	}
	if len(people) == 1 {
		r.IdentityState = "single_name_employer_candidate"
		for i := range r.Decisions {
			d := &r.Decisions[i]
			d.SameCandidatePerson = people[d.Claim.PersonID]
			d.ScreeningMatch = d.SameCandidatePerson && usableAnchors[d.Claim.PersonID] && d.NameMatch && d.Claim.Issue == "" && d.RoleState == "leadership_or_control" && (d.TimeState == "within_reported_period" || d.TimeState == "on_reported_as_of_date")
		}
	}
	return r, nil
}

func nameMatch(raw *string, name string) bool {
	if raw == nil {
		return false
	}
	n := org.Normalize(*raw)
	return n != "" && n == org.Normalize(name)
}

func roleState(role Role) string {
	switch role {
	case Executive, BoardDirector, ControllingOwner:
		return "leadership_or_control"
	case Employee:
		return "reported_employment_only"
	case Founder:
		return "founder_not_current_authority"
	case Owner:
		return "ownership_without_control_evidence"
	case UnknownRole:
		return "role_unknown"
	default:
		return "unsupported_role"
	}
}

func validDay(s string) bool {
	_, err := time.Parse(time.DateOnly, s)
	return len(s) == 10 && err == nil
}

func validPeriod(c Claim) bool {
	for _, date := range []string{c.ValidFrom, c.ValidThrough, c.AsOf} {
		if date != "" && !validDay(date) {
			return false
		}
	}
	return !(c.AsOf != "" && (c.ValidFrom != "" || c.ValidThrough != "")) && !(c.ValidFrom != "" && c.ValidThrough != "" && c.ValidFrom > c.ValidThrough)
}

func roleTime(raw *string, c Claim) string {
	if raw == nil || *raw == "" {
		return "unknown_time"
	}
	date, ok := receiptDay(*raw)
	if !ok {
		return "unusable_reported_date"
	}
	if c.AsOf != "" {
		if c.AsOf == date {
			return "on_reported_as_of_date"
		}
		return "unknown_time"
	}
	if (c.ValidFrom != "" && date < c.ValidFrom) || (c.ValidThrough != "" && date > c.ValidThrough) {
		return "outside_reported_period"
	}
	if c.ValidFrom != "" && c.ValidThrough != "" {
		return "within_reported_period"
	}
	return "unknown_time"
}

// Compare calendar dates without replacing the raw FEC text. The retained
// Schedule A field uses zone-free timestamps; do not invent a timezone or turn
// an unparsed value into a cycle-derived date. Role validity here is day-grained.
func receiptDay(raw string) (string, bool) {
	if validDay(raw) {
		return raw, true
	}
	if len(raw) < 19 || len(raw) > 29 {
		return "", false
	}
	if len(raw) > 19 {
		if raw[19] != '.' || len(raw) == 20 {
			return "", false
		}
		for _, c := range raw[20:] {
			if c < '0' || c > '9' {
				return "", false
			}
		}
	}
	t, err := time.Parse("2006-01-02 15:04:05.999999999", raw)
	if err != nil || t.Format("2006-01-02 15:04:05") != raw[:19] {
		return "", false
	}
	return raw[:10], true
}

func validReference(r Reference) bool {
	_, err := hex.DecodeString(r.SHA256)
	return len(r.SHA256) == 64 && r.SHA256 == strings.ToLower(r.SHA256) && err == nil && strings.TrimSpace(r.Locator) != ""
}

func qualifiedID(s string) bool {
	namespace, id, ok := strings.Cut(s, ":")
	return ok && strings.TrimSpace(namespace) != "" && strings.TrimSpace(id) != ""
}
