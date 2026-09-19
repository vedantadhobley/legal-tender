package personaffiliation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func observation(n int, day string) RoleObservation {
	_, c := fixture()
	c.Source = Reference{SHA256: fmt.Sprintf("%064x", n+10), Locator: fmt.Sprintf("claims/%d", n)}
	c.ValidFrom, c.ValidThrough, c.AsOf = "", "", day
	origin := c.Source
	return RoleObservation{Claim: c, RoleID: "fixture:ceo", Polarity: "asserted", Origin: &origin}
}

func evaluateEvidence(t *testing.T, a Appearance, infer bool, observations ...RoleObservation) EvidenceAssessment {
	t.Helper()
	r, err := AssessEvidence(a, observations, infer)
	if err != nil {
		t.Fatal(err)
	}
	if r.Policy != EvidencePolicy || r.IdentityResolved || r.GraphPublicationApproved || r.FinancialAttribution || !reflect.DeepEqual(r.Appearance, a) || len(r.Decisions) != len(observations) {
		t.Fatal("evidence loss or identity/financial boundary")
	}
	members := 0
	for _, timeline := range r.Timelines {
		members += len(timeline.Members)
	}
	if members != len(observations) {
		t.Fatal("timeline occurrence loss")
	}
	return r
}

func TestEvidenceContinuityAndConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*[]RoleObservation)
		infer  bool
		want   string
	}{
		{"opt_in_only", nil, false, "no_source_coverage_at_day"},
		{"bracket", nil, true, "inferred_continuity"},
		{"one_sided", func(o *[]RoleObservation) { *o = (*o)[:1] }, true, "no_source_coverage_at_day"},
		{"origin_unknown", func(o *[]RoleObservation) { (*o)[0].Origin = nil }, true, "continuity_origin_unknown"},
		{"copied_observation", func(o *[]RoleObservation) { (*o)[1].Origin = (*o)[0].Origin }, true, "continuity_reuses_observation"},
		{"broad_role_only", func(o *[]RoleObservation) { (*o)[0].RoleID, (*o)[1].RoleID = "", "" }, true, "continuity_role_unspecified"},
		{"unassessed_constraint", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Claim.Issue = "coarse_end_date_not_day_bound"
			*o = append(*o, x)
		}, true, "continuity_unassessed_evidence"},
		{"ended_inside", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Claim.ValidThrough = "2024-05-15"
			*o = append(*o, x)
		}, true, "continuity_blocked_by_source_constraint"},
		{"reappointed_inside", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Claim.ValidFrom = "2024-06-15"
			*o = append(*o, x)
		}, true, "continuity_blocked_by_source_constraint"},
		{"ended_at_left", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Claim.ValidThrough = "2024-05-01"
			*o = append(*o, x)
		}, true, "continuity_blocked_by_source_constraint"},
		{"started_at_left", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Claim.ValidFrom = "2024-05-01"
			*o = append(*o, x)
		}, true, "inferred_continuity"},
		{"denial_inside", func(o *[]RoleObservation) {
			x := observation(3, "2024-05-20")
			x.Polarity = "denied"
			*o = append(*o, x)
		}, true, "continuity_blocked_by_source_constraint"},
		{"denial_on_day", func(o *[]RoleObservation) {
			x := observation(3, "2024-06-01")
			x.Polarity = "denied"
			*o = append(*o, x)
		}, true, "source_denies_at_day"},
		{"undated_denial", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Polarity = "denied"
			*o = append(*o, x)
		}, true, "continuity_unassessed_evidence"},
		{"open_denial", func(o *[]RoleObservation) {
			x := observation(3, "")
			x.Polarity, x.Claim.ValidFrom = "denied", "2024-05-15"
			*o = append(*o, x)
		}, true, "continuity_blocked_by_source_constraint"},
		{"outside_denial", func(o *[]RoleObservation) {
			x := observation(3, "2024-08-01")
			x.Polarity = "denied"
			*o = append(*o, x)
		}, true, "inferred_continuity"},
		{"source_supported", func(o *[]RoleObservation) { *o = append(*o, observation(3, "2024-06-01")) }, true, "source_supported_at_day"},
		{"conflicting", func(o *[]RoleObservation) {
			x := observation(4, "2024-06-01")
			x.Polarity = "denied"
			*o = append(*o, observation(3, "2024-06-01"), x)
		}, true, "conflicting_source_assertions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := fixture()
			observations := []RoleObservation{observation(1, "2024-05-01"), observation(2, "2024-07-01")}
			if tc.change != nil {
				tc.change(&observations)
			}
			r := evaluateEvidence(t, a, tc.infer, observations...)
			if len(r.Timelines) != 1 || r.Timelines[0].State != tc.want {
				t.Fatal(r.Timelines)
			}
			c := r.Timelines[0].Continuity
			if tc.want == "inferred_continuity" {
				if c == nil || c.Before != "2024-05-01" || c.After != "2024-07-01" || c.GapDays != 61 || len(c.References) != 2 {
					t.Fatal("lost interpolation evidence", c)
				}
			} else if c != nil {
				t.Fatal("unearned continuity")
			}
		})
	}
}

func TestEvidenceNearestObservationsAndReplay(t *testing.T) {
	a, _ := fixture()
	observations := []RoleObservation{observation(1, "2024-05-01"), observation(2, "2024-07-01"), observation(3, "2024-01-01"), observation(4, "2024-12-31")}
	copyOfFirst := observation(5, "2024-05-01")
	copyOfFirst.Origin = observations[0].Origin
	observations = append(observations, copyOfFirst)
	before, _ := json.Marshal(observations)
	r := evaluateEvidence(t, a, true, observations...)
	after, _ := json.Marshal(observations)
	if string(before) != string(after) || r.Timelines[0].Continuity.GapDays != 61 || len(r.Timelines[0].Continuity.References) != 3 {
		t.Fatal("input mutation, non-nearest observations or lost copies")
	}
	slices.Reverse(observations)
	if again := evaluateEvidence(t, a, true, observations...); !reflect.DeepEqual(r, again) {
		t.Fatal("ordering changed decisions")
	}
	// A nearer observation with unknown origin cannot be skipped for an older
	// clean pair. No selection by whichever evidence permits interpolation.
	x := observation(6, "2024-05-20")
	x.Origin = nil
	if got := evaluateEvidence(t, a, true, append(observations, x)...); got.Timelines[0].State != "continuity_origin_unknown" {
		t.Fatal("unknown nearest observation hidden")
	}
}

func TestEvidenceIdentityRoleAndOrganizationRemainSeparate(t *testing.T) {
	a, _ := fixture()
	one, two := observation(1, "2024-05-01"), observation(2, "2024-07-01")
	one.Claim.OrganizationName = "Example Corp"
	rival := observation(3, "1999-01-01")
	rival.Claim.PersonID = "fixture:namesake"
	r := evaluateEvidence(t, a, true, one, two, rival)
	if len(r.CandidatePersonIDs) != 2 || r.CandidateState != "ambiguous_name_employer_candidates" || r.Decisions[0].EmployerMatch.Rule != "legal_suffix_variant" {
		t.Fatal("rival erased by dates or name rule changed")
	}
	for _, change := range []func(*RoleObservation){
		func(o *RoleObservation) { o.Claim.PersonID = "other:person-a" },
		func(o *RoleObservation) { o.Claim.OrganizationID = "fixture:subsidiary" },
		func(o *RoleObservation) { o.Claim.Role = BoardDirector },
		func(o *RoleObservation) { o.RoleID = "fixture:cfo" },
	} {
		other := two
		change(&other)
		got := evaluateEvidence(t, a, true, one, other)
		if len(got.Timelines) != 2 || got.Timelines[0].Continuity != nil || got.Timelines[1].Continuity != nil {
			t.Fatal("cross-person, company or role interpolation")
		}
	}
	for _, role := range []Role{Employee, Founder, Owner, Executive, BoardDirector, ControllingOwner} {
		x := observation(4, "2024-06-01")
		x.Claim.Role = role
		got := evaluateEvidence(t, a, true, x)
		if got.Decisions[0].RoleState != roleState(role) || got.Timelines[0].State != "source_supported_at_day" {
			t.Fatal("role meaning collapsed into leadership")
		}
	}
	// A source-timeline statement is useful even when the appearance name does
	// not correspond. It remains a source claim, not that donor's affiliation.
	a.Receipt.Name = strptr("Someone Else")
	r = evaluateEvidence(t, a, true, one, two)
	if len(r.CandidatePersonIDs) != 0 || r.Timelines[0].State != "inferred_continuity" {
		t.Fatal("source relationship incorrectly coupled to donor identity")
	}
}

func TestEvidencePeriodsDatesAndValidation(t *testing.T) {
	a, c := fixture()
	x := RoleObservation{Claim: c, Polarity: "asserted"}
	if got := evaluateEvidence(t, a, true, x); got.Timelines[0].State != "source_supported_at_day" {
		t.Fatal("explicit period lost")
	}
	for _, day := range []string{"2024-01-01", "2024-12-31", "2024-06-01 12:34:56.123"} {
		a.Receipt.ReceiptDate = &day
		if got := evaluateEvidence(t, a, false, x); got.Timelines[0].State != "source_supported_at_day" {
			t.Fatal("inclusive endpoint/timestamp behavior changed")
		}
	}
	for _, tc := range []struct{ raw, want string }{{"", "receipt_date_unknown"}, {"2024-02-30", "receipt_date_unusable"}, {"2025-01-01", "no_source_coverage_at_day"}} {
		a.Receipt.ReceiptDate = &tc.raw
		if got := evaluateEvidence(t, a, true, x); got.Timelines[0].State != tc.want {
			t.Fatal("date exclusion became non-service assertion")
		}
	}
	a, _ = fixture()
	for _, change := range []func(*RoleObservation){
		func(o *RoleObservation) { o.Polarity = "missing" },
		func(o *RoleObservation) { o.Claim.Source.SHA256 = "bad" },
		func(o *RoleObservation) { o.Claim.PersonID = "bare-name" },
		func(o *RoleObservation) { o.Claim.Role = "invented_role" },
		func(o *RoleObservation) { o.RoleID = "unqualified" },
		func(o *RoleObservation) { o.Claim.AsOf = "2024-06-01" },
		func(o *RoleObservation) { o.Claim.ValidFrom = "2025-01-01" },
		func(o *RoleObservation) { o.Claim.ValidFrom = "2024" },
		func(o *RoleObservation) { o.Origin = &Reference{} },
	} {
		bad := x
		change(&bad)
		if _, err := AssessEvidence(a, []RoleObservation{bad}, true); err == nil {
			t.Fatal("invalid evidence accepted")
		}
	}
	if _, err := AssessEvidence(a, []RoleObservation{x, x}, true); err == nil {
		t.Fatal("same occurrence repeated")
	}
	if _, err := AssessEvidence(a, make([]RoleObservation, maxClaims+1), true); err == nil {
		t.Fatal("unbounded input")
	}
	r := evaluateEvidence(t, a, true, observation(1, "0001-01-01"), observation(2, "9999-12-31"))
	if r.Timelines[0].Continuity.GapDays != 3652058 {
		t.Fatal("gap duration overflow or silent arbitrary age threshold")
	}
}

func TestEvidenceUnassessedInputsAndDisjointTerms(t *testing.T) {
	a, _ := fixture()
	for _, unknown := range []bool{false, true} {
		x := observation(5, "2024-06-01")
		x.RoleID, x.Polarity = "", "denied"
		if unknown {
			x.Claim.Role = UnknownRole
		}
		got := evaluateEvidence(t, a, true, observation(6, "2024-05-01"), observation(7, "2024-07-01"), x)
		for _, timeline := range got.Timelines {
			if timeline.RoleID == "fixture:ceo" && (timeline.State != "continuity_unscoped_role_evidence" || !timeline.HasUnassessedEvidence || timeline.Continuity != nil) {
				t.Fatal("unscoped role constraint hidden by grouping")
			}
		}
	}
	first, second := observation(1, ""), observation(2, "")
	first.Claim.ValidFrom, first.Claim.ValidThrough = "2023-01-01", "2024-05-01"
	second.Claim.ValidFrom, second.Claim.ValidThrough = "2024-07-01", "2025-01-01"
	r := evaluateEvidence(t, a, true, first, second)
	if r.Timelines[0].State != "no_source_coverage_at_day" || len(r.Timelines[0].Contrary) != 0 {
		t.Fatal("disjoint terms manufactured a denial or continuity")
	}
	positive, issue := observation(3, "2024-06-01"), observation(4, "2024-06-01")
	issue.Polarity, issue.Claim.Issue = "denied", "unassessed_source_qualifier"
	r = evaluateEvidence(t, a, true, positive, issue)
	if r.Timelines[0].State != "source_supported_at_day" || !r.Timelines[0].HasUnassessedEvidence || len(r.Timelines[0].Members) != 2 {
		t.Fatal("unassessed opposing evidence was hidden or silently accepted")
	}
	positive.Claim.Role = UnknownRole
	r = evaluateEvidence(t, a, true, positive)
	if r.Timelines[0].State != "no_source_coverage_at_day" || !r.Timelines[0].HasUnassessedEvidence {
		t.Fatal("unknown role acquired supported semantics")
	}
	a.Receipt.Name, a.Receipt.Employer, a.Receipt.ReceiptDate = nil, nil, nil
	r = evaluateEvidence(t, a, true, first)
	if len(r.CandidatePersonIDs) != 0 || r.Timelines[0].State != "receipt_date_unknown" {
		t.Fatal("missing reported fields filled by a candidate or cycle")
	}
	a.Source = Reference{}
	if _, err := AssessEvidence(a, nil, false); err == nil {
		t.Fatal("unbound appearance accepted")
	}
}
