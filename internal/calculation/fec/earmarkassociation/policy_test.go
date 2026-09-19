package earmarkassociation

import "testing"

func ptr[T any](v T) *T { return &v }

func TestPolicyMoneyIsObservationNotQualification(t *testing.T) {
	for _, amount := range []*int64{nil, ptr(int64(0)), ptr(int64(-125)), ptr(int64(375))} {
		original := Evidence{ReceiptType: ptr("15E"), Entity: ptr("IND"), Amount: amount}
		other := &Related{Evidence: Evidence{Memo: true, Entity: ptr("PAC"), Contributor: ptr("C00000003"), CleanContributor: ptr("C00000003"), Amount: ptr(int64(375))}, Topology: Topology{Peers: 1}}
		got, err := Decide(original, Topology{Peers: 1}, other)
		if err != nil || got.State != "reported_earmark_memo_association" || got.AdditionalAmount != "0" || got.TerminalEligible {
			t.Fatal(got, err)
		}
		want := "different_reported_amount"
		if amount == nil {
			want = "unknown_reported_amount"
		} else if *amount == 375 {
			want = "same_reported_amount"
		}
		if got.AmountComparison != want {
			t.Fatal(got)
		}
	}
}

func TestPolicyRequiresCompleteCallerEvidence(t *testing.T) {
	e := Evidence{ReceiptType: ptr("15E"), Entity: ptr("IND")}
	for _, tc := range []struct {
		e        Evidence
		topology Topology
		other    *Related
	}{
		{Evidence{}, Topology{}, nil},
		{Evidence{Memo: true, ReceiptType: ptr("15E")}, Topology{}, nil},
		{e, Topology{Peers: 1}, nil},
		{e, Topology{Peers: 2}, &Related{}},
		{e, Topology{}, &Related{}},
	} {
		if _, err := Decide(tc.e, tc.topology, tc.other); err == nil {
			t.Fatal("incomplete association input accepted")
		}
	}
}

func TestRoleInspectionDoesNotRelaxTopology(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*Evidence, *Evidence)
	}{
		{"compatible", RolesCompatible, func(a, b *Evidence) {}},
		{"original entity", "original_contributor_role_unresolved", func(a, b *Evidence) { a.Entity = ptr("ORG") }},
		{"not memo", "related_role_unresolved", func(a, b *Evidence) { b.Memo = false }},
		{"memo entity", "related_role_unresolved", func(a, b *Evidence) { b.Entity = nil }},
		{"memo type", "related_role_unresolved", func(a, b *Evidence) { b.ReceiptType = ptr("15") }},
		{"missing ID", "related_committee_id_unresolved", func(a, b *Evidence) { b.CleanContributor = nil }},
		{"conflict", "conflicting_committee_evidence", func(a, b *Evidence) { a.ConduitID = ptr("C00000009") }},
		{"empty optional fields", RolesCompatible, func(a, b *Evidence) { a.Contributor = ptr(""); b.ReceiptType = ptr("") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := Evidence{ReceiptType: ptr("15E"), Entity: ptr("IND")}
			b := Evidence{Memo: true, Entity: ptr("PAC"), Contributor: ptr("C00000003"), CleanContributor: ptr("C00000003")}
			tc.change(&a, &b)
			if got := InspectRoles(a, b); got.State != tc.want || (got.CommitteeID != nil) != (tc.want == RolesCompatible) {
				t.Fatal(got)
			}
			for _, peers := range []uint64{1, 2, 1000000} {
				got, err := Decide(a, Topology{Peers: 1}, &Related{Evidence: b, Topology: Topology{Peers: peers}})
				want := tc.want
				if want == RolesCompatible {
					want = "reported_earmark_memo_association"
				}
				if peers != 1 {
					want = "shared_related_record_unresolved"
				}
				if err != nil || got.State != want || got.TerminalEligible || got.AdditionalAmount != "0" {
					t.Fatal(got, err)
				}
			}
		})
	}
	if InspectRoles(Evidence{}, Evidence{}).State != "original_not_non_memo_earmark" {
		t.Fatal("inapplicable original accepted")
	}
}
