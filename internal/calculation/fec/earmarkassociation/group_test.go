package earmarkassociation

import (
	"testing"
)

func TestGroupAssociationIsNotAnAmountOrNameRule(t *testing.T) {
	for _, amount := range []*int64{nil, ptr(int64(-999)), ptr(int64(0)), ptr(int64(250))} {
		memo := Evidence{Memo: true, Entity: ptr("PAC"), Contributor: ptr("C00000003"), CleanContributor: ptr("C00000003"), Amount: ptr(int64(500))}
		g, err := NewGroup(9, memo, Topology{Peers: 2})
		if err != nil {
			t.Fatal(err)
		}
		original := Evidence{ReceiptType: ptr("15E"), Entity: ptr("IND"), Amount: amount}
		for _, id := range []uint64{2, 7} {
			if err := g.Observe(id, original, Topology{Peers: 1}, 9); err != nil {
				t.Fatal(err)
			}
			old, err := Decide(original, Topology{Peers: 1}, &Related{Evidence: memo, Topology: Topology{Peers: 2}})
			if err != nil || old.State != "shared_related_record_unresolved" {
				t.Fatal("old rule changed", old, err)
			}
		}
		d := g.Decide()
		if d.State != SharedAssociation || d.ConduitID == nil || *d.ConduitID != "C00000003" || d.AdditionalAmount != "0" || d.TerminalEligible {
			t.Fatal(d)
		}
	}
}

func TestGroupRejectsIncompleteUnsafeMixedAndNonLeafShapes(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"incomplete", "incomplete_group_peer_coverage"},
		{"unsafe_root", "ambiguous_or_incomplete_reference_evidence"},
		{"unsafe_peer", "ambiguous_or_incomplete_reference_evidence"},
		{"nonleaf", "non_leaf_group_peers_unresolved"},
		{"conflict", "conflicting_group_committee_evidence"},
		{"non_earmark", "unsupported_group_peer_roles"},
		{"nonmemo_root", "unsupported_group_peer_roles"},
		{"unregistered_root", "unsupported_group_peer_roles"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memo := Evidence{Memo: true, Entity: ptr("PAC"), Contributor: ptr("C00000003"), CleanContributor: ptr("C00000003")}
			if tc.name == "nonmemo_root" {
				memo.Memo = false
			}
			if tc.name == "unregistered_root" {
				memo.Contributor = nil
			}
			g, err := NewGroup(9, memo, Topology{Peers: 2, Unsafe: tc.name == "unsafe_root"})
			if err != nil {
				t.Fatal(err)
			}
			a := Evidence{Entity: ptr("IND"), ReceiptType: ptr("15E")}
			if err := g.Observe(1, a, Topology{Peers: 1}, 9); err != nil {
				t.Fatal(err)
			}
			if tc.name != "incomplete" {
				top, sole := Topology{Peers: 1, Unsafe: tc.name == "unsafe_peer"}, uint64(9)
				if tc.name == "nonleaf" {
					top.Peers = 2
					sole = 0
				}
				if tc.name == "conflict" {
					a.ConduitID = ptr("C00000004")
				}
				if tc.name == "non_earmark" {
					a.ReceiptType = nil
				}
				if err := g.Observe(2, a, top, sole); err != nil {
					t.Fatal(err)
				}
			}
			d := g.Decide()
			if d.State != tc.want || d.ConduitID != nil || d.TerminalEligible || d.AdditionalAmount != "0" {
				t.Fatal(d)
			}
		})
	}
}

func TestGroupMembershipAndStreaming(t *testing.T) {
	m := Evidence{Memo: true, Entity: ptr("PAC"), Contributor: ptr("C00000003"), CleanContributor: ptr("C00000003")}
	a := Evidence{Entity: ptr("IND"), ReceiptType: ptr("15E")}
	if _, err := NewGroup(0, m, Topology{Peers: 2}); err == nil {
		t.Fatal("zero root")
	}
	if _, err := NewGroup(9, m, Topology{Peers: 1}); err == nil {
		t.Fatal("not shared")
	}
	for _, tc := range []struct{ id, peers, only uint64 }{{0, 1, 9}, {9, 1, 9}, {2, 1, 8}, {2, 2, 9}, {2, 0, 0}} {
		g, _ := NewGroup(9, m, Topology{Peers: 2})
		if err := g.Observe(tc.id, a, Topology{Peers: tc.peers}, tc.only); err == nil {
			t.Fatal("bad member", tc)
		}
	}
	g, _ := NewGroup(100001, m, Topology{Peers: 100000})
	*m.Contributor = "CHANGED"
	for id := uint64(1); id <= 100000; id++ {
		if err := g.Observe(id, a, Topology{Peers: 1}, 100001); err != nil {
			t.Fatal(err)
		}
	}
	if g.Decide().State != SharedAssociation || len(g.roles) != 1 {
		t.Fatal("group memory or ownership")
	}
	if err := g.Observe(100000, a, Topology{Peers: 1}, 100001); err == nil {
		t.Fatal("duplicate/excess member")
	}
}
