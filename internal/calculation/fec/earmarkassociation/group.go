package earmarkassociation

import "fmt"

const GroupPolicy = "fec/complete-earmark-memo-star-association@1.0.0"
const SharedAssociation = "reported_shared_earmark_memo_association"

// Group requires a complete, exact same-report peer stream from its caller.
// It checks the supported role shape, not economic payment or donor identity.
// Peers arrive in increasing source-ordinal order; no group-sized map is needed.
type Group struct {
	root, previous, seen uint64
	related              Evidence
	topology             Topology
	unsafe, nonLeaf      uint64
	roles                map[string]uint64
	committeeID          *string
}
type GroupDecision struct {
	Policy           string            `json:"policy"`
	State            string            `json:"state"`
	ExpectedPeers    uint64            `json:"expected_exact_peers"`
	ObservedPeers    uint64            `json:"observed_exact_peers"`
	UnsafePeers      uint64            `json:"unsafe_peers"`
	NonLeafPeers     uint64            `json:"non_leaf_peers"`
	Roles            map[string]uint64 `json:"role_rows"`
	ConduitID        *string           `json:"reported_conduit_committee_id"`
	AdditionalAmount string            `json:"additional_amount_minor_units"`
	TerminalEligible bool              `json:"terminal_eligible"`
}

func NewGroup(root uint64, related Evidence, topology Topology) (*Group, error) {
	if root == 0 || topology.Peers < 2 {
		return nil, fmt.Errorf("shared root and at least two exact peers required")
	}
	// Retain values, never decoder-owned pointers. No memo text is consulted.
	copyString := func(p *string) *string {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	related.ReceiptType = copyString(related.ReceiptType)
	related.Entity = copyString(related.Entity)
	related.Contributor = copyString(related.Contributor)
	related.CleanContributor = copyString(related.CleanContributor)
	related.ConduitID = copyString(related.ConduitID)
	if related.Amount != nil {
		n := *related.Amount
		related.Amount = &n
	}
	return &Group{root: root, related: related, topology: topology, roles: map[string]uint64{}}, nil
}

// Observe requires actual adjacency, including unsupported and unsafe peers.
// The caller establishes recipient/report/transaction scope and exact direction;
// a sole peer must be this shared root, not a different memo elsewhere.
func (g *Group) Observe(ordinal uint64, v Evidence, topology Topology, onlyPeer uint64) error {
	if ordinal == 0 || ordinal == g.root || ordinal <= g.previous || g.seen >= g.topology.Peers || topology.Peers == 0 || (topology.Peers == 1 && onlyPeer != g.root) || (topology.Peers != 1 && onlyPeer != 0) {
		return fmt.Errorf("invalid, duplicate or excess group peer")
	}
	g.previous = ordinal
	g.seen++
	if topology.Unsafe {
		g.unsafe++
	}
	if topology.Peers != 1 {
		g.nonLeaf++
	}
	r := InspectRoles(v, g.related)
	g.roles[r.State]++
	if r.CommitteeID != nil {
		g.committeeID = r.CommitteeID
	}
	return nil
}

// Decide is additive to the one-to-one rule. Amount equality, sign, dates,
// committee names and transaction suffixes are never eligibility gates.
func (g *Group) Decide() GroupDecision {
	d := GroupDecision{Policy: GroupPolicy, State: "unsupported_group_peer_roles", ExpectedPeers: g.topology.Peers, ObservedPeers: g.seen, UnsafePeers: g.unsafe, NonLeafPeers: g.nonLeaf, Roles: map[string]uint64{}, AdditionalAmount: "0"}
	for role, n := range g.roles {
		d.Roles[role] = n
	}
	switch {
	case g.topology.Unsafe || g.unsafe > 0:
		d.State = "ambiguous_or_incomplete_reference_evidence"
	case g.seen != g.topology.Peers:
		d.State = "incomplete_group_peer_coverage"
	case g.nonLeaf > 0:
		d.State = "non_leaf_group_peers_unresolved"
	case g.roles["conflicting_committee_evidence"] > 0:
		d.State = "conflicting_group_committee_evidence"
	case g.roles[RolesCompatible] == g.seen:
		d.State = SharedAssociation
		id := *g.committeeID
		d.ConduitID = &id
	}
	return d
}
