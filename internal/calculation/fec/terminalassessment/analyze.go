package terminalassessment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/graphutil"
)

func validID(id string) bool {
	return len(id) == 9 && id[0] == 'C' && strings.Trim(id[1:], "0123456789") == ""
}
func digest(s string) bool {
	return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == ""
}

// Analyze compares the complete endpoint union, not a depth-limited query or
// merged payment ledger. Callers bind/verify these source-derived inputs.
func Analyze(ctx context.Context, committees []Committee, a, b []Observation) (Result, error) {
	out := Result{Policy: Policy, Definitions: Definitions(), Committees: append([]Committee{}, committees...), Comparison: []Comparison{}, OriginState: "not_established", Blockers: []string{
		"terminal_policy_not_selected",
		"selected_committee_observations_not_complete_funding_denominators",
		"noncommittee_receipt_roles_and_person_corporation_resolution_not_assessed",
		"source_cycle_connectivity_not_chronological_fund_availability",
		"opening_balances_cross_cycle_and_other_funding_not_established",
		"observation_edges_not_reconciled_economic_payments",
	}}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	sort.Slice(out.Committees, func(i, j int) bool { return out.Committees[i].ID < out.Committees[j].ID })
	identities := map[string]Committee{}
	for _, c := range out.Committees {
		if !validID(c.ID) || !digest(c.MasterFactSetID) || identities[c.ID].ID != "" {
			return out, fmt.Errorf("invalid or duplicate committee identity")
		}
		if c.IdentityState == SameCycleMaster {
			if c.MasterFactID == nil || !digest(*c.MasterFactID) {
				return out, fmt.Errorf("same-cycle identity requires master fact")
			}
		} else if c.IdentityState != MissingMaster || c.MasterFactID != nil {
			return out, fmt.Errorf("unsupported committee identity state")
		}
		identities[c.ID] = c
	}
	var err error
	if out.A, err = analyzeLedger(ctx, "schedule_a", out.Committees, identities, a); err != nil {
		return out, err
	}
	if out.B, err = analyzeLedger(ctx, "schedule_b", out.Committees, identities, b); err != nil {
		return out, err
	}
	counts := map[[2]string]uint64{}
	for i := range out.Committees {
		x, y := out.A.Nodes[i].State, out.B.Nodes[i].State
		if x == Absent && y == Absent {
			return out, fmt.Errorf("committee outside selected endpoint union")
		}
		counts[[2]string{x, y}]++
	}
	for _, x := range []string{Absent, Frontier, Inbound} {
		for _, y := range []string{Absent, Frontier, Inbound} {
			out.Comparison = append(out.Comparison, Comparison{x, y, counts[[2]string{x, y}]})
		}
	}
	return out, ctx.Err()
}

func analyzeLedger(ctx context.Context, name string, committees []Committee, identities map[string]Committee, observations []Observation) (Ledger, error) {
	out := Ledger{Name: name, State: "complete_selected_endpoint_topology", Observations: uint64(len(observations)), Nodes: []Node{}, Components: []Component{}, Rules: []RuleCount{}}
	for _, d := range Definitions() {
		out.Rules = append(out.Rules, RuleCount{ID: d.ID})
	}
	nodes := map[string]*Node{}
	adj, reverse := map[string][]string{}, map[string][]string{}
	keys := map[string]bool{}
	for _, e := range observations {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if !digest(e.Key) || keys[e.Key] || identities[e.From].ID == "" || identities[e.To].ID == "" {
			return out, fmt.Errorf("invalid, duplicate or identity-unbound %s observation", name)
		}
		keys[e.Key] = true
		for _, id := range []string{e.From, e.To} {
			if nodes[id] == nil {
				nodes[id] = &Node{CommitteeID: id, Blockers: []string{}}
			}
		}
		nodes[e.From].Outgoing++
		nodes[e.To].Incoming++
		if e.From == e.To {
			nodes[e.From].SelfLoops++
		}
		adj[e.From] = append(adj[e.From], e.To)
		reverse[e.To] = append(reverse[e.To], e.From)
	}
	ids := make([]string, 0, len(nodes))
	for _, c := range committees {
		if nodes[c.ID] != nil {
			ids = append(ids, c.ID)
		}
	}
	groups, err := graphutil.StrongComponents(ctx, ids, adj, reverse)
	if err != nil {
		return out, err
	}
	membership := map[string]int{}
	for i, members := range groups {
		// Component IDs identify a member set within this ledger and policy.
		// The outer result pins generation/source identity and all edge counts.
		body, _ := json.Marshal(struct {
			Policy, Ledger string
			Members        []string
		}{Policy, name, members})
		h := sha256.Sum256(body)
		c := Component{ID: hex.EncodeToString(h[:]), Members: members, Cyclic: len(members) > 1}
		for _, id := range members {
			membership[id] = i
			if nodes[id].SelfLoops > 0 {
				c.Cyclic = true
			}
			if identities[id].IdentityState == MissingMaster {
				c.MissingMasters++
			}
		}
		out.Components = append(out.Components, c)
	}
	for _, e := range observations {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		from, to := membership[e.From], membership[e.To]
		if from == to {
			out.Components[from].Internal++
		} else {
			out.Components[from].Outgoing++
			out.Components[to].Incoming++
		}
	}
	var internal, externalIn, externalOut uint64
	for _, c := range out.Components {
		internal += c.Internal
		externalIn += c.Incoming
		externalOut += c.Outgoing
		if c.Cyclic {
			out.CyclicComponents++
		}
		if c.Incoming == 0 {
			out.RootComponents++
			if c.Cyclic {
				out.CyclicRootComponents++
			}
		}
	}
	var incoming, outgoing uint64
	for _, identity := range committees {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		n := nodes[identity.ID]
		if n == nil {
			out.Nodes = append(out.Nodes, Node{CommitteeID: identity.ID, State: Absent, Blockers: []string{"no_selected_ledger_membership"}})
			for i := range out.Rules {
				out.Rules[i].NotApplicable++
			}
			continue
		}
		out.Present++
		incoming += n.Incoming
		outgoing += n.Outgoing
		c := out.Components[membership[identity.ID]]
		n.ComponentID = c.ID
		frontier, identified, root := n.Incoming == 0, identity.IdentityState == SameCycleMaster && n.Incoming == 0, c.Incoming == 0
		n.Frontier, n.IdentityFrontier, n.RootComponent = &frontier, &identified, &root
		n.State = Inbound
		if frontier {
			n.State = Frontier
		}
		if identity.IdentityState == MissingMaster {
			out.MissingMasters++
			n.Blockers = append(n.Blockers, "unresolved_same_cycle_master")
		}
		if c.Cyclic {
			n.Blockers = append(n.Blockers, "cyclic_component_not_individual_origin")
		}
		for i, match := range []bool{frontier, identified, root} {
			if match {
				out.Rules[i].Matched++
			} else {
				out.Rules[i].NotMatched++
			}
		}
		out.Nodes = append(out.Nodes, *n)
	}
	if incoming != out.Observations || outgoing != out.Observations || internal+externalIn != out.Observations || externalIn != externalOut {
		return out, fmt.Errorf("selected topology conservation failed")
	}
	return out, nil
}
