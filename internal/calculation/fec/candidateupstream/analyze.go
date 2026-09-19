package candidateupstream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"

	flows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

var candidatePattern = regexp.MustCompile(`^[HSP][0-9A-Z]{8}$`)

func validateSelection(cycle, candidate string) error {
	year, err := strconv.Atoi(cycle)
	if err != nil || len(cycle) != 4 || year < 1976 || year%2 != 0 || !candidatePattern.MatchString(candidate) {
		return fmt.Errorf("an even four-digit FEC cycle and exact candidate ID are required")
	}
	return nil
}

func digest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	} // Only local JSON-safe value structs are hashed.
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type amountSum struct {
	rows, positiveRows, negativeRows, zeroRows uint64
	signed, positive, negative                 big.Int
}

func (s *amountSum) add(n int64) {
	s.rows++
	v := big.NewInt(n)
	s.signed.Add(&s.signed, v)
	switch {
	case n > 0:
		s.positiveRows++
		s.positive.Add(&s.positive, v)
	case n < 0:
		s.negativeRows++
		s.negative.Add(&s.negative, v)
	default:
		s.zeroRows++
	}
}
func (s *amountSum) value() Amount {
	return Amount{s.rows, s.positiveRows, s.negativeRows, s.zeroRows, s.signed.String(), s.positive.String(), s.negative.String()}
}

// analyze consumes already verified source observations. All source rows keep
// their ordinal; neither parallel paths nor strongly connected components own
// another copy of the candidate's dollars. No arbitrary traversal depth applies.
func analyze(ctx context.Context, cycle, candidate string, inputs Inputs, linkages []receipts.LinkageFact, masters map[string]string, observations []flow.Observation) (Result, error) {
	if err := validateSelection(cycle, candidate); err != nil {
		return Result{}, err
	}
	r := Result{
		SchemaVersion: Version, Policy: Policy, Cycle: cycle, Candidate: candidate,
		State: "complete_selected_cohort_trace", Scope: "receiver_reported_committee_receipts_only",
		Ledger: "schedule_a", TimeSemantics: "selected_source_cycle_connectivity_not_chronological_allocation",
		AttributionState: "unresolved_funding_basis", Inputs: inputs,
		Relationships:         receipts.AuthorizedCommitteeRelationships(candidate, linkages),
		CandidateObservations: []CandidateObservation{}, UpstreamOrdinals: []uint64{},
		Nodes: []Node{}, CyclicComponents: []Component{},
		Exclusions: []string{"individual_and_other_noncommittee_receipts", "complete_candidate_receipts", "complete_committee_funding_denominators", "opening_balances_and_cross_cycle_flows", "economic_payment_identity", "chronological_fund_availability", "terminal_donor_identity_and_classification", "independent_expenditures", "schedule_b_amount_allocation"},
	}
	authorized, unresolved := map[string]bool{}, map[string]bool{}
	for _, rel := range r.Relationships {
		if !flows.ValidCommitteeID(&rel.CommitteeID) {
			return r, fmt.Errorf("invalid linkage committee identity")
		}
		switch rel.State {
		case "authorized":
			authorized[rel.CommitteeID] = true
		case "unresolved":
			unresolved[rel.CommitteeID] = true
		}
	}
	if len(authorized) == 0 {
		return r, fmt.Errorf("candidate has no unambiguous same-cycle authorized committee")
	}
	// Own the ordering, not the caller's slice; ordinal order defines replay and
	// the deterministic shortest-hop witness when several observations compete.
	rows := append([]flow.Observation(nil), observations...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Ordinal < rows[j].Ordinal })
	incoming := map[string][]int{}
	var linked, external, internal, uncertain amountSum
	for i, o := range rows {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		role, decision := flows.ClassifyReceiptRole(&o.Type)
		observationRole, roleOK := flow.ReceiverFlowRole(role)
		if o.Ordinal == 0 || i > 0 && rows[i-1].Ordinal == o.Ordinal || o.SubID == "" || !flows.ValidCommitteeID(&o.Sender) || !flows.ValidCommitteeID(&o.Recipient) || decision != flows.DecisionIncluded || !roleOK || role != o.ReportingRole || observationRole != o.Role {
			return r, fmt.Errorf("invalid, repeated, or wrong-policy receiver observation")
		}
		incoming[o.Recipient] = append(incoming[o.Recipient], i)
		if !authorized[o.Recipient] && !unresolved[o.Recipient] {
			continue
		}
		linked.add(o.Amount)
		disposition := "external_to_authorized_scope"
		switch {
		case unresolved[o.Recipient] || unresolved[o.Sender]:
			disposition = "unresolved_authorization_boundary"
			uncertain.add(o.Amount)
		case authorized[o.Sender]:
			disposition = "within_authorized_scope"
			internal.add(o.Amount)
		default:
			external.add(o.Amount)
		}
		r.CandidateObservations = append(r.CandidateObservations, CandidateObservation{o, disposition})
	}
	r.Accounting = Accounting{CandidateLinked: linked.value(), External: external.value(), Internal: internal.value(), UnresolvedScope: uncertain.value(), TerminalAllocated: "0", UnresolvedAttribution: external.value()}
	// Traverse the complete selected receiver cohort in reverse. Missing masters
	// do not erase reported edges. Repeated visits do not allocate money again.
	hops, witness := map[string]int{}, map[string]uint64{}
	queue := make([]string, 0, len(authorized))
	for id := range authorized {
		queue = append(queue, id)
		hops[id] = 0
	}
	sort.Strings(queue)
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		id := queue[i]
		for _, index := range incoming[id] {
			o := rows[index]
			if _, exists := hops[o.Sender]; exists {
				continue
			}
			hops[o.Sender], witness[o.Sender] = hops[id]+1, o.Ordinal
			queue = append(queue, o.Sender)
		}
	}
	sort.Strings(queue)
	adj, rev := map[string][]string{}, map[string][]string{}
	self := map[string]bool{}
	for _, o := range rows {
		if _, reaches := hops[o.Recipient]; !reaches {
			continue
		}
		adj[o.Sender] = append(adj[o.Sender], o.Recipient)
		rev[o.Recipient] = append(rev[o.Recipient], o.Sender)
		if o.Sender == o.Recipient {
			self[o.Sender] = true
		}
		// Unresolved-scope receipts are already preserved as candidate evidence.
		if !authorized[o.Recipient] && !unresolved[o.Recipient] {
			r.UpstreamOrdinals = append(r.UpstreamOrdinals, o.Ordinal)
		}
	}
	components, err := strongComponents(ctx, queue, adj, rev)
	if err != nil {
		return r, err
	}
	cyclic := map[string]string{}
	for _, component := range components {
		if len(component) == 1 && !self[component[0]] {
			continue
		}
		id := digest(component)
		r.CyclicComponents = append(r.CyclicComponents, Component{id, component})
		for _, node := range component {
			cyclic[node] = id
		}
	}
	for _, id := range queue {
		n := Node{CommitteeID: id, Authorized: authorized[id], Hops: hops[id], Incoming: uint64(len(incoming[id])), Reasons: []string{}}
		if v, ok := masters[id]; ok {
			n.MasterFactID = &v
		} else {
			n.Reasons = append(n.Reasons, "missing_same_cycle_committee_master")
		}
		if v, ok := witness[id]; ok {
			n.WitnessOrdinal = &v
		}
		if v, ok := cyclic[id]; ok {
			n.CyclicComponent = &v
			n.Reasons = append(n.Reasons, "cyclic_reported_connectivity")
		}
		if unresolved[id] {
			n.Reasons = append(n.Reasons, "unresolved_candidate_authorization")
		}
		if !n.Authorized {
			n.Reasons = append(n.Reasons, "complete_funding_basis_not_established")
			if n.Incoming == 0 {
				n.Reasons = append(n.Reasons, "no_selected_incoming_committee_observations")
			}
		}
		r.Nodes = append(r.Nodes, n)
	}
	r.CalculationID = digest(r)
	return r, ctx.Err()
}
