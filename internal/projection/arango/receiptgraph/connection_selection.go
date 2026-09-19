package receiptgraph

import (
	"encoding/json"
	"fmt"
	"strings"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

const ConnectionGatePolicy = "fec/receipt-candidate-connection-gate@1.0.0"

type CandidateSelection struct {
	Candidate string `json:"candidate_id"`
	Method    string `json:"method"`
}

// Iterate eligible exact identifiers in canonical order. Eligibility requires
// an authorized root present in the selected receiver ledger; no amount,
// prominence, identity attributes or candidate-specific rule selects a fixture.
func selectGateCandidate(links []authorization, recipients map[string]bool) (CandidateSelection, error) {
	selected := CandidateSelection{Method: "first_canonical_id_with_selected_receiver_boundary"}
	seen := map[string]bool{}
	for _, a := range links {
		if a.State != "authorized" {
			continue
		}
		committee := strings.TrimPrefix(a.From, entities+"/")
		candidate := strings.TrimPrefix(a.To, entities+"/")
		if a.From != entities+"/"+committee || a.To != entities+"/"+candidate || !committeePattern.MatchString(committee) || !candidatePattern.MatchString(candidate) || a.FinancialEligibility {
			return CandidateSelection{}, fmt.Errorf("invalid gate authorization")
		}
		pair := committee + "\x00" + candidate
		if seen[pair] {
			return CandidateSelection{}, fmt.Errorf("duplicate gate authorization")
		}
		seen[pair] = true
		if recipients[committee] && (selected.Candidate == "" || candidate < selected.Candidate) {
			selected.Candidate = candidate
		}
	}
	if selected.Candidate == "" {
		return selected, fmt.Errorf("no authorized candidate scope for connection gate")
	}
	return selected, nil
}

// The first four categories form a disjoint complete connectivity census.
// The remaining categories overlap and must not be summed as a population.
var connectionCaseNames = []string{
	"direct_authorized_receipt", "upstream_receipt", "no_selected_candidate_path", "unresolved_recipient",
	"qualified_conduit_connected", "unqualified_conduit", "missing_recipient_master", "cyclic_upstream_committee",
	"memo_receipt", "negative_receipt", "zero_receipt", "unknown_amount_receipt",
}

type selectedReceipt struct {
	Row      p.Row       `json:"participant"`
	Decision *c.Decision `json:"conduit_decision"`
}

type connectionSelector struct {
	nodes   map[string]upstream.Node
	masters map[string]entity
	rows    uint64
	counts  []uint64
	first   []*selectedReceipt
}

func newConnectionSelector(nodes []upstream.Node, masters map[string]entity) (*connectionSelector, error) {
	s := &connectionSelector{nodes: map[string]upstream.Node{}, masters: masters, counts: make([]uint64, len(connectionCaseNames)), first: make([]*selectedReceipt, len(connectionCaseNames))}
	for _, n := range nodes {
		if _, exists := s.nodes[n.CommitteeID]; exists {
			return nil, fmt.Errorf("duplicate selector node")
		}
		s.nodes[n.CommitteeID] = n
	}
	return s, nil
}

func (s *connectionSelector) observe(row p.Row, decision *c.Decision) error {
	if row.Ordinal <= 0 || uint64(row.Ordinal) != s.rows+1 {
		return fmt.Errorf("connection selection is not a complete ordered occurrence stream")
	}
	s.rows++
	var mask uint16
	var node upstream.Node
	var reaches bool
	valid := row.Recipient != nil && committeePattern.MatchString(*row.Recipient)
	if !valid {
		mask |= 1 << 3
	} else {
		node, reaches = s.nodes[*row.Recipient]
		switch {
		case !reaches:
			mask |= 1 << 2
		case node.Authorized:
			mask |= 1
		default:
			mask |= 1 << 1
		}
		if _, exists := s.masters[*row.Recipient]; !exists {
			mask |= 1 << 6
		}
		if reaches && node.CyclicComponent != nil {
			mask |= 1 << 7
		}
	}
	if decision != nil {
		if decision.Ordinal != uint64(row.Ordinal) {
			return fmt.Errorf("selector decision belongs to another occurrence")
		}
		if decision.ConduitID != nil && reaches {
			mask |= 1 << 4
		}
		if decision.ConduitID == nil {
			mask |= 1 << 5
		}
	}
	if row.Memo {
		mask |= 1 << 8
	}
	switch {
	case row.Amount == nil:
		mask |= 1 << 11
	case *row.Amount < 0:
		mask |= 1 << 9
	case *row.Amount == 0:
		mask |= 1 << 10
	}
	var owned *selectedReceipt
	for i := range connectionCaseNames {
		if mask&(1<<i) == 0 {
			continue
		}
		s.counts[i]++
		if s.first[i] != nil {
			continue
		}
		if owned == nil {
			// Only at most one owned record per case, not one allocation per
			// input row. The stream borrows Parquet and decision buffers.
			b, err := json.Marshal(selectedReceipt{row, decision})
			if err != nil {
				return err
			}
			owned = &selectedReceipt{}
			if err := json.Unmarshal(b, owned); err != nil {
				return err
			}
		}
		s.first[i] = owned
	}
	return nil
}

func (s *connectionSelector) validate(rows uint64) error {
	if s.rows != rows {
		return fmt.Errorf("incomplete connection census")
	}
	var total uint64
	for i, count := range s.counts {
		if count > rows || (count == 0) != (s.first[i] == nil) {
			return fmt.Errorf("connection case count/witness mismatch")
		}
		if i < 4 {
			if count > rows-total {
				return fmt.Errorf("connection census overflow")
			}
			total += count
		}
	}
	if total != rows {
		return fmt.Errorf("connectivity states do not conserve source occurrences")
	}
	return nil
}
