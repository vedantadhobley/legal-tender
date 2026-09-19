package candidateevidence

import (
	"context"
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

type PathExample struct {
	From            string             `json:"from_committee_id"`
	To              string             `json:"authorized_committee_id"`
	Hops            []flow.Observation `json:"hops"`
	MissingDates    bool               `json:"has_missing_reported_dates"`
	DateReversal    bool               `json:"has_decreasing_reported_dates"`
	SameDay         bool               `json:"has_same_day_hops"`
	Nonpositive     bool               `json:"has_nonpositive_amount"`
	AllocatedAmount *string            `json:"allocated_amount_minor_units"`
}

// Select mechanically by hop distance then committee ID, never name, amount,
// ideology or preferred result. Every path is a complete existing witness chain.
func pathExamples(ctx context.Context, e fundingbasis.CandidateEvidence) ([]PathExample, error) {
	nodes := map[string]candidateupstream.Node{}
	for _, n := range e.Trace.Nodes {
		nodes[n.CommitteeID] = n
	}
	rows := map[uint64]flow.Observation{}
	for _, o := range e.Witnesses {
		if _, exists := rows[o.Ordinal]; exists {
			return nil, fmt.Errorf("duplicate path witness")
		}
		rows[o.Ordinal] = o
	}
	ordered := append([]candidateupstream.Node(nil), e.Trace.Nodes...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Hops != ordered[j].Hops {
			return ordered[i].Hops < ordered[j].Hops
		}
		return ordered[i].CommitteeID < ordered[j].CommitteeID
	})
	out := []PathExample{}
	lastDistance := 0
	for _, start := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if start.Hops <= lastDistance || start.Authorized {
			continue
		}
		p := PathExample{From: start.CommitteeID, Hops: []flow.Observation{}}
		n := start
		var previousDate *int32
		for !n.Authorized {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if n.WitnessOrdinal == nil {
				return nil, fmt.Errorf("path lacks source witness")
			}
			o, ok := rows[*n.WitnessOrdinal]
			if !ok || o.Sender != n.CommitteeID {
				return nil, fmt.Errorf("path source witness mismatch")
			}
			next, ok := nodes[o.Recipient]
			if !ok || next.Hops != n.Hops-1 {
				return nil, fmt.Errorf("path does not approach authorized scope")
			}
			p.Hops = append(p.Hops, o)
			if o.Amount <= 0 {
				p.Nonpositive = true
			}
			if o.Date == nil {
				p.MissingDates = true
			} else if previousDate != nil {
				p.DateReversal = p.DateReversal || *o.Date < *previousDate
				p.SameDay = p.SameDay || *o.Date == *previousDate
			}
			// Compare consecutive known dates, retaining gaps independently.
			if o.Date != nil {
				previousDate = o.Date
			}
			n = next
		}
		if len(p.Hops) != start.Hops {
			return nil, fmt.Errorf("path length disagrees with trace")
		}
		p.To = n.CommitteeID
		out = append(out, p)
		lastDistance = start.Hops
		if len(out) == 3 {
			break
		}
	}
	return out, ctx.Err()
}
