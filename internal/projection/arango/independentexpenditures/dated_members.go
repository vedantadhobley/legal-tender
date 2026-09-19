package independentexpenditures

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	effective "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// DatedMember is source-grain evidence, not a stored per-fact Arango edge.
// Parent identifies the unchanged aggregate; it is never the member's amount.
type DatedMember struct {
	FactID            string          `json:"fact_id"`
	DecisionID        string          `json:"candidate_decision_id,omitempty"`
	EffectiveState    string          `json:"effective_state"`
	AmountReason      string          `json:"amount_reason"`
	RouteReasons      []string        `json:"effective_route_reasons"`
	ResolutionState   string          `json:"candidate_resolution_state"`
	Stance            string          `json:"support_oppose"`
	Amount            *string         `json:"reported_amount_minor_units"`
	ExpenditureDate   *int32          `json:"expenditure_date_days"`
	DisseminationDate *int32          `json:"dissemination_date_days"`
	Link              *graphread.Link `json:"source_member_topology"`
	Parent            *graphread.Link `json:"parent_aggregate_topology"`
}

type memberGroupKey struct{ spender, candidate, stance string }
type memberTotal struct {
	count, positive, negative, zero         uint64
	amount, confirmed, resolved, unverified big.Int
	states                                  EdgeResolutionCounts
}

// VisitDatedMembers verifies complete source/decision membership and every
// aggregate's count, sign counts, resolution states and exact signed amounts.
// Callers must discard callbacks if complete replay fails.
func (r *ResolvedReader) VisitDatedMembers(ctx context.Context, visit func(DatedMember) error) error {
	if visit == nil {
		return fmt.Errorf("Schedule E member visitor required")
	}
	return r.visitMembers(ctx, func(m DatedMember, _ occ.ScheduleEFact, _ *resolution.Decision) error { return visit(m) })
}

func (r *ResolvedReader) visitMembers(ctx context.Context, visit func(DatedMember, occ.ScheduleEFact, *resolution.Decision) error) error {
	if r.storageRoot == "" || visit == nil {
		return fmt.Errorf("opened resolved source reader required")
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return err
	}
	groups := map[memberGroupKey]resolvedExpenditureEdge{}
	totals := map[memberGroupKey]*memberTotal{}
	for _, e := range r.m.Edges {
		k := memberGroupKey{strings.TrimPrefix(e.From, entitiesCollection+"/committee_"), strings.TrimPrefix(e.To, entitiesCollection+"/candidate_"), e.SupportOppose}
		if _, exists := groups[k]; exists {
			return fmt.Errorf("duplicate resolved aggregate endpoints")
		}
		groups[k], totals[k] = e, &memberTotal{}
	}
	err := resolution.VisitPublishedFacts(ctx, r.storageRoot, r.m.Inputs.CandidateResolutionCalculationSetID, r.m.Inputs.CandidateResolutionManifestSHA256,
		func(f occ.ScheduleEFact, evaluation effective.FactEvaluation, d *resolution.Decision) error {
			m := DatedMember{FactID: f.FactID, EffectiveState: evaluation.Decision, AmountReason: evaluation.AmountReason,
				RouteReasons: evaluation.RouteReasons, ResolutionState: "not_admitted_by_effective_policy"}
			if f.TypedFields.Candidate.SupportOpposeCode != nil {
				m.Stance = *f.TypedFields.Candidate.SupportOpposeCode
			}
			if evaluation.Amount != nil {
				s := evaluation.Amount.String()
				m.Amount = &s
			}
			var err error
			m.ExpenditureDate, err = memberDate(f.TypedFields.Expenditure.ExpenditureOn)
			if err != nil {
				return err
			}
			m.DisseminationDate, err = memberDate(f.TypedFields.Expenditure.DisseminatedOn)
			if err != nil {
				return err
			}
			if d != nil {
				m.DecisionID, m.ResolutionState = d.DecisionID, d.State
				// The published decision validator enforces that only confirmed,
				// resolved and unverified decisions carry a resolved candidate ID.
				if d.ResolvedCandidateID != nil {
					k := memberGroupKey{d.SpenderCommitteeID, *d.ResolvedCandidateID, d.SupportOppose}
					e, ok := groups[k]
					if !ok {
						return fmt.Errorf("source member has no pinned resolved aggregate")
					}
					family := "independent_support"
					if d.SupportOppose == "O" {
						family = "independent_opposition"
					}
					m.Link = &graphread.Link{Family: family + "_observation", Key: f.FactID, From: k.spender, To: k.candidate}
					m.Parent = &graphread.Link{Family: family, Key: e.ResultID, From: k.spender, To: k.candidate}
					totals[k].add(*d, evaluation.Amount)
				}
			}
			return visit(m, f, d)
		})
	if err != nil {
		return err
	}
	for k, e := range groups {
		if !totals[k].matches(e) {
			return fmt.Errorf("Schedule E source membership differs from resolved aggregate %s", e.ResultID)
		}
	}
	return r.VerifyCompletion(ctx)
}

func memberDate(day *string) (*int32, error) {
	if day == nil {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02", *day)
	if err != nil || t.Format("2006-01-02") != *day {
		return nil, fmt.Errorf("invalid normalized Schedule E day")
	}
	d := int32(t.Unix() / 86400)
	return &d, nil
}

func (t *memberTotal) add(d resolution.Decision, amount *big.Int) {
	t.count++
	t.amount.Add(&t.amount, amount)
	switch amount.Sign() {
	case -1:
		t.negative++
	case 0:
		t.zero++
	case 1:
		t.positive++
	}
	switch d.State {
	case resolution.StateConfirmed:
		t.states.Confirmed++
		t.confirmed.Add(&t.confirmed, amount)
	case resolution.StateResolved:
		t.states.Resolved++
		t.resolved.Add(&t.resolved, amount)
	case resolution.StateUnverified:
		t.states.Unverified++
		t.unverified.Add(&t.unverified, amount)
	}
}

func (t *memberTotal) matches(e resolvedExpenditureEdge) bool {
	return t.count == e.ExpenditureCount && t.positive == e.PositiveCount && t.negative == e.NegativeCount && t.zero == e.ZeroCount &&
		t.amount.String() == e.AmountMinorUnits && t.states == e.ResolutionCounts &&
		t.confirmed.String() == e.ResolutionAmounts.ConfirmedMinorUnits && t.resolved.String() == e.ResolutionAmounts.ResolvedMinorUnits && t.unverified.String() == e.ResolutionAmounts.UnverifiedMinorUnits
}
