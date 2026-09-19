package fundinggeneration

import (
	"context"
	"encoding/json"
	"testing"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func checkedSpendingFixture(t *testing.T) (WindowConnectionsResult, connectionCounts, spendingGateCensus) {
	t.Helper()
	r, q := spendingFixture(t)
	out, err := r.ConnectionPaths(context.Background(), q)
	liveCheck(t, err)
	marshal := func(v any) json.RawMessage { b, err := json.Marshal(v); liveCheck(t, err); return b }
	counts := connectionCounts{}
	census := spendingGateCensus{Facts: map[string]spendingReference{}, Parents: map[string]resolution.AggregateResult{}}
	for _, p := range r.partitions {
		c := connectionCensus{days: map[int32]uint64{}}
		for _, row := range p.source.(*windowStub).rows {
			if row.Date == nil {
				c.unknown++
			} else {
				c.days[*row.Date]++
			}
		}
		counts[p.publication.GenerationID] = map[flow.Ledger]connectionCensus{q.Ledger: c}
	}
	for i := range out.Links {
		l := &out.Links[i]
		var m ie.DatedMember
		liveCheck(t, json.Unmarshal(l.Evidence.Document, &m))
		var input ie.InputReferences
		for _, p := range out.Inputs {
			if p.GenerationID == l.GenerationID {
				input = p.Generation.OutsideSpending.Inputs
			}
		}
		source := occ.ScheduleEFact{FactID: m.FactID, NaturalKey: "fixture", SourceFields: json.RawMessage(`{"original":"preserved"}`)}
		decision := resolution.Decision{FactID: m.FactID, DecisionID: m.DecisionID, State: m.ResolutionState, AmountMinorUnits: *m.Amount}
		parent := resolution.AggregateResult{ResultID: m.Parent.Key, SpenderCommitteeID: m.Link.From, CandidateID: m.Link.To, SupportOppose: m.Stance,
			SignedAmountMinorUnits: *m.Amount, ExpenditureCount: 1, PositiveCount: 1,
			ResolutionCounts: resolution.AggregateResolutionCounts{Confirmed: 1}, ResolutionAmounts: resolution.AggregateResolutionAmounts{ConfirmedMinorUnits: *m.Amount, ResolvedMinorUnits: "0", UnverifiedMinorUnits: "0"}}
		parentDoc := marshal(map[string]any{"result_id": parent.ResultID, "calculation_set_id": parent.CalculationSetID, "candidate_resolution_calculation_set_id": input.CandidateResolutionCalculationSetID,
			"schedule_e_fact_set_id": input.ScheduleEFactSetID, "_from": "entities/committee_" + parent.SpenderCommitteeID, "_to": "entities/candidate_" + parent.CandidateID,
			"support_oppose": parent.SupportOppose, "amount_minor_units": parent.SignedAmountMinorUnits, "expenditure_count": parent.ExpenditureCount, "positive_count": parent.PositiveCount,
			"resolution_counts": parent.ResolutionCounts, "resolution_amounts": parent.ResolutionAmounts})
		l.Evidence = graphread.Item{Key: m.FactID, Document: marshal(m), EvidenceKind: "verified_schedule_e_source_member_with_resolved_aggregate_parent",
			Evidence: marshal(ie.MemberEvidence{Source: source, Decision: decision, Inputs: input, Parent: graphread.Item{Key: parent.ResultID, EvidenceKind: "verified_resolved_calculation_group", Document: parentDoc}})}
		census.Facts[m.FactID] = spendingReference{Generation: l.GenerationID, SourceHash: valueID(source), DecisionHash: valueID(decision), Member: m}
		census.Parents[spendingGroupKey(l.GenerationID, m.Link.From, m.Link.To, m.Stance)] = parent
		census.Summaries = append(census.Summaries, spendingCensusSummary{Generation: l.GenerationID, Facts: 1, Decisions: 1, Projectable: 1})
	}
	out.ResultID = ""
	out.ResultID = valueID(out)
	return out, counts, census
}

func TestSpendingGateRejectsResealedWrongEvidence(t *testing.T) {
	out, counts, census := checkedSpendingFixture(t)
	liveCheck(t, checkSpendingGateResult(out, counts, census))
	alterEvidence := func(v *WindowConnectionsResult, change func(*ie.MemberEvidence)) {
		var e ie.MemberEvidence
		liveCheck(t, json.Unmarshal(v.Links[0].Evidence.Evidence, &e))
		change(&e)
		b, err := json.Marshal(e)
		liveCheck(t, err)
		v.Links[0].Evidence.Evidence = b
	}
	for _, mutate := range []func(*WindowConnectionsResult){
		func(v *WindowConnectionsResult) { v.FinancialEligibility = true },
		func(v *WindowConnectionsResult) { v.TerminalEligible = true },
		func(v *WindowConnectionsResult) { v.SpendingCoverage[0].Buckets[0].Amount = "999" },
		func(v *WindowConnectionsResult) { v.SpendingCoverage[0].Buckets[0].KnownAmountRows = 0 },
		func(v *WindowConnectionsResult) { v.SpendingCoverage[0].Buckets[0].Selection = "before_window" },
		func(v *WindowConnectionsResult) { v.SpendingCoverage = v.SpendingCoverage[:1] },
		func(v *WindowConnectionsResult) { v.Links[0].Date = nil },
		func(v *WindowConnectionsResult) { v.Links[0].TemporalBasis = "cycle" },
		func(v *WindowConnectionsResult) { v.Links[0].Topology.Family = "independent_support" },
		func(v *WindowConnectionsResult) { v.Query.SpendingDate = "dissemination" },
		func(v *WindowConnectionsResult) { v.Vertices[0].Facets[0].OutsideSpending = nil },
		func(v *WindowConnectionsResult) {
			alterEvidence(v, func(e *ie.MemberEvidence) { e.Source.SourceFields = json.RawMessage(`{"original":"changed"}`) })
		},
		func(v *WindowConnectionsResult) {
			alterEvidence(v, func(e *ie.MemberEvidence) { e.Decision.AmountMinorUnits = "999" })
		},
		func(v *WindowConnectionsResult) {
			alterEvidence(v, func(e *ie.MemberEvidence) { e.Inputs.ScheduleEFactSetID = valueID("foreign") })
		},
		func(v *WindowConnectionsResult) {
			alterEvidence(v, func(e *ie.MemberEvidence) {
				var d map[string]any
				liveCheck(t, json.Unmarshal(e.Parent.Document, &d))
				d["amount_minor_units"] = "1"
				b, err := json.Marshal(d)
				liveCheck(t, err)
				e.Parent.Document = b
			})
		},
		func(v *WindowConnectionsResult) { v.Paths = nil; v.Links = nil },
	} {
		out, counts, census := checkedSpendingFixture(t)
		mutate(&out)
		out.ResultID = ""
		out.ResultID = valueID(out)
		if checkSpendingGateResult(out, counts, census) == nil {
			t.Fatal("accepted resealed incorrect spending evidence")
		}
	}
}

func TestSpendingWitnessSelectionNeedsDistinctNativeDates(t *testing.T) {
	_, _, c := checkedSpendingFixture(t)
	if _, ok := selectSpendingDateWitness(c, "S", "expenditure"); ok {
		t.Fatal("selected single-date group")
	}
	var added spendingReference
	for _, ref := range c.Facts {
		added = ref
		break
	}
	m := added.Member
	m.FactID, m.ExpenditureDate = valueID("another fact"), dayPtr(*m.ExpenditureDate+1)
	link := *m.Link
	link.Key = m.FactID
	m.Link = &link
	added.Member = m
	c.Facts[m.FactID] = added
	if _, ok := selectSpendingDateWitness(c, "S", "expenditure"); !ok {
		t.Fatal("lost multi-date group")
	}
	if _, ok := selectSpendingDateWitness(c, "S", "dissemination"); ok {
		t.Fatal("filled unknown dissemination dates")
	}
}
