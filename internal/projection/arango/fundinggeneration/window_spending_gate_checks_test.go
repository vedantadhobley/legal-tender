package fundinggeneration

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
)

func spendingDate(m ie.DatedMember, basis string) *int32 {
	if basis == "dissemination" {
		return m.DisseminationDate
	}
	return m.ExpenditureDate
}

func censusBucketKey(b WindowSpendingBucket) string {
	// The comparison key excludes values being checked. Do not reuse the
	// production bucket hash or its order-dependent output construction.
	routes, _ := json.Marshal(b.RouteReasons)
	return fmt.Sprintf("%q|%q|%s|%q|%q|%q|%t", b.EffectiveState, b.AmountReason, routes, b.ResolutionState, b.Stance, b.Selection, b.Projectable)
}

func expectedSpendingCoverage(c spendingGateCensus, q WindowConnectionQuery) map[string]map[string]WindowSpendingBucket {
	out := map[string]map[string]WindowSpendingBucket{}
	for _, s := range c.Summaries {
		out[s.Generation] = map[string]WindowSpendingBucket{}
	}
	for _, ref := range c.Facts {
		m := ref.Member
		b := WindowSpendingBucket{EffectiveState: m.EffectiveState, AmountReason: m.AmountReason, RouteReasons: m.RouteReasons, ResolutionState: m.ResolutionState,
			Stance: m.Stance, Selection: independentDateSelection(spendingDate(m, q.SpendingDate), q.Window), Projectable: m.Link != nil}
		k := censusBucketKey(b)
		v, exists := out[ref.Generation][k]
		if !exists {
			v = b
			v.Amount = "0"
		}
		v.Rows++
		if m.Amount != nil {
			a, _ := new(big.Int).SetString(*m.Amount, 10)
			sum, _ := new(big.Int).SetString(v.Amount, 10)
			v.Amount = sum.Add(sum, a).String()
			v.KnownAmountRows++
		}
		out[ref.Generation][k] = v
	}
	return out
}

func checkSpendingGateResult(out WindowConnectionsResult, counts connectionCounts, census spendingGateCensus) error {
	if out.SchemaVersion != WindowSpendingConnectionsVersion || out.Policy != WindowSpendingConnectionsPolicy || !out.Query.isSpending() || len(out.Contexts) != 0 {
		return fmt.Errorf("spending contract changed")
	}
	err := checkConnectionGateCommon(out, counts, func(link WindowConnectionLink) error {
		ref, ok := census.Facts[link.Topology.Key]
		if !ok || ref.Generation != link.GenerationID || ref.Member.Link == nil || *ref.Member.Link != link.Topology || link.Topology.Family != out.Query.Ending+"_observation" {
			return fmt.Errorf("foreign spending source member")
		}
		var member ie.DatedMember
		var evidence ie.MemberEvidence
		if err := json.Unmarshal(link.Evidence.Document, &member); err != nil {
			return err
		}
		if err := json.Unmarshal(link.Evidence.Evidence, &evidence); err != nil {
			return err
		}
		if !reflect.DeepEqual(member, ref.Member) || valueID(evidence.Source) != ref.SourceHash || valueID(evidence.Decision) != ref.DecisionHash {
			return fmt.Errorf("spending source, decision or native dates differ from independent artifacts")
		}
		if !sameDate(link.Date, spendingDate(member, out.Query.SpendingDate)) || link.TemporalBasis != "schedule_e_"+out.Query.SpendingDate+"_reported_date" ||
			link.Evidence.EvidenceKind != "verified_schedule_e_source_member_with_resolved_aggregate_parent" {
			return fmt.Errorf("spending date basis or evidence grain differs")
		}
		var input ie.InputReferences
		for _, p := range out.Inputs {
			if p.GenerationID == link.GenerationID {
				input = p.Generation.OutsideSpending.Inputs
			}
		}
		if evidence.Inputs != input {
			return fmt.Errorf("spending source inputs differ")
		}
		parent := census.Parents[spendingGroupKey(ref.Generation, member.Link.From, member.Link.To, member.Stance)]
		if err := checkSpendingParent(evidence, parent); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	want := expectedSpendingCoverage(census, out.Query)
	if len(out.SpendingCoverage) != len(want) {
		return fmt.Errorf("spending census publication count differs")
	}
	seen := map[string]bool{}
	for _, c := range out.SpendingCoverage {
		w, ok := want[c.GenerationID]
		if !ok || seen[c.GenerationID] || c.Population != "all_schedule_e_source_facts_in_this_publication" {
			return fmt.Errorf("spending census population differs")
		}
		seen[c.GenerationID] = true
		got := map[string]WindowSpendingBucket{}
		var rows uint64
		for _, b := range c.Buckets {
			k := censusBucketKey(b)
			if _, exists := got[k]; exists {
				return fmt.Errorf("duplicate spending census bucket")
			}
			got[k] = b
			rows += b.Rows
		}
		if !reflect.DeepEqual(got, w) || rows != c.SourceFacts {
			return fmt.Errorf("independent spending date/count/signed-amount census differs")
		}
	}
	for _, v := range out.Vertices {
		for _, f := range v.Facets {
			if f.OutsideSpending == nil {
				return fmt.Errorf("outside historical facet missing")
			}
		}
	}
	if out.Query.ReceiptOrdinal == 0 && out.Query.MaxHops == 0 {
		// Check exact bounded member membership, not only that returned records
		// look plausible. Direct source-member endings have one link per path.
		wantIDs := []string{}
		for _, ref := range census.Facts {
			m := ref.Member
			if m.Link == nil || m.Link.Family != out.Query.Ending+"_observation" || m.Link.From != out.Query.From || m.Link.To != out.Query.Target {
				continue
			}
			selection := independentDateSelection(spendingDate(m, out.Query.SpendingDate), out.Query.Window)
			if selection == "included" || selection == "undated_included" {
				wantIDs = append(wantIDs, m.FactID)
			}
		}
		sort.Strings(wantIDs)
		if len(out.Paths) != min(len(wantIDs), out.Query.Limit) {
			return fmt.Errorf("direct window member count differs")
		}
		for i, link := range out.Links {
			if i >= len(wantIDs) || link.Topology.Key != wantIDs[i] {
				return fmt.Errorf("direct window member selection differs")
			}
		}
		if len(out.Links) != len(out.Paths) {
			return fmt.Errorf("direct source-member path grain differs")
		}
	}
	return nil
}

func checkSpendingParent(e ie.MemberEvidence, want resolution.AggregateResult) error {
	var parent struct {
		Result      string                                `json:"result_id"`
		Calculation string                                `json:"calculation_set_id"`
		Resolution  string                                `json:"candidate_resolution_calculation_set_id"`
		FactSet     string                                `json:"schedule_e_fact_set_id"`
		From        string                                `json:"_from"`
		To          string                                `json:"_to"`
		Stance      string                                `json:"support_oppose"`
		Amount      string                                `json:"amount_minor_units"`
		Count       uint64                                `json:"expenditure_count"`
		Positive    uint64                                `json:"positive_count"`
		Negative    uint64                                `json:"negative_count"`
		Zero        uint64                                `json:"zero_count"`
		Counts      resolution.AggregateResolutionCounts  `json:"resolution_counts"`
		Amounts     resolution.AggregateResolutionAmounts `json:"resolution_amounts"`
	}
	if err := json.Unmarshal(e.Parent.Document, &parent); err != nil {
		return err
	}
	if e.Parent.Key != want.ResultID || e.Parent.EvidenceKind != "verified_resolved_calculation_group" || parent.Result != want.ResultID || parent.Calculation != want.CalculationSetID || parent.Resolution != e.Inputs.CandidateResolutionCalculationSetID || parent.FactSet != e.Inputs.ScheduleEFactSetID ||
		parent.From != "entities/committee_"+want.SpenderCommitteeID || parent.To != "entities/candidate_"+want.CandidateID || parent.Stance != want.SupportOppose ||
		parent.Amount != want.SignedAmountMinorUnits || parent.Count != want.ExpenditureCount || parent.Positive != want.PositiveCount || parent.Negative != want.NegativeCount || parent.Zero != want.ZeroCount || parent.Counts != want.ResolutionCounts || parent.Amounts != want.ResolutionAmounts {
		return fmt.Errorf("parent aggregate differs from complete published calculation")
	}
	return nil
}
