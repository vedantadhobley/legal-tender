package fundinggeneration

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
)

// V1 receipt/authorization responses and existing aggregate graph APIs remain
// unchanged. Only source-grain spending queries use this additive contract.
const WindowSpendingConnectionsVersion = "legal-tender.funding-window-connections.v2"
const WindowSpendingConnectionsPolicy = "fec/dated-schedule-e-source-connections@1.0.0"

type WindowSpendingBucket struct {
	EffectiveState  string   `json:"effective_state"`
	AmountReason    string   `json:"amount_reason"`
	RouteReasons    []string `json:"effective_route_reasons"`
	ResolutionState string   `json:"candidate_resolution_state"`
	Stance          string   `json:"support_oppose"`
	Selection       string   `json:"date_selection"`
	Projectable     bool     `json:"has_candidate_relationship"`
	Rows            uint64   `json:"source_facts"`
	KnownAmountRows uint64   `json:"known_amount_facts"`
	Amount          string   `json:"known_reported_minor_units"`
}

type WindowSpendingCoverage struct {
	GenerationID string                 `json:"generation_id"`
	Population   string                 `json:"population"`
	SourceFacts  uint64                 `json:"source_facts"`
	Buckets      []WindowSpendingBucket `json:"buckets"`
}

func spendingLimitations(previous []string) []string {
	out := []string{}
	for _, s := range previous {
		if s != "schedule_e_group_amounts_not_dated_endings_in_this_contract" {
			out = append(out, s)
		}
	}
	return append(out,
		"spending_links_are_verified_source_members_not_stored_per_fact_graph_edges",
		"parent_aggregate_amount_is_not_the_selected_member_or_window_amount",
		"support_and_opposition_are_separate_outside_spending_not_candidate_receipts",
		"explicit_expenditure_or_dissemination_date_no_fallback_or_cycle_date",
		"source_coverage_includes_other_candidates_stances_and_policy_exclusions_not_only_returned_paths",
		"date_selection_does_not_change_effective_or_candidate_resolution_policy")
}

func (r *WindowReader) spendingEndings(ctx context.Context, q WindowConnectionQuery, routes map[string]connectionRoute) (pathTopology, []WindowSpendingCoverage, error) {
	topology, coverage := pathTopology{}, []WindowSpendingCoverage{}
	seenFacts, seenSets := map[string]bool{}, map[string]bool{}
	var total uint64
	for i, p := range r.partitions {
		if p.outside == nil {
			return nil, nil, fmt.Errorf("verified Schedule E source required")
		}
		factSet := p.publication.Generation.OutsideSpending.Inputs.ScheduleEFactSetID
		if !validDigest(factSet) || seenSets[factSet] {
			return nil, nil, fmt.Errorf("overlapping or invalid Schedule E fact input")
		}
		seenSets[factSet] = true
		c := WindowSpendingCoverage{GenerationID: p.publication.GenerationID, Population: "all_schedule_e_source_facts_in_this_publication", Buckets: []WindowSpendingBucket{}}
		buckets := map[string]*WindowSpendingBucket{}
		err := p.outside.VisitDatedMembers(ctx, func(m ie.DatedMember) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			total++
			if total > MaxWindowObservations {
				return fmt.Errorf("Schedule E observations exceed window memory guard")
			}
			if !validDigest(m.FactID) || seenFacts[m.FactID] {
				return fmt.Errorf("duplicate or invalid Schedule E source member")
			}
			seenFacts[m.FactID] = true
			c.SourceFacts++
			date := m.ExpenditureDate
			if q.SpendingDate == "dissemination" {
				date = m.DisseminationDate
			}
			selection := receiptDateSelection(date, q.Window)
			b := WindowSpendingBucket{EffectiveState: m.EffectiveState, AmountReason: m.AmountReason, RouteReasons: m.RouteReasons,
				ResolutionState: m.ResolutionState, Stance: m.Stance, Selection: selection, Projectable: m.Link != nil}
			key := valueID(b)
			bucket := buckets[key]
			if bucket == nil {
				b.Amount = "0"
				bucket = &b
				buckets[key] = bucket
			}
			bucket.Rows++
			if m.Amount != nil {
				amount, ok := new(big.Int).SetString(*m.Amount, 10)
				if !ok || amount.String() != *m.Amount {
					return fmt.Errorf("noncanonical Schedule E member amount")
				}
				sum, _ := new(big.Int).SetString(bucket.Amount, 10)
				bucket.Amount = sum.Add(sum, amount).String()
				bucket.KnownAmountRows++
			}
			if m.Link == nil {
				return nil
			}
			link := *m.Link
			if m.Parent == nil || link.Key != m.FactID || m.Amount == nil || !validDigest(m.DecisionID) ||
				link.Family != m.Parent.Family+"_observation" || link.From != m.Parent.From || link.To != m.Parent.To ||
				!validDigest(m.Parent.Key) || graphread.Kind(link.From) != "committee" || graphread.Kind(link.To) != "candidate" {
				return fmt.Errorf("invalid Schedule E source-member topology")
			}
			if link.Family != q.Ending+"_observation" || link.To != q.Target || selection != "included" && selection != "undated_included" {
				return nil
			}
			if _, exists := routes[link.ID()]; exists {
				return fmt.Errorf("duplicate spending source route")
			}
			routes[link.ID()] = connectionRoute{windowRoute: windowRoute{i, date}, original: link, spending: &m}
			return topology.add(link)
		})
		if err != nil {
			return nil, nil, err
		}
		keys := make([]string, 0, len(buckets))
		for k := range buckets {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var rows uint64
		for _, k := range keys {
			c.Buckets = append(c.Buckets, *buckets[k])
			rows += buckets[k].Rows
		}
		if rows != c.SourceFacts {
			return nil, nil, fmt.Errorf("Schedule E window coverage is not conserved")
		}
		coverage = append(coverage, c)
	}
	return topology, coverage, topology.order()
}

func (r *WindowReader) spendingEvidence(ctx context.Context, paths [][]graphread.Link, routes map[string]connectionRoute) (map[string]graphread.Item, error) {
	want := make([][]ie.DatedMember, len(r.partitions))
	seen := map[string]bool{}
	for _, path := range paths {
		for _, link := range path {
			route, ok := routes[link.ID()]
			if !ok {
				return nil, fmt.Errorf("selected connection route absent")
			}
			if route.spending != nil && !seen[link.ID()] {
				want[route.partition] = append(want[route.partition], *route.spending)
				seen[link.ID()] = true
			}
		}
	}
	out := map[string]graphread.Item{}
	for i, members := range want {
		if len(members) == 0 {
			continue
		}
		items, err := r.partitions[i].outside.DatedMemberEvidence(ctx, members)
		if err != nil {
			return nil, err
		}
		if len(items) != len(members) {
			return nil, fmt.Errorf("spending source readback count differs")
		}
		for _, m := range members {
			item, ok := items[m.FactID]
			if !ok || item.Key != m.FactID {
				return nil, fmt.Errorf("spending source readback identity differs")
			}
			out[connectionLinkID(r.partitions[i].publication.GenerationID, *m.Link)] = item
		}
	}
	return out, nil
}
