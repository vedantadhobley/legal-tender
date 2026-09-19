package flowevidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

func (r *Reader) Facet(ctx context.Context, id string) (graphread.Facet, error) {
	if graphread.Kind(id) == "candidate" {
		return graphread.Facet{State: "not_applicable"}, nil
	}
	raw, err := r.Entity(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return graphread.Facet{State: "not_present_in_projection"}, nil
	}
	if err != nil {
		return graphread.Facet{}, err
	}
	raw, err = graphread.Canonical(raw)
	return graphread.Facet{State: "present", Document: raw}, err
}

func (r *Reader) NeighborhoodPage(ctx context.Context, kind, id, after string, limit int) (graphread.Page, error) {
	if err := graphread.ValidPage(id, after, limit); err != nil {
		return graphread.Page{}, err
	}
	if kind != "receiver_reported_committee_observation" && kind != "sender_reported_committee_observation" && kind != "reconciliation_candidate" {
		return graphread.Page{}, fmt.Errorf("unsupported committee-flow family")
	}
	if graphread.Kind(id) != "committee" {
		return graphread.Empty("not_applicable"), nil
	}
	if _, ok := r.expectedEntity(id); !ok {
		return graphread.Empty("not_present_in_projection"), nil
	}
	page := graphread.Empty("available")
	if kind == "reconciliation_candidate" {
		selected := []component{}
		for _, c := range r.m.components {
			if c.Key > after && (c.Sender == id || c.Recipient == id) {
				selected = append(selected, c)
			}
		}
		sort.Slice(selected, func(i, j int) bool { return selected[i].Key < selected[j].Key })
		page.HasMore = len(selected) > limit
		for index, c := range selected[:min(len(selected), limit+1)] {
			raw, err := r.Component(ctx, c.Key)
			if err != nil {
				return graphread.Page{}, err
			}
			raw, err = graphread.Canonical(raw)
			if err != nil {
				return graphread.Page{}, err
			}
			if index >= limit {
				continue
			}
			evidence, _ := json.Marshal(struct {
				Calculation string `json:"calculation_set_id"`
				Component   string `json:"component_id"`
				Inputs      any    `json:"inputs"`
			}{c.CalculationID, c.Key, r.View().Inputs})
			page.Items = append(page.Items, graphread.Item{Key: c.Key, Document: raw, EvidenceKind: "verified_reconciliation_component_summary", Evidence: evidence})
			page.NextAfter = c.Key
		}
		return page, nil
	}
	side := ScheduleA
	if kind == "sender_reported_committee_observation" {
		side = ScheduleB
	}
	p, err := r.Query(ctx, ReadQuery{Kind: "observations", Ledger: side, Committee: id, Direction: "any", Limit: limit, After: after})
	if err != nil {
		return graphread.Page{}, err
	}
	page.HasMore, page.NextAfter = p.HasMore, p.Last
	for _, raw := range p.Items {
		var edge observation
		if err := json.Unmarshal(raw, &edge); err != nil {
			return graphread.Page{}, err
		}
		source, err := r.Source(ctx, side, edge.Key)
		if err != nil {
			return graphread.Page{}, err
		}
		raw, err = graphread.Canonical(raw)
		if err != nil {
			return graphread.Page{}, err
		}
		evidence, _ := json.Marshal(source)
		page.Items = append(page.Items, graphread.Item{Key: edge.Key, Document: raw, EvidenceKind: "complete_reported_source_occurrence", Evidence: evidence})
	}
	return page, nil
}

// MissingMasterIDs supports deterministic acceptance cases, not an identity rule.
func (r *Reader) MissingMasterIDs() []string {
	out := []string{}
	for _, e := range r.m.entities {
		if e.Master == nil {
			out = append(out, e.CommitteeID)
		}
	}
	sort.Strings(out)
	return out
}
