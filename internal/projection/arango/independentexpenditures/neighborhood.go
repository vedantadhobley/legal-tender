package independentexpenditures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

func (r *ResolvedReader) readDocument(ctx context.Context, collection, key string, want any) (json.RawMessage, error) {
	rows, err := r.c.query(ctx, r.m.Database, "FOR d IN @@collection FILTER d._key == @key LIMIT 2 RETURN UNSET(d, '_id', '_rev')", map[string]any{"@collection": collection, "key": key})
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("missing or duplicate resolved document")
	}
	got, err := graphread.Canonical(rows[0])
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(want)
	if err != nil {
		return nil, err
	}
	expected, err := graphread.Canonical(raw)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(got, expected) {
		return nil, fmt.Errorf("resolved document differs from exact calculation backing")
	}
	return expected, nil
}

func (r *ResolvedReader) Entity(ctx context.Context, id string) (graphread.Facet, error) {
	kind := graphread.Kind(id)
	if kind == "" {
		return graphread.Facet{}, fmt.Errorf("invalid FEC entity")
	}
	key := kind + "_" + id
	for _, e := range r.m.Entities {
		if e.Key == key {
			raw, err := r.readDocument(ctx, entitiesCollection, key, e)
			return graphread.Facet{State: "present", Document: raw}, err
		}
	}
	return graphread.Facet{State: "not_present_in_projection"}, nil
}

func (r *ResolvedReader) Page(ctx context.Context, kind, id, after string, limit int) (graphread.Page, error) {
	if err := graphread.ValidPage(id, after, limit); err != nil {
		return graphread.Page{}, err
	}
	stance := "S"
	if kind == "independent_opposition" {
		stance = "O"
	} else if kind != "independent_support" {
		return graphread.Page{}, fmt.Errorf("unsupported outside-spending family")
	}
	handle := entitiesCollection + "/" + graphread.Kind(id) + "_" + id
	selected := []resolvedExpenditureEdge{}
	for _, e := range r.m.Edges {
		if e.ResultID > after && e.SupportOppose == stance && (e.From == handle || e.To == handle) {
			selected = append(selected, e)
		}
	}
	// Public ordering uses the complete result identity, not its shortened DB key.
	sort.Slice(selected, func(i, j int) bool { return selected[i].ResultID < selected[j].ResultID })
	page := graphread.Empty("available")
	page.HasMore = len(selected) > limit
	for index, e := range selected[:min(len(selected), limit+1)] {
		raw, err := r.readDocument(ctx, edgesCollection, e.Key, e)
		if err != nil {
			return graphread.Page{}, err
		}
		if index >= limit {
			continue
		}
		evidence, _ := json.Marshal(struct {
			Result string          `json:"result_id"`
			Inputs InputReferences `json:"inputs"`
		}{e.ResultID, r.m.Inputs})
		page.Items = append(page.Items, graphread.Item{Key: e.ResultID, Document: raw, EvidenceKind: "verified_resolved_calculation_group", Evidence: evidence})
		page.NextAfter = e.ResultID
	}
	return page, nil
}

func (r *ResolvedReader) EntityIDs(kind string) []string {
	out := []string{}
	for _, e := range r.m.Entities {
		if e.EntityType == kind {
			out = append(out, e.EntityID)
		}
	}
	sort.Strings(out)
	return out
}

func (r *ResolvedReader) StanceCandidateID(stance string) string {
	id := ""
	for _, e := range r.m.Edges {
		if e.SupportOppose == stance {
			v := e.To[len(entitiesCollection+"/candidate_"):]
			if id == "" || v < id {
				id = v
			}
		}
	}
	return id
}
