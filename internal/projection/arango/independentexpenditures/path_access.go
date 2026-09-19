package independentexpenditures

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	"strings"
)

func (r *ResolvedReader) CandidateLinks(family string) ([]graphread.Link, error) {
	stance := "S"
	if family == "independent_opposition" {
		stance = "O"
	} else if family != "independent_support" {
		return nil, fmt.Errorf("unsupported outside ending")
	}
	out := []graphread.Link{}
	for _, e := range r.m.Edges {
		if e.SupportOppose == stance {
			out = append(out, graphread.Link{Family: family, Key: e.ResultID, From: strings.TrimPrefix(e.From, entitiesCollection+"/committee_"), To: strings.TrimPrefix(e.To, entitiesCollection+"/candidate_")})
		}
	}
	return out, nil
}

func (r *ResolvedReader) PathEvidence(ctx context.Context, family, key string) (graphread.Item, error) {
	for _, e := range r.m.Edges {
		if e.ResultID == key {
			if !(family == "independent_support" && e.SupportOppose == "S" || family == "independent_opposition" && e.SupportOppose == "O") {
				return graphread.Item{}, fmt.Errorf("outside stance differs")
			}
			raw, err := r.readDocument(ctx, edgesCollection, e.Key, e)
			if err != nil {
				return graphread.Item{}, err
			}
			evidence, _ := json.Marshal(struct {
				Result string          `json:"result_id"`
				Inputs InputReferences `json:"inputs"`
			}{e.ResultID, r.m.Inputs})
			return graphread.Item{Key: key, Document: raw, EvidenceKind: "verified_resolved_calculation_group", Evidence: evidence}, nil
		}
	}
	return graphread.Item{}, fmt.Errorf("outside result absent from pinned projection")
}
