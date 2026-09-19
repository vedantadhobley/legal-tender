package flowevidence

import (
	"context"
	"encoding/json"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

// VisitLinks exposes the already fully verified selected ledger, without a
// second money model or a new database scan for each traversal expansion.
func (r *Reader) VisitLinks(ctx context.Context, side Ledger, visit func(graphread.Link) error) error {
	if _, err := edgeCollection(side); err != nil {
		return err
	}
	family := "receiver_reported_committee_observation"
	if side == ScheduleB {
		family = "sender_reported_committee_observation"
	}
	for _, e := range r.m.edges(side) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(graphread.Link{Family: family, Key: e.Key, From: e.Sender, To: e.Recipient}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) PathEvidence(ctx context.Context, side Ledger, key string) (graphread.Item, error) {
	source, err := r.Source(ctx, side, key)
	if err != nil {
		return graphread.Item{}, err
	}
	edge, ok := r.expectedEdge(side, key)
	if !ok {
		return graphread.Item{}, ErrNotFound
	}
	document, err := json.Marshal(edge)
	if err != nil {
		return graphread.Item{}, err
	}
	evidence, err := json.Marshal(source)
	if err != nil {
		return graphread.Item{}, err
	}
	return graphread.Item{Key: key, Document: document, EvidenceKind: "complete_reported_source_occurrence", Evidence: evidence}, nil
}
