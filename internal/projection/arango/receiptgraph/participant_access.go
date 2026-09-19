package receiptgraph

import (
	"context"

	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func (r *CycleReader) ScanParticipants(ctx context.Context, workers int, visit func(int, p.Row) error, progress func(string)) (p.Census, error) {
	return r.loaded.inspector.Scan(ctx, workers, visit, progress)
}

// All same-cycle committee masters, not just endpoints in a selected ledger.
// Values are copied; absence means absent in this exact reference publication.
func (r *CycleReader) CommitteeMasterFacts() map[string]string {
	out := map[string]string{}
	for id, e := range r.loaded.masters {
		if e.Kind == "committee" && e.FactID != nil {
			out[id] = *e.FactID
		}
	}
	return out
}
