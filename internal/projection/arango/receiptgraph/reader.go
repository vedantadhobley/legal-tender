package receiptgraph

import (
	"context"
	"fmt"
	"maps"
)

// CycleReader binds a completed publication without running its writer. Opening
// checks source ancestry, schema, metadata and counts, not every receipt field.
type CycleReader struct {
	cl         *client
	completion completion
	raw        []byte
	sha        string
	loaded     loaded
	options    Options
}

type CycleView struct {
	Projection    Reference         `json:"projection"`
	Database      string            `json:"database"`
	Inputs        Inputs            `json:"inputs"`
	SourceRows    uint64            `json:"source_rows"`
	Counts        map[string]uint64 `json:"collection_counts"`
	Unrouted      uint64            `json:"unrouted_receipts"`
	ConduitStates map[string]uint64 `json:"conduit_states"`
	Verification  string            `json:"verification_scope"`
}

func OpenCycleReader(ctx context.Context, o Options, manifest, sha string) (*CycleReader, error) {
	if o.First != 0 || o.Rows != 0 {
		return nil, fmt.Errorf("generation reader cannot select a receipt sample")
	}
	raw, err := manifestBytes(manifest)
	if err != nil {
		return nil, err
	}
	v, err := decodeCycleCompletion(raw, sha)
	if err != nil {
		return nil, err
	}
	l, cl, err := openCompletedCycle(ctx, o, raw, v)
	if err != nil {
		return nil, err
	}
	return &CycleReader{cl: cl, completion: v, raw: raw, sha: sha, loaded: l, options: o}, nil
}

func (r *CycleReader) View() CycleView {
	v := r.completion
	return CycleView{Reference{v.Key, r.sha}, "lt_receipt_cycle_" + v.Inputs.Cycle + "_" + v.Key[:32], v.Inputs, v.Rows, maps.Clone(v.Counts), v.Unrouted, maps.Clone(v.States), "published_full_cycle_completion_source_ancestry_schema_and_live_counts"}
}

func (r *CycleReader) VerifyCompletion(ctx context.Context) error {
	return verifyCycleCompletion(ctx, r.cl, r.raw, r.completion)
}
