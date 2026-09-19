package candidateupstream

import (
	"context"
	"fmt"
	"sort"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

// RunWithWitnesses uses the same load and analysis as Run. It adds only the
// exact source edges needed to expand each node's shortest-hop witness, not a
// second per-candidate copy of the full observation ledger.
func RunWithWitnesses(ctx context.Context, o Options) (Result, []flow.Observation, error) {
	return runWithReferenceWitnesses(ctx, o, nil)
}

// RunWithReferenceWitnesses retains the receipt publication's exact reference
// facts under a freshly verified content-equivalence capability. The ordinary
// Run/RunWithWitnesses source-archive contract stays unchanged.
func RunWithReferenceWitnesses(ctx context.Context, o Options, references *flow.ReferenceContext) (Result, []flow.Observation, error) {
	if references == nil {
		return Result{}, nil, fmt.Errorf("verified reference context required")
	}
	return runWithReferenceWitnesses(ctx, o, references)
}

func runWithReferenceWitnesses(ctx context.Context, o Options, references *flow.ReferenceContext) (Result, []flow.Observation, error) {
	if err := validateSelection(o.Cycle, o.Candidate); err != nil {
		return Result{}, nil, err
	}
	if o.StorageRoot == "" || o.Bundle == "" || o.Linkages == "" {
		return Result{}, nil, fmt.Errorf("storage root, observation bundle, and linkage manifest required")
	}
	if o.Progress != nil {
		o.Progress("verifying exact graph inputs and tracing candidate ancestry")
	}
	l, err := loadContext(ctx, o, references)
	if err != nil {
		return Result{}, nil, err
	}
	r, err := analyze(ctx, o.Cycle, o.Candidate, l.inputs, l.linkages, l.masters, l.observations)
	if err != nil {
		return Result{}, nil, err
	}
	w, err := connectionWitnesses(ctx, r, l.observations)
	return r, w, err
}

func connectionWitnesses(ctx context.Context, r Result, observations []flow.Observation) ([]flow.Observation, error) {
	nodes := make(map[string]Node, len(r.Nodes))
	wanted := map[uint64]Node{}
	for _, n := range r.Nodes {
		nodes[n.CommitteeID] = n
		if n.WitnessOrdinal != nil {
			wanted[*n.WitnessOrdinal] = n
		} else if !n.Authorized {
			return nil, fmt.Errorf("non-root committee lacks connectivity witness")
		}
	}
	out := make([]flow.Observation, 0, len(wanted))
	for _, o := range observations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, ok := wanted[o.Ordinal]
		if !ok {
			continue
		}
		next, ok := nodes[o.Recipient]
		if !ok || o.Sender != n.CommitteeID || next.Hops != n.Hops-1 {
			return nil, fmt.Errorf("connectivity witness has inconsistent endpoints or distance")
		}
		out = append(out, o)
		delete(wanted, o.Ordinal)
	}
	if len(wanted) != 0 {
		return nil, fmt.Errorf("connectivity witness missing from source evidence")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ordinal < out[j].Ordinal })
	return out, ctx.Err()
}
