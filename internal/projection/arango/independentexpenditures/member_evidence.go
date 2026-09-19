package independentexpenditures

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

type MemberEvidence struct {
	Source   occ.ScheduleEFact   `json:"source_fact"`
	Decision resolution.Decision `json:"candidate_resolution_decision"`
	Parent   graphread.Item      `json:"parent_aggregate"`
	Inputs   InputReferences     `json:"inputs"`
}

// DatedMemberEvidence scans once for a bounded batch, not once per path or
// member. Full facts are retained only for the requested returned witnesses.
func (r *ResolvedReader) DatedMemberEvidence(ctx context.Context, want []DatedMember) (map[string]graphread.Item, error) {
	if len(want) < 1 || len(want) > graphread.MaxLimit {
		return nil, fmt.Errorf("1..%d spending witnesses required", graphread.MaxLimit)
	}
	requested := map[string]DatedMember{}
	for _, m := range want {
		if m.Link == nil || m.Parent == nil || requested[m.FactID].FactID != "" {
			return nil, fmt.Errorf("projectable unique spending witnesses required")
		}
		requested[m.FactID] = m
	}
	type selected struct {
		member   DatedMember
		source   occ.ScheduleEFact
		decision resolution.Decision
	}
	found := map[string]selected{}
	err := r.visitMembers(ctx, func(m DatedMember, f occ.ScheduleEFact, d *resolution.Decision) error {
		w, ok := requested[m.FactID]
		if !ok {
			return nil
		}
		if d == nil || !reflect.DeepEqual(m, w) {
			return fmt.Errorf("selected spending member changed during readback")
		}
		found[m.FactID] = selected{m, f, *d}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(found) != len(requested) {
		return nil, fmt.Errorf("selected spending source member absent")
	}
	out, parents := map[string]graphread.Item{}, map[string]graphread.Item{}
	for _, w := range want {
		v := found[w.FactID]
		parent, exists := parents[w.Parent.ID()]
		if !exists {
			parent, err = r.PathEvidence(ctx, w.Parent.Family, w.Parent.Key)
			if err != nil {
				return nil, err
			}
			parents[w.Parent.ID()] = parent
		}
		doc, err := json.Marshal(v.member)
		if err != nil {
			return nil, err
		}
		evidence, err := json.Marshal(MemberEvidence{v.source, v.decision, parent, r.m.Inputs})
		if err != nil {
			return nil, err
		}
		out[w.FactID] = graphread.Item{Key: w.FactID, Document: doc, EvidenceKind: "verified_schedule_e_source_member_with_resolved_aggregate_parent", Evidence: evidence}
	}
	return out, r.VerifyCompletion(ctx)
}
