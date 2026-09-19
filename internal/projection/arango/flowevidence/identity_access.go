package flowevidence

import (
	"context"
	"encoding/json"
)

// VisitCommitteeDocuments exposes full reported master attributes from the
// already source/graph-verified snapshot, without one network request per ID.
func (r *Reader) VisitCommitteeDocuments(ctx context.Context, visit func(string, json.RawMessage) error) error {
	for _, e := range r.m.entities {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := visit(e.CommitteeID, b); err != nil {
			return err
		}
	}
	return nil
}

// CommitteeIdentity is a value copy from the fully source/graph-verified model.
// A missing same-cycle master does not prove the reported committee is invalid
// or that historical registration evidence is absent.
type CommitteeIdentity struct {
	ID, State, FactSetID, FactID string
}

func (r *Reader) CommitteeIdentities() []CommitteeIdentity {
	out := make([]CommitteeIdentity, 0, len(r.m.entities))
	for _, e := range r.m.entities {
		v := CommitteeIdentity{ID: e.CommitteeID, State: e.IdentityState, FactSetID: e.MasterFactSetID}
		if e.MasterFactID != nil {
			v.FactID = *e.MasterFactID
		}
		out = append(out, v)
	}
	return out
}
