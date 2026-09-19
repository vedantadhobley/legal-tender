package flowevidence

import (
	"context"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

// DatedLink is a value view of a verified observation, not a payment or a
// relationship-validity interval. Date is the reported calendar day, if known.
type DatedLink struct {
	Link      graphread.Link
	FactSetID string
	Ordinal   uint64
	Date      *int32
}

// VisitDatedLinks adds source identity/time to the existing topology accessor.
// Date is copied so callbacks cannot change the verified reader's observation.
func (r *Reader) VisitDatedLinks(ctx context.Context, side Ledger, visit func(DatedLink) error) error {
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
		v := DatedLink{Link: graphread.Link{Family: family, Key: e.Key, From: e.Sender, To: e.Recipient}, FactSetID: e.FactSetID, Ordinal: e.Ordinal}
		if e.Date != nil {
			d := *e.Date
			v.Date = &d
		}
		if err := visit(v); err != nil {
			return err
		}
	}
	return nil
}
