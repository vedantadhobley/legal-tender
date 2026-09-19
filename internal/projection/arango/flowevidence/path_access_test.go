package flowevidence

import (
	"context"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	"testing"
)

func TestPathTopologyKeepsLedgersParallelAndSignedOccurrences(t *testing.T) {
	r := readerFixture(t)
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		links := []graphread.Link{}
		err := r.VisitLinks(context.Background(), side, func(l graphread.Link) error { links = append(links, l); return nil })
		if err != nil || len(links) != len(r.m.edges(side)) {
			t.Fatal(err)
		}
		for i, l := range links {
			if l.Key != r.m.edges(side)[i].Key || l.From != "C00000001" || l.To != "C00000002" {
				t.Fatal(l)
			}
			if (side == ScheduleA) != (l.Family == "receiver_reported_committee_observation") {
				t.Fatal("mixed ledgers")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r.VisitLinks(ctx, ScheduleA, func(graphread.Link) error { t.Fatal("visited canceled topology"); return nil }) == nil {
		t.Fatal("ignored cancellation")
	}
	if r.VisitLinks(context.Background(), "both", func(graphread.Link) error { return nil }) == nil {
		t.Fatal("combined ledger")
	}
}
