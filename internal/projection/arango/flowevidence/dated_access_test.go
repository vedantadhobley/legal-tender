package flowevidence

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

func TestDatedLinksKeepExactTopologySourceAndOwnedDates(t *testing.T) {
	r := readerFixture(t)
	day := int32(19000)
	r.m.a[0].Date = &day
	for _, ledger := range []Ledger{ScheduleA, ScheduleB} {
		var old []graphread.Link
		if err := r.VisitLinks(context.Background(), ledger, func(l graphread.Link) error { old = append(old, l); return nil }); err != nil {
			t.Fatal(err)
		}
		var links []graphread.Link
		i := 0
		if err := r.VisitDatedLinks(context.Background(), ledger, func(v DatedLink) error {
			e := r.m.edges(ledger)[i]
			if v.FactSetID != e.FactSetID || v.Ordinal != e.Ordinal || !reflect.DeepEqual(v.Date, e.Date) {
				t.Fatal("lost source locator or date")
			}
			if v.Date != nil {
				*v.Date++
				if *e.Date != 19000 {
					t.Fatal("callback mutated verified source")
				}
			}
			links = append(links, v.Link)
			i++
			return nil
		}); err != nil || !reflect.DeepEqual(old, links) {
			t.Fatal("changed topology", err)
		}
	}
	if err := r.VisitDatedLinks(context.Background(), "both", func(DatedLink) error { t.Fatal("visited mixed ledger"); return nil }); err == nil {
		t.Fatal("mixed ledger accepted")
	}
	want := errors.New("consumer failed")
	if err := r.VisitDatedLinks(context.Background(), ScheduleA, func(DatedLink) error { return want }); !errors.Is(err, want) {
		t.Fatal("lost callback error", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.VisitDatedLinks(ctx, ScheduleA, func(DatedLink) error { t.Fatal("visited canceled reader"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
