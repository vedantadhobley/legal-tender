package receiptgraph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func ptr[T any](v T) *T { return &v }
func TestSourceAppearancesAndSeparateConduit(t *testing.T) {
	fact := strings.Repeat("a", 64)
	for _, amount := range []*int64{nil, ptr(int64(0)), ptr(int64(-103)), ptr(int64(9007199254740993))} {
		row := p.Row{Ordinal: 1, Recipient: ptr("C00000001"), Amount: amount, Memo: true, SourceRoute: "unresolved", Contributor: ptr(""), CleanContributor: nil}
		a, r, v, e := project(fact, row, nil)
		if e != nil || r == nil || v != nil || a.IdentityResolved || r.FinancialEligibility || !reflect.DeepEqual(a.Row, row) {
			t.Fatalf("source appearance changed: %v", e)
		}
		b, _ := json.Marshal(r)
		var got receipt
		if e = json.Unmarshal(b, &got); e != nil || !reflect.DeepEqual(got.Amount, amount) {
			t.Fatalf("exact cents lost: %v", e)
		}
		next := row
		next.Ordinal = 2
		a2, _, _, _ := project(fact, next, nil)
		if a.Key == a2.Key {
			t.Fatal("identical descriptions merged physical occurrences")
		}
	}
	row := p.Row{Ordinal: 4, Recipient: ptr("C00000001"), Amount: ptr(int64(-100)), ReceiptType: ptr("15E")}
	d := c.Decision{Ordinal: 4, Related: 8, State: "reported_earmark_memo_association", ConduitID: ptr("C00000002"), AmountComparison: "different_reported_amount"}
	a, r, v, e := project(fact, row, &d)
	if e != nil || r == nil || v == nil || v.To == r.To || v.AdditionalAmount != "0" || v.FinancialEligibility || a.ConduitState != d.State {
		t.Fatalf("distinct association boundary: %v", e)
	}
	d.ConduitID = nil
	d.State = "shared_related_record_unresolved"
	a, r, v, e = project(fact, row, &d)
	if e != nil || r == nil || v != nil || a.ConduitState != d.State {
		t.Fatal("unresolved evidence dropped or promoted")
	}
	for _, id := range []*string{nil, ptr(""), ptr(" C00000001"), ptr("C00000001/")} {
		row.Recipient = id
		a, r, _, e = project(fact, row, nil)
		if e != nil || r != nil || a.RecipientState != "unresolved_reported_recipient" {
			t.Fatal("invalid recipient silently normalized")
		}
	}
	d.Ordinal = 9
	if _, _, _, e = project(fact, row, &d); e == nil {
		t.Fatal("wrong occurrence accepted")
	}
}
func TestBatchesOwnValuesAndLayoutInvariant(t *testing.T) {
	var baseline map[string]string
	for _, size := range []int{1, 7, 1000} {
		collected := map[string][]json.RawMessage{}
		b := newBatches(size, func(v batch) error {
			dec := json.NewDecoder(strings.NewReader(string(v.data)))
			for _, key := range v.keys {
				var raw json.RawMessage
				if e := dec.Decode(&raw); e != nil {
					return e
				}
				var x struct {
					Key string `json:"_key"`
				}
				_ = json.Unmarshal(raw, &x)
				if x.Key != key {
					return errors.New("key mismatch")
				}
				collected[v.collection] = append(collected[v.collection], raw)
			}
			return nil
		})
		value := ptr("original")
		for i := 0; i < 99; i++ {
			v := map[string]any{"_key": fmt.Sprint(i), "value": value}
			if e := b.add(appearances, fmt.Sprint(i), v); e != nil {
				t.Fatal(e)
			}
		}
		*value = "changed"
		if e := b.finish(); e != nil {
			t.Fatal(e)
		}
		if len(collected[appearances]) != 99 || strings.Contains(string(collected[appearances][98]), "changed") {
			t.Fatal("borrowed data retained")
		}
		if baseline == nil {
			baseline = b.digests()
		} else if !reflect.DeepEqual(baseline, b.digests()) {
			t.Fatal("batch size changed canonical documents")
		}
	}
}
func TestWorkersConserveAndCancel(t *testing.T) {
	for _, workers := range []int{1, 4, 8} {
		var mu sync.Mutex
		seen := map[int]bool{}
		e := withWorkers(context.Background(), workers, func(ctx context.Context, b batch) error {
			mu.Lock()
			defer mu.Unlock()
			var n int
			_, _ = fmt.Sscan(string(b.data), &n)
			if seen[n] {
				return errors.New("duplicate")
			}
			seen[n] = true
			return nil
		}, func(send func(batch) error) error {
			for i := 0; i < 1000; i++ {
				if e := send(batch{data: []byte(fmt.Sprint(i))}); e != nil {
					return e
				}
			}
			return nil
		})
		if e != nil || len(seen) != 1000 {
			t.Fatalf("workers lost rows: %v", e)
		}
	}
	sentinel := errors.New("readback failure")
	e := withWorkers(context.Background(), 4, func(context.Context, batch) error { return sentinel }, func(send func(batch) error) error {
		for i := 0; i < 10000; i++ {
			if e := send(batch{}); e != nil {
				return e
			}
		}
		return nil
	})
	if !errors.Is(e, sentinel) {
		t.Fatalf("worker failure hidden: %v", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = withWorkers(ctx, 4, func(context.Context, batch) error { return nil }, func(send func(batch) error) error { return send(batch{}) }); !errors.Is(e, context.Canceled) {
		t.Fatal("cancellation hidden")
	}
}
func TestBoundsAndPublisherLock(t *testing.T) {
	if _, e := Run(context.Background(), Options{}); e == nil {
		t.Fatal("unbounded invocation accepted")
	}
	dir := t.TempDir()
	release, e := lock(dir, "test")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = lock(dir, "test"); e == nil {
		t.Fatal("concurrent writer allowed")
	}
	release()
	release, e = lock(dir, "test")
	if e != nil {
		t.Fatal(e)
	}
	release()
}
