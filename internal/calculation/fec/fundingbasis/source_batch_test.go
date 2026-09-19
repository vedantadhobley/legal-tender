package fundingbasis

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestBatchSourceEqualsIndependentLookups(t *testing.T) {
	r, _ := inventoryFixture(t)
	ids := []uint64{24, 1, 15, 8, 2}
	got, err := ReadSourceOccurrences(context.Background(), r.root, r.manifest, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) || ids[0] != 24 {
		t.Fatal("count or input changed")
	}
	for i, row := range got {
		want, err := ReadSourceOccurrence(context.Background(), r.root, r.manifest, row.Ordinal)
		if err != nil || !reflect.DeepEqual(want, row) {
			t.Fatal("batch/source disagreement", row.Ordinal, err)
		}
		if i > 0 && got[i-1].Ordinal >= row.Ordinal {
			t.Fatal("source order")
		}
	}
}
func TestBatchSourceFailsClosed(t *testing.T) {
	r, _ := inventoryFixture(t)
	for _, ids := range [][]uint64{nil, {0}, {25}, {1, 1}, make([]uint64, 4097)} {
		if _, err := ReadSourceOccurrences(context.Background(), r.root, r.manifest, ids); err == nil {
			t.Fatal("invalid batch accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadSourceOccurrences(ctx, r.root, r.manifest, []uint64{1}); err == nil {
		t.Fatal("cancellation ignored")
	}
	r.manifest.Shards[0].SHA256 = strings.Repeat("a", 64)
	if _, err := ReadSourceOccurrences(context.Background(), r.root, r.manifest, []uint64{1}); err == nil {
		t.Fatal("corruption accepted")
	}
}
