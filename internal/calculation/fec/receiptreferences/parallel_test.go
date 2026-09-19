package receiptreferences

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestParallelScanBufferOwnershipAndFailure(t *testing.T) {
	for _, workers := range []int{1, 4, 8} {
		seen := map[int64]bool{}
		read := func(ctx context.Context, i int, batch func([]Row) error) error {
			rows := make([]Row, 7)
			for page := 0; page < 5; page++ {
				for j := range rows {
					rows[j].Ordinal = int64(i*35 + page*7 + j + 1)
				}
				if err := batch(rows); err != nil {
					return err
				}
				for j := range rows {
					rows[j].Ordinal = -1
				}
			}
			return nil
		}
		visit := func(r Row) error {
			if r.Ordinal < 1 || seen[r.Ordinal] {
				return fmt.Errorf("corrupt reused row")
			}
			seen[r.Ordinal] = true
			return nil
		}
		if err := parallelScan(context.Background(), 20, workers, read, visit, func(string) {}); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 700 {
			t.Fatal("lost batch")
		}
		failure := errors.New("consumer stopped")
		if err := parallelScan(context.Background(), 20, workers, read, func(Row) error { return failure }, func(string) {}); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
}
