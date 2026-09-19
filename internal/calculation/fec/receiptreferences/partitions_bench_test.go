package receiptreferences

import (
	"context"
	"fmt"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// This exercises both complete input passes, all sort/merge/join stages, final
// ordered assembly and readback. It is synthetic, not a real-corpus latency claim.
func BenchmarkFullReferencePipeline(b *testing.B) {
	const count = 1000000
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			for attempt := 0; attempt < b.N; attempt++ {
				space, err := xsort.NewWorkspace(filepath.Join(b.TempDir(), "work"), 2<<30)
				if err != nil {
					b.Fatal(err)
				}
				started := time.Now()
				result, err := calculateReferences(context.Background(), space, Options{Workers: workers, RunRows: 100000, FanIn: 8, FilterBytes: 8 << 20}, benchmarkSource(count), func(string) {})
				if err != nil {
					b.Fatal(err)
				}
				if result.SourceRows != count || result.ReferenceRows != count || result.Decisions.Rows != count {
					b.Fatal("benchmark conservation")
				}
				b.ReportMetric(count/time.Since(started).Seconds(), "rows/s")
				peak, _ := space.Stats()
				b.ReportMetric(float64(peak), "peak-disk-B")
				var usage syscall.Rusage
				if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(usage.Maxrss*1024), "peak-rss-B")
				b.Logf("decisions=%s incidences=%s neighbors=%s stages=%v", result.Decisions.ValuesSHA256, result.ExactIncidences.ValuesSHA256, result.Neighbors.ValuesSHA256, result.StageMS)
			}
		})
	}
}

func benchmarkSource(count int) batchSource {
	return func(ctx context.Context, visit func([]Row) error) error {
		committee, schedule, line, normalized := "C00000001", "SA", "11AI", "valid"
		transactions := make([]string, 100)
		for i := range transactions {
			transactions[i] = fmt.Sprintf("t%d", i)
		}
		rows := make([]Row, 8192)
		for start := 0; start < count; start += len(rows) {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch := rows[:min(len(rows), count-start)]
			for i := range batch {
				n := start + i
				file := fmt.Sprint(n/100 + 1)
				batch[i] = Row{Ordinal: int64(n + 1), Cycle: 2024, Normalization: normalized, Recipient: &committee, File: &file, Transaction: &transactions[n%100], BackReference: &transactions[(n+1)%100], BackSchedule: &schedule, Schedule: &schedule, Line: &line}
			}
			if err := visit(batch); err != nil {
				return err
			}
			clear(batch)
		}
		return nil
	}
}
