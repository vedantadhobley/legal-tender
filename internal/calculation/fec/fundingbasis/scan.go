package fundingbasis

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const maxBuckets = 250000

func openShard(root string, shard occ.ScheduleAColumnarShard) (*os.File, *parquet.File, error) {
	path, err := artifact.Resolve(root, shard.StorageKey)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	p, err := parquet.OpenFile(f, int64(shard.Bytes))
	if err == nil {
		schema, e := scheduleaparquet.NewSchema()
		err = e
		if err == nil && (p.Schema().String() != schema.Parquet().String() || p.NumRows() != int64(shard.Facts)) {
			err = fmt.Errorf("Schedule A shard %d schema/count mismatch", shard.Index)
		}
	}
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, p, nil
}

func validRow(r receiptRow, ordinal uint64, cycle int64) error {
	if r.Ordinal <= 0 || uint64(r.Ordinal) != ordinal || r.Cycle != cycle || r.Normalization != "valid" {
		return fmt.Errorf("invalid Schedule A row identity/normalization at ordinal %d", ordinal)
	}
	if !(r.AmountState == "reported_value" && r.Amount != nil || r.AmountState == "source_null" && r.Amount == nil) {
		return fmt.Errorf("inconsistent Schedule A amount state at ordinal %d", ordinal)
	}
	return nil
}

func scanShard(ctx context.Context, root, cycle string, shard occ.ScheduleAColumnarShard) (map[Key]*Bucket, error) {
	f, p, err := openShard(root, shard)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[receiptRow](p)
	defer r.Close()
	cycleNumber, err := strconv.ParseInt(cycle, 10, 64)
	if err != nil {
		return nil, err
	}
	buckets := make(map[Key]*Bucket)
	buffer := make([]receiptRow, 8192)
	var count uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := r.Read(buffer)
		for _, row := range buffer[:n] {
			if err := validRow(row, shard.FirstSourceRowOrdinal+count, cycleNumber); err != nil {
				return nil, err
			}
			count++
			key := classify(row)
			b := buckets[key]
			if b == nil {
				if len(buckets) >= maxBuckets {
					return nil, fmt.Errorf("receipt inventory bucket limit exceeded")
				}
				key.Recipient.Value = strings.Clone(key.Recipient.Value)
				b = &Bucket{Key: key, First: uint64(row.Ordinal)}
				buckets[key] = b
			}
			b.Last = uint64(row.Ordinal)
			if err := b.Measures.observe(row); err != nil {
				return nil, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
	if count != shard.Facts {
		return nil, fmt.Errorf("Schedule A shard row count mismatch")
	}
	return buckets, nil
}

func scan(ctx context.Context, root string, m occ.ScheduleAColumnarManifest, workers int, progress func(string)) ([]Bucket, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type output struct {
		index   int
		buckets map[Key]*Bucket
		err     error
	}
	jobs := make(chan int)
	results := make(chan output, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				b, err := scanShard(ctx, root, m.Cycle, m.Shards[i])
				select {
				case results <- output{i, b, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for i := range m.Shards {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	combined := make(map[Key]*Bucket)
	completed := 0
	var firstError error
	for out := range results {
		if firstError != nil {
			continue
		}
		if out.err != nil {
			firstError = out.err
			cancel()
			continue
		}
		for key, b := range out.buckets {
			target := combined[key]
			if target == nil {
				if len(combined) >= maxBuckets {
					firstError = fmt.Errorf("receipt inventory bucket limit exceeded")
					break
				}
				target = &Bucket{Key: key, First: b.First, Last: b.Last, Shards: make([]byte, (len(m.Shards)+7)/8)}
				combined[key] = target
			}
			if b.First < target.First {
				target.First = b.First
			}
			if b.Last > target.Last {
				target.Last = b.Last
			}
			target.Shards[out.index/8] |= 1 << uint(out.index%8)
			if err := target.Measures.merge(b.Measures); err != nil {
				firstError = err
				break
			}
		}
		if firstError != nil {
			cancel()
			continue
		}
		completed++
		if progress != nil && (completed%16 == 0 || completed == len(m.Shards)) {
			progress(fmt.Sprintf("inventoried %d/%d Schedule A shards", completed, len(m.Shards)))
		}
	}
	if firstError != nil {
		return nil, firstError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if completed != len(m.Shards) {
		return nil, fmt.Errorf("incomplete Schedule A scan")
	}
	result := make([]Bucket, 0, len(combined))
	for _, b := range combined {
		result = append(result, *b)
	}
	return result, nil
}
