package receiptparticipants

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Scan verifies every compact shard and the complete publication census.
// Each worker calls visit serially; different workers may call it concurrently.
// Rows are borrowed until visit returns. Callers must discard all accumulated
// output on error: no partial scan is an accepted population.
func (i *Inspector) Scan(ctx context.Context, workers int, visit func(int, Row) error, progress func(string)) (Census, error) {
	if workers < 1 || workers > 8 || visit == nil || i.participant.State != "complete_cycle_participant_index" {
		return Census{}, fmt.Errorf("complete participant publication, callback and 1..8 workers required")
	}
	return scanFiles(ctx, i.participant, workers, func(ctx context.Context, f File, worker int) (Census, error) {
		return ReadShard(ctx, i.dir, f, func(r Row) error { return visit(worker, r) })
	}, progress)
}

func scanFiles(ctx context.Context, publication Result, workers int, read func(context.Context, File, int) (Census, error), progress func(string)) (Census, error) {
	if err := ctx.Err(); err != nil {
		return Census{}, err
	}
	if progress == nil {
		progress = func(string) {}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		census Census
		err    error
	}
	results := make(chan result, workers)
	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				n := int(next.Add(1) - 1)
				if n >= len(publication.Files) {
					return
				}
				c, err := read(ctx, publication.Files[n], worker)
				results <- result{c, err}
				if err != nil {
					cancel()
					return
				}
			}
		})
	}
	go func() { wg.Wait(); close(results) }()
	complete := 0
	census := newCensus()
	var first error
	for r := range results {
		if r.err != nil {
			if first == nil || errors.Is(first, context.Canceled) {
				first = r.err
			}
			cancel()
			continue
		}
		census.merge(r.census)
		complete++
		if complete%16 == 0 || complete == len(publication.Files) {
			progress(fmt.Sprintf("verified participant profile scan: %d/%d shards, %d/%d rows", complete, len(publication.Files), census.Rows, publication.SourceRows))
		}
	}
	if first != nil {
		return Census{}, first
	}
	if err := ctx.Err(); err != nil {
		return Census{}, err
	}
	if complete != len(publication.Files) || !equalCensus(census, publication.Census) || census.Rows != publication.SourceRows {
		return Census{}, fmt.Errorf("complete participant scan census differs")
	}
	return census, nil
}
