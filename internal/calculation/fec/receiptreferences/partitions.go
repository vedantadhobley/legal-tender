package receiptreferences

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

type batchSource func(context.Context, func([]Row) error) error

// Report scope is the partition boundary, not a resolved identity. Every own-key
// lookup, back-reference, duplicate and reverse incidence of a valid report stays
// in one worker. Hash collisions merely put independent reports in the same worker.
func reportPartition(r Row, workers int) int {
	if workers == 1 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(key(r.Recipient, r.File, nil)))
	return int(h.Sum64() % uint64(workers))
}

func calculateReferences(ctx context.Context, space *xsort.Workspace, o Options, source batchSource, log func(string)) (Result, error) {
	if o.Workers < 1 || o.Workers > 8 || o.FilterBytes < o.Workers {
		return Result{}, fmt.Errorf("invalid processing worker/filter budget")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// At most 32 input decoders across concurrent engines, regardless of the
	// requested merge fan-in. Every decoder retains its existing 32 MiB limit.
	workerFanIn := min(o.FanIn, 32/o.Workers)
	engines := make([]*engine, o.Workers)
	stats := make([]PartitionStats, o.Workers)
	for i := range engines {
		workerOptions := o
		workerOptions.FanIn = workerFanIn
		workerOptions.FilterBytes = o.FilterBytes / o.Workers
		if i < o.FilterBytes%o.Workers {
			workerOptions.FilterBytes++
		}
		var err error
		engines[i], err = newEngine(ctx, space, workerOptions)
		if err != nil {
			return Result{}, err
		}
		stats[i] = PartitionStats{Index: i, StageMS: map[string]int64{}}
	}
	stageMS := map[string]int64{}
	for pass, name := range []string{"requests", "memberships"} {
		started := time.Now()
		log(fmt.Sprintf("%s: complete source scan into %d report workers", name, o.Workers))
		visitors := make([]func(Row) error, len(engines))
		for i, e := range engines {
			visitors[i] = e.request
			if pass == 1 {
				visitors[i] = e.membership
			}
		}
		if err := dispatchScan(ctx, source, visitors); err != nil {
			return Result{}, err
		}
		stageMS[name] = time.Since(started).Milliseconds()
		log(fmt.Sprintf("%s scan complete in %dms", name, stageMS[name]))
	}
	var wg sync.WaitGroup
	var errorMu sync.Mutex
	var first error
	for i, e := range engines {
		stats[i].SourceRows, stats[i].ReferenceRows = e.out.SourceRows, e.out.ReferenceRows
		wg.Go(func() {
			for _, stage := range []struct {
				name string
				run  func() error
			}{{"lookup", e.join}, {"decisions", e.decisions}, {"neighbors", e.neighbors}} {
				log(fmt.Sprintf("worker %d/%d: %s started (%d reference rows)", i+1, o.Workers, stage.name, e.out.ReferenceRows))
				started := time.Now()
				if err := stage.run(); err != nil {
					errorMu.Lock()
					if first == nil {
						first = fmt.Errorf("worker %d %s: %w", i, stage.name, err)
					}
					errorMu.Unlock()
					cancel()
					return
				}
				stats[i].StageMS[stage.name] = time.Since(started).Milliseconds()
				log(fmt.Sprintf("worker %d/%d: %s complete in %dms", i+1, o.Workers, stage.name, stats[i].StageMS[stage.name]))
			}
		})
	}
	wg.Wait()
	if first != nil {
		return Result{}, first
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	r := Result{SchemaVersion: Version, Policy: engines[0].out.Policy, Scope: engines[0].out.Scope, States: map[string]uint64{}, RunRows: o.RunRows, FanIn: o.FanIn, FilterBytes: o.FilterBytes, Workers: o.Workers, WorkerFanIn: workerFanIn, Partitions: stats, StageMS: stageMS}
	for _, e := range engines {
		r.SourceRows += e.out.SourceRows
		r.ReferenceRows += e.out.ReferenceRows
		r.LookupMemberRows += e.out.LookupMemberRows
		for state, count := range e.out.States {
			r.States[state] += count
		}
	}
	// Each artifact's final merge is ordered; the four independent artifacts can
	// assemble concurrently. At most four times eight input decoders are active,
	// after the domain workers have stopped, under the same shared byte cap.
	assemblySlots := make(chan struct{}, min(4, o.Workers))
	for _, family := range []struct {
		name string
		get  func(Result) xsort.File
		out  *xsort.File
	}{
		{"lookup", func(r Result) xsort.File { return r.LookupEvidence }, &r.LookupEvidence},
		{"decisions", func(r Result) xsort.File { return r.Decisions }, &r.Decisions},
		{"incidences", func(r Result) xsort.File { return r.ExactIncidences }, &r.ExactIncidences},
		{"neighbors", func(r Result) xsort.File { return r.Neighbors }, &r.Neighbors},
	} {
		wg.Go(func() {
			assemblySlots <- struct{}{}
			defer func() { <-assemblySlots }()
			log("final ordered assembly: " + family.name)
			started := time.Now()
			files := make([]xsort.File, len(engines))
			for i, e := range engines {
				files[i] = family.get(e.out)
			}
			var err error
			*family.out, err = xsort.MergeRuns(ctx, space, files, o.FanIn)
			errorMu.Lock()
			defer errorMu.Unlock()
			if err != nil {
				if first == nil {
					first = fmt.Errorf("assembly %s: %w", family.name, err)
				}
				cancel()
				return
			}
			stageMS["assemble_"+family.name] = time.Since(started).Milliseconds()
			log(fmt.Sprintf("final %s assembly complete in %dms", family.name, stageMS["assemble_"+family.name]))
		})
	}
	wg.Wait()
	if first != nil {
		return Result{}, first
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return r, nil
}

// One batch per worker may be borrowed. Each successful send must be
// acknowledged even after cancellation before the source can reuse its strings.
func dispatchScan(ctx context.Context, source batchSource, visitors []func(Row) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type work struct {
		rows []Row
		ack  chan struct{}
	}
	queues := make([]chan work, len(visitors))
	var wg sync.WaitGroup
	var errorMu sync.Mutex
	var first error
	for i, visit := range visitors {
		queues[i] = make(chan work, 1)
		wg.Go(func() {
			for w := range queues[i] {
				for _, row := range w.rows {
					if ctx.Err() != nil {
						break
					}
					if err := visit(row); err != nil {
						errorMu.Lock()
						if first == nil {
							first = err
						}
						errorMu.Unlock()
						cancel()
						break
					}
				}
				close(w.ack)
			}
		})
	}
	batches := make([][]Row, len(visitors))
	err := source(ctx, func(rows []Row) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range batches {
			clear(batches[i])
			batches[i] = batches[i][:0]
		}
		for _, row := range rows {
			i := reportPartition(row, len(visitors))
			batches[i] = append(batches[i], row)
		}
		acks := make([]chan struct{}, 0, len(visitors))
		for i, rows := range batches {
			if len(rows) == 0 {
				continue
			}
			ack := make(chan struct{})
			queues[i] <- work{rows: rows, ack: ack}
			acks = append(acks, ack)
		}
		for _, ack := range acks {
			<-ack
		}
		return ctx.Err()
	})
	for _, q := range queues {
		close(q)
	}
	wg.Wait()
	if first != nil {
		return first
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}
