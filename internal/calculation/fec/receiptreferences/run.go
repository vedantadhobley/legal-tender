package receiptreferences

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func Run(ctx context.Context, o Options) (Result, error) {
	started := time.Now()
	if o.ScanWorkers < 1 || o.ScanWorkers > 8 {
		return Result{}, fmt.Errorf("1..8 source scanners required")
	}
	if o.Workers < 1 || o.Workers > 8 || o.FilterBytes < o.Workers {
		return Result{}, fmt.Errorf("1..8 processing workers and at least one filter byte per worker required")
	}
	if o.StorageRoot == "" || o.Manifest == "" || o.Cycle == "" || o.OutputDirectory == "" || o.RunRows < 1 || o.RunRows > 100000 || o.FanIn < 2 || o.FanIn > 16 || o.FilterBytes < 1 || o.FilterBytes > 256<<20 || o.MaxWorkspaceBytes == 0 || o.MaxWorkspaceBytes > 64<<30 {
		return Result{}, fmt.Errorf("exact source/cycle, new output, bounded runs/fan-in/filter and <=64GiB workspace required")
	}
	if b, err := hex.DecodeString(o.BuildSHA256); err != nil || len(b) != 32 || hex.EncodeToString(b) != o.BuildSHA256 {
		return Result{}, fmt.Errorf("canonical executable digest required")
	}
	if _, err := os.Lstat(o.OutputDirectory); !os.IsNotExist(err) {
		return Result{}, fmt.Errorf("output directory must be new")
	}
	var progressMu sync.Mutex
	log := func(s string) {
		progressMu.Lock()
		defer progressMu.Unlock()
		if o.Progress != nil {
			o.Progress(s)
		}
	}
	log("verifying complete immutable Schedule A fact backing")
	m, digest, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.Manifest)
	if err != nil {
		return Result{}, err
	}
	if filepath.Base(o.Manifest) != m.FactSetID+".json" {
		return Result{}, fmt.Errorf("immutable manifest path required; latest pointers are not accepted")
	}
	if m.Cycle != o.Cycle || m.Counts.Facts == 0 || m.Counts.Facts != m.Counts.SourceOccurrences || m.Counts.Facts != m.Counts.ValidFacts || m.Counts.InvalidFacts != 0 || m.Counts.ExcludedOccurrences != 0 {
		return Result{}, fmt.Errorf("requested dense valid fact cycle required")
	}
	var rows uint64
	for i, s := range m.Shards {
		if s.Index != uint64(i) || s.Facts == 0 || s.SourceRows != s.Facts || s.ValidFacts != s.Facts || s.FirstSourceRowOrdinal != rows+1 || s.LastSourceRowOrdinal != rows+s.Facts {
			return Result{}, fmt.Errorf("non-dense source shard")
		}
		rows += s.Facts
	}
	if rows != m.Counts.Facts {
		return Result{}, fmt.Errorf("source shard conservation")
	}
	var fs syscall.Statfs_t
	if err = syscall.Statfs(filepath.Dir(o.OutputDirectory), &fs); err != nil {
		return Result{}, err
	}
	if fs.Bavail*uint64(fs.Bsize) < o.MaxWorkspaceBytes {
		return Result{}, fmt.Errorf("workspace cap exceeds available disk")
	}
	if err = os.Mkdir(o.OutputDirectory, 0750); err != nil {
		return Result{}, err
	}
	space, err := xsort.NewWorkspace(filepath.Join(o.OutputDirectory, "data"), o.MaxWorkspaceBytes)
	if err != nil {
		return Result{}, err
	}
	source := func(ctx context.Context, visit func([]Row) error) error {
		return scanBatches(ctx, o, m, visit, log)
	}
	r, err := calculateReferences(ctx, space, o, source, log)
	if err != nil {
		return Result{}, err
	}
	r.ScanWorkers = o.ScanWorkers
	r.State = "complete_cycle_reference_join"
	r.BuildSHA256 = o.BuildSHA256
	r.FactSetID = m.FactSetID
	r.ManifestSHA256 = digest
	r.Cycle = m.Cycle
	r.PeakWorkspaceBytes, r.RetainedBytes = space.Stats()
	r.ElapsedMS = time.Since(started).Milliseconds()
	var usage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return Result{}, err
	}
	r.PeakRSSBytes = uint64(usage.Maxrss) * 1024
	var total uint64
	for _, n := range r.States {
		total += n
	}
	if total != m.Counts.Facts || r.Decisions.Rows != r.ReferenceRows {
		return Result{}, fmt.Errorf("final row conservation")
	}
	r.CalculationID = logicalID(r)
	if err = saveResult(o.OutputDirectory, r); err != nil {
		return Result{}, err
	}
	return r, nil
}

// Physical filenames, run geometry, filter false positives and timings do not
// affect the logical reference calculation identity.
func logicalID(r Result) string {
	v := struct {
		Version, Build, FactSet, Manifest, Cycle, Policy, Scope string
		Rows, References                                        uint64
		States                                                  map[string]uint64
		Decisions, Incidences, Neighbors                        string
	}{
		r.SchemaVersion, r.BuildSHA256, r.FactSetID, r.ManifestSHA256, r.Cycle, r.Policy, r.Scope, r.SourceRows, r.ReferenceRows, r.States, r.Decisions.ValuesSHA256, r.ExactIncidences.ValuesSHA256, r.Neighbors.ValuesSHA256}
	h := sha256.Sum256(marshal(v))
	return hex.EncodeToString(h[:])
}
func saveResult(dir string, r Result) error {
	return saveJSONResult(dir, r)
}
func saveJSONResult(dir string, r any) error {
	path := filepath.Join(dir, ".complete.tmp")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = json.NewEncoder(f).Encode(r); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Link(path, filepath.Join(dir, "manifest.json")); err != nil {
		return err
	}
	return os.Remove(path)
}
func scan(ctx context.Context, o Options, m occ.ScheduleAColumnarManifest, visit func(Row) error, log func(string)) error {
	return scanBatches(ctx, o, m, func(rows []Row) error {
		for _, row := range rows {
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	}, log)
}

func scanBatches(ctx context.Context, o Options, m occ.ScheduleAColumnarManifest, visit func([]Row) error, log func(string)) error {
	cycle, err := strconv.ParseInt(m.Cycle, 10, 64)
	if err != nil {
		return err
	}
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		return err
	}
	return parallelScanBatches(ctx, len(m.Shards), o.ScanWorkers, func(ctx context.Context, i int, batch func([]Row) error) error {
		return scanShard(ctx, o.StorageRoot, m.Shards[i], cycle, schema, batch)
	}, visit, log)
}

// A reader retains its buffer until the single consumer acknowledges it. This
// avoids a whole-corpus copy and prevents Parquet buffer reuse from racing the
// serialized domain stage. At most one batch per reader is in flight.
func parallelScan(ctx context.Context, shards, workers int, read func(context.Context, int, func([]Row) error) error, visit func(Row) error, log func(string)) error {
	return parallelScanBatches(ctx, shards, workers, read, func(rows []Row) error {
		for _, row := range rows {
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	}, log)
}

func parallelScanBatches(ctx context.Context, shards, workers int, read func(context.Context, int, func([]Row) error) error, visit func([]Row) error, log func(string)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type message struct {
		rows []Row
		ack  chan struct{}
		done bool
		err  error
	}
	results := make(chan message, workers)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= shards {
					return
				}
				batch := func(rows []Row) error {
					ack := make(chan struct{})
					select {
					case results <- message{rows: rows, ack: ack}:
					case <-ctx.Done():
						return ctx.Err()
					}
					// Cancellation cannot release memory still owned by the consumer.
					<-ack
					return ctx.Err()
				}
				err := read(ctx, i, batch)
				select {
				case results <- message{done: true, err: err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	go func() { wg.Wait(); close(results) }()
	var first error
	completed := 0
	for got := range results {
		if first == nil {
			if got.err != nil {
				first = got.err
				cancel()
			} else if got.done {
				completed++
				if completed%16 == 0 || completed == shards {
					log(fmt.Sprintf("scanned %d/%d complete source shards", completed, shards))
				}
			} else {
				if err := visit(got.rows); err != nil {
					first = err
					cancel()
				}
			}
		}
		if got.ack != nil {
			close(got.ack)
		}
	}
	if first != nil {
		return first
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if completed != shards {
		return fmt.Errorf("incomplete parallel scan")
	}
	return nil
}
func scanShard(ctx context.Context, root string, s occ.ScheduleAColumnarShard, cycle int64, schema *scheduleaparquet.Schema, visit func([]Row) error) error {
	return scanShardWithReader(ctx, root, s, cycle, schema, newNarrowReferenceReader, visit)
}

type referenceRowReader interface {
	Read([]Row) (int, error)
	Close() error
}

// The factory also permits differential tests against the previous generic
// reader without changing source validation, membership or publication rules.
func scanShardWithReader(ctx context.Context, root string, s occ.ScheduleAColumnarShard, cycle int64, schema *scheduleaparquet.Schema, reader func(*parquet.File) (referenceRowReader, error), visit func([]Row) error) error {
	path, err := artifact.Resolve(root, s.StorageKey)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	bytesRead := uint64(0)
	checkBuffer := make([]byte, 128<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := f.Read(checkBuffer)
		if n > 0 {
			h.Write(checkBuffer[:n])
			bytesRead += uint64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if bytesRead != s.Bytes || hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return fmt.Errorf("opened source shard digest/size mismatch")
	}
	p, err := parquet.OpenFile(f, int64(s.Bytes))
	if err != nil {
		return err
	}
	if p.Schema().String() != schema.Parquet().String() || p.NumRows() != int64(s.Facts) {
		return fmt.Errorf("physical schema/count mismatch")
	}
	r, err := reader(p)
	if err != nil {
		return err
	}
	defer r.Close()
	buffer := make([]Row, 8192)
	var seen uint64
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(buffer)
		for _, v := range buffer[:n] {
			if err := validateRow(v, s.FirstSourceRowOrdinal+seen, cycle); err != nil {
				return err
			}
			seen++
		}
		if n > 0 {
			if err := visit(buffer[:n]); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if seen != s.Facts {
		return fmt.Errorf("source row conservation")
	}
	return nil
}
