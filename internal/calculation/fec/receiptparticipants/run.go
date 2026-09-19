package receiptparticipants

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func equalCensus(a, b Census) bool { a.keys = nil; b.keys = nil; return reflect.DeepEqual(a, b) }

func Run(ctx context.Context, o Options) (Result, error) { return run(ctx, o, nil) }

// Benchmark uses whole explicit shards, capped at eight. Even selecting every
// shard of a small fixture cannot promote its output to a complete publication.
func Benchmark(ctx context.Context, o Options, indices []int) (Result, error) {
	if len(indices) == 0 || len(indices) > 8 {
		return Result{}, fmt.Errorf("1..8 explicit benchmark shards required")
	}
	indices = slices.Clone(indices)
	slices.Sort(indices)
	for i, n := range indices {
		if n < 0 || (i > 0 && indices[i-1] == n) {
			return Result{}, fmt.Errorf("distinct nonnegative benchmark shard indices required")
		}
	}
	return run(ctx, o, indices)
}

func run(ctx context.Context, o Options, selected []int) (Result, error) {
	started := time.Now()
	if o.StorageRoot == "" || o.Manifest == "" || o.Cycle == "" || o.OutputDirectory == "" || !digest(o.BuildSHA256) || o.Workers < 1 || o.Workers > 8 || o.MaxOutputBytes == 0 || o.MaxOutputBytes > 32<<30 {
		return Result{}, fmt.Errorf("exact source/cycle, new output, build digest, 1..8 workers and <=32GiB output budget required")
	}
	if _, err := os.Lstat(o.OutputDirectory); !os.IsNotExist(err) {
		return Result{}, fmt.Errorf("output directory must be new")
	}
	log := o.Progress
	if log == nil {
		log = func(string) {}
	}
	log("verifying immutable Schedule A publication ancestry")
	m, manifestSHA, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.Manifest)
	if err != nil {
		return Result{}, err
	}
	if filepath.Base(o.Manifest) != m.FactSetID+".json" || m.Cycle != o.Cycle || m.Counts.Facts == 0 || m.Counts.Facts > math.MaxInt64 || m.Counts.Facts != m.Counts.ValidFacts || m.Counts.SourceOccurrences != m.Counts.Facts || m.Counts.InvalidFacts != 0 || m.Counts.ExcludedOccurrences != 0 {
		return Result{}, fmt.Errorf("exact immutable dense valid fact cycle required")
	}
	var sourceRows uint64
	for i, s := range m.Shards {
		if s.Index != uint64(i) || s.Facts == 0 || s.SourceRows != s.Facts || s.ValidFacts != s.Facts || s.FirstSourceRowOrdinal != sourceRows+1 || s.LastSourceRowOrdinal != sourceRows+s.Facts {
			return Result{}, fmt.Errorf("non-dense source shard")
		}
		sourceRows += s.Facts
	}
	if sourceRows != m.Counts.Facts {
		return Result{}, fmt.Errorf("source manifest conservation")
	}
	indices := selected
	if selected == nil {
		indices = make([]int, len(m.Shards))
		for i := range indices {
			indices[i] = i
		}
	} else {
		for _, i := range indices {
			if i >= len(m.Shards) || m.Shards[i].Facts > 1000000 {
				return Result{}, fmt.Errorf("benchmark shard absent or exceeds one million rows")
			}
		}
	}
	var fs syscall.Statfs_t
	if err = syscall.Statfs(filepath.Dir(o.OutputDirectory), &fs); err != nil {
		return Result{}, err
	}
	if fs.Bavail*uint64(fs.Bsize) < o.MaxOutputBytes+(1<<20) {
		return Result{}, fmt.Errorf("output cap and manifest reserve exceed free disk")
	}
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	if err = os.Mkdir(o.OutputDirectory, 0750); err != nil {
		return Result{}, err
	}
	dir := filepath.Join(o.OutputDirectory, "data")
	if err = os.Mkdir(dir, 0750); err != nil {
		return Result{}, err
	}
	r, err := publish(ctx, o, m, indices, dir, log)
	if err != nil {
		return Result{}, err
	}
	r.SchemaVersion = Version
	r.State = "complete_cycle_participant_index"
	r.Scope = "complete_published_schedule_a_cycle"
	if selected != nil {
		r.State = "complete_bounded_participant_benchmark"
		r.Scope = "selected_whole_shards_not_complete_cycle"
	}
	r.BuildSHA256 = o.BuildSHA256
	r.Policy = Policy
	r.InventoryPolicy = fundingbasis.Policy
	r.SourceRolePolicy = fundingbasis.EvidencePolicy
	r.IndividualPolicy = receipts.ContractID + "@" + receipts.ContractVersion
	r.CommitteePolicy = committeeflows.ContractID + "@" + committeeflows.ContractVersion
	r.FactSetID = m.FactSetID
	r.ManifestSHA256 = manifestSHA
	r.Cycle = m.Cycle
	r.SourceRows = sourceRows
	r.AppearanceRole = ContributorRole
	r.AdditionalConduitAmount = "0"
	r.Workers = o.Workers
	r.ElapsedMS = time.Since(started).Milliseconds()
	var usage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return Result{}, err
	}
	r.PeakRSSBytes = uint64(usage.Maxrss) * 1024
	r.CalculationID = logicalID(r)
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	if err = save(o.OutputDirectory, r); err != nil {
		return Result{}, err
	}
	return r, nil
}

func publish(ctx context.Context, o Options, m occ.ScheduleAColumnarManifest, indices []int, dir string, log func(string)) (Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cycle, err := parseCycle(m.Cycle)
	if err != nil {
		return Result{}, err
	}
	cap := &budget{limit: o.MaxOutputBytes}
	type shardResult struct {
		position int
		file     File
		census   Census
		err      error
	}
	results := make(chan shardResult, o.Workers)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range o.Workers {
		wg.Go(func() {
			for {
				position := int(next.Add(1) - 1)
				if position >= len(indices) {
					return
				}
				shard := m.Shards[indices[position]]
				f, c, e := writeShard(ctx, dir, shard, cap, func(ctx context.Context, visit func([]fundingbasis.SourceEvidenceRow) error) error {
					return scanSource(ctx, o.StorageRoot, shard, cycle, visit)
				})
				results <- shardResult{position, f, c, e}
				if e != nil {
					cancel()
					return
				}
			}
		})
	}
	go func() { wg.Wait(); close(results) }()
	r := Result{Files: make([]File, len(indices)), Census: newCensus()}
	var firstError error
	completed := 0
	for got := range results {
		if got.err != nil {
			if firstError == nil || errors.Is(firstError, context.Canceled) {
				firstError = got.err
			}
			cancel()
			continue
		}
		if firstError != nil {
			continue
		}
		r.Files[got.position] = got.file
		r.Census.merge(got.census)
		r.OutputBytes += got.file.Bytes
		completed++
		log(fmt.Sprintf("verified participant shard %d (%d/%d), %d source-grain rows", indices[got.position], completed, len(indices), got.file.Rows))
	}
	if firstError != nil {
		return Result{}, firstError
	}
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	var expected uint64
	for _, i := range indices {
		expected += m.Shards[i].Facts
	}
	if completed != len(indices) || r.Census.Rows != expected || r.OutputBytes != cap.used {
		return Result{}, fmt.Errorf("participant publication conservation")
	}
	return r, nil
}

func save(dir string, r Result) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b)+1 > 1<<20 {
		return fmt.Errorf("participant manifest exceeds 1MiB reserve")
	}
	path := filepath.Join(dir, ".manifest.tmp")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(append(b, '\n')); err != nil {
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
