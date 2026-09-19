package receiptreferences

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// Opt-in diagnostic only: sample scope cannot establish complete report
// membership. No reference decisions, publication manifest or pointer is emitted.
// Pin an already accepted manifest digest and verify every selected shard through
// the production reader, without rehashing the unselected full-corpus backing.
func TestProfileRealSourceScans(t *testing.T) {
	root := os.Getenv("LT_REFERENCE_PROFILE_STORAGE")
	if root == "" {
		t.Skip("bounded real-source profile not configured")
	}
	manifest := os.Getenv("LT_REFERENCE_PROFILE_MANIFEST")
	expected := os.Getenv("LT_REFERENCE_PROFILE_SHA256")
	output := os.Getenv("LT_REFERENCE_PROFILE_OUTPUT")
	mode := os.Getenv("LT_REFERENCE_PROFILE_MODE")
	readerName := os.Getenv("LT_REFERENCE_PROFILE_READER")
	reader := newNarrowReferenceReader
	switch readerName {
	case "", "narrow":
		readerName = "narrow"
	case "generic":
		reader = genericReferenceReader
	default:
		t.Fatal("reader must be narrow or generic")
	}
	readers, err := strconv.Atoi(os.Getenv("LT_REFERENCE_PROFILE_READERS"))
	if err != nil || (readers != 1 && readers != 4 && readers != 8) || output == "" || (mode != "decode" && mode != "dispatch" && mode != "ingest") {
		t.Fatal("new output, mode decode/dispatch/ingest and 1/4/8 readers required")
	}
	b, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(b)
	if hex.EncodeToString(digest[:]) != expected {
		t.Fatal("pinned manifest digest mismatch")
	}
	var m occ.ScheduleAColumnarManifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if filepath.Base(manifest) != m.FactSetID+".json" || m.SchemaVersion != occ.ScheduleAColumnarFactSetSchemaVersion || m.Counts.InvalidFacts != 0 || m.Counts.Facts != m.Counts.ValidFacts {
		t.Fatal("accepted dense valid immutable manifest required")
	}
	shards, err := profileShards(m.Shards, 8)
	if err != nil {
		t.Fatal(err)
	}
	cycle, err := strconv.ParseInt(m.Cycle, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(output, 0750); err != nil {
		t.Fatal("profile output must be new:", err)
	}
	space, err := xsort.NewWorkspace(filepath.Join(output, "scratch"), 2<<30)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	const workers = 8
	engines := make([]*engine, workers)
	if mode == "ingest" {
		for i := range engines {
			engines[i], err = newEngine(ctx, space, Options{RunRows: 100000, FanIn: 4, FilterBytes: (64 << 20) / workers})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	result := sourceProfileResult{Scope: "diagnostic_shard_sample_not_complete_report_membership", FactSetID: m.FactSetID, ManifestSHA256: expected, Mode: mode, Readers: readers, Workers: workers, Shards: shards}
	result.SourceReader = readerName
	var expectedRows uint64
	for _, s := range shards {
		expectedRows += s.Facts
	}
	passes := []string{"scan"}
	if mode == "ingest" {
		passes = []string{"requests", "memberships"}
	}
	for pass, name := range passes {
		var waiting, scanning, visiting atomic.Int64
		var rows atomic.Uint64
		visited := make([]uint64, workers)
		visitors := make([]func(Row) error, workers)
		for i := range visitors {
			visitors[i] = func(r Row) error {
				visited[i]++
				if mode != "ingest" {
					return nil
				}
				if pass == 0 {
					return engines[i].request(r)
				}
				return engines[i].membership(r)
			}
		}
		source := func(ctx context.Context, visit func([]Row) error) error {
			return parallelScanBatches(ctx, len(shards), readers, func(ctx context.Context, i int, batch func([]Row) error) error {
				started := time.Now()
				defer func() { scanning.Add(time.Since(started).Nanoseconds()) }()
				return scanShardWithReader(ctx, root, shards[i], cycle, schema, reader, func(b []Row) error {
					rows.Add(uint64(len(b)))
					started := time.Now()
					err := batch(b)
					waiting.Add(time.Since(started).Nanoseconds())
					return err
				})
			}, func(b []Row) error {
				started := time.Now()
				err := visit(b)
				visiting.Add(time.Since(started).Nanoseconds())
				return err
			}, func(string) {})
		}
		beforeCPU, beforeIO := profileCPU(t), profileIO(t)
		started := time.Now()
		pprof.Do(ctx, pprof.Labels("stage", mode+"_"+name), func(ctx context.Context) {
			if mode == "decode" {
				err = source(ctx, func(b []Row) error { visited[0] += uint64(len(b)); return nil })
			} else {
				err = dispatchScan(ctx, source, visitors)
			}
		})
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		afterCPU, afterIO := profileCPU(t), profileIO(t)
		var total uint64
		for _, n := range visited {
			total += n
		}
		if rows.Load() != expectedRows || total != expectedRows {
			t.Fatal("selected source/visitor row conservation")
		}
		for k, v := range afterIO {
			afterIO[k] = v - beforeIO[k]
		}
		p := sourceProfilePass{Name: name, Rows: total, WallSeconds: elapsed.Seconds(), CPUSeconds: afterCPU - beforeCPU, ReaderBackpressureSeconds: float64(waiting.Load()) / 1e9, ReaderActiveSeconds: float64(scanning.Load()-waiting.Load()) / 1e9, ConsumerSeconds: float64(visiting.Load()) / 1e9, IO: afterIO}
		result.Passes = append(result.Passes, p)
		t.Logf("stage %s: %s", name, marshal(p))
	}
	for _, e := range engines {
		if e != nil {
			result.ReferenceRows += e.out.ReferenceRows
			result.LookupMemberRows += e.out.LookupMemberRows
		}
	}
	result.PeakWorkspaceBytes, _ = space.Stats()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	result.PeakRSSBytes = uint64(usage.Maxrss) * 1024
	// Scratch contains only the runs naturally flushed by ingestion. Tail buffers
	// and final merges belong to later stages and are deliberately not timed here.
	f, err := os.OpenFile(filepath.Join(output, "metrics.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(result); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

type sourceProfileResult struct {
	SourceReader                           string
	Scope, FactSetID, ManifestSHA256, Mode string
	Readers, Workers                       int
	Shards                                 []occ.ScheduleAColumnarShard
	Passes                                 []sourceProfilePass
	ReferenceRows, LookupMemberRows        uint64
	PeakRSSBytes, PeakWorkspaceBytes       uint64
}

type sourceProfilePass struct {
	Name                                           string
	Rows                                           uint64
	WallSeconds, CPUSeconds                        float64
	ReaderBackpressureSeconds, ReaderActiveSeconds float64
	ConsumerSeconds                                float64
	IO                                             map[string]uint64
}

// Evenly spaced whole shards: no amount, name, role or candidate selection.
func profileShards(shards []occ.ScheduleAColumnarShard, count int) ([]occ.ScheduleAColumnarShard, error) {
	if count < 1 || count > 16 || len(shards) < count {
		return nil, fmt.Errorf("1..16 distinct sample shards required")
	}
	out := make([]occ.ScheduleAColumnarShard, count)
	for i := range out {
		s := shards[i*len(shards)/count]
		if s.Facts == 0 || s.Facts > 1000000 || s.Facts != s.ValidFacts || s.Facts != s.SourceRows || s.LastSourceRowOrdinal-s.FirstSourceRowOrdinal+1 != s.Facts {
			return nil, fmt.Errorf("bounded dense valid source shard required")
		}
		out[i] = s
	}
	return out, nil
}

func profileCPU(t *testing.T) float64 {
	t.Helper()
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		t.Fatal(err)
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

func profileIO(t *testing.T) map[string]uint64 {
	t.Helper()
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		fields := strings.Fields(line)
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		out[strings.TrimSuffix(fields[0], ":")] = n
	}
	return out
}

func TestProfileShardSelection(t *testing.T) {
	shards := make([]occ.ScheduleAColumnarShard, 265)
	for i := range shards {
		shards[i] = occ.ScheduleAColumnarShard{Index: uint64(i), Facts: 1000000, ValidFacts: 1000000, SourceRows: 1000000, FirstSourceRowOrdinal: uint64(i*1000000 + 1), LastSourceRowOrdinal: uint64((i + 1) * 1000000)}
	}
	for _, count := range []int{1, 8, 16} {
		got, err := profileShards(shards, count)
		if err != nil || len(got) != count {
			t.Fatal("sample shape", err)
		}
		for i, s := range got {
			if s.Index != uint64(i*len(shards)/count) || (i > 0 && s.Index <= got[i-1].Index) {
				t.Fatal("sample not distinct and evenly spaced")
			}
		}
	}
	for _, count := range []int{0, 17, 266} {
		if _, err := profileShards(shards, count); err == nil {
			t.Fatal("unbounded sample accepted")
		}
	}
	shards[0].Facts++
	if _, err := profileShards(shards, 8); err == nil {
		t.Fatal("non-dense sample accepted")
	}
}
