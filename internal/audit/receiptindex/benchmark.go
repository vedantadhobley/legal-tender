package receiptindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const Version = "legal-tender.fec.receipt-reference-sort-benchmark.v1"

type Options struct {
	StorageRoot, Manifest, Cycle, OutputDirectory, BuildSHA256 string
	ShardIndex                                                 int
	MaxRows, RunRows                                           int
	MaxOutputBytes                                             uint64
	Progress                                                   func(string)
}

type Evidence struct {
	Version          string                     `json:"schema_version"`
	BuildSHA256      string                     `json:"executable_sha256"`
	FactSetID        string                     `json:"fact_set_id"`
	ManifestSHA256   string                     `json:"manifest_sha256"`
	Cycle            string                     `json:"cycle"`
	SourceShard      occ.ScheduleAColumnarShard `json:"source_shard"`
	SourceRows       uint64                     `json:"source_rows_in_fact_set"`
	SampleRows       uint64                     `json:"sample_rows"`
	First            uint64                     `json:"first_source_row_ordinal"`
	Last             uint64                     `json:"last_source_row_ordinal"`
	RunRows          int                        `json:"rows_per_sort_run"`
	MaxOutputBytes   uint64                     `json:"max_output_bytes"`
	SourceOrder      []File                     `json:"source_order_runs"`
	ReportOrder      []File                     `json:"report_order_runs"`
	Scope            string                     `json:"scope"`
	ProductionReady  bool                       `json:"production_index_ready"`
	IdentityResolved bool                       `json:"contributor_identity_resolved"`
}

type Result struct {
	State          string   `json:"state"`
	EvidenceID     string   `json:"evidence_id"`
	Evidence       Evidence `json:"evidence"`
	VerificationMS int64    `json:"source_verification_ms"`
	RunMS          int64    `json:"scan_write_readback_ms"`
	SortMS         int64    `json:"sort_ms"`
	OutputBytes    uint64   `json:"output_bytes"`
	PeakRSSBytes   uint64   `json:"process_peak_rss_bytes"`
}

func validateOptions(o Options) error {
	if o.StorageRoot == "" || o.Manifest == "" || o.Cycle == "" || o.OutputDirectory == "" || o.ShardIndex < 0 ||
		o.MaxRows < 1 || o.MaxRows > 1_000_000 || o.RunRows < 1 || o.RunRows > 100_000 ||
		o.MaxOutputBytes == 0 || o.MaxOutputBytes > 1<<30 {
		return fmt.Errorf("exact source, cycle, new output directory, nonnegative shard, 1..1000000 rows, 1..100000 run rows and 1..1GiB output budget required")
	}
	if (o.MaxRows+o.RunRows-1)/o.RunRows > 100 {
		return fmt.Errorf("benchmark exceeds 100 sort runs")
	}
	if b, err := hex.DecodeString(o.BuildSHA256); err != nil || len(b) != 32 || o.BuildSHA256 != hex.EncodeToString(b) {
		return fmt.Errorf("canonical executable SHA256 required")
	}
	return nil
}

// Run measures a caller-selected shard prefix. It does not extrapolate global
// cardinality, resolve absent references, or accept a cycle-wide physical model.
func Run(ctx context.Context, o Options) (Result, error) {
	if err := validateOptions(o); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if _, err := os.Lstat(o.OutputDirectory); !os.IsNotExist(err) {
		return Result{}, fmt.Errorf("output directory must be new and inspectable")
	}
	start := time.Now()
	if o.Progress != nil {
		o.Progress("verifying published fact manifest and complete backing")
	}
	m, digest, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.Manifest)
	if err != nil {
		return Result{}, err
	}
	if m.Cycle != o.Cycle || o.ShardIndex >= len(m.Shards) || m.Counts.Facts == 0 || m.Counts.Facts != m.Counts.ValidFacts ||
		m.Counts.Facts != m.Counts.SourceOccurrences || m.Counts.ExcludedOccurrences != 0 || m.Counts.InvalidFacts != 0 {
		return Result{}, fmt.Errorf("benchmark requires the requested dense valid cycle and an existing shard")
	}
	s := m.Shards[o.ShardIndex]
	if s.Facts == 0 || s.Facts != s.SourceRows || s.LastSourceRowOrdinal-s.FirstSourceRowOrdinal+1 != s.Facts {
		return Result{}, fmt.Errorf("source shard is not dense")
	}
	result := Result{State: "complete_bounded_benchmark", VerificationMS: time.Since(start).Milliseconds()}
	result.Evidence = Evidence{Version: Version, BuildSHA256: o.BuildSHA256, FactSetID: m.FactSetID, ManifestSHA256: digest,
		Cycle: m.Cycle, SourceShard: s, SourceRows: m.Counts.Facts, First: s.FirstSourceRowOrdinal, RunRows: o.RunRows,
		MaxOutputBytes: o.MaxOutputBytes, Scope: "selected_shard_prefix_not_complete_report_or_cycle", SourceOrder: []File{}, ReportOrder: []File{}}
	path, err := artifact.Resolve(o.StorageRoot, s.StorageKey)
	if err != nil {
		return Result{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	defer f.Close()
	// Check the exact opened bytes as well as publication ancestry.
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return Result{}, err
	}
	if hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return Result{}, fmt.Errorf("selected shard changed after source verification")
	}
	p, err := parquet.OpenFile(f, int64(s.Bytes))
	if err != nil {
		return Result{}, err
	}
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		return Result{}, err
	}
	if p.Schema().String() != schema.Parquet().String() || p.NumRows() != int64(s.Facts) {
		return Result{}, fmt.Errorf("source schema/count mismatch")
	}
	if err := os.Mkdir(o.OutputDirectory, 0o750); err != nil {
		return Result{}, err
	}
	r := parquet.NewGenericReader[Row](p)
	defer r.Close()
	cycle, err := strconv.ParseInt(m.Cycle, 10, 64)
	if err != nil {
		return Result{}, err
	}
	limit := min(uint64(o.MaxRows), s.Facts)
	cap := &budget{limit: o.MaxOutputBytes}
	start = time.Now()
	buffer := make([]Row, min(8192, o.RunRows))
	var seen uint64
	for seen < limit {
		rows := make([]Row, 0, o.RunRows)
		for len(rows) < o.RunRows && seen < limit {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			n, err := r.Read(buffer[:min(len(buffer), o.RunRows-len(rows), int(limit-seen))])
			for _, row := range buffer[:n] {
				if err := validateRow(row, s.FirstSourceRowOrdinal+seen, cycle); err != nil {
					return Result{}, err
				}
				rows = append(rows, cloneRow(row))
				seen++
			}
			if err != nil && err != io.EOF {
				return Result{}, err
			}
			if (err == io.EOF && seen != s.Facts) || n == 0 {
				return Result{}, fmt.Errorf("premature source EOF or no progress")
			}
		}
		i := len(result.Evidence.SourceOrder)
		unsorted, err := measure(ctx, o.OutputDirectory, fmt.Sprintf("source-%03d.parquet", i), rows, cap)
		if err != nil {
			return Result{}, err
		}
		sortStart := time.Now()
		slices.SortFunc(rows, compare)
		result.SortMS += time.Since(sortStart).Milliseconds()
		ordered, err := measure(ctx, o.OutputDirectory, fmt.Sprintf("report-%03d.parquet", i), rows, cap)
		if err != nil {
			return Result{}, err
		}
		result.Evidence.SourceOrder = append(result.Evidence.SourceOrder, unsorted)
		result.Evidence.ReportOrder = append(result.Evidence.ReportOrder, ordered)
		if o.Progress != nil {
			o.Progress(fmt.Sprintf("verified both layouts for %d/%d rows", seen, limit))
		}
	}
	result.Evidence.SampleRows, result.Evidence.Last = seen, s.FirstSourceRowOrdinal+seen-1
	result.OutputBytes, result.RunMS = cap.used, time.Since(start).Milliseconds()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return Result{}, err
	}
	result.PeakRSSBytes = uint64(usage.Maxrss) * 1024
	b, err := json.Marshal(result.Evidence)
	if err != nil {
		return Result{}, err
	}
	sum := sha256.Sum256(b)
	result.EvidenceID = hex.EncodeToString(sum[:])
	return result, nil
}
