// Package fecschedulealayout benchmarks bounded physical layouts for the
// processed Schedule A corpus. It is a probe, not a production publisher.
package fecschedulealayout

// SchemaVersion identifies the benchmark result contract.
const SchemaVersion = "legal-tender.schedule-a-layout-benchmark.v2"

// Result records one bounded, equal-row layout comparison.
type Result struct {
	SchemaVersion string             `json:"schema_version"`
	State         string             `json:"state"`
	Cycle         string             `json:"cycle"`
	SampleRows    uint64             `json:"sample_rows"`
	Source        Source             `json:"source"`
	Configuration Configuration      `json:"configuration"`
	ZstdCOPY      LayoutMeasurement  `json:"zstd_copy"`
	Parquet       ParquetMeasurement `json:"parquet"`
	SourceScan    ScanMeasurement    `json:"source_projection_scan"`
	ParquetScan   ScanMeasurement    `json:"parquet_projection_scan"`
	RoundTrip     RoundTrip          `json:"round_trip"`
	Resources     Resources          `json:"resources"`
	Extrapolation Extrapolation      `json:"extrapolation"`
	Gates         Gates              `json:"gates"`
	Verdict       string             `json:"verdict"`
	Diagnostics   []string           `json:"diagnostics"`
}

// Source identifies the bounded input without claiming a whole-file digest.
type Source struct {
	Path                  string `json:"path"`
	CompressedBytes       int64  `json:"compressed_bytes"`
	ExpectedTotalRows     uint64 `json:"expected_total_rows"`
	ExpectedSHA256        string `json:"expected_sha256,omitempty"`
	SampleFirstRowOrdinal uint64 `json:"sample_first_row_ordinal"`
	SampleLastRowOrdinal  uint64 `json:"sample_last_row_ordinal"`
}

// Configuration records the physical candidate and benchmark boundaries.
type Configuration struct {
	ColumnCount      int      `json:"column_count"`
	Representation   string   `json:"representation"`
	ParquetLibrary   string   `json:"parquet_library"`
	Compression      string   `json:"compression"`
	RowsPerFile      uint64   `json:"rows_per_file"`
	RowsPerRowGroup  uint64   `json:"rows_per_row_group"`
	ProjectionFields []string `json:"projection_fields"`
}

// LayoutMeasurement records one equal-row encoding pass.
type LayoutMeasurement struct {
	Rows                 uint64  `json:"rows"`
	CompressedBytes      int64   `json:"compressed_bytes"`
	UncompressedBytes    uint64  `json:"uncompressed_bytes,omitempty"`
	BytesPerRow          float64 `json:"bytes_per_row"`
	DurationMilliseconds int64   `json:"duration_ms"`
	RowsPerSecond        float64 `json:"rows_per_second"`
	SHA256               string  `json:"sha256,omitempty"`
	SemanticSHA256       string  `json:"semantic_sha256"`
}

// ParquetMeasurement extends the common encoding measures with immutable
// ordinal-range shards.
type ParquetMeasurement struct {
	LayoutMeasurement
	Files     []ParquetFile `json:"files"`
	RowGroups uint64        `json:"row_groups"`
}

// ParquetFile maps physical row order to exact source-row ordinals.
type ParquetFile struct {
	Path            string `json:"path"`
	FirstRowOrdinal uint64 `json:"first_row_ordinal"`
	LastRowOrdinal  uint64 `json:"last_row_ordinal"`
	Rows            uint64 `json:"rows"`
	RowGroups       uint64 `json:"row_groups"`
	Bytes           int64  `json:"bytes"`
	SHA256          string `json:"sha256"`
}

// ScanMeasurement records the five-column receipt-decision projection.
type ScanMeasurement struct {
	Rows                 uint64         `json:"rows"`
	DurationMilliseconds int64          `json:"duration_ms"`
	RowsPerSecond        float64        `json:"rows_per_second"`
	ProjectionSHA256     string         `json:"projection_sha256"`
	Decisions            DecisionCounts `json:"decisions"`
}

// DecisionCounts mirrors the accepted receipt predicate without candidate
// routing. It lets the two physical readers prove calculation equivalence.
type DecisionCounts struct {
	Included                  uint64 `json:"included"`
	ExcludedNonIndividual     uint64 `json:"excluded_non_individual"`
	ExcludedMemoSubtotal      uint64 `json:"excluded_memo_subtotal"`
	UnresolvedIndividualClass uint64 `json:"unresolved_individual_class"`
	UnresolvedAmount          uint64 `json:"unresolved_amount"`
	IncludedAmountMinorUnits  string `json:"included_amount_minor_units"`
}

// RoundTrip records lossless logical-value and projection equivalence.
type RoundTrip struct {
	FullSemanticDigestEqual      bool    `json:"full_semantic_digest_equal"`
	FullScanDurationMilliseconds int64   `json:"full_scan_duration_ms"`
	FullScanRowsPerSecond        float64 `json:"full_scan_rows_per_second"`
	ProjectionDigestEqual        bool    `json:"projection_digest_equal"`
	DecisionCountsEqual          bool    `json:"decision_counts_equal"`
}

// Resources records the benchmark process high-water mark. It excludes the
// kernel page cache; the Docker cgroup remains the outer memory circuit breaker.
type Resources struct {
	ProcessPeakRSSBytes uint64 `json:"process_peak_rss_bytes"`
	GoMemorySysBytes    uint64 `json:"go_memory_sys_bytes"`
}

// Extrapolation projects only from this bounded sequential sample. It is not
// accepted as a capacity claim until the ten-million-row gate runs.
type Extrapolation struct {
	ParquetToEqualRowZstdRatio float64 `json:"parquet_to_equal_row_zstd_ratio"`
	ProjectedParquetBytes      uint64  `json:"projected_parquet_bytes"`
	ProjectedParquetWriteMS    int64   `json:"projected_parquet_write_ms"`
	ProjectionScanSpeedup      float64 `json:"projection_scan_speedup"`
}

// Gates are deliberately strict enough to prevent a fixture-scale format
// choice from becoming production architecture.
type Gates struct {
	MinimumDecisionRows       uint64  `json:"minimum_decision_rows"`
	MaximumSizeRatio          float64 `json:"maximum_size_ratio"`
	MinimumWriteRowsPerSecond float64 `json:"minimum_write_rows_per_second"`
	MaximumProjectedWriteMS   int64   `json:"maximum_projected_write_ms"`
	MinimumScanSpeedup        float64 `json:"minimum_scan_speedup"`
	MaximumProcessPeakRSS     uint64  `json:"maximum_process_peak_rss_bytes"`
	EnoughRows                bool    `json:"enough_rows"`
	Size                      bool    `json:"size"`
	WriteThroughput           bool    `json:"write_throughput"`
	ProjectedWriteDuration    bool    `json:"projected_write_duration"`
	ColumnPruning             bool    `json:"column_pruning"`
	Memory                    bool    `json:"memory"`
	RoundTrip                 bool    `json:"round_trip"`
}
