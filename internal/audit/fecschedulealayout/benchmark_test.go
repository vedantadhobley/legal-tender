package fecschedulealayout

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestRunProvesRoundTripAndProjectionEquivalence(t *testing.T) {
	root := repositoryRoot(t)
	fixtureRoot := filepath.Join(root, "contracts", "sources", "fec", "schedule-a", "v1", "fixtures", "dump-2026-08-23")
	sourcePath := filepath.Join(t.TempDir(), "sample.copy.zst")
	outputPath := filepath.Join(t.TempDir(), "benchmark")

	output, err := os.Create(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(output, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"targeted-individual.copy", "targeted-memo-committee.copy", "negative-adjustment.copy"} {
		content, err := os.ReadFile(filepath.Join(fixtureRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := encoder.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := Run(context.Background(), Options{
		SourcePath: sourcePath, Cycle: "2026", Rows: 3, ExpectedTotalRows: 3,
		OutputDirectory: outputPath, RowsPerFile: 2, RowsPerRowGroup: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "complete" || result.Verdict != "provisional" {
		t.Fatalf("unexpected result state=%s verdict=%s round_trip=%+v source=%+v parquet=%+v", result.State, result.Verdict, result.RoundTrip, result.SourceScan.Decisions, result.ParquetScan.Decisions)
	}
	if result.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %q; want %q", result.SchemaVersion, SchemaVersion)
	}
	if !result.RoundTrip.FullSemanticDigestEqual || !result.RoundTrip.ProjectionDigestEqual || !result.RoundTrip.DecisionCountsEqual {
		t.Fatalf("round trip failed: %+v", result.RoundTrip)
	}
	if result.ZstdCOPY.Rows != 3 || result.Parquet.Rows != 3 || result.SourceScan.Rows != 3 || result.ParquetScan.Rows != 3 {
		t.Fatalf("row conservation failed: %+v", result)
	}
	if len(result.Parquet.Files) != 2 || result.Parquet.Files[0].FirstRowOrdinal != 1 || result.Parquet.Files[0].LastRowOrdinal != 2 || result.Parquet.Files[1].FirstRowOrdinal != 3 {
		t.Fatalf("unexpected shards: %+v", result.Parquet.Files)
	}
	if result.SourceScan.Decisions != result.ParquetScan.Decisions {
		t.Fatalf("decision mismatch: source=%+v parquet=%+v", result.SourceScan.Decisions, result.ParquetScan.Decisions)
	}
	wantProjectedBytes := uint64(math.Round(float64(result.Source.CompressedBytes) * result.Extrapolation.ParquetToEqualRowZstdRatio))
	if result.Extrapolation.ProjectedParquetBytes != wantProjectedBytes {
		t.Fatalf("projected bytes = %d; want source-size ratio estimate %d", result.Extrapolation.ProjectedParquetBytes, wantProjectedBytes)
	}
	if !result.Gates.ProjectedWriteDuration || result.Gates.MaximumProjectedWriteMS != (45*time.Minute).Milliseconds() {
		t.Fatalf("unexpected projected-write gate: %+v", result.Gates)
	}
	if _, err := os.Stat(filepath.Join(outputPath, "benchmark.json")); err != nil {
		t.Fatalf("benchmark manifest missing: %v", err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", ".."))
}
