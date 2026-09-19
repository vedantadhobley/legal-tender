package occurrence

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

func TestPublishScheduleAColumnarFactsPreservesSourceAndTypedProjection(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := append([]byte(nil), exactFixture(t, "targeted-individual.copy")...)
	rows = append(rows, exactFixture(t, "targeted-memo-committee.copy")...)
	rows = append(rows, exactFixture(t, "negative-adjustment.copy")...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "schedule-columnar", "", "schedule-columnar-source", rows)
	occurrenceManifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-columnar-occurrence", Options{
		StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", occurrenceManifest.OccurrenceSetID+".json")
	manifest, err := PublishScheduleAColumnarFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-columnar-facts", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 2, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != ScheduleAColumnarFactSetSchemaVersion || manifest.PhysicalSchemaVersion != scheduleaparquet.PhysicalSchemaVersion {
		t.Fatalf("unexpected columnar contracts: %+v", manifest)
	}
	if manifest.Counts.SourceOccurrences != 3 || manifest.Counts.Facts != 3 || manifest.Counts.ValidFacts != 3 || manifest.Counts.ExcludedOccurrences != 0 {
		t.Fatalf("unexpected columnar counts: %+v", manifest.Counts)
	}
	if len(manifest.Shards) != 2 || manifest.Shards[0].SourceRows != 2 || manifest.Shards[0].Facts != 2 || manifest.Shards[1].SourceRows != 1 {
		t.Fatalf("unexpected columnar shards: %+v", manifest.Shards)
	}
	if manifest.Shards[0].FirstRawByteOffset != 0 || manifest.Shards[0].LastRawByteEnd != uint64(len(rows))-uint64(len(exactFixture(t, "negative-adjustment.copy"))) ||
		manifest.Shards[1].FirstRawByteOffset != manifest.Shards[0].LastRawByteEnd || manifest.Shards[1].LastRawByteEnd != uint64(len(rows)) {
		t.Fatalf("unexpected source byte ranges: %+v", manifest.Shards)
	}

	physical, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	firstPath, err := resolveStorageKey(storageRoot, manifest.Shards[0].StorageKey)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := scheduleaparquet.VerifyFile(firstPath, physical)
	if err != nil {
		t.Fatal(err)
	}
	if verification.Rows != 2 || verification.SemanticSHA256 != manifest.Shards[0].SemanticSHA256 {
		t.Fatalf("unexpected shard verification: %+v", verification)
	}
	file, err := os.Open(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewReader(file)
	buffer := make([]parquet.Row, 1)
	count, err := reader.ReadRows(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		if count == 0 {
			t.Fatal(err)
		}
	}
	if count != 1 {
		t.Fatalf("read %d rows; want 1", count)
	}
	value := func(name string) parquet.Value {
		index, ok := physical.ColumnIndex(name)
		if !ok {
			t.Fatalf("column %s is absent", name)
		}
		for _, candidate := range buffer[0] {
			if candidate.Column() == index {
				return candidate
			}
		}
		t.Fatalf("row omits column %s", name)
		return parquet.Value{}
	}
	if got := string(value("contb_receipt_amt").ByteArray()); got != "50.00" {
		t.Fatalf("source amount = %q; want 50.00", got)
	}
	if got := value(scheduleaparquet.ColumnReceiptAmountMinorUnits).Int64(); got != 5000 {
		t.Fatalf("typed minor units = %d; want 5000", got)
	}
	if !value("is_individual").Boolean() || value(scheduleaparquet.ColumnSourceRowOrdinal).Int64() != 1 || value(scheduleaparquet.ColumnSourceRawByteOffset).Int64() != 0 {
		t.Fatalf("source boolean or locator was not preserved")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	replayed, err := PublishScheduleAColumnarFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "different-run-is-idempotent", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 2, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, replayed) {
		t.Fatalf("idempotent publication changed manifest")
	}

	content, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	content[len(content)/2] ^= 0xff
	if err := os.WriteFile(firstPath, content, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishScheduleAColumnarFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "corrupt-shard-replay", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 2, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
	}); err == nil || !strings.Contains(err.Error(), "does not match its published digest") {
		t.Fatalf("corrupt published shard error = %v", err)
	}
}

func TestPublishScheduleAColumnarFactsResumesCompletedShards(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := append([]byte(nil), exactFixture(t, "targeted-individual.copy")...)
	rows = append(rows, exactFixture(t, "targeted-memo-committee.copy")...)
	rows = append(rows, exactFixture(t, "negative-adjustment.copy")...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "schedule-columnar-resume", "", "schedule-columnar-resume-source", rows)
	occurrenceManifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-columnar-resume-occurrence", Options{
		StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", occurrenceManifest.OccurrenceSetID+".json")
	ctx, cancel := context.WithCancel(context.Background())
	_, err = PublishScheduleAColumnarFacts(ctx, releaseManifest, releaseDigest, occurrencePath, "schedule-columnar-interrupted", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 1, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
		Progress: func(message string) {
			if strings.Contains(message, "columnar shard 0") {
				cancel()
			}
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted publication error = %v; want context cancellation", err)
	}
	physical, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	configuration := ScheduleAColumnarConfiguration{
		RowsPerShard: 1, RowsPerRowGroup: 1, ColumnCount: physical.ColumnCount(), Compression: "parquet-zstd",
		Locator: "source artifact plus one-based row ordinal, raw byte offset, and raw byte length",
	}
	factSetID := scheduleAColumnarFactSetID(occurrenceManifest.OccurrenceSetID, manifestFileSHA256(t, occurrencePath), configuration)
	checkpointPath := filepath.Join(storageRoot, scheduleAColumnarBase(), "staging", factSetID, "checkpoint.json")
	checkpoint, err := readScheduleAColumnarCheckpoint(checkpointPath, factSetID, scheduleAOccurrenceInput{
		OccurrenceSetID: occurrenceManifest.OccurrenceSetID, Cycle: occurrenceManifest.Cycle,
	}, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Shards) != 1 {
		t.Fatalf("checkpoint shards = %d; want 1", len(checkpoint.Shards))
	}
	firstDigest := checkpoint.Shards[0].SHA256

	manifest, err := PublishScheduleAColumnarFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-columnar-resumed", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 1, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Shards) != 3 || manifest.Shards[0].SHA256 != firstDigest {
		t.Fatalf("completed shard was not reused: %+v", manifest.Shards)
	}
	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Fatalf("completed checkpoint still exists: %v", err)
	}
}

func TestPublishScheduleAColumnarFactsConsumesCompactOccurrenceSelection(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	unique := exactFixture(t, "targeted-individual.copy")
	duplicate := exactFixture(t, "negative-adjustment.copy")
	rows := append(append(append(append([]byte(nil), unique...), duplicate...), duplicate...), []byte("too\tfew\tfields\n")...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "schedule-columnar-compact", "", "schedule-columnar-compact-source", rows)
	occurrenceManifest, err := PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "schedule-columnar-compact-occurrence",
		Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, compactEvidenceBase, "manifests", occurrenceManifest.OccurrenceSetID+".json")
	manifest, err := PublishScheduleAColumnarFacts(
		context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-columnar-compact-facts",
		Options{StorageRoot: storageRoot, RowsPerColumnarShard: 4, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.OccurrenceSetID != occurrenceManifest.OccurrenceSetID || manifest.Counts.SourceOccurrences != 4 ||
		manifest.Counts.Facts != 1 || manifest.Counts.ExcludedOccurrences != 3 || manifest.Counts.SourceInvalid != 1 ||
		manifest.Counts.SourceDuplicates != 2 {
		t.Fatalf("compact occurrence selection was not conserved: %+v", manifest)
	}
	if len(manifest.Shards) != 1 || manifest.Shards[0].SourceRows != 4 || manifest.Shards[0].Facts != 1 {
		t.Fatalf("unexpected compact-backed columnar shards: %+v", manifest.Shards)
	}
}

func TestPublishScheduleAColumnarFactsAdoptsVerifiedShardsForCleanCompactAncestry(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := append([]byte(nil), exactFixture(t, "targeted-individual.copy")...)
	rows = append(rows, exactFixture(t, "negative-adjustment.copy")...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "schedule-columnar-adopt", "", "schedule-columnar-adopt-source", rows)
	legacy, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-columnar-adopt-legacy", Options{
		StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", legacy.OccurrenceSetID+".json")
	first, err := PublishScheduleAColumnarFacts(context.Background(), releaseManifest, releaseDigest, legacyPath, "schedule-columnar-adopt-first", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 1, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	compact, err := PublishScheduleACompactOccurrences(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-columnar-adopt-compact", Options{
		StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	compactPath := filepath.Join(storageRoot, compactEvidenceBase, "manifests", compact.OccurrenceSetID+".json")
	second, err := PublishScheduleAColumnarFacts(context.Background(), releaseManifest, releaseDigest, compactPath, "schedule-columnar-adopt-second", Options{
		StorageRoot: storageRoot, RowsPerColumnarShard: 1, RowsPerColumnarRowGroup: 1, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.FactSetID == first.FactSetID || second.OccurrenceSetID != compact.OccurrenceSetID || !reflect.DeepEqual(second.Shards, first.Shards) ||
		second.SourceReplay != first.SourceReplay || second.RunID != "schedule-columnar-adopt-second" {
		t.Fatalf("verified columnar shards were not adopted exactly: first=%+v second=%+v", first, second)
	}
}

func manifestFileSHA256(t *testing.T, path string) string {
	t.Helper()
	digest, err := scheduleAColumnarFileSHA256(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
