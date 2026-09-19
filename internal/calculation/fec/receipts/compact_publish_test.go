package receipts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestPublishCompactUsesPredicateAndSparseExceptions(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	sourceReleaseID := "fec-" + repeatedHex("a")
	columnarPath := writeCompactColumnarInput(t, storageRoot, sourceReleaseID)
	linkagePath := writeClassicFactInput(t, storageRoot, sourceReleaseID, "candidate-committee-linkage", "fec.candidate_committee_linkage.v1", repeatedHex("b"), []fecoccurrence.ClassicFact{{
		SchemaVersion: fecoccurrence.ClassicFactSchemaVersion, FactID: "link-1", FactType: "fec.candidate_committee_linkage.v1", Dataset: "candidate-committee-linkage", Cycle: "2024", SourceReleaseID: sourceReleaseID, State: "valid",
		TypedFields: fecoccurrence.LinkageTypedFields{CandidateID: "H0AA00005", CommitteeID: "C00392928", DesignationCode: "A"},
	}})
	weballPath := writeClassicFactInput(t, storageRoot, sourceReleaseID, "all-candidates-summary", "fec.candidate_summary_all.v1", repeatedHex("c"), []fecoccurrence.ClassicFact{
		summaryClassicFact(sourceReleaseID, "all-candidates-summary", "fec.candidate_summary_all.v1", "summary-all", "5000", "10000"),
	})
	weblPath := writeClassicFactInput(t, storageRoot, sourceReleaseID, "current-campaigns-summary", "fec.campaign_summary.v1", repeatedHex("d"), []fecoccurrence.ClassicFact{
		summaryClassicFact(sourceReleaseID, "current-campaigns-summary", "fec.campaign_summary.v1", "summary-current", "5000", "10000"),
	})
	input := CompactPublishInput{
		ColumnarFactManifestPath: columnarPath, LinkageFactManifestPath: linkagePath,
		AllCandidatesFactManifestPath: weballPath, CurrentCampaignsManifestPath: weblPath,
	}
	fixed := time.Date(2026, 8, 31, 16, 0, 0, 0, time.UTC)
	manifest, err := PublishCompact(context.Background(), input, "compact-calculation", CompactPublishOptions{
		StorageRoot: storageRoot, Clock: func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.DecisionCounts.Included != 1 || manifest.DecisionCounts.UnresolvedAmount != 1 || manifest.DecisionCounts.IncludedAmountMinorUnits != "5000" ||
		manifest.ResultCounts.SourceRows != 2 || manifest.ResultCounts.RoutedRows != 2 || manifest.ResultCounts.Candidates != 1 || manifest.ResultCounts.PartialCandidates != 1 ||
		manifest.Exceptions.RecordCount != 1 || manifest.Results.RecordCount != 1 {
		t.Fatalf("unexpected compact calculation: %+v", manifest)
	}
	exceptions, err := storageartifact.Open[CompactMembershipException](context.Background(), storageRoot, manifest.Exceptions)
	if err != nil {
		t.Fatal(err)
	}
	exception, ok, err := exceptions.Next()
	if err != nil || !ok || exception.SourceRowOrdinal != 2 || exception.State != "unresolved_amount" {
		t.Fatalf("unexpected compact exception: ok=%v err=%v value=%+v", ok, err, exception)
	}
	if _, ok, err := exceptions.Next(); err != nil || ok {
		t.Fatalf("compact exception artifact did not end cleanly: ok=%v err=%v", ok, err)
	}
	if err := exceptions.Close(); err != nil {
		t.Fatal(err)
	}
	resultsReader, err := storageartifact.Open[Result](context.Background(), storageRoot, manifest.Results)
	if err != nil {
		t.Fatal(err)
	}
	result, ok, err := resultsReader.Next()
	if err != nil || !ok || result.CandidateID != "H0AA00005" || pointerValue(result.MoneyMeasure.Amount.LowerMinorUnits) != "5000" || result.State != "partial" {
		t.Fatalf("unexpected compact candidate result: ok=%v err=%v value=%+v", ok, err, result)
	}
	if _, ok, err := resultsReader.Next(); err != nil || ok {
		t.Fatalf("compact result artifact did not end cleanly: ok=%v err=%v", ok, err)
	}
	if err := resultsReader.Close(); err != nil {
		t.Fatal(err)
	}
	replayed, err := PublishCompact(context.Background(), input, "different-run", CompactPublishOptions{
		StorageRoot: storageRoot, Clock: func() time.Time { return fixed.Add(time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, replayed) {
		t.Fatalf("same-input compact calculation was not reused")
	}
}

func writeCompactColumnarInput(t *testing.T, storageRoot, sourceReleaseID string) string {
	t.Helper()
	fixturePath := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", "schedule-a", "v1", "fixtures", "dump-2026-08-23", "targeted-individual.copy")
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	rows := append(append([]byte(nil), fixture...), fixture...)
	decoder := schedulea.NewDecoder(bytes.NewReader(rows))
	physical, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(t.TempDir(), "compact-input.parquet")
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewWriter(file, physical.Parquet(), parquet.Compression(&parquetzstd.Codec{Level: parquetzstd.SpeedDefault, Concurrency: 1}))
	receiptDate := int32(time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC).Sub(time.Unix(0, 0).UTC()).Hours() / 24)
	var encoded parquet.Row
	var rawOffset uint64
	for decoder.Scan() {
		row := decoder.Row()
		amount := int64(5000)
		amountState := "reported_value"
		amountPointer := &amount
		if row.Number() == 2 {
			amountState = "invalid"
			amountPointer = nil
		}
		encoded, err = physical.Encode(encoded, row, scheduleaparquet.Metadata{
			SourceRowOrdinal: row.Number(), SourceRawByteOffset: rawOffset, SourceRawByteLength: uint64(len(row.Raw())),
		}, scheduleaparquet.Derived{
			NormalizationState: "valid", ReceiptAmountMinorUnits: amountPointer, ReceiptAmountState: amountState,
			AggregateYTDState: "reported_value", ReceiptDateDays: &receiptDate, TwoYearTransactionPeriod: 2024,
		})
		if err != nil {
			_ = writer.Close()
			_ = file.Close()
			t.Fatal(err)
		}
		if _, err := writer.WriteRows([]parquet.Row{encoded}); err != nil {
			_ = writer.Close()
			_ = file.Close()
			t.Fatal(err)
		}
		rawOffset += uint64(len(row.Raw()))
	}
	if err := decoder.Err(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(temporary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	sha := hex.EncodeToString(digest[:])
	storageKey := filepath.ToSlash(filepath.Join("facts", "fec", "schedule-a", "columnar", "shards", "sha256", sha[:2], sha+".parquet"))
	destination := filepath.Join(storageRoot, filepath.FromSlash(storageKey))
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, destination); err != nil {
		t.Fatal(err)
	}
	manifest := fecoccurrence.ScheduleAColumnarManifest{
		Schema: "manifest.schema.json", SchemaVersion: fecoccurrence.ScheduleAColumnarFactSetSchemaVersion,
		FactSetID: repeatedHex("e"), FactType: fecoccurrence.ScheduleAFactType, Cycle: "2024", SourceContract: fecoccurrence.ScheduleASourceContract,
		SourceReleaseID: sourceReleaseID, SourceReleaseManifestSHA256: repeatedHex("1"), OccurrenceSetID: repeatedHex("2"), OccurrenceManifestSHA256: repeatedHex("3"),
		RunID: "fixture", State: "published", NormalizerVersion: fecoccurrence.ScheduleANormalizerVersion,
		FactSchemaVersion: fecoccurrence.ScheduleAFactSchemaVersion, PhysicalSchemaVersion: scheduleaparquet.PhysicalSchemaVersion,
		PublisherVersion: fecoccurrence.ScheduleAColumnarPublisherVersion, ParquetLibrary: scheduleaparquet.LibraryVersion,
		PublishedAt: time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC),
		Counts:      fecoccurrence.ScheduleAFactCounts{SourceOccurrences: 2, Facts: 2, ValidFacts: 2},
		Shards: []fecoccurrence.ScheduleAColumnarShard{{
			Index: 0, FirstSourceRowOrdinal: 1, LastSourceRowOrdinal: 2, FirstRawByteOffset: 0, LastRawByteEnd: uint64(len(rows)),
			SourceRows: 2, Facts: 2, ValidFacts: 2, RowGroups: 1, Bytes: uint64(len(content)), SHA256: sha, SemanticSHA256: repeatedHex("4"), StorageKey: storageKey,
		}},
		Checks: []fecoccurrence.Check{{ID: "fixture", Passed: true, Severity: "block", Detail: "fixture"}},
	}
	path := filepath.Join(storageRoot, "facts", "fec", "schedule-a", "columnar", "manifests", manifest.FactSetID+".json")
	if err := writeAtomicJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	return path
}
