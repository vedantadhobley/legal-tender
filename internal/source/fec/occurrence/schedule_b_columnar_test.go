package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
)

func TestPublishScheduleBColumnarFactsPreservesRowsAndTypedProjection(t *testing.T) {
	storageRoot := t.TempDir()
	relation := "disclosure.fec_fitem_sched_b_2023_2024"
	rows := []string{
		scheduleBFixtureRow("100", map[string]string{"cmte_id": "C00000001", "recipient_cmte_id": "C00000002", "disb_amt": "42.50", "disb_dt": "2024-01-02 00:00:00", "comm_dt": "2024-01-01 12:34:56.123456", "rpt_yr": "2024", "pg_date": "2024-01-03 04:05:06"}),
		scheduleBFixtureRow("101", map[string]string{"disb_amt": "-1.00", "semi_an_bundled_refund": "0.25", "memo_cd": "X"}),
		scheduleBFixtureRow("102", map[string]string{"recipient_nm": "PAYEE\\tWITH TAB", "disb_amt": "0.00"}),
	}
	sqlPath := filepath.Join(storageRoot, "fixture.sql")
	columns := scheduleb.Columns()
	names := make([]string, len(columns))
	for index, column := range columns {
		names[index] = column.Name
	}
	sql := "COPY " + relation + " (" + strings.Join(names, ", ") + ") FROM stdin;\n" + strings.Join(rows, "\n") + "\n\\.\n"
	if err := os.WriteFile(sqlPath, []byte(sql), 0o640); err != nil {
		t.Fatal(err)
	}
	pgRestorePath := filepath.Join(storageRoot, "pg_restore-fixture")
	script := fmt.Sprintf("#!/bin/sh\nexec /bin/cp %q /dev/stdout\n", sqlPath)
	if err := os.WriteFile(pgRestorePath, []byte(script), 0o750); err != nil {
		t.Fatal(err)
	}
	archive := []byte("PGDMP fixture Schedule B archive\n")
	archiveKey := "raw/fec/schedule-b/artifacts/sha256/fixture.dump"
	archivePath := filepath.Join(storageRoot, filepath.FromSlash(archiveKey))
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, archive, 0o640); err != nil {
		t.Fatal(err)
	}
	archiveDigest := sha256.Sum256(archive)
	releaseManifest := scheduleBFixtureRelease(t, archiveKey, int64(len(archive)), hex.EncodeToString(archiveDigest[:]))
	releaseContent, err := jsonMarshalIndentLine(releaseManifest)
	if err != nil {
		t.Fatal(err)
	}
	releaseDigest := sha256.Sum256(releaseContent)

	manifest, err := PublishScheduleBColumnarFacts(context.Background(), releaseManifest, hex.EncodeToString(releaseDigest[:]), "2024", "fixture-publish", ScheduleBColumnarOptions{
		StorageRoot: storageRoot, PGRestorePath: pgRestorePath, WorkDir: filepath.Join(storageRoot, "work"),
		RowsPerShard: 2, RowsPerRowGroup: 1, FreeFloorBytes: 1, WorkingMarginBytes: 1,
		DiskAvailable: func(string) (uint64, error) { return 1 << 30, nil },
		Clock:         func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Counts.SourceRows != 3 || manifest.Counts.Facts != 3 || manifest.Counts.UniqueSubIDs != 3 || len(manifest.Shards) != 2 {
		t.Fatalf("unexpected Schedule B publication: %+v", manifest)
	}
	if manifest.SourceReplay.COPYBytes != uint64(len(strings.Join(rows, "\n")+"\n")) || manifest.Relation != relation {
		t.Fatalf("unexpected source replay: %+v", manifest.SourceReplay)
	}
	loaded, loadedDigest, loadErr := LoadPublishedScheduleBColumnarManifest(context.Background(), storageRoot, filepath.Join(storageRoot, scheduleBColumnarBase(), "current", "2024.json"))
	if loadErr != nil || !reflect.DeepEqual(loaded, manifest) || len(loadedDigest) != 64 {
		t.Fatalf("downstream Schedule B loading failed: %v", loadErr)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := LoadPublishedScheduleBColumnarManifest(canceled, storageRoot, filepath.Join(storageRoot, scheduleBColumnarBase(), "current", "2024.json")); !errors.Is(err, context.Canceled) {
		t.Fatalf("downstream loader ignored cancellation: %v", err)
	}
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range manifest.Shards {
		verification, err := schedulebparquet.VerifyFile(filepath.Join(storageRoot, filepath.FromSlash(shard.StorageKey)), schema)
		if err != nil {
			t.Fatal(err)
		}
		if verification.Rows != shard.Facts || verification.SemanticSHA256 != shard.SemanticSHA256 {
			t.Fatalf("unexpected shard verification: %+v", verification)
		}
	}
	firstPath := filepath.Join(storageRoot, filepath.FromSlash(manifest.Shards[0].StorageKey))
	file, err := os.Open(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewReader(file)
	buffer := make([]parquet.Row, 2)
	count, readErr := reader.ReadRows(buffer)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		t.Fatal(readErr)
	}
	if count != 2 {
		t.Fatalf("read %d rows; want 2", count)
	}
	value := func(row int, name string) parquet.Value {
		index, ok := schema.ColumnIndex(name)
		if !ok {
			t.Fatalf("column %s is absent", name)
		}
		for _, candidate := range buffer[row] {
			if candidate.Column() == index {
				return candidate
			}
		}
		t.Fatalf("row omits column %s", name)
		return parquet.Value{}
	}
	if got := string(value(0, "disb_amt").ByteArray()); got != "42.50" {
		t.Fatalf("source amount = %q; want 42.50", got)
	}
	if got := value(0, schedulebparquet.ColumnDisbursementAmountMinorUnits).Int64(); got != 4250 {
		t.Fatalf("typed minor units = %d; want 4250", got)
	}
	if !value(1, schedulebparquet.ColumnMemoedSubtotal).Boolean() || value(0, schedulebparquet.ColumnSourceRowOrdinal).Int64() != 1 {
		t.Fatalf("memo projection or locator was not preserved")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	replayed, err := PublishScheduleBColumnarFacts(context.Background(), releaseManifest, hex.EncodeToString(releaseDigest[:]), "2024", "another-run", ScheduleBColumnarOptions{
		StorageRoot: storageRoot, PGRestorePath: filepath.Join(storageRoot, "does-not-exist"), RowsPerShard: 2, RowsPerRowGroup: 1,
		FreeFloorBytes: 1, WorkingMarginBytes: 1, DiskAvailable: func(string) (uint64, error) { return 1 << 30, nil },
	})
	if err != nil || replayed.FactSetID != manifest.FactSetID {
		t.Fatalf("idempotent publication failed: %+v, %v", replayed, err)
	}
	legacy := manifest
	legacy.FactSetID = legacyScheduleBColumnarFactSetID(
		legacy.SourceReleaseID,
		legacy.SourceReleaseManifestSHA256,
		legacy.SourceArtifactSHA256,
		legacy.Relation,
		legacy.Cycle,
		legacy.Configuration,
	)
	legacy.Checks = scheduleBColumnarChecks(legacy)
	newManifestPath := filepath.Join(storageRoot, scheduleBColumnarBase(), "manifests", manifest.FactSetID+".json")
	if err := os.Remove(newManifestPath); err != nil {
		t.Fatal(err)
	}
	legacyManifestPath := filepath.Join(storageRoot, scheduleBColumnarBase(), "manifests", legacy.FactSetID+".json")
	if err := writeAtomicJSON(legacyManifestPath, legacy); err != nil {
		t.Fatal(err)
	}
	currentFactPath := filepath.Join(storageRoot, scheduleBColumnarBase(), "current", "2024.json")
	if err := writeAtomicJSON(currentFactPath, legacy); err != nil {
		t.Fatal(err)
	}
	migrated, err := PublishScheduleBColumnarFacts(context.Background(), releaseManifest, hex.EncodeToString(releaseDigest[:]), "2024", "stable-identity-migration", ScheduleBColumnarOptions{
		StorageRoot: storageRoot, PGRestorePath: filepath.Join(storageRoot, "does-not-exist"), RowsPerShard: 2, RowsPerRowGroup: 1,
		FreeFloorBytes: 1, WorkingMarginBytes: 1, DiskAvailable: func(string) (uint64, error) { return 1 << 30, nil },
	})
	if err != nil || migrated.FactSetID != manifest.FactSetID || !reflect.DeepEqual(migrated.Shards, manifest.Shards) {
		t.Fatalf("legacy release-bound identity was not adopted: %+v, %v", migrated, err)
	}
	releaseHistoryPath := filepath.Join(storageRoot, "releases", "fec", "manifests", releaseManifest.ReleaseID+".json")
	if err := os.MkdirAll(filepath.Dir(releaseHistoryPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(releaseHistoryPath, releaseContent, 0o640); err != nil {
		t.Fatal(err)
	}
	nextRelease := releaseManifest
	nextRelease.ReleaseID = "fec-" + strings.Repeat("d", 64)
	nextRelease.PriorReleaseID = releaseManifest.ReleaseID
	nextRelease.RunID = "fixture-next-release"
	nextRelease.PublishedAt = nextRelease.PublishedAt.Add(time.Hour)
	nextContent, err := jsonMarshalIndentLine(nextRelease)
	if err != nil {
		t.Fatal(err)
	}
	nextDigest := sha256.Sum256(nextContent)
	stable, err := PublishScheduleBColumnarFacts(context.Background(), nextRelease, hex.EncodeToString(nextDigest[:]), "2024", "source-stable-replay", ScheduleBColumnarOptions{
		StorageRoot: storageRoot, PGRestorePath: filepath.Join(storageRoot, "does-not-exist"), RowsPerShard: 2, RowsPerRowGroup: 1,
		FreeFloorBytes: 1, WorkingMarginBytes: 1, DiskAvailable: func(string) (uint64, error) { return 1 << 30, nil },
	})
	if err != nil || stable.FactSetID != manifest.FactSetID || stable.SourceReleaseID != releaseManifest.ReleaseID {
		t.Fatalf("source-stable release replay rebuilt or rebased facts: %+v, %v", stable, err)
	}

	corrupt, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	corrupt[len(corrupt)/2] ^= 0xff
	if err := os.WriteFile(firstPath, corrupt, 0o640); err != nil {
		t.Fatal(err)
	}
	_, err = PublishScheduleBColumnarFacts(context.Background(), releaseManifest, hex.EncodeToString(releaseDigest[:]), "2024", "corrupt-replay", ScheduleBColumnarOptions{
		StorageRoot: storageRoot, PGRestorePath: filepath.Join(storageRoot, "does-not-exist"), RowsPerShard: 2, RowsPerRowGroup: 1,
		FreeFloorBytes: 1, WorkingMarginBytes: 1, DiskAvailable: func(string) (uint64, error) { return 1 << 30, nil },
	})
	if err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("same-size shard corruption was not rejected: %v", err)
	}
	if _, _, err := LoadPublishedScheduleBColumnarManifest(context.Background(), storageRoot, currentFactPath); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("downstream loader accepted corrupt Schedule B backing: %v", err)
	}
}

func scheduleBFixtureRow(subID string, overrides map[string]string) string {
	values := make([]string, scheduleb.FieldCount)
	for index := range values {
		values[index] = `\N`
	}
	set := func(name, value string) {
		index, ok := scheduleb.ColumnIndex(name)
		if !ok {
			panic("unknown Schedule B fixture column " + name)
		}
		values[index] = value
	}
	set("sub_id", subID)
	set("filing_form", "F3X")
	set("two_year_transaction_period", "2024")
	for name, value := range overrides {
		set(name, value)
	}
	return strings.Join(values, "\t")
}

func scheduleBFixtureRelease(t *testing.T, scheduleBStorageKey string, scheduleBBytes int64, scheduleBSHA string) fecrelease.ReleaseManifest {
	t.Helper()
	inventory := fecrelease.ActiveInventory()
	selectedAt := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	artifactSHAs := make(map[string]string, len(inventory.Sources))
	artifacts := make([]fecrelease.PublishedArtifact, 0, len(inventory.Sources))
	for index, source := range inventory.Sources {
		byteCount := int64(1)
		digest := fmt.Sprintf("%064x", index+1)
		storageKey := "raw/fec/fixture/" + digest
		if source.SourceID == fecrelease.ScheduleBSourceID {
			byteCount, digest, storageKey = scheduleBBytes, scheduleBSHA, scheduleBStorageKey
		}
		artifactSHAs[source.SourceID] = digest
		contentLength := byteCount
		artifacts = append(artifacts, fecrelease.PublishedArtifact{
			SelectedSource: fecrelease.SelectedSource{SourceID: source.SourceID, RequestURL: source.RequestURL, FinalURL: source.RequestURL, ObservedAt: selectedAt, VersionIdentity: "etag:fixture-etag", VersionBasis: "etag", ETag: "fixture-etag", ContentLength: &contentLength},
			ByteCount:      byteCount, SHA256: digest, StorageKey: storageKey, AcquiredAt: selectedAt.Add(time.Minute),
		})
	}
	outputs := make([]fecrelease.StagedOutput, 0, 25)
	addOutput := func(sourceID, period, kind, selection string, fieldCount *int) {
		digest := strings.Repeat("a", 64)
		storageBase, suffix := "raw/fec/selected", ".zst"
		if sourceID == fecrelease.ScheduleASourceID {
			storageBase, suffix = "raw/fec/schedule-a/extracts", ".copy.zst"
		} else if sourceID == fecrelease.ScheduleESourceID {
			storageBase, suffix = "raw/fec/schedule-e/extracts", ".copy.zst"
		}
		output := fecrelease.StagedOutput{SourceID: sourceID, SelectionKind: kind, Selection: selection, Period: period, Disposition: "staged", SourceArtifactSHA256: artifactSHAs[sourceID], UncompressedByteCount: 1, UncompressedSHA256: strings.Repeat("b", 64), Compression: "zstd", CompressionLevel: 3, CompressedByteCount: 1, CompressedSHA256: digest, StorageKey: storageBase + "/sha256/aa/" + digest + suffix, DecompressionValidated: true, StagedAt: selectedAt.Add(time.Minute), ContractedFieldCount: fieldCount}
		if kind == "member" {
			output.Representation = "selected_member_zstd"
		} else {
			output.Representation = "postgresql_copy_text_data_rows_zstd"
			rowCount := uint64(1)
			output.RowCount = &rowCount
		}
		outputs = append(outputs, output)
	}
	for _, source := range inventory.Sources {
		for _, member := range source.SelectedMembers {
			addOutput(source.SourceID, source.Periods[0], "member", member, nil)
		}
		for _, relation := range source.RelationSelections {
			if relation.Materialization == fecrelease.RelationMaterializationArchiveDirect {
				continue
			}
			fieldCount := relation.FieldCount
			addOutput(source.SourceID, relation.Scope, "relation", relation.Name, &fieldCount)
		}
	}
	checks := []fecrelease.ReleaseCheck{
		{ID: "input_identity", Passed: true, Severity: "block", Detail: "fixture"},
		{ID: "source_artifact_membership", Passed: true, Severity: "block", Detail: "fixture"},
		{ID: "selected_output_membership", Passed: true, Severity: "block", Detail: "fixture"},
		{ID: "output_integrity", Passed: true, Severity: "block", Detail: "fixture"},
		{ID: "storage_budget", Passed: true, Severity: "block", Detail: "fixture"},
	}
	manifest := fecrelease.ReleaseManifest{Schema: "release-manifest.schema.json", SchemaVersion: fecrelease.ManifestSchemaVersion, InventoryVersion: inventory.InventoryVersion, ReleaseID: "fec-" + strings.Repeat("c", 64), RunID: "fixture-release", PlanSHA256: strings.Repeat("d", 64), AcquisitionSHA256: strings.Repeat("e", 64), StageSHA256: strings.Repeat("f", 64), State: "published", SelectedAt: selectedAt, PublishedAt: selectedAt.Add(2 * time.Minute), Periods: append([]string(nil), inventory.Periods...), Artifacts: artifacts, StagedOutputs: outputs, Checks: checks}
	if issues := fecrelease.ValidateKnownManifest(manifest); len(issues) != 0 {
		t.Fatalf("invalid v3 release fixture: %+v", issues)
	}
	return manifest
}

func jsonMarshalIndentLine(value any) ([]byte, error) {
	content, err := json.MarshalIndent(value, "", "  ")
	return append(content, '\n'), err
}
