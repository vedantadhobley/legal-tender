package occurrence

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

var fixtureTime = time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)

func fixtureDiskAvailable(string) (uint64, error) { return 1 << 50, nil }

func TestPublishConservesOccurrencesAndEmitsSemanticChanges(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	negative := exactFixture(t, "negative-adjustment.copy")
	targeted := exactFixture(t, "targeted-individual.copy")
	firstRows := append(append([]byte(nil), negative...), targeted...)
	firstRelease, firstDigest := sourceReleaseFixture(t, storageRoot, "release-one", "", "schedule-a-one", firstRows)

	first, err := Publish(context.Background(), firstRelease, firstDigest, "2026", "occurrence-one", Options{
		StorageRoot:   storageRoot,
		ShardCount:    4,
		Clock:         func() time.Time { return fixtureTime },
		DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	if first.Counts.Total != 2 || first.Counts.Valid != 2 || first.Counts.Invalid != 0 || first.Counts.UniqueKeys != 2 {
		t.Fatalf("unexpected first counts: %+v", first.Counts)
	}
	if first.Changes.Added != 2 || first.Artifacts.Occurrences.RecordCount != 2 || first.Artifacts.Issues.RecordCount != 0 {
		t.Fatalf("unexpected first artifacts/changes: %+v %+v", first.Artifacts, first.Changes)
	}
	occurrences := readArtifactRecords[Occurrence](t, storageRoot, first.Artifacts.Occurrences)
	if len(occurrences) != 2 || occurrences[0].RowOrdinal != 1 || occurrences[1].RawByteOffset != uint64(len(negative)) {
		t.Fatalf("occurrence locators are not conserved: %+v", occurrences)
	}
	if occurrences[0].RawContentSHA256 == occurrences[1].RawContentSHA256 || occurrences[0].RecordVersionID == occurrences[1].RecordVersionID {
		t.Fatal("distinct source rows collapsed to one identity")
	}

	modifiedNegative := bytes.Replace(negative, []byte("-5000.00"), []byte("-4999.00"), 1)
	added := exactFixture(t, "action-code-n.copy")
	secondRows := append(append([]byte(nil), modifiedNegative...), added...)
	secondRelease, secondDigest := sourceReleaseFixture(t, storageRoot, "release-two", firstRelease.ReleaseID, "schedule-a-two", secondRows)
	second, err := Publish(context.Background(), secondRelease, secondDigest, "2026", "occurrence-two", Options{
		StorageRoot:   storageRoot,
		ShardCount:    4,
		Clock:         func() time.Time { return fixtureTime.Add(time.Hour) },
		DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	if second.PriorOccurrenceSetID != first.OccurrenceSetID || second.Changes.Added != 1 || second.Changes.Changed != 1 || second.Changes.Absent != 1 || second.Changes.Unchanged != 0 {
		t.Fatalf("unexpected semantic change set: %+v", second)
	}
	currentAfterSecond, err := readManifestIfPresent(filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "current", "2026.json"))
	if err != nil || currentAfterSecond == nil || currentAfterSecond.OccurrenceSetID != second.OccurrenceSetID {
		t.Fatalf("current occurrence pointer did not advance: current=%+v second=%s err=%v", currentAfterSecond, second.OccurrenceSetID, err)
	}
	replayOutput, replaySourceSHA, err := selectedScheduleAOutput(secondRelease, "2026")
	if err != nil {
		t.Fatal(err)
	}
	if second.SourceArtifactSHA256 != replaySourceSHA || second.StagedOutputSHA256 != replayOutput.CompressedSHA256 {
		t.Fatalf("returned second manifest has wrong identity: second=%s/%s release=%s/%s", second.SourceArtifactSHA256, second.StagedOutputSHA256, replaySourceSHA, replayOutput.CompressedSHA256)
	}
	if currentAfterSecond.SourceArtifactSHA256 != replaySourceSHA || currentAfterSecond.StagedOutputSHA256 != replayOutput.CompressedSHA256 {
		t.Fatalf("fixture no-op identity mismatch: current source/output=%s/%s replay=%s/%s", currentAfterSecond.SourceArtifactSHA256, currentAfterSecond.StagedOutputSHA256, replaySourceSHA, replayOutput.CompressedSHA256)
	}
	changes := readArtifactRecords[Change](t, storageRoot, second.Artifacts.Changes)
	seen := map[string]bool{}
	for _, change := range changes {
		seen[change.Change] = true
	}
	if len(changes) != 3 || !seen["added"] || !seen["changed"] || !seen["absent"] {
		t.Fatalf("unexpected change records: %+v", changes)
	}

	replayed, err := Publish(context.Background(), secondRelease, secondDigest, "2026", "another-run", Options{
		StorageRoot:   storageRoot,
		ShardCount:    4,
		Clock:         func() time.Time { return fixtureTime.Add(2 * time.Hour) },
		DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("idempotent Publish() error = %v", err)
	}
	if replayed.OccurrenceSetID != second.OccurrenceSetID || replayed.RunID != second.RunID || !replayed.PublishedAt.Equal(second.PublishedAt) {
		t.Fatalf("idempotent replay changed publication: %+v", replayed)
	}
}

func TestPublishReusesUnchangedScheduleAAcrossCoordinatedReleases(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactFixture(t, "negative-adjustment.copy")
	firstRelease, firstDigest := sourceReleaseFixture(t, storageRoot, "release-one", "", "same-schedule-a", rows)
	first, err := Publish(context.Background(), firstRelease, firstDigest, "2026", "first-run", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	secondRelease, secondDigest := sourceReleaseFixture(t, storageRoot, "release-two", firstRelease.ReleaseID, "same-schedule-a", rows)
	second, err := Publish(context.Background(), secondRelease, secondDigest, "2026", "second-run", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	if second.OccurrenceSetID != first.OccurrenceSetID || second.SourceReleaseID != first.SourceReleaseID {
		t.Fatalf("unchanged Schedule A was reprocessed: first=%+v second=%+v", first, second)
	}
}

func TestPublishRejectsCurrentPointerThatDiffersFromImmutableManifest(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactFixture(t, "negative-adjustment.copy")
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "release-current", "", "schedule-a-current", rows)
	manifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "current-one", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	manifest.RunID = "tampered-current"
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	currentPath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "current", "2026.json")
	writeFile(t, currentPath, append(content, '\n'))
	_, err = Publish(context.Background(), releaseManifest, releaseDigest, "2026", "current-two", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err == nil || !strings.Contains(err.Error(), "differs from its immutable manifest") {
		t.Fatalf("Publish() error = %v; want immutable-backing rejection", err)
	}
}

func TestPublishRejectsMissingCurrentArtifact(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactFixture(t, "negative-adjustment.copy")
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "release-missing", "", "schedule-a-missing", rows)
	manifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "missing-one", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(storageRoot, filepath.FromSlash(manifest.Artifacts.Occurrences.StorageKey))
	if err := os.Remove(artifactPath); err != nil {
		t.Fatal(err)
	}
	_, err = Publish(context.Background(), releaseManifest, releaseDigest, "2026", "missing-two", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err == nil || !strings.Contains(err.Error(), "occurrences artifact") {
		t.Fatalf("Publish() error = %v; want missing-artifact rejection", err)
	}
}

func TestPublishPreservesInvalidAndDuplicateOccurrencesAsIssues(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	row := exactFixture(t, "negative-adjustment.copy")
	invalid := []byte("too\tfew\tfields\n")
	rows := append(append(append([]byte(nil), row...), row...), invalid...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "release-issues", "", "schedule-a-issues", rows)
	manifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "issue-run", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if manifest.Counts.Total != 3 || manifest.Counts.Valid != 2 || manifest.Counts.Invalid != 1 || manifest.Counts.DuplicateKeys != 1 || manifest.Counts.DuplicateOccurrences != 2 {
		t.Fatalf("unexpected issue counts: %+v", manifest.Counts)
	}
	if manifest.Artifacts.Issues.RecordCount != 3 || manifest.Changes.Invalid != 1 {
		t.Fatalf("invalid/duplicate issues were not materialized: %+v %+v", manifest.Artifacts.Issues, manifest.Changes)
	}
	issues := readArtifactRecords[Issue](t, storageRoot, manifest.Artifacts.Issues)
	codes := map[string]int{}
	for _, issue := range issues {
		codes[issue.Code]++
	}
	if codes["duplicate_natural_key"] != 2 || codes["field_count"] != 1 {
		t.Fatalf("unexpected structured issues: %+v", issues)
	}
}

func TestSemanticDigestIgnoresEquivalentCOPYEscaping(t *testing.T) {
	t.Parallel()
	raw := exactFixture(t, "negative-adjustment.copy")
	escaped := bytes.Replace(raw, []byte("MCCONNELL"), []byte(`MCCONN\105LL`), 1)
	if bytes.Equal(raw, escaped) {
		t.Fatal("fixture did not contain expected contributor text")
	}
	left := decodeOneRow(t, raw)
	right := decodeOneRow(t, escaped)
	leftDigest, err := semanticDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := semanticDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest {
		t.Fatalf("equivalent decoded values changed semantic digest: %s != %s", leftDigest, rightDigest)
	}
	if sha256.Sum256(raw) == sha256.Sum256(escaped) {
		t.Fatal("raw content digest did not change")
	}
}

func TestPublicationPathCannotEscapeStorageRoot(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	if err := requirePathInside(storageRoot, filepath.Join(storageRoot, "evidence", "current.json")); err != nil {
		t.Fatal(err)
	}
	if err := requirePathInside(storageRoot, filepath.Join(filepath.Dir(storageRoot), "outside.json")); err == nil {
		t.Fatal("path outside storage root was accepted")
	}
}

func TestSourceReleaseAncestryRejectsInvalidPriorID(t *testing.T) {
	t.Parallel()
	candidate := fecrelease.ReleaseManifest{
		ReleaseID:      "fec-" + strings.Repeat("a", 64),
		PriorReleaseID: "../../outside",
	}
	_, err := releaseDescendsFrom(
		context.Background(),
		t.TempDir(),
		candidate,
		"fec-"+strings.Repeat("b", 64),
	)
	if err == nil || !strings.Contains(err.Error(), "prior ID is invalid") {
		t.Fatalf("releaseDescendsFrom() error = %v; want invalid-prior rejection", err)
	}
}

func TestPublishChecksStorageBeforeReadingTheSelectedStream(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactFixture(t, "negative-adjustment.copy")
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "release-storage", "", "schedule-a-storage", rows)
	selected, _, err := selectedScheduleAOutput(releaseManifest, "2026")
	if err != nil {
		t.Fatal(err)
	}
	selectedPath := filepath.Join(storageRoot, filepath.FromSlash(selected.StorageKey))
	if err := os.Remove(selectedPath); err != nil {
		t.Fatal(err)
	}
	_, err = Publish(context.Background(), releaseManifest, releaseDigest, "2026", "storage-run", Options{
		StorageRoot:        storageRoot,
		ShardCount:         2,
		FreeFloorBytes:     100,
		WorkingMarginBytes: 100,
		DiskAvailable:      func(string) (uint64, error) { return 199, nil },
	})
	if err == nil || !strings.Contains(err.Error(), "free-space floor") {
		t.Fatalf("Publish() error = %v; want storage preflight failure", err)
	}
}

func sourceReleaseFixture(t *testing.T, storageRoot, releaseLabel, priorID, scheduleVersion string, rows []byte) (fecrelease.ReleaseManifest, string) {
	t.Helper()
	inventory := fecrelease.InitialInventory()
	releaseID := "fec-" + digestParts("fixture-release", releaseLabel)
	scheduleSourceSHA := digestParts("fixture-schedule-a-source", scheduleVersion)
	compressed, compressedSHA := compress(t, rows)
	uncompressedSHABytes := sha256.Sum256(rows)
	uncompressedSHA := hex.EncodeToString(uncompressedSHABytes[:])
	scheduleStorageKey := filepath.ToSlash(filepath.Join("raw", "fec", "schedule-a", "extracts", "sha256", compressedSHA[:2], compressedSHA+".copy.zst"))
	writeFile(t, filepath.Join(storageRoot, filepath.FromSlash(scheduleStorageKey)), compressed)

	observedAt := fixtureTime.Add(-time.Hour)
	artifacts := make([]fecrelease.PublishedArtifact, 0, len(inventory.Sources))
	sourceSHAs := make(map[string]string, len(inventory.Sources))
	for _, source := range inventory.Sources {
		sourceSHA := digestParts("fixture-source", releaseLabel, source.SourceID)
		if source.SourceID == fecrelease.ScheduleASourceID {
			sourceSHA = scheduleSourceSHA
		}
		sourceSHAs[source.SourceID] = sourceSHA
		length := int64(1)
		artifacts = append(artifacts, fecrelease.PublishedArtifact{
			SelectedSource: fecrelease.SelectedSource{
				SourceID: source.SourceID, RequestURL: source.RequestURL, FinalURL: source.RequestURL,
				ObservedAt: observedAt, VersionIdentity: "version_id:" + releaseLabel, VersionBasis: "version_id", VersionID: releaseLabel,
				ContentLength: &length,
			},
			ByteCount: 1, SHA256: sourceSHA, StorageKey: "raw/fec/sha256/" + sourceSHA, AcquiredAt: observedAt.Add(time.Minute),
		})
	}

	outputs := make([]fecrelease.StagedOutput, 0, 24)
	for _, source := range inventory.Sources {
		for _, member := range source.SelectedMembers {
			digest := digestParts("fixture-selected", releaseLabel, source.SourceID, member)
			outputs = append(outputs, fixtureOutput(source.SourceID, source.Periods[0], "member", member, sourceSHAs[source.SourceID], digest, 1, 1, digest, nil, nil))
		}
		for index, relation := range source.SelectedRelations {
			period := source.Periods[index]
			rowCount := uint64(1)
			fieldCount := schedulea.FieldCount
			digest := digestParts("fixture-selected", releaseLabel, source.SourceID, relation)
			uncompressedDigest := digest
			byteCount := uint64(1)
			compressedByteCount := uint64(1)
			if period == "2026" {
				digest = compressedSHA
				uncompressedDigest = uncompressedSHA
				byteCount = uint64(len(rows))
				compressedByteCount = uint64(len(compressed))
				rowCount = uint64(bytes.Count(rows, []byte{'\n'}))
			}
			outputs = append(outputs, fixtureOutput(source.SourceID, period, "relation", relation, sourceSHAs[source.SourceID], digest, compressedByteCount, byteCount, uncompressedDigest, &rowCount, &fieldCount))
		}
	}
	checks := []fecrelease.ReleaseCheck{
		{ID: "input_identity", Passed: true, Severity: "block", Detail: "fixture input identity"},
		{ID: "source_artifact_membership", Passed: true, Severity: "block", Detail: "fixture source membership"},
		{ID: "selected_output_membership", Passed: true, Severity: "block", Detail: "fixture output membership"},
		{ID: "output_integrity", Passed: true, Severity: "block", Detail: "fixture output integrity"},
		{ID: "storage_budget", Passed: true, Severity: "block", Detail: "fixture storage budget"},
	}
	manifest := fecrelease.ReleaseManifest{
		Schema: "release-manifest.schema.json", SchemaVersion: fecrelease.ManifestSchemaVersion,
		InventoryVersion: inventory.InventoryVersion, ReleaseID: releaseID, PriorReleaseID: priorID,
		RunID: "fixture-release", PlanSHA256: strings.Repeat("a", 64), AcquisitionSHA256: strings.Repeat("b", 64), StageSHA256: strings.Repeat("c", 64),
		State: "published", SelectedAt: observedAt, PublishedAt: observedAt.Add(time.Minute), Periods: append([]string(nil), inventory.Periods...),
		Artifacts: artifacts, StagedOutputs: outputs, Checks: checks,
	}
	if validationIssues := fecrelease.ValidateManifest(inventory, manifest); len(validationIssues) != 0 {
		t.Fatalf("invalid source release fixture: %+v", validationIssues)
	}
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	content = append(content, '\n')
	manifestPath := filepath.Join(storageRoot, "releases", "fec", "manifests", releaseID+".json")
	writeFile(t, manifestPath, content)
	digestBytes := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digestBytes[:])
}

func fixtureOutput(sourceID, period, kind, selection, sourceSHA, compressedSHA string, compressedBytes, uncompressedBytes uint64, uncompressedSHA string, rowCount *uint64, fieldCount *int) fecrelease.StagedOutput {
	storageKey := filepath.ToSlash(filepath.Join("raw", "fec", "selected", "sha256", compressedSHA[:2], compressedSHA+".zst"))
	representation := "selected_member_zstd"
	if kind == "relation" {
		family := "schedule-a"
		if sourceID == fecrelease.ScheduleESourceID {
			family = "schedule-e"
		}
		storageKey = filepath.ToSlash(filepath.Join("raw", "fec", family, "extracts", "sha256", compressedSHA[:2], compressedSHA+".copy.zst"))
		representation = "postgresql_copy_text_data_rows_zstd"
	}
	return fecrelease.StagedOutput{
		SourceID: sourceID, Disposition: "staged", SelectionKind: kind, Selection: selection, Period: period,
		Representation: representation, SourceArtifactSHA256: sourceSHA, RowCount: rowCount, ContractedFieldCount: fieldCount,
		UncompressedByteCount: uncompressedBytes, UncompressedSHA256: uncompressedSHA,
		Compression: "zstd", CompressionLevel: 3, CompressedByteCount: compressedBytes, CompressedSHA256: compressedSHA,
		StorageKey: storageKey, DecompressionValidated: true, StagedAt: fixtureTime,
	}
}

func exactFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", "schedule-a", "v1", "fixtures", "dump-2026-08-23", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func compress(t *testing.T, content []byte) ([]byte, string) {
	t.Helper()
	var buffer bytes.Buffer
	encoder, err := zstd.NewWriter(&buffer, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)), zstd.WithEncoderConcurrency(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(buffer.Bytes())
	return buffer.Bytes(), hex.EncodeToString(digest[:])
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}
}

func readArtifactRecords[T any](t *testing.T, storageRoot string, artifact Artifact) []T {
	t.Helper()
	file, err := os.Open(filepath.Join(storageRoot, filepath.FromSlash(artifact.StorageKey)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	decoder, err := zstd.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	jsonDecoder := json.NewDecoder(bufio.NewReader(decoder))
	result := make([]T, 0)
	for {
		var record T
		if err := jsonDecoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		result = append(result, record)
	}
	return result
}

func decodeOneRow(t *testing.T, content []byte) *schedulea.Row {
	t.Helper()
	decoder := schedulea.NewDecoder(bytes.NewReader(content))
	if !decoder.Scan() {
		t.Fatalf("decode fixture: %v", decoder.Err())
	}
	row := decoder.Row()
	if err := schedulea.Validate(row, "2026"); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestNaturalIndexIsGloballySortedAcrossShards(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	parent := t.TempDir()
	writer, err := newArtifactWriter(context.Background(), storageRoot, parent, "natural-index")
	if err != nil {
		t.Fatal(err)
	}
	issues, err := newArtifactWriter(context.Background(), storageRoot, parent, "issues")
	if err != nil {
		t.Fatal(err)
	}
	shards, err := newShardSet(context.Background(), parent, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer shards.Remove()
	for index, key := range []string{"fec:schedule-a:2026:9", "fec:schedule-a:2026:1", "fec:schedule-a:2026:5"} {
		if err := shards.Add(shardEntry{NaturalKey: key, OccurrenceID: digestParts("occ", fmt.Sprint(index)), RecordVersionID: digestParts("version", fmt.Sprint(index)), SemanticDigest: digestParts("semantic", fmt.Sprint(index)), RowOrdinal: uint64(index + 1), Valid: true}); err != nil {
			t.Fatal(err)
		}
	}
	var counts Counts
	if err := shards.BuildIndex(writer, issues, &counts); err != nil {
		t.Fatal(err)
	}
	artifact, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	_, err = issues.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	records := readArtifactRecords[NaturalIndexEntry](t, storageRoot, artifact)
	expectedOrdinals := map[string]uint64{
		"fec:schedule-a:2026:1": 2,
		"fec:schedule-a:2026:5": 3,
		"fec:schedule-a:2026:9": 1,
	}
	for _, record := range records {
		if record.RowOrdinal != expectedOrdinals[record.NaturalKey] {
			t.Fatalf("natural index lost source row ordinal: %+v", records)
		}
	}
	for index := 1; index < len(records); index++ {
		if records[index-1].NaturalKey >= records[index].NaturalKey {
			t.Fatalf("natural index is not sorted: %+v", records)
		}
	}
}

func TestNaturalIndexMergeClearsOmittedFieldsBetweenEntries(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	parent := t.TempDir()
	writer, err := newArtifactWriter(context.Background(), storageRoot, parent, "natural-index")
	if err != nil {
		t.Fatal(err)
	}
	issues, err := newArtifactWriter(context.Background(), storageRoot, parent, "issues")
	if err != nil {
		t.Fatal(err)
	}
	shards, err := newShardSet(context.Background(), parent, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer shards.Remove()
	if err := shards.Add(shardEntry{
		NaturalKey: "fec:test:2024:1", OccurrenceID: digestParts("occ", "1"),
		RecordVersionID: digestParts("version", "1"), SemanticDigest: digestParts("semantic", "1"),
		RowOrdinal: 1, Valid: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := shards.Add(shardEntry{
		NaturalKey: "fec:test:2024:2", OccurrenceID: digestParts("occ", "2"),
		RecordVersionID: digestParts("version", "2"), RowOrdinal: 2, Valid: false,
	}); err != nil {
		t.Fatal(err)
	}
	var counts Counts
	if err := shards.BuildIndex(writer, issues, &counts); err != nil {
		t.Fatal(err)
	}
	artifact, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := issues.Finalize(); err != nil {
		t.Fatal(err)
	}
	records := readArtifactRecords[NaturalIndexEntry](t, storageRoot, artifact)
	if len(records) != 2 || records[0].State != "unique" || records[1].State != "invalid" {
		t.Fatalf("unexpected natural-index states: %+v", records)
	}
	invalid := records[1]
	if invalid.RowOrdinal != 0 || invalid.OccurrenceID != "" || invalid.RecordVersionID != "" || invalid.SemanticDigest != "" {
		t.Fatalf("invalid entry inherited unique-only fields: %+v", invalid)
	}
	if err := validateNaturalIndexEntry(invalid); err != nil {
		t.Fatalf("invalid entry failed contract validation: %v", err)
	}
}
