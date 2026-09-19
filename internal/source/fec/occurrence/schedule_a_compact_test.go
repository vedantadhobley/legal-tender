package occurrence

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPublishScheduleACompactOccurrencesUsesImplicitBootstrapMembership(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	negative := exactFixture(t, "negative-adjustment.copy")
	targeted := exactFixture(t, "targeted-individual.copy")
	rows := append(append([]byte(nil), negative...), targeted...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "compact-one", "", "compact-schedule-one", rows)

	manifest, err := PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "compact-one",
		Options{StorageRoot: storageRoot, ShardCount: 4, Clock: func() time.Time { return fixtureTime }, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatalf("PublishScheduleACompactOccurrences() error = %v", err)
	}
	if manifest.SchemaVersion != ScheduleACompactManifestSchemaVersion || manifest.ChangeMode != "bootstrap-dense-membership" {
		t.Fatalf("unexpected compact manifest contract: %+v", manifest)
	}
	if manifest.Counts.Total != 2 || manifest.Counts.Valid != 2 || manifest.Counts.UniqueKeys != 2 || manifest.Counts.Invalid != 0 {
		t.Fatalf("unexpected compact counts: %+v", manifest.Counts)
	}
	if manifest.Changes.Added != 2 || manifest.Deltas.RecordCount != 0 || manifest.RowExceptions.RecordCount != 0 {
		t.Fatalf("bootstrap membership was materialized per row: changes=%+v deltas=%+v exceptions=%+v", manifest.Changes, manifest.Deltas, manifest.RowExceptions)
	}
	var indexed uint64
	for _, partition := range manifest.IndexPartitions {
		indexed += partition.Index.RecordCount
		if partition.Index.UncompressedBytes != partition.Index.RecordCount*ScheduleACompactIndexRecordBytes {
			t.Fatalf("partition %d is not fixed width: %+v", partition.Partition, partition.Index)
		}
		if _, err := readScheduleACompactPartitionStates(context.Background(), storageRoot, partition, 4); err != nil {
			t.Fatalf("read compact partition %d: %v", partition.Partition, err)
		}
	}
	if indexed != 2 {
		t.Fatalf("indexed records = %d; want 2", indexed)
	}

	replayed, err := PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "compact-replay",
		Options{StorageRoot: storageRoot, ShardCount: 4, Clock: func() time.Time { return fixtureTime.Add(time.Hour) }, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatalf("idempotent compact publication error = %v", err)
	}
	if replayed.OccurrenceSetID != manifest.OccurrenceSetID || replayed.RunID != manifest.RunID || !replayed.PublishedAt.Equal(manifest.PublishedAt) {
		t.Fatalf("idempotent compact publication changed identity: first=%+v replay=%+v", manifest, replayed)
	}
}

func TestScheduleACompactIndexMatchesLogicalNaturalIndex(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	negative := exactFixture(t, "negative-adjustment.copy")
	targeted := exactFixture(t, "targeted-individual.copy")
	rows := append(append([]byte(nil), negative...), targeted...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "compact-equivalence", "", "compact-equivalence", rows)
	legacy, err := Publish(
		context.Background(), releaseManifest, releaseDigest, "2026", "legacy-equivalence",
		Options{StorageRoot: storageRoot, ShardCount: 4, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "compact-equivalence",
		Options{StorageRoot: storageRoot, ShardCount: 4, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	compactStates := map[uint64]compactKeyState{}
	for _, partition := range compact.IndexPartitions {
		states, err := readScheduleACompactPartitionStates(context.Background(), storageRoot, partition, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range states {
			compactStates[state.SubID] = state
		}
	}
	legacyStates := readArtifactRecords[NaturalIndexEntry](t, storageRoot, legacy.Artifacts.NaturalIndex)
	if len(compactStates) != len(legacyStates) {
		t.Fatalf("compact states = %d; legacy states = %d", len(compactStates), len(legacyStates))
	}
	for _, legacyState := range legacyStates {
		parts := strings.Split(legacyState.NaturalKey, ":")
		key, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		state, ok := compactStates[key]
		if !ok || state.State != legacyState.State || state.RowOrdinal != legacyState.RowOrdinal ||
			hexDigest(state.ComparisonDigest) != legacyState.SemanticDigest {
			t.Fatalf("compact state differs from logical natural index: compact=%+v legacy=%+v", state, legacyState)
		}
	}
}

func hexDigest(digest [32]byte) string {
	const alphabet = "0123456789abcdef"
	encoded := make([]byte, len(digest)*2)
	for index, value := range digest {
		encoded[index*2] = alphabet[value>>4]
		encoded[index*2+1] = alphabet[value&0x0f]
	}
	return string(encoded)
}

func TestPublishScheduleACompactOccurrencesEmitsOnlyActualLaterDeltas(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	negative := exactFixture(t, "negative-adjustment.copy")
	targeted := exactFixture(t, "targeted-individual.copy")
	firstRows := append(append([]byte(nil), negative...), targeted...)
	firstRelease, firstDigest := sourceReleaseFixture(t, storageRoot, "compact-delta-one", "", "compact-delta-one", firstRows)
	first, err := PublishScheduleACompactOccurrences(
		context.Background(), firstRelease, firstDigest, "2026", "compact-delta-one",
		Options{StorageRoot: storageRoot, ShardCount: 4, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}

	modifiedNegative := bytes.Replace(negative, []byte("-5000.00"), []byte("-4999.00"), 1)
	added := exactFixture(t, "action-code-n.copy")
	secondRows := append(append([]byte(nil), modifiedNegative...), added...)
	secondRelease, secondDigest := sourceReleaseFixture(t, storageRoot, "compact-delta-two", firstRelease.ReleaseID, "compact-delta-two", secondRows)
	second, err := PublishScheduleACompactOccurrences(
		context.Background(), secondRelease, secondDigest, "2026", "compact-delta-two",
		Options{StorageRoot: storageRoot, ShardCount: 4, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.PriorOccurrenceSetID != first.OccurrenceSetID || second.ChangeMode != "explicit-inter-release-delta" ||
		second.Changes.Added != 1 || second.Changes.Changed != 1 || second.Changes.Absent != 1 || second.Changes.Unchanged != 0 || second.Deltas.RecordCount != 3 {
		t.Fatalf("unexpected compact deltas: %+v", second)
	}
	deltas := readArtifactRecords[ScheduleACompactDelta](t, storageRoot, second.Deltas)
	seen := map[string]bool{}
	for _, delta := range deltas {
		seen[delta.Change] = true
	}
	if len(deltas) != 3 || !seen["added"] || !seen["changed"] || !seen["absent"] {
		t.Fatalf("actual compact deltas were not preserved: %+v", deltas)
	}
}

func TestPublishScheduleACompactOccurrencesPreservesSparseExceptions(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	row := exactFixture(t, "negative-adjustment.copy")
	invalid := []byte("too\tfew\tfields\n")
	rows := append(append(append([]byte(nil), row...), row...), invalid...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "compact-issues", "", "compact-issues", rows)
	manifest, err := PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "compact-issues",
		Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Counts.Total != 3 || manifest.Counts.Valid != 2 || manifest.Counts.Invalid != 1 ||
		manifest.Counts.UniqueKeys != 0 || manifest.Counts.DuplicateKeys != 1 || manifest.Counts.DuplicateOccurrences != 2 {
		t.Fatalf("unexpected compact exception counts: %+v", manifest.Counts)
	}
	if manifest.RowExceptions.RecordCount != 3 || manifest.Changes.Invalid != 1 || manifest.Deltas.RecordCount != 1 {
		t.Fatalf("compact exceptions or invalid key delta missing: %+v", manifest)
	}
	exceptions := readArtifactRecords[ScheduleACompactRowException](t, storageRoot, manifest.RowExceptions)
	codes := map[string]int{}
	for _, exception := range exceptions {
		codes[exception.Code]++
	}
	if codes["field_count"] != 1 || codes["duplicate_natural_key"] != 2 {
		t.Fatalf("unexpected compact row exceptions: %+v", exceptions)
	}
}

func TestPublishScheduleACompactOccurrencesRejectsCorruptPublishedIndex(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactFixture(t, "negative-adjustment.copy")
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "compact-corrupt", "", "compact-corrupt", rows)
	manifest, err := PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "compact-corrupt",
		Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable},
	)
	if err != nil {
		t.Fatal(err)
	}
	var artifact ScheduleACompactArtifact
	for _, partition := range manifest.IndexPartitions {
		if partition.Index.RecordCount != 0 {
			artifact = partition.Index
			break
		}
	}
	path := filepath.Join(storageRoot, filepath.FromSlash(artifact.StorageKey))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0}, 0); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = PublishScheduleACompactOccurrences(
		context.Background(), releaseManifest, releaseDigest, "2026", "compact-corrupt-retry",
		Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable},
	)
	if err == nil || !strings.Contains(err.Error(), "compact index") {
		t.Fatalf("corrupt compact index error = %v", err)
	}
}
