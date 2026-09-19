package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestPublishScheduleEOccurrencesPartitionsAllHistoryWithoutCollapsingRows(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	first := exactScheduleEFixture(t, "escaped-copy-text.copy")  // 2024
	second := exactScheduleEFixture(t, "fractional-amount.copy") // 2026
	third := exactScheduleEFixture(t, "negative-amount.copy")    // 2012
	rows := append(append(append([]byte(nil), first...), second...), third...)
	releaseManifest, releaseDigest := sourceReleaseV2Fixture(t, storageRoot, "schedule-e-one", "", rows)

	manifest, err := PublishScheduleEOccurrences(context.Background(), releaseManifest, releaseDigest, "2024", "schedule-e-occurrences", Options{
		StorageRoot: storageRoot, Clock: func() time.Time { return fixtureTime }, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("PublishScheduleEOccurrences() error = %v", err)
	}
	if manifest.Counts.SourceRows != 3 || manifest.Counts.SelectedRows != 1 || manifest.Counts.OtherCycleRows != 2 || manifest.Counts.NullCycleRows != 0 {
		t.Fatalf("unexpected Schedule E occurrence counts: %+v", manifest.Counts)
	}
	occurrences := readArtifactRecords[ScheduleEOccurrence](t, storageRoot, manifest.Occurrences)
	if len(occurrences) != 1 || occurrences[0].RowOrdinal != 1 || occurrences[0].RawByteOffset != 0 || occurrences[0].RawByteLength != uint64(len(first)) {
		t.Fatalf("selected occurrence lost physical identity: %+v", occurrences)
	}
	if occurrences[0].Cycle != "2024" || !strings.HasPrefix(occurrences[0].NaturalKey, "fec:schedule-e:2024:") || occurrences[0].PublisherRecordReference == "" {
		t.Fatalf("selected occurrence lost publisher identity: %+v", occurrences[0])
	}

	replayed, err := PublishScheduleEOccurrences(context.Background(), releaseManifest, releaseDigest, "2024", "another-run", Options{
		StorageRoot: storageRoot, Clock: func() time.Time { return fixtureTime.Add(time.Hour) }, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.OccurrenceSetID != manifest.OccurrenceSetID || replayed.RunID != manifest.RunID {
		t.Fatalf("idempotent Schedule E publication changed evidence: %+v", replayed)
	}
}

func sourceReleaseV2Fixture(t *testing.T, storageRoot, releaseLabel, priorID string, scheduleERows []byte) (fecrelease.ReleaseManifest, string) {
	t.Helper()
	return sourceReleaseWithInventoryFixture(t, storageRoot, releaseLabel, priorID, scheduleERows, fecrelease.ScheduleEInventory())
}

func TestPublishScheduleEOccurrencesFromV4(t *testing.T) {
	root := t.TempDir()
	source, digest := sourceReleaseWithInventoryFixture(t, root, "v4-with-summary", "", exactScheduleEFixture(t, "escaped-copy-text.copy"), fecrelease.CommitteeSummaryInventory())
	m, err := PublishScheduleEOccurrences(context.Background(), source, digest, "2024", "v4-schedule-e", Options{StorageRoot: root, Clock: func() time.Time { return fixtureTime }, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	if m.Counts.SourceRows != 1 || m.Counts.SelectedRows != 1 {
		t.Fatal(m.Counts)
	}
}

func sourceReleaseWithInventoryFixture(t *testing.T, storageRoot, releaseLabel, priorID string, scheduleERows []byte, inventory fecrelease.Inventory) (fecrelease.ReleaseManifest, string) {
	t.Helper()
	releaseID := "fec-" + digestParts("fixture-v2-release", releaseLabel)
	compressed, compressedSHA := compress(t, scheduleERows)
	uncompressedDigest := sha256.Sum256(scheduleERows)
	uncompressedSHA := hex.EncodeToString(uncompressedDigest[:])
	scheduleESourceSHA := digestParts("fixture-schedule-e-source", releaseLabel)
	scheduleEStorageKey := filepath.ToSlash(filepath.Join("raw", "fec", "schedule-e", "extracts", "sha256", compressedSHA[:2], compressedSHA+".copy.zst"))
	writeFile(t, filepath.Join(storageRoot, filepath.FromSlash(scheduleEStorageKey)), compressed)

	observedAt := fixtureTime.Add(-time.Hour)
	artifacts := make([]fecrelease.PublishedArtifact, 0, len(inventory.Sources))
	sourceSHAs := make(map[string]string, len(inventory.Sources))
	for _, source := range inventory.Sources {
		sourceSHA := digestParts("fixture-v2-source", releaseLabel, source.SourceID)
		if source.SourceID == fecrelease.ScheduleESourceID {
			sourceSHA = scheduleESourceSHA
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

	outputs := make([]fecrelease.StagedOutput, 0, 25)
	for _, source := range inventory.Sources {
		for _, member := range source.SelectedMembers {
			digest := digestParts("fixture-v2-selected", releaseLabel, source.SourceID, member)
			outputs = append(outputs, fixtureOutput(source.SourceID, source.Periods[0], "member", member, sourceSHAs[source.SourceID], digest, 1, 1, digest, nil, nil))
		}
		for index, relation := range source.SelectedRelations {
			fieldCount := 81
			rowCount := uint64(1)
			digest := digestParts("fixture-v2-selected", releaseLabel, source.SourceID, relation)
			outputs = append(outputs, fixtureOutput(source.SourceID, source.Periods[index], "relation", relation, sourceSHAs[source.SourceID], digest, 1, 1, digest, &rowCount, &fieldCount))
		}
		for _, relation := range source.RelationSelections {
			if relation.Materialization == fecrelease.RelationMaterializationArchiveDirect {
				continue
			}
			fieldCount := relation.FieldCount
			rowCount := uint64(1)
			digest := digestParts("fixture-v2-selected", releaseLabel, source.SourceID, relation.Name)
			compressedBytes, uncompressedBytes := uint64(1), uint64(1)
			relationUncompressedSHA := digest
			if source.SourceID == fecrelease.ScheduleESourceID {
				digest = compressedSHA
				compressedBytes = uint64(len(compressed))
				uncompressedBytes = uint64(len(scheduleERows))
				relationUncompressedSHA = uncompressedSHA
				rowCount = uint64(strings.Count(string(scheduleERows), "\n"))
			}
			outputs = append(outputs, fixtureOutput(source.SourceID, relation.Scope, "relation", relation.Name, sourceSHAs[source.SourceID], digest, compressedBytes, uncompressedBytes, relationUncompressedSHA, &rowCount, &fieldCount))
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
		RunID: "fixture-v2-release", PlanSHA256: strings.Repeat("a", 64), AcquisitionSHA256: strings.Repeat("b", 64), StageSHA256: strings.Repeat("c", 64),
		State: "published", SelectedAt: observedAt, PublishedAt: observedAt.Add(time.Minute), Periods: append([]string(nil), inventory.Periods...),
		Artifacts: artifacts, StagedOutputs: outputs, Checks: checks,
	}
	if issues := fecrelease.ValidateManifest(inventory, manifest); len(issues) != 0 {
		t.Fatalf("invalid v2 source release fixture: %+v", issues)
	}
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	content = append(content, '\n')
	writeFile(t, filepath.Join(storageRoot, "releases", "fec", "manifests", releaseID+".json"), content)
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:])
}

func exactScheduleEFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", "schedule-e", "v1", "fixtures", "dump-2026-08-30", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}
