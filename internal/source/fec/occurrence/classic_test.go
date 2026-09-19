package occurrence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestPublishClassicConservesRowsAndEmitsChanges(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	firstRow := classicFixture(t, classic.CandidateMaster, "current-candidate.txt")
	secondRow := classicFixture(t, classic.CandidateMaster, "partial-address.txt")
	firstRows := append(append([]byte(nil), firstRow...), secondRow...)
	firstRelease, firstDigest := classicSourceReleaseFixture(t, storageRoot, "classic-one", "", classic.CandidateMaster, "2024", firstRows)

	first, err := PublishClassic(context.Background(), firstRelease, firstDigest, string(classic.CandidateMaster), "2024", "classic-first", Options{
		StorageRoot: storageRoot, ShardCount: 4, Clock: func() time.Time { return fixtureTime }, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("first PublishClassic() error = %v", err)
	}
	if first.Counts.Total != 2 || first.Counts.Valid != 2 || first.Counts.UniqueKeys != 2 || first.Changes.Added != 2 {
		t.Fatalf("unexpected first classic counts: %+v %+v", first.Counts, first.Changes)
	}
	occurrences := readArtifactRecords[Occurrence](t, storageRoot, first.Artifacts.Occurrences)
	if len(occurrences) != 2 || occurrences[1].RawByteOffset != uint64(len(firstRow)) || occurrences[0].NaturalKey == nil {
		t.Fatalf("classic occurrence locators are not conserved: %+v", occurrences)
	}

	modified := bytes.Replace(firstRow, []byte("AVERHART, JAMES"), []byte("AVERHART, JIM  "), 1)
	added := bytes.Replace(secondRow, []byte("H0AL02087"), []byte("H1AL02087"), 1)
	secondRows := append(append([]byte(nil), modified...), added...)
	secondRelease, secondDigest := classicSourceReleaseFixture(t, storageRoot, "classic-two", firstRelease.ReleaseID, classic.CandidateMaster, "2024", secondRows)
	second, err := PublishClassic(context.Background(), secondRelease, secondDigest, string(classic.CandidateMaster), "2024", "classic-second", Options{
		StorageRoot: storageRoot, ShardCount: 4, Clock: func() time.Time { return fixtureTime.Add(time.Hour) }, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("second PublishClassic() error = %v", err)
	}
	if second.PriorOccurrenceSetID != first.OccurrenceSetID || second.Changes.Added != 1 || second.Changes.Changed != 1 || second.Changes.Absent != 1 {
		t.Fatalf("unexpected classic semantic changes: %+v", second.Changes)
	}
	changes := readArtifactRecords[Change](t, storageRoot, second.Artifacts.Changes)
	seen := map[string]bool{}
	for _, change := range changes {
		seen[change.Change] = true
	}
	if len(changes) != 3 || !seen["added"] || !seen["changed"] || !seen["absent"] {
		t.Fatalf("unexpected classic change records: %+v", changes)
	}
}

func TestPublishClassicPreservesInvalidAndDuplicateRows(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	row := classicFixture(t, classic.CommitteeMaster, "connected-organization.txt")
	rows := append(append(append([]byte(nil), row...), row...), []byte("bad|row")...)
	releaseManifest, releaseDigest := classicSourceReleaseFixture(t, storageRoot, "classic-issues", "", classic.CommitteeMaster, "2024", rows)
	manifest, err := PublishClassic(context.Background(), releaseManifest, releaseDigest, string(classic.CommitteeMaster), "2024", "classic-issues", Options{
		StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatalf("PublishClassic() error = %v", err)
	}
	if manifest.Counts.Total != 3 || manifest.Counts.Valid != 2 || manifest.Counts.Invalid != 1 || manifest.Counts.DuplicateKeys != 1 || manifest.Counts.DuplicateOccurrences != 2 {
		t.Fatalf("unexpected classic issue counts: %+v", manifest.Counts)
	}
	issues := readArtifactRecords[Issue](t, storageRoot, manifest.Artifacts.Issues)
	codes := map[string]int{}
	for _, issue := range issues {
		codes[issue.Code]++
	}
	if codes["duplicate_natural_key"] != 2 || codes["field_count"] != 1 || codes["missing_line_feed"] != 1 {
		t.Fatalf("unexpected classic issues: %+v", issues)
	}
}

func TestPublishClassicReusesUnchangedMemberAcrossReleases(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := classicFixture(t, classic.CandidateCommitteeLinkage, "current-election.txt")
	firstRelease, firstDigest := classicSourceReleaseFixture(t, storageRoot, "classic-reuse-one", "", classic.CandidateCommitteeLinkage, "2024", rows)
	first, err := PublishClassic(context.Background(), firstRelease, firstDigest, string(classic.CandidateCommitteeLinkage), "2024", "classic-reuse-first", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	secondRelease, secondDigest := classicSourceReleaseFixture(t, storageRoot, "classic-reuse-two", firstRelease.ReleaseID, classic.CandidateCommitteeLinkage, "2024", rows)
	second, err := PublishClassic(context.Background(), secondRelease, secondDigest, string(classic.CandidateCommitteeLinkage), "2024", "classic-reuse-second", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	if second.OccurrenceSetID != first.OccurrenceSetID || second.SourceReleaseID != first.SourceReleaseID {
		t.Fatalf("unchanged classic member was reprocessed: first=%+v second=%+v", first, second)
	}
}

func classicFixture(t *testing.T, dataset classic.Dataset, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", string(dataset), "v1", "fixtures", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func classicSourceReleaseFixture(t *testing.T, storageRoot, releaseLabel, priorID string, dataset classic.Dataset, cycle string, rows []byte) (fecrelease.ReleaseManifest, string) {
	t.Helper()
	scheduleRows := exactFixture(t, "negative-adjustment.copy")
	manifest, _ := sourceReleaseFixture(t, storageRoot, releaseLabel, priorID, "schedule-"+releaseLabel, scheduleRows)
	spec, err := classic.Lookup(string(dataset))
	if err != nil {
		t.Fatal(err)
	}
	sourceID := "fec:" + spec.Code + ":" + cycle
	rowsDigest := sha256.Sum256(rows)
	sourceSHA := digestParts("fixture-classic-source", string(dataset), cycle, hex.EncodeToString(rowsDigest[:]))
	for index := range manifest.Artifacts {
		if manifest.Artifacts[index].SourceID == sourceID {
			manifest.Artifacts[index].SHA256 = sourceSHA
			manifest.Artifacts[index].StorageKey = "raw/fec/sha256/" + sourceSHA
		}
	}
	compressed, compressedSHA := compress(t, rows)
	uncompressedDigest := sha256.Sum256(rows)
	member := classicMember(spec, cycle)
	for index := range manifest.StagedOutputs {
		output := &manifest.StagedOutputs[index]
		if output.SourceID != sourceID {
			continue
		}
		*output = fixtureOutput(sourceID, cycle, "member", member, sourceSHA, compressedSHA, uint64(len(compressed)), uint64(len(rows)), hex.EncodeToString(uncompressedDigest[:]), nil, nil)
		writeFile(t, filepath.Join(storageRoot, filepath.FromSlash(output.StorageKey)), compressed)
	}
	if validationIssues := fecrelease.ValidateManifest(fecrelease.InitialInventory(), manifest); len(validationIssues) != 0 {
		t.Fatalf("invalid classic source release fixture: %+v", validationIssues)
	}
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	content = append(content, '\n')
	manifestPath := filepath.Join(storageRoot, "releases", "fec", "manifests", manifest.ReleaseID+".json")
	writeFile(t, manifestPath, content)
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:])
}

func TestClassicCurrentPointerRejectsTampering(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := classicFixture(t, classic.CurrentCampaignsSummary, "candidate-loan.txt")
	releaseManifest, releaseDigest := classicSourceReleaseFixture(t, storageRoot, "classic-current", "", classic.CurrentCampaignsSummary, "2024", rows)
	manifest, err := PublishClassic(context.Background(), releaseManifest, releaseDigest, string(classic.CurrentCampaignsSummary), "2024", "classic-current-one", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	manifest.RunID = "tampered"
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	currentPath := filepath.Join(storageRoot, classicEvidenceBase(mustClassicSpec(t, classic.CurrentCampaignsSummary)), "current", "2024.json")
	writeFile(t, currentPath, append(content, '\n'))
	_, err = PublishClassic(context.Background(), releaseManifest, releaseDigest, string(classic.CurrentCampaignsSummary), "2024", "classic-current-two", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err == nil || !strings.Contains(err.Error(), "differs from its immutable manifest") {
		t.Fatalf("PublishClassic() error = %v; want immutable-backing rejection", err)
	}
}

func mustClassicSpec(t *testing.T, dataset classic.Dataset) classic.Spec {
	t.Helper()
	spec, err := classic.Lookup(string(dataset))
	if err != nil {
		t.Fatal(err)
	}
	return spec
}
