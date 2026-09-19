package independentexpenditures

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	fecclassic "github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestPublishProjectionBundleFreezesExactSameReleaseInputs(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	input := writeProjectionBundleFixture(t, storageRoot, "fec-"+strings.Repeat("b", 64))
	fixed := time.Date(2026, 8, 31, 20, 0, 0, 0, time.UTC)

	manifest, err := PublishProjectionBundle(context.Background(), input, "projection-bundle", ProjectionBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024", Clock: func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.State != "ready" || manifest.Counts.CalculationResults != 1 ||
		manifest.Counts.CandidateFacts != 1 || manifest.Counts.CommitteeFacts != 1 {
		t.Fatalf("unexpected projection bundle: %+v", manifest)
	}
	if manifest.InputFactSets[0].Role != "candidate_master" || manifest.InputFactSets[1].Role != "committee_master" {
		t.Fatalf("unexpected projection bundle roles: %+v", manifest.InputFactSets)
	}

	currentPath := filepath.Join(storageRoot, projectionBundleBase(), "current", "2024.json")
	loaded, _, resolved, err := LoadProjectionBundle(context.Background(), storageRoot, currentPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, loaded) || resolved.CalculationManifestPath == "" ||
		resolved.CandidateManifestPath == "" || resolved.CommitteeManifestPath == "" {
		t.Fatalf("bundle did not resolve exact immutable inputs: %+v", resolved)
	}
	replayed, err := PublishProjectionBundle(context.Background(), input, "different-run", ProjectionBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024", Clock: func() time.Time { return fixed.Add(time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, replayed) {
		t.Fatal("same-input projection bundle was not reused")
	}
}

func TestPublishProjectionBundleRejectsMixedRelease(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	input := writeProjectionBundleFixture(t, storageRoot, "fec-"+strings.Repeat("b", 64))
	input.CommitteeManifestPath = writeProjectionBundleClassicFacts(
		t, storageRoot, "committee-master", "fec-"+strings.Repeat("9", 64), strings.Repeat("8", 64),
	)
	_, err := PublishProjectionBundle(context.Background(), input, "mixed-release", ProjectionBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024",
	})
	if err == nil || !strings.Contains(err.Error(), "do not share the calculation cycle and source release") {
		t.Fatalf("error = %v; want mixed-release rejection", err)
	}
}

func TestProjectionBundleAndCalculationRequireImmutableManifestBacking(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	input := writeProjectionBundleFixture(t, storageRoot, "fec-"+strings.Repeat("b", 64))
	manifest, err := PublishProjectionBundle(context.Background(), input, "projection-bundle-checks", ProjectionBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024",
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest.Checks[0].ID = "substituted_check"
	if err := validateProjectionBundleManifest(manifest); err == nil || !strings.Contains(err.Error(), "required projection bundle check") {
		t.Fatalf("error = %v; want exact-check-set rejection", err)
	}

	calculation, _, err := readStrictJSON[Manifest](input.CalculationManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	calculation.RunID = "forged-pointer"
	if err := writeAtomicJSON(input.CalculationManifestPath, calculation); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadPublishedManifest(context.Background(), storageRoot, input.CalculationManifestPath); err == nil ||
		!strings.Contains(err.Error(), "differs from immutable manifest") {
		t.Fatalf("error = %v; want immutable calculation rejection", err)
	}
}

func writeProjectionBundleFixture(t *testing.T, storageRoot, sourceReleaseID string) ProjectionBundleInput {
	t.Helper()
	fact := testFact(
		"7", "125", nil, stringPointer("C00000001"), stringPointer("H4AA00001"),
		stringPointer("S"), stringPointer("A"), stringPointer("TX-7"), stringPointer("24E"),
	)
	fact.SourceReleaseID = sourceReleaseID
	factManifestPath := publishTestFactSet(t, storageRoot, []fecoccurrence.ScheduleEFact{fact})
	calculation, err := Publish(context.Background(), PublishInput{ScheduleEFactManifestPath: factManifestPath}, "projection-calculation", PublishOptions{
		StorageRoot: storageRoot, Clock: func() time.Time { return time.Date(2026, 8, 31, 19, 30, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return ProjectionBundleInput{
		CalculationManifestPath: filepath.Join(storageRoot, calculationBase(), "current", calculation.Cycle+".json"),
		CandidateManifestPath: writeProjectionBundleClassicFacts(
			t, storageRoot, "candidate-master", sourceReleaseID, strings.Repeat("1", 64),
		),
		CommitteeManifestPath: writeProjectionBundleClassicFacts(
			t, storageRoot, "committee-master", sourceReleaseID, strings.Repeat("2", 64),
		),
	}
}

func writeProjectionBundleClassicFacts(t *testing.T, storageRoot, dataset, sourceReleaseID, occurrenceSetID string) string {
	t.Helper()
	spec, err := fecclassic.Lookup(dataset)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join("facts", "fec", "classic", dataset)
	temporary := filepath.Join(storageRoot, "tmp", dataset)
	writer, err := storageartifact.NewWriter(context.Background(), storageRoot, temporary, base, "facts")
	if err != nil {
		t.Fatal(err)
	}
	fact := fecoccurrence.ClassicFact{
		SchemaVersion: fecoccurrence.ClassicFactSchemaVersion, FactID: dataset + "-fact",
		FactType: spec.FactType, Dataset: dataset, Cycle: "2024", NaturalKey: dataset,
		OccurrenceSetID: occurrenceSetID, OccurrenceID: strings.Repeat("3", 64),
		RecordVersionID: strings.Repeat("4", 64), SourceReleaseID: sourceReleaseID,
		SourceContract: spec.SourceContract, State: "valid", IssueCodes: []string{}, SourceFields: map[string]string{},
		TypedFields: map[string]string{"id": dataset},
	}
	if err := writer.WriteJSON(fact); err != nil {
		writer.Abort()
		t.Fatal(err)
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	factSetID := occurrenceTestDigestParts(
		"fec.classic.fact-set.v1", dataset, occurrenceSetID,
		fecoccurrence.ClassicNormalizerVersion, fecoccurrence.ClassicFactSchemaVersion,
	)
	manifest := fecoccurrence.ClassicFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: fecoccurrence.ClassicFactSetSchemaVersion,
		FactSetID: factSetID, Dataset: dataset, FactType: spec.FactType, Cycle: "2024",
		SourceContract: spec.SourceContract, SourceReleaseID: sourceReleaseID,
		SourceReleaseManifestSHA256: strings.Repeat("5", 64), OccurrenceSetID: occurrenceSetID,
		OccurrenceManifestSHA256: strings.Repeat("6", 64), RunID: "projection-master-fixture",
		State: "published", NormalizerVersion: fecoccurrence.ClassicNormalizerVersion,
		FactSchemaVersion: fecoccurrence.ClassicFactSchemaVersion,
		PublishedAt:       time.Date(2026, 8, 31, 19, 45, 0, 0, time.UTC),
		Counts: fecoccurrence.ClassicFactCounts{
			SourceOccurrences: 1, Facts: 1, ValidFacts: 1,
		},
		Facts: fecoccurrence.Artifact{
			RecordCount: descriptor.RecordCount, UncompressedBytes: descriptor.UncompressedBytes,
			UncompressedSHA256: descriptor.UncompressedSHA256, CompressedBytes: descriptor.CompressedBytes,
			CompressedSHA256: descriptor.CompressedSHA256, Compression: descriptor.Compression,
			StorageKey: descriptor.StorageKey,
		},
		Checks: []fecoccurrence.Check{
			{ID: "occurrence_lineage", Passed: true, Severity: "block", Detail: "test"},
			{ID: "source_conservation", Passed: true, Severity: "block", Detail: "test"},
			{ID: "fact_state_conservation", Passed: true, Severity: "block", Detail: "test"},
			{ID: "artifact_integrity", Passed: true, Severity: "block", Detail: "test"},
			{ID: "unique_projection", Passed: true, Severity: "block", Detail: "test"},
		},
	}
	path := filepath.Join(storageRoot, base, "manifests", factSetID+".json")
	if err := writeAtomicJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	return path
}

func occurrenceTestDigestParts(parts ...string) string {
	hasher := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
