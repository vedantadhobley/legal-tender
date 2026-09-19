package receipts

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestPublishRejectsFactSetWithDuplicateSourceReferences(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	schedulePath := writeScheduleFactInput(t, storageRoot, "fec-"+repeatedHex("a"))
	manifest, _, err := readStrictJSON[fecoccurrence.ScheduleAFactManifest](schedulePath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Counts.SourceDuplicates = 1
	if err := writeAtomicJSON(schedulePath, manifest); err != nil {
		t.Fatal(err)
	}
	_, err = Publish(context.Background(), PublishInput{ScheduleAFactManifestPath: schedulePath}, "duplicate-test", PublishOptions{StorageRoot: storageRoot})
	if err == nil || !strings.Contains(err.Error(), "excludes 1 duplicate source occurrences") {
		t.Fatalf("expected duplicate-source block, got %v", err)
	}
}

func TestPublishReconcilesOneCycleAndReusesIdenticalInputs(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	sourceReleaseID := "fec-" + repeatedHex("a")
	schedulePath := writeScheduleFactInput(t, storageRoot, sourceReleaseID)
	linkagePath := writeClassicFactInput(t, storageRoot, sourceReleaseID, "candidate-committee-linkage", "fec.candidate_committee_linkage.v1", repeatedHex("b"), []fecoccurrence.ClassicFact{{
		SchemaVersion: fecoccurrence.ClassicFactSchemaVersion, FactID: "link-1", FactType: "fec.candidate_committee_linkage.v1", Dataset: "candidate-committee-linkage", Cycle: "2024", SourceReleaseID: sourceReleaseID, State: "valid",
		TypedFields: fecoccurrence.LinkageTypedFields{CandidateID: "H0AA00005", CommitteeID: "C00000005", DesignationCode: "A"},
	}})
	weballPath := writeClassicFactInput(t, storageRoot, sourceReleaseID, "all-candidates-summary", "fec.candidate_summary_all.v1", repeatedHex("c"), []fecoccurrence.ClassicFact{
		summaryClassicFact(sourceReleaseID, "all-candidates-summary", "fec.candidate_summary_all.v1", "summary-all", "250", "1000"),
	})
	weblPath := writeClassicFactInput(t, storageRoot, sourceReleaseID, "current-campaigns-summary", "fec.campaign_summary.v1", repeatedHex("d"), []fecoccurrence.ClassicFact{
		summaryClassicFact(sourceReleaseID, "current-campaigns-summary", "fec.campaign_summary.v1", "summary-current", "275", "1100"),
	})
	fixedTime := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
	input := PublishInput{
		ScheduleAFactManifestPath: schedulePath, LinkageFactManifestPath: linkagePath,
		AllCandidatesFactManifestPath: weballPath, CurrentCampaignsManifestPath: weblPath,
	}
	manifest, err := Publish(context.Background(), input, "calculation-test", PublishOptions{StorageRoot: storageRoot, Clock: func() time.Time { return fixedTime }})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Counts.ReceiptFacts != 1 || manifest.Counts.ReceiptDecisions != 1 || manifest.Counts.LinkageFacts != 1 || manifest.Counts.SummaryFacts != 2 || manifest.Counts.Candidates != 1 || manifest.Counts.Complete != 1 || manifest.Counts.Reconciliations != 2 || manifest.Counts.DateBounded != 2 {
		t.Fatalf("unexpected publication counts: %+v", manifest.Counts)
	}
	decisionReader, err := storageartifact.Open[ReceiptDecision](context.Background(), storageRoot, manifest.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok, err := decisionReader.Next()
	if err != nil || !ok || decision.FactID != "receipt-1" || decision.State != "included" || pointerValue(decision.AmountMinorUnits) != "100" {
		t.Fatalf("unexpected receipt decision: ok=%v err=%v decision=%+v", ok, err, decision)
	}
	if _, ok, err := decisionReader.Next(); err != nil || ok {
		t.Fatalf("decision artifact did not end cleanly: ok=%v err=%v", ok, err)
	}
	if err := decisionReader.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := storageartifact.Open[Result](context.Background(), storageRoot, manifest.Results)
	if err != nil {
		t.Fatal(err)
	}
	result, ok, err := reader.Next()
	if err != nil || !ok {
		t.Fatalf("read candidate result: ok=%v err=%v", ok, err)
	}
	if result.CandidateID != "H0AA00005" || pointerValue(result.MoneyMeasure.Amount.LowerMinorUnits) != "100" || len(result.Reconciliations) != 2 {
		t.Fatalf("unexpected published candidate result: %+v", result)
	}
	if _, ok, err := reader.Next(); err != nil || ok {
		t.Fatalf("candidate artifact did not end cleanly: ok=%v err=%v", ok, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	reused, err := Publish(context.Background(), input, "different-run", PublishOptions{StorageRoot: storageRoot, Clock: func() time.Time { return fixedTime.Add(time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	if !reused.PublishedAt.Equal(fixedTime) || reused.RunID != "calculation-test" || reused.CalculationSetID != manifest.CalculationSetID {
		t.Fatalf("identical inputs did not reuse immutable publication: %+v", reused)
	}
}

func writeScheduleFactInput(t *testing.T, storageRoot, sourceReleaseID string) string {
	t.Helper()
	base := filepath.Join("facts", "fec", "schedule-a")
	descriptor := writeArtifact(t, storageRoot, base, []fecoccurrence.ScheduleAFact{{
		SchemaVersion: fecoccurrence.ScheduleAFactSchemaVersion, FactID: "receipt-1", FactType: fecoccurrence.ScheduleAFactType,
		Cycle: "2024", NaturalKey: "receipt-natural-key", SourceReleaseID: sourceReleaseID, State: "valid", SourceFields: json.RawMessage("{}"),
		TypedFields: fecoccurrence.ScheduleAReceiptTypedFields{
			Recipient:   fecoccurrence.ScheduleARecipientFields{CommitteeID: stringPointer("C00000005")},
			Contributor: fecoccurrence.ScheduleAContributorFields{PublisherClassedIndividual: boolPointer(true)},
			Receipt: fecoccurrence.ScheduleAReceiptFields{
				ReceivedOn: stringPointer("2024-01-15"),
				Amount:     fecoccurrence.ScheduleAMoneyObservation{ObservationState: "reported_value", ReportedMinorUnits: stringPointer("100")},
			},
		},
	}})
	manifest := fecoccurrence.ScheduleAFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: fecoccurrence.ScheduleAFactSetSchemaVersion,
		FactSetID: repeatedHex("e"), FactType: fecoccurrence.ScheduleAFactType, FactSchemaVersion: fecoccurrence.ScheduleAFactSchemaVersion,
		Cycle: "2024", SourceReleaseID: sourceReleaseID, State: "published", RunID: "fixture", PublishedAt: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
		Counts: fecoccurrence.ScheduleAFactCounts{SourceOccurrences: 1, Facts: 1, ValidFacts: 1}, Facts: occurrenceDescriptor(descriptor),
	}
	path := filepath.Join(storageRoot, base, "manifests", manifest.FactSetID+".json")
	if err := writeAtomicJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeClassicFactInput(t *testing.T, storageRoot, sourceReleaseID, dataset, factType, factSetID string, facts []fecoccurrence.ClassicFact) string {
	t.Helper()
	base := filepath.Join("facts", "fec", "classic", dataset)
	descriptor := writeArtifact(t, storageRoot, base, facts)
	manifest := fecoccurrence.ClassicFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: fecoccurrence.ClassicFactSetSchemaVersion,
		FactSetID: factSetID, Dataset: dataset, FactType: factType, FactSchemaVersion: fecoccurrence.ClassicFactSchemaVersion,
		Cycle: "2024", SourceReleaseID: sourceReleaseID, State: "published", RunID: "fixture", PublishedAt: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
		Counts: fecoccurrence.ClassicFactCounts{SourceOccurrences: uint64(len(facts)), Facts: uint64(len(facts)), ValidFacts: uint64(len(facts))}, Facts: occurrenceDescriptor(descriptor),
	}
	path := filepath.Join(storageRoot, base, "manifests", manifest.FactSetID+".json")
	if err := writeAtomicJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	return path
}

func summaryClassicFact(sourceReleaseID, dataset, factType, factID, individual, total string) fecoccurrence.ClassicFact {
	coverage := "2024-12-31"
	return fecoccurrence.ClassicFact{
		SchemaVersion: fecoccurrence.ClassicFactSchemaVersion, FactID: factID, FactType: factType, Dataset: dataset, Cycle: "2024", SourceReleaseID: sourceReleaseID, State: "valid",
		TypedFields: fecoccurrence.SummaryTypedFields{
			CandidateID: "H0AA00005", CoverageThrough: &coverage, SourceCycle: 2024,
			Money: map[string]fecoccurrence.SummaryMoneyObservation{
				"TTL_INDIV_CONTRIB": {RawValue: individual, ReportedMinorUnits: &individual, ObservationState: "reported_value"},
				"TTL_RECEIPTS":      {RawValue: total, ReportedMinorUnits: &total, ObservationState: "reported_value"},
			},
		},
	}
}

func writeArtifact[T any](t *testing.T, storageRoot, base string, values []T) storageartifact.Descriptor {
	t.Helper()
	temporary := t.TempDir()
	writer, err := storageartifact.NewWriter(context.Background(), storageRoot, temporary, base, "facts")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if err := writer.WriteJSON(value); err != nil {
			writer.Abort()
			t.Fatal(err)
		}
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func occurrenceDescriptor(input storageartifact.Descriptor) fecoccurrence.Artifact {
	return fecoccurrence.Artifact{
		RecordCount: input.RecordCount, UncompressedBytes: input.UncompressedBytes, UncompressedSHA256: input.UncompressedSHA256,
		CompressedBytes: input.CompressedBytes, CompressedSHA256: input.CompressedSHA256, Compression: input.Compression, StorageKey: input.StorageKey,
	}
}

func repeatedHex(value string) string {
	return value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value +
		value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value +
		value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value +
		value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value
}
