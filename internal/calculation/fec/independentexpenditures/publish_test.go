package independentexpenditures

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestPublishPolicyAndConservation(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	facts := []fecoccurrence.ScheduleEFact{
		testFact("1", "10050", nil, stringPointer("C00000001"), stringPointer("H4AA00001"), stringPointer("S"), stringPointer("A"), stringPointer("TX-1"), stringPointer("24E")),
		testFact("2", "-550", nil, stringPointer("C00000001"), stringPointer("H4AA00001"), stringPointer("S"), stringPointer("C"), stringPointer("TX-1"), stringPointer("24E")),
		testFact("3", "2500", nil, stringPointer("C00000001"), stringPointer("H4AA00001"), stringPointer("O"), stringPointer("N"), stringPointer("TX-2"), nil),
		testFact("4", "900", stringPointer("X"), stringPointer("C00000001"), stringPointer("H4AA00001"), stringPointer("S"), stringPointer("N"), stringPointer("TX-3"), stringPointer("24E")),
		testFact("5", "", nil, stringPointer("C00000001"), stringPointer("H4AA00001"), stringPointer("S"), stringPointer("A"), stringPointer("TX-4"), stringPointer("24E")),
		testFact("6", "700", nil, stringPointer("C00000001"), nil, stringPointer("S"), stringPointer("A"), stringPointer("TX-5"), stringPointer("24E")),
	}
	factManifestPath := publishTestFactSet(t, storageRoot, facts)
	fixedTime := time.Date(2026, 8, 31, 20, 0, 0, 0, time.UTC)
	manifest, err := Publish(context.Background(), PublishInput{ScheduleEFactManifestPath: factManifestPath}, "test-run", PublishOptions{
		StorageRoot: storageRoot,
		Clock:       func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	wantDecisions := DecisionCounts{SourceFacts: 6, Included: 4, ExcludedMemo: 1, ExcludedMemoAmountUnresolved: 0, UnresolvedAmount: 1}
	if !reflect.DeepEqual(manifest.DecisionCounts, wantDecisions) {
		t.Fatalf("decision counts = %+v; want %+v", manifest.DecisionCounts, wantDecisions)
	}
	wantRoutes := RouteCounts{Attributed: 3, Unattributed: 1, MissingCandidate: 1, ResultGroups: 2}
	if !reflect.DeepEqual(manifest.RouteCounts, wantRoutes) {
		t.Fatalf("route counts = %+v; want %+v", manifest.RouteCounts, wantRoutes)
	}
	wantAmounts := AmountTotals{
		IncludedMinorUnits: "12700", ExcludedMemoMinorUnits: "900",
		AttributedMinorUnits: "12000", UnattributedMinorUnits: "700",
	}
	if !reflect.DeepEqual(manifest.Amounts, wantAmounts) {
		t.Fatalf("amounts = %+v; want %+v", manifest.Amounts, wantAmounts)
	}
	if manifest.SourceShapeCounts.ActionAdd != 3 || manifest.SourceShapeCounts.ActionChange != 1 || manifest.SourceShapeCounts.ActionNoChange != 2 ||
		manifest.SourceShapeCounts.MissingExpenditureType != 1 || manifest.SourceShapeCounts.RepeatedTransactionKeys != 1 ||
		manifest.SourceShapeCounts.RepeatedTransactionOccurrences != 2 {
		t.Fatalf("unexpected source shapes: %+v", manifest.SourceShapeCounts)
	}

	results := readArtifactRecords[Result](t, storageRoot, manifest.Results)
	wantResults := []Result{
		{
			SchemaVersion: ResultSchemaVersion, ResultID: results[0].ResultID, CalculationSetID: manifest.CalculationSetID,
			Cycle: "2024", SpenderCommitteeID: "C00000001", CandidateID: "H4AA00001", SupportOppose: "O",
			SignedAmountMinorUnits: "2500", ExpenditureCount: 1, PositiveCount: 1, MissingExpenditureTypeCount: 1,
		},
		{
			SchemaVersion: ResultSchemaVersion, ResultID: results[1].ResultID, CalculationSetID: manifest.CalculationSetID,
			Cycle: "2024", SpenderCommitteeID: "C00000001", CandidateID: "H4AA00001", SupportOppose: "S",
			SignedAmountMinorUnits: "9500", ExpenditureCount: 2, PositiveCount: 1, NegativeCount: 1,
		},
	}
	if !reflect.DeepEqual(results, wantResults) {
		t.Fatalf("results = %+v; want %+v", results, wantResults)
	}
	exceptions := readArtifactRecords[Exception](t, storageRoot, manifest.Exceptions)
	if len(exceptions) != 2 || exceptions[0].State != "unresolved_amount" || exceptions[1].State != "included_unattributed" ||
		!reflect.DeepEqual(exceptions[1].ReasonCodes, []string{"missing_candidate"}) || exceptions[1].AmountMinorUnits == nil || *exceptions[1].AmountMinorUnits != "700" {
		t.Fatalf("unexpected exceptions: %+v", exceptions)
	}

	replayed, err := Publish(context.Background(), PublishInput{ScheduleEFactManifestPath: factManifestPath}, "another-run", PublishOptions{StorageRoot: storageRoot})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, replayed) {
		t.Fatal("idempotent publication did not reuse the immutable manifest")
	}
}

func TestPublishRejectsNoticeLikeFact(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	fact := testFact("7", "100", nil, stringPointer("C00000001"), stringPointer("H4AA00001"), stringPointer("S"), stringPointer("A"), stringPointer("TX-7"), stringPointer("24E"))
	fact.TypedFields.Filing.ReportTypeCode = stringPointer("24")
	factManifestPath := publishTestFactSet(t, storageRoot, []fecoccurrence.ScheduleEFact{fact})
	_, err := Publish(context.Background(), PublishInput{ScheduleEFactManifestPath: factManifestPath}, "test-notice", PublishOptions{StorageRoot: storageRoot})
	if err == nil || !strings.Contains(err.Error(), "notice-like") {
		t.Fatalf("error = %v; want notice-like rejection", err)
	}
	if _, statErr := os.Stat(filepath.Join(storageRoot, calculationBase(), "current", "2024.json")); !os.IsNotExist(statErr) {
		t.Fatalf("notice rejection published a current manifest: %v", statErr)
	}
}

func testFact(
	idCharacter, amount string,
	memo, spender, candidate, stance, action, transactionID, expenditureType *string,
) fecoccurrence.ScheduleEFact {
	observation := fecoccurrence.ScheduleEMoneyObservation{
		SemanticRole: "independent_expenditure", Currency: "USD", MeasurementKind: "reported_point",
		SourceRuleVersion: fecoccurrence.ScheduleESourceContract,
	}
	if amount == "" {
		observation.ObservationState = "source_null"
	} else {
		observation.ObservationState = "reported_value"
		observation.RawValue = stringPointer(amount)
		observation.ReportedMinorUnits = stringPointer(amount)
		scale := 2
		observation.SourceScale = &scale
	}
	factID := strings.Repeat(idCharacter, 64)
	return fecoccurrence.ScheduleEFact{
		SchemaVersion: fecoccurrence.ScheduleEFactSchemaVersion,
		FactID:        factID, FactType: fecoccurrence.ScheduleEFactType, Cycle: "2024",
		NaturalKey:      "fec:schedule-e:2024:" + idCharacter,
		OccurrenceSetID: strings.Repeat("c", 64), OccurrenceID: strings.Repeat("d", 64), RecordVersionID: strings.Repeat("e", 64),
		SourceReleaseID: "fec-" + strings.Repeat("b", 64), SourceContract: fecoccurrence.ScheduleESourceContract,
		State: "valid", IssueCodes: []string{}, SourceFields: json.RawMessage(`{}`),
		TypedFields: fecoccurrence.ScheduleEIndependentExpenditureTypedFields{
			Spender:   fecoccurrence.ScheduleESpenderFields{CommitteeID: spender},
			Candidate: fecoccurrence.ScheduleECandidateFields{CandidateID: candidate, SupportOpposeCode: stance},
			Election:  fecoccurrence.ScheduleEElectionFields{Cycle: 2024},
			Expenditure: fecoccurrence.ScheduleEExpenditureFields{
				Amount: observation, MemoCode: memo, ExpenditureTypeCode: expenditureType,
			},
			Filing: fecoccurrence.ScheduleEFilingFields{
				ActionCode: action, TransactionID: transactionID, SubmissionID: idCharacter,
				FilingForm: "F3X", ReportTypeCode: stringPointer("Q3"),
			},
		},
	}
}

func publishTestFactSet(t *testing.T, storageRoot string, facts []fecoccurrence.ScheduleEFact) string {
	t.Helper()
	ctx := context.Background()
	temporary := filepath.Join(storageRoot, "tmp")
	writer, err := storageartifact.NewWriter(ctx, storageRoot, temporary, filepath.Join("facts", "fec", "schedule-e"), "facts")
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range facts {
		if err := writer.WriteJSON(fact); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	artifact := fecoccurrence.Artifact{
		RecordCount: descriptor.RecordCount, UncompressedBytes: descriptor.UncompressedBytes,
		UncompressedSHA256: descriptor.UncompressedSHA256, CompressedBytes: descriptor.CompressedBytes,
		CompressedSHA256: descriptor.CompressedSHA256, Compression: descriptor.Compression, StorageKey: descriptor.StorageKey,
	}
	manifest := fecoccurrence.ScheduleEFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: fecoccurrence.ScheduleEFactSetSchemaVersion,
		FactSetID: strings.Repeat("a", 64), FactType: fecoccurrence.ScheduleEFactType, Cycle: "2024",
		SourceContract: fecoccurrence.ScheduleESourceContract, SourceReleaseID: "fec-" + strings.Repeat("b", 64),
		SourceReleaseManifestSHA256: strings.Repeat("f", 64), OccurrenceSetID: strings.Repeat("c", 64),
		OccurrenceManifestSHA256: strings.Repeat("1", 64), RunID: "fact-run", State: "published",
		NormalizerVersion: fecoccurrence.ScheduleENormalizerVersion, FactSchemaVersion: fecoccurrence.ScheduleEFactSchemaVersion,
		PublishedAt: time.Date(2026, 8, 31, 19, 0, 0, 0, time.UTC),
		Counts:      fecoccurrence.ScheduleEFactCounts{SourceOccurrences: uint64(len(facts)), Facts: uint64(len(facts)), ValidFacts: uint64(len(facts))},
		Facts:       artifact,
		Checks: []fecoccurrence.Check{
			{ID: "occurrence_lineage", Passed: true, Severity: "block", Detail: "test"},
			{ID: "selected_output_integrity", Passed: true, Severity: "block", Detail: "test"},
			{ID: "lossless_projection", Passed: true, Severity: "block", Detail: "test"},
			{ID: "fact_state_conservation", Passed: true, Severity: "block", Detail: "test"},
		},
	}
	path := filepath.Join(storageRoot, "facts", "fec", "schedule-e", "manifests", manifest.FactSetID+".json")
	if err := writeAtomicJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	return path
}

func readArtifactRecords[T any](t *testing.T, storageRoot string, descriptor storageartifact.Descriptor) []T {
	t.Helper()
	reader, err := storageartifact.Open[T](context.Background(), storageRoot, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Abort()
	var result []T
	for {
		value, ok, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		result = append(result, value)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return result
}

func stringPointer(value string) *string { return &value }
