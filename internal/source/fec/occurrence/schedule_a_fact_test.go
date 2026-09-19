package occurrence

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func TestPublishScheduleAFactsPreservesSignedReceiptAndSourceFields(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactFixture(t, "negative-adjustment.copy")
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "schedule-fact", "", "schedule-fact-source", rows)
	occurrenceManifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-fact-occurrence", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", occurrenceManifest.OccurrenceSetID+".json")
	factManifest, err := PublishScheduleAFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-fact-normalize", Options{StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	if factManifest.Counts.SourceOccurrences != 1 || factManifest.Counts.Facts != 1 || factManifest.Counts.ValidFacts != 1 || factManifest.Counts.ExcludedOccurrences != 0 {
		t.Fatalf("unexpected fact counts: %+v", factManifest.Counts)
	}
	facts := readArtifactRecords[ScheduleAFact](t, storageRoot, factManifest.Facts)
	if len(facts) != 1 {
		t.Fatalf("facts = %d; want 1", len(facts))
	}
	fact := facts[0]
	var sourceFields map[string]any
	if err := json.Unmarshal(fact.SourceFields, &sourceFields); err != nil {
		t.Fatal(err)
	}
	if sourceFields["contb_receipt_amt"] != "-5000.00" || sourceFields["is_individual"] != false || fact.State != "valid" {
		t.Fatalf("unexpected source fields or state: %+v", fact)
	}
	amount := fact.TypedFields.Receipt.Amount
	if amount.RawValue == nil || *amount.RawValue != "-5000.00" || amount.ReportedMinorUnits == nil || *amount.ReportedMinorUnits != "-500000" || amount.SourceScale == nil || *amount.SourceScale != 2 {
		t.Fatalf("unexpected receipt amount: %+v", amount)
	}
	if fact.TypedFields.Receipt.ReceivedOn == nil || *fact.TypedFields.Receipt.ReceivedOn != "2025-05-09" {
		t.Fatalf("unexpected receipt date: %+v", fact.TypedFields.Receipt)
	}
	if fact.TypedFields.Election.TransactionPeriod != 2026 || fact.TypedFields.Filing.SubmissionID != "4061020251204972544" {
		t.Fatalf("unexpected typed identity: %+v", fact.TypedFields)
	}
}

func TestScheduleAFactNormalizationDerivesMemoWithoutCountingIt(t *testing.T) {
	t.Parallel()
	record := scheduleAFixtureRecord(t, "targeted-memo-committee.copy")
	typed, issues, err := normalizeScheduleAReceipt(record)
	if err != nil || len(issues) != 0 {
		t.Fatalf("normalize memo fact: issues=%v err=%v", issues, err)
	}
	if !typed.Receipt.MemoedSubtotal || typed.Receipt.MemoCode == nil || *typed.Receipt.MemoCode != "X" {
		t.Fatalf("memo derivation was not preserved: %+v", typed.Receipt)
	}
	if typed.Receipt.Amount.ReportedMinorUnits == nil || *typed.Receipt.Amount.ReportedMinorUnits != "5000" {
		t.Fatalf("memo amount was not preserved independently: %+v", typed.Receipt.Amount)
	}
	if typed.Filing.BackReferenceTransactionID == nil || *typed.Filing.BackReferenceTransactionID != "DA4957" {
		t.Fatalf("back reference was not preserved: %+v", typed.Filing)
	}
}

func TestScheduleAFactNormalizationKeepsNullReceiptTimeDistinct(t *testing.T) {
	t.Parallel()
	record := scheduleAFixtureRecord(t, "null-receipt-date.copy")
	typed, issues, err := normalizeScheduleAReceipt(record)
	if err != nil || len(issues) != 0 {
		t.Fatalf("normalize null receipt time: issues=%v err=%v", issues, err)
	}
	if typed.Receipt.ReceivedAtLocal != nil || typed.Receipt.ReceivedOn != nil {
		t.Fatalf("null receipt time became a value: %+v", typed.Receipt)
	}
}

func TestScheduleAFactProjectionExcludesDuplicateSubmissionIDs(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	row := exactFixture(t, "targeted-individual.copy")
	rows := append(append([]byte(nil), row...), row...)
	releaseManifest, releaseDigest := sourceReleaseFixture(t, storageRoot, "schedule-fact-duplicate", "", "schedule-fact-duplicate-source", rows)
	occurrenceManifest, err := Publish(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-fact-duplicate-occurrence", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", occurrenceManifest.OccurrenceSetID+".json")
	factManifest, err := PublishScheduleAFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-fact-duplicate-normalize", Options{StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	if factManifest.Counts.Facts != 0 || factManifest.Counts.ExcludedOccurrences != 2 || factManifest.Counts.SourceDuplicates != 2 || factManifest.Facts.RecordCount != 0 {
		t.Fatalf("unexpected duplicate projection: %+v", factManifest.Counts)
	}
}

func TestParseUSDMinorUnitsPreservesScaleAndRejectsSubcents(t *testing.T) {
	t.Parallel()
	minor, scale, issue := parseUSDMinorUnits("1.2300")
	if minor != "123" || scale != 4 || issue != "" {
		t.Fatalf("trailing-zero scale parse = %q, %d, %q", minor, scale, issue)
	}
	minor, scale, issue = parseUSDMinorUnits("1.234")
	if minor != "" || scale != 3 || issue != "minor_unit_precision" {
		t.Fatalf("subcent parse = %q, %d, %q", minor, scale, issue)
	}
}

func scheduleAFixtureRecord(t *testing.T, name string) schedulea.Record {
	t.Helper()
	decoder := schedulea.NewDecoder(bytes.NewReader(exactFixture(t, name)))
	if !decoder.Scan() {
		t.Fatalf("fixture has no row: %v", decoder.Err())
	}
	record, err := schedulea.Freeze(decoder.Row(), "2026")
	if err != nil {
		t.Fatal(err)
	}
	return record
}
