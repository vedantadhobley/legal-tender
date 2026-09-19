package occurrence

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublishScheduleEFactsPreservesEverySelectedRowAndExactSourceFields(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	escaped := exactScheduleEFixture(t, "escaped-copy-text.copy")
	memo := exactScheduleEFixture(t, "memo-x.copy")
	rows := append(append([]byte(nil), escaped...), memo...)
	releaseManifest, releaseDigest := sourceReleaseV2Fixture(t, storageRoot, "schedule-e-facts", "", rows)
	occurrenceManifest, err := PublishScheduleEOccurrences(context.Background(), releaseManifest, releaseDigest, "2024", "schedule-e-fact-occurrences", Options{
		StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, scheduleEEvidenceBase(), "manifests", occurrenceManifest.OccurrenceSetID+".json")
	factManifest, err := PublishScheduleEFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-e-facts", Options{
		StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if factManifest.Counts.SourceOccurrences != 2 || factManifest.Counts.Facts != 2 || factManifest.Counts.ValidFacts != 2 || factManifest.Counts.InvalidFacts != 0 {
		t.Fatalf("unexpected Schedule E fact counts: %+v", factManifest.Counts)
	}
	facts := readArtifactRecords[ScheduleEFact](t, storageRoot, factManifest.Facts)
	if len(facts) != 2 {
		t.Fatalf("facts = %d; want 2", len(facts))
	}
	first := facts[0]
	var sourceFields map[string]any
	if err := json.Unmarshal(first.SourceFields, &sourceFields); err != nil {
		t.Fatal(err)
	}
	description, _ := sourceFields["exp_desc"].(string)
	if !strings.Contains(description, "\t") || sourceFields["exp_amt"] != "50.00" || sourceFields["s_o_ind"] != "O" || sourceFields["action_cd"] != "C" {
		t.Fatalf("exact Schedule E source fields were not preserved: %+v", sourceFields)
	}
	amount := first.TypedFields.Expenditure.Amount
	if amount.RawValue == nil || *amount.RawValue != "50.00" || amount.ReportedMinorUnits == nil || *amount.ReportedMinorUnits != "5000" || amount.SourceScale == nil || *amount.SourceScale != 2 {
		t.Fatalf("unexpected typed expenditure amount: %+v", amount)
	}
	if first.TypedFields.Candidate.CandidateID == nil || *first.TypedFields.Candidate.CandidateID != "P80001571" || first.TypedFields.Candidate.SupportOpposeCode == nil || *first.TypedFields.Candidate.SupportOpposeCode != "O" {
		t.Fatalf("candidate context was not preserved: %+v", first.TypedFields.Candidate)
	}
	if facts[1].TypedFields.Expenditure.MemoCode == nil || *facts[1].TypedFields.Expenditure.MemoCode != "X" {
		t.Fatalf("memo row was collapsed or reinterpreted: %+v", facts[1].TypedFields.Expenditure)
	}

	replayed, err := PublishScheduleEFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "another-run", Options{
		StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.FactSetID != factManifest.FactSetID || replayed.RunID != factManifest.RunID {
		t.Fatalf("idempotent Schedule E fact publication changed output: %+v", replayed)
	}
}

func TestPublishScheduleEFactsConvertsPublisherMoneyToExactMinorUnits(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := exactScheduleEFixture(t, "fractional-amount.copy")
	releaseManifest, releaseDigest := sourceReleaseV2Fixture(t, storageRoot, "schedule-e-fractional-fact", "", rows)
	occurrenceManifest, err := PublishScheduleEOccurrences(context.Background(), releaseManifest, releaseDigest, "2026", "schedule-e-fractional-occurrence", Options{
		StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, scheduleEEvidenceBase(), "manifests", occurrenceManifest.OccurrenceSetID+".json")
	factManifest, err := PublishScheduleEFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "schedule-e-fractional-fact", Options{
		StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable,
	})
	if err != nil {
		t.Fatal(err)
	}
	facts := readArtifactRecords[ScheduleEFact](t, storageRoot, factManifest.Facts)
	amount := facts[0].TypedFields.Expenditure.Amount
	if amount.ReportedMinorUnits == nil || *amount.ReportedMinorUnits != "6099" || amount.ObservationState != "reported_value" {
		t.Fatalf("unexpected exact money observation: %+v", amount)
	}
	if facts[0].TypedFields.Election.CalendarYTDOfficeSought.ReportedMinorUnits == nil || *facts[0].TypedFields.Election.CalendarYTDOfficeSought.ReportedMinorUnits != "7946084" {
		t.Fatalf("unexpected calendar YTD observation: %+v", facts[0].TypedFields.Election.CalendarYTDOfficeSought)
	}
}
