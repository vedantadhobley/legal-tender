package occurrence

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
)

func TestPublishClassicFactsNormalizesCandidateAssertion(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := classicFixture(t, classic.CandidateMaster, "current-candidate.txt")
	releaseManifest, releaseDigest := classicSourceReleaseFixture(t, storageRoot, "fact-candidate", "", classic.CandidateMaster, "2024", rows)
	occurrence, err := PublishClassic(context.Background(), releaseManifest, releaseDigest, string(classic.CandidateMaster), "2024", "fact-candidate-occurrence", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, classicEvidenceBase(mustClassicSpec(t, classic.CandidateMaster)), "manifests", occurrence.OccurrenceSetID+".json")
	manifest, err := PublishClassicFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "fact-candidate-normalize", Options{StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatalf("PublishClassicFacts() error = %v", err)
	}
	if manifest.Counts.SourceOccurrences != 1 || manifest.Counts.Facts != 1 || manifest.Counts.ValidFacts != 1 || manifest.Counts.ExcludedOccurrences != 0 {
		t.Fatalf("unexpected candidate fact counts: %+v", manifest.Counts)
	}
	facts := readArtifactRecords[ClassicFact](t, storageRoot, manifest.Facts)
	if len(facts) != 1 {
		t.Fatalf("facts = %d; want 1", len(facts))
	}
	if _, ok := facts[0].TypedFields.(map[string]any); !ok {
		t.Fatalf("artifact typed fields did not decode as a JSON object: %T", facts[0].TypedFields)
	}
	// Artifact JSON decoding into an interface produces a map. Assert the exact
	// raw and envelope values here; concrete normalizer fields are tested below.
	if facts[0].SourceFields["CAND_NAME"] != "AVERHART, JAMES" || facts[0].FactType != "fec.candidate_assertion.v1" || facts[0].State != "valid" {
		t.Fatalf("unexpected candidate fact: %+v", facts[0])
	}
	normalized, issues, err := normalizeClassicFields(mustClassicSpec(t, classic.CandidateMaster), "2024", facts[0].SourceFields)
	if err != nil || len(issues) != 0 {
		t.Fatalf("normalize candidate: issues=%v err=%v", issues, err)
	}
	candidate := normalized.(CandidateTypedFields)
	if candidate.CandidateID != "H0AL01097" || candidate.CandidateElectionYear != 2024 || candidate.PrincipalCampaignCommitteeID == nil || *candidate.PrincipalCampaignCommitteeID != "C00708867" {
		t.Fatalf("unexpected typed candidate: %+v", candidate)
	}
}

func TestPublishClassicFactsPreservesSummaryMoneyAsSignedCents(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	rows := classicFixture(t, classic.AllCandidatesSummary, "prior-coverage-negative-refund.txt")
	releaseManifest, releaseDigest := classicSourceReleaseFixture(t, storageRoot, "fact-summary", "", classic.AllCandidatesSummary, "2024", rows)
	occurrence, err := PublishClassic(context.Background(), releaseManifest, releaseDigest, string(classic.AllCandidatesSummary), "2024", "fact-summary-occurrence", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, classicEvidenceBase(mustClassicSpec(t, classic.AllCandidatesSummary)), "manifests", occurrence.OccurrenceSetID+".json")
	manifest, err := PublishClassicFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "fact-summary-normalize", Options{StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	facts := readArtifactRecords[ClassicFact](t, storageRoot, manifest.Facts)
	normalized, issues, err := normalizeClassicFields(mustClassicSpec(t, classic.AllCandidatesSummary), "2024", facts[0].SourceFields)
	if err != nil || len(issues) != 0 {
		t.Fatalf("normalize summary: issues=%v err=%v", issues, err)
	}
	summary := normalized.(SummaryTypedFields)
	if summary.CoverageThrough == nil || *summary.CoverageThrough != "2023-03-31" {
		t.Fatalf("coverage = %v", summary.CoverageThrough)
	}
	refund := summary.Money["INDIV_REFUNDS"]
	if refund.ReportedMinorUnits == nil || *refund.ReportedMinorUnits != "-915250" || refund.RawValue != "-9152.5" || refund.MeasurementKind != "summary_value" {
		t.Fatalf("unexpected refund observation: %+v", refund)
	}
}

func TestClassicFactProjectionExcludesDuplicatePublisherKeys(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	row := classicFixture(t, classic.CommitteeMaster, "connected-organization.txt")
	rows := append(append([]byte(nil), row...), row...)
	releaseManifest, releaseDigest := classicSourceReleaseFixture(t, storageRoot, "fact-duplicate", "", classic.CommitteeMaster, "2024", rows)
	occurrence, err := PublishClassic(context.Background(), releaseManifest, releaseDigest, string(classic.CommitteeMaster), "2024", "fact-duplicate-occurrence", Options{StorageRoot: storageRoot, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(storageRoot, classicEvidenceBase(mustClassicSpec(t, classic.CommitteeMaster)), "manifests", occurrence.OccurrenceSetID+".json")
	manifest, err := PublishClassicFacts(context.Background(), releaseManifest, releaseDigest, occurrencePath, "fact-duplicate-normalize", Options{StorageRoot: storageRoot, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Counts.Facts != 0 || manifest.Counts.ExcludedOccurrences != 2 || manifest.Counts.SourceDuplicates != 2 || manifest.Facts.RecordCount != 0 {
		t.Fatalf("unexpected duplicate fact projection: %+v", manifest.Counts)
	}
}

func TestSummaryMoneyRejectsUnsupportedMinorUnitPrecision(t *testing.T) {
	t.Parallel()
	observation, issue := normalizeSummaryMoney("1.234")
	if issue != "minor_unit_precision" || observation.ObservationState != "invalid" || observation.ReportedMinorUnits != nil {
		t.Fatalf("unexpected precision result: %+v issue=%q", observation, issue)
	}
}
