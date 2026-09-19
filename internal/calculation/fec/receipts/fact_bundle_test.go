package receipts

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func TestPublishFactBundleFreezesExactSameReleaseInputs(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	sourceReleaseID := "fec-" + repeatedHex("a")
	input := writeFactBundleFixture(t, storageRoot, sourceReleaseID)
	fixed := time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)

	manifest, err := PublishFactBundle(context.Background(), input, "fact-bundle", FactBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024", Clock: func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.State != "ready" || manifest.Cycle != "2024" || manifest.SourceReleaseID != sourceReleaseID ||
		manifest.Counts.ScheduleAFacts != 2 || manifest.Counts.CandidateCommitteeLinkages != 1 ||
		manifest.Counts.AllCandidatesSummaries != 1 || manifest.Counts.CurrentCampaignsSummaries != 1 {
		t.Fatalf("unexpected fact bundle: %+v", manifest)
	}
	wantRoles := []string{"all_candidates_summary", "candidate_committee_linkage", "current_campaigns_summary", "schedule_a_receipts"}
	for index, role := range wantRoles {
		if manifest.InputFactSets[index].Role != role {
			t.Fatalf("input role %d = %q; want %q", index, manifest.InputFactSets[index].Role, role)
		}
	}
	currentPath := filepath.Join(storageRoot, factBundleBase(), "current", "2024.json")
	if _, err := os.Stat(currentPath); err != nil {
		t.Fatalf("stat current fact bundle: %v", err)
	}
	replayed, err := PublishFactBundle(context.Background(), input, "different-run", FactBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024", Clock: func() time.Time { return fixed.Add(time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, replayed) {
		t.Fatal("same-input fact bundle was not reused")
	}

	calculation, err := PublishCompactFromFactBundle(context.Background(), currentPath, "bundle-calculation", CompactPublishOptions{
		StorageRoot: storageRoot, Clock: func() time.Time { return fixed.Add(2 * time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if calculation.Cycle != manifest.Cycle || calculation.SourceReleaseID != manifest.SourceReleaseID ||
		!reflect.DeepEqual(calculation.InputFactSets, manifest.InputFactSets) {
		t.Fatalf("calculation did not consume exact fact bundle: %+v", calculation)
	}
}

func TestPublishFactBundleRejectsMixedRelease(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	input := writeFactBundleFixture(t, storageRoot, "fec-"+repeatedHex("a"))
	input.CurrentCampaignsManifestPath = writeClassicFactInput(
		t, storageRoot, "fec-"+repeatedHex("9"), "current-campaigns-summary",
		"fec.campaign_summary.v1", repeatedHex("9"), []fecoccurrence.ClassicFact{
			summaryClassicFact("fec-"+repeatedHex("9"), "current-campaigns-summary", "fec.campaign_summary.v1", "mixed", "5000", "10000"),
		},
	)
	_, err := PublishFactBundle(context.Background(), input, "mixed-release", FactBundleOptions{
		StorageRoot: storageRoot, ExpectedCycle: "2024",
	})
	if err == nil || !strings.Contains(err.Error(), "do not share Schedule A cycle and source release") {
		t.Fatalf("error = %v; want mixed-release rejection", err)
	}
}

func TestValidateFactBundleRequiresExactBlockingChecks(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	manifest, err := PublishFactBundle(
		context.Background(),
		writeFactBundleFixture(t, storageRoot, "fec-"+repeatedHex("a")),
		"fact-bundle-checks",
		FactBundleOptions{StorageRoot: storageRoot, ExpectedCycle: "2024"},
	)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Checks[0].ID = "substituted_check"
	if err := validateFactBundleManifest(manifest); err == nil || !strings.Contains(err.Error(), "required fact bundle check") {
		t.Fatalf("error = %v; want exact-check-set rejection", err)
	}
}

func writeFactBundleFixture(t *testing.T, storageRoot, sourceReleaseID string) FactBundleInput {
	t.Helper()
	return FactBundleInput{
		ColumnarFactManifestPath: writeCompactColumnarInput(t, storageRoot, sourceReleaseID),
		LinkageFactManifestPath: writeClassicFactInput(t, storageRoot, sourceReleaseID, "candidate-committee-linkage", "fec.candidate_committee_linkage.v1", repeatedHex("b"), []fecoccurrence.ClassicFact{{
			SchemaVersion: fecoccurrence.ClassicFactSchemaVersion, FactID: "link-1", FactType: "fec.candidate_committee_linkage.v1", Dataset: "candidate-committee-linkage", Cycle: "2024", SourceReleaseID: sourceReleaseID, State: "valid",
			TypedFields: fecoccurrence.LinkageTypedFields{CandidateID: "H0AA00005", CommitteeID: "C00392928", DesignationCode: "A"},
		}}),
		AllCandidatesFactManifestPath: writeClassicFactInput(t, storageRoot, sourceReleaseID, "all-candidates-summary", "fec.candidate_summary_all.v1", repeatedHex("c"), []fecoccurrence.ClassicFact{
			summaryClassicFact(sourceReleaseID, "all-candidates-summary", "fec.candidate_summary_all.v1", "summary-all", "5000", "10000"),
		}),
		CurrentCampaignsManifestPath: writeClassicFactInput(t, storageRoot, sourceReleaseID, "current-campaigns-summary", "fec.campaign_summary.v1", repeatedHex("d"), []fecoccurrence.ClassicFact{
			summaryClassicFact(sourceReleaseID, "current-campaigns-summary", "fec.campaign_summary.v1", "summary-current", "5000", "10000"),
		}),
	}
}
