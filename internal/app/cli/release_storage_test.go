package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestStorageReviewRejectsArgumentsAndNonUpdatePlan(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2},
		{[]string{"--help"}, 0},
		{[]string{"--plan", "missing", "--storage-root", "missing", "extra"}, 2},
		{[]string{"--plan", "missing", "--storage-root", "missing"}, 1},
		{[]string{"--plan", filepath.Join(repositoryRoot(t), "contracts/releases/fec/v1/fixtures/no-change.json"), "--storage-root", t.TempDir()}, 1},
	} {
		var stdout, stderr bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "review-release-storage"}, tc.args...), &stdout, &stderr); code != tc.code {
			t.Fatalf("args=%v code=%d stderr=%s", tc.args, code, stderr.String())
		}
	}
}

func TestStorageReviewEmitsIncompleteSeedWithoutWrites(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "raw", "fec", "schedule-a"), 0o750); err != nil {
		t.Fatal(err)
	}
	planContent, err := os.ReadFile(filepath.Join(repositoryRoot(t), "contracts/releases/fec/v1/fixtures/update-available.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture fecrelease.ReleasePlan
	if err := json.Unmarshal(planContent, &fixture); err != nil {
		t.Fatal(err)
	}
	inventory := fecrelease.InitialInventory()
	now := time.Now().UTC()
	discovery := fecrelease.Discovery{SchemaVersion: fecrelease.DiscoverySchemaVersion, InventoryVersion: inventory.InventoryVersion, StartedAt: now, CompletedAt: now}
	for _, source := range fixture.SelectedSources {
		length := int64(10)
		discovery.Observations = append(discovery.Observations, fecrelease.Observation{SourceID: source.SourceID, RequestMethod: "HEAD", RequestURL: source.RequestURL, FinalURL: source.RequestURL, ObservedAt: now, Status: fecrelease.ObservationAvailable, HTTPStatus: 200, VersionIdentity: "version_id:test", VersionBasis: "version_id", VersionID: "test", ContentLength: &length})
	}
	plan := fecrelease.Plan(inventory, discovery, nil, now)
	if plan.Status != fecrelease.PlanUpdateAvailable {
		t.Fatalf("plan=%+v", plan)
	}
	content, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Run([]string{"pipeline", "fec", "review-release-storage", "--plan", path, "--storage-root", root}, &stdout, &stderr)
	var report fecrelease.StorageReview
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode=%v stderr=%s", err, stderr.String())
	}
	if code != 1 || report.Scenario.Complete || report.Scenario.FitsBudget || len(report.Scenario.Outputs) != 24 {
		t.Fatalf("code=%d report=%+v", code, report)
	}
	entries, err := os.ReadDir(filepath.Join(root, "raw", "fec"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("review wrote state: %v %v", entries, err)
	}
}
