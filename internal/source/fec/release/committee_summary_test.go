package release

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCommitteeSummaryInventoryAndMigration(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v4/inventory.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var contract Inventory
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	inventory := CommitteeSummaryInventory()
	if !reflect.DeepEqual(inventory, contract) {
		t.Fatal("v4 compiled inventory differs from contract")
	}
	if issues := ValidateInventory(inventory); len(issues) != 0 {
		t.Fatal(issues)
	}
	if len(inventory.Sources) != 27 || len(desiredStageOutputs(inventory)) != 25 {
		t.Fatal("whole CSV must not create a ZIP or COPY extract")
	}
	prior := ActiveInventory()
	if !reflect.DeepEqual(inventory.Sources[:len(prior.Sources)], prior.Sources) {
		t.Fatal("v4 mutated v3")
	}
	discovery := availableDiscovery(inventory, "same")
	current := publishedManifest(prior, availableDiscovery(prior, "same"))
	plan := Plan(inventory, discovery, &current, testPlannedAt)
	assertValidPlan(t, inventory, plan)
	if plan.Status != PlanUpdateAvailable || len(plan.ReusedSourceIDs) != 23 || len(plan.ChangedSourceIDs) != 4 {
		t.Fatal(plan)
	}
	for i, cycle := range inventory.Periods {
		if plan.ChangedSourceIDs[i] != "fec:committee-summary:"+cycle {
			t.Fatal(plan.ChangedSourceIDs)
		}
	}
	current = publishedManifest(inventory, discovery)
	if got := Plan(inventory, discovery, &current, testPlannedAt); got.Status != PlanNoChange {
		t.Fatal(got)
	}
	discovery.Observations = discovery.Observations[:26]
	if got := Plan(inventory, discovery, &current, testPlannedAt); got.Status != PlanInvalid || got.CandidateReleaseID != "" {
		t.Fatal("missing summary authorized acquisition")
	}
	inventory.Sources[23].ArtifactFormat = ""
	if len(ValidateInventory(inventory)) == 0 {
		t.Fatal("artifact-format drift accepted")
	}
	copy := CommitteeSummaryInventory()
	copy.Periods[0] = "2018"
	copy.Sources[23].Periods[0] = "2018"
	replay, ok := InventoryForVersion(CommitteeSummaryInventoryVersion)
	if !ok || replay.Periods[0] != "2020" || replay.Sources[23].Periods[0] != "2020" {
		t.Fatal("mutable v4 inventory")
	}
}

func TestCommitteeSummaryContainerValidation(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repositoryRoot(t), "contracts/sources/fec/committee-summary/v1/fixtures/sample.csv"))
	if err != nil {
		t.Fatal(err)
	}
	source := CommitteeSummaryInventory().Sources[25]
	for name, input := range map[string][]byte{
		"valid_with_issues": raw,
		"wrong_header":      append([]byte("unexpected"), raw...),
		"blank_record":      append(append([]byte(nil), raw...), '\n'),
		"wrong_cycle":       raw,
		"truncated":         raw[:len(raw)-1],
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "captured.csv")
			if err := os.WriteFile(path, input, 0o600); err != nil {
				t.Fatal(err)
			}
			selected := source
			if name == "wrong_cycle" {
				selected.Periods = []string{"2022"}
			}
			check, err := validateContainer(context.Background(), selected, path, "")
			if name == "valid_with_issues" {
				if err != nil || !strings.Contains(check, "rows=8") || !strings.Contains(check, "rows_with_issues=2") {
					t.Fatal(check, err)
				}
			} else if err == nil {
				t.Fatal("invalid CSV passed", check)
			}
			if retained, err := os.ReadFile(path); err != nil || string(retained) != string(input) {
				t.Fatal("capture was modified")
			}
		})
	}
}
