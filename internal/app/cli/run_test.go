// CLI tests bind command dispatch and JSON output to the verification package.
package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestPlanReleaseCommand(t *testing.T) {
	t.Parallel()
	inventory := fecrelease.InitialInventory()
	observedAt := time.Date(2026, 8, 31, 8, 0, 0, 0, time.UTC)
	discovery := fecrelease.Discovery{
		SchemaVersion:    fecrelease.DiscoverySchemaVersion,
		InventoryVersion: inventory.InventoryVersion,
		StartedAt:        observedAt,
		CompletedAt:      observedAt,
		Observations:     make([]fecrelease.Observation, 0, len(inventory.Sources)),
	}
	for _, source := range inventory.Sources {
		discovery.Observations = append(discovery.Observations, fecrelease.Observation{
			SourceID:        source.SourceID,
			RequestMethod:   http.MethodHead,
			RequestURL:      source.RequestURL,
			FinalURL:        source.RequestURL,
			ObservedAt:      observedAt,
			Status:          fecrelease.ObservationAvailable,
			HTTPStatus:      http.StatusOK,
			VersionIdentity: "version_id:cli-fixture",
			VersionBasis:    "version_id",
			VersionID:       "cli-fixture",
		})
	}
	path := filepath.Join(t.TempDir(), "discovery.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create discovery fixture: %v", err)
	}
	if err := json.NewEncoder(file).Encode(discovery); err != nil {
		t.Fatalf("encode discovery fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close discovery fixture: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "plan-release", "--observations", path}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("exit code = %d; stderr = %s", exitCode, stderr.String())
	}
	var result fecrelease.ReleasePlan
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode command output: %v\n%s", err, stdout.String())
	}
	if result.Status != fecrelease.PlanUpdateAvailable || len(result.SelectedSources) != 21 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if issues := fecrelease.ValidatePlan(inventory, result); len(issues) != 0 {
		t.Fatalf("command emitted invalid release plan: %+v", issues)
	}
}

func TestDiscoverRejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "discover", "--timeout", "0s"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
}

func TestAcquireRejectsNoChangeBeforeNetwork(t *testing.T) {
	t.Parallel()
	planPath := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v1/fixtures/no-change.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{
		"pipeline", "fec", "acquire",
		"--plan", planPath,
		"--storage-root", t.TempDir(),
		"--run-id", "no-change",
	}, &stdout, &stderr)
	if exitCode != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "does not authorize acquisition") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestAcquireBlocksUpdatePlanWithoutContentLengths(t *testing.T) {
	t.Parallel()
	planPath := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v1/fixtures/update-available.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{
		"pipeline", "fec", "acquire",
		"--plan", planPath,
		"--storage-root", t.TempDir(),
		"--run-id", "missing-lengths",
	}, &stdout, &stderr)
	var result fecrelease.AcquisitionResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode acquisition result: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	if exitCode != 1 || result.Status != fecrelease.AcquisitionBlocked || len(result.Issues) != 1 || result.Issues[0].Code != "content_length_required" {
		t.Fatalf("exit=%d result=%+v stderr=%q", exitCode, result, stderr.String())
	}
}

func TestVerifyScheduleACommand(t *testing.T) {
	t.Parallel()
	physical, err := os.ReadFile(filepath.Join(repositoryRoot(t), "contracts/sources/fec/schedule-a/v1/fixtures/dump-2026-08-23/negative-adjustment.copy"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "fixture.copy.zst")
	input, err := os.Create(inputPath)
	if err != nil {
		t.Fatalf("create compressed fixture: %v", err)
	}
	encoder, err := zstd.NewWriter(input, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatalf("create zstd writer: %v", err)
	}
	if _, err := encoder.Write(physical); err != nil {
		t.Fatalf("compress fixture: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close zstd writer: %v", err)
	}
	if err := input.Close(); err != nil {
		t.Fatalf("close compressed fixture: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{
		"pipeline", "fec", "verify-schedule-a",
		"--input", inputPath,
		"--period", "2026",
		"--expected-rows", "1",
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("exit code = %d; stderr = %s", exitCode, stderr.String())
	}
	var result struct {
		SchemaVersion string `json:"schema_version"`
		Complete      bool   `json:"complete"`
		Rows          uint64 `json:"rows"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode command output: %v\n%s", err, stdout.String())
	}
	if result.SchemaVersion != "legal-tender.schedule-a-verification.v1" || !result.Complete || result.Rows != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestVerifyScheduleARequiresInput(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "verify-schedule-a"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
}

func TestVerifyScheduleECommand(t *testing.T) {
	t.Parallel()
	inputPath := filepath.Join(repositoryRoot(t), "contracts/sources/fec/schedule-e/v1/fixtures/dump-2026-08-30/fractional-amount.copy")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{
		"pipeline", "fec", "verify-schedule-e",
		"--input", inputPath,
		"--cycle", "2026",
		"--expected-rows", "1",
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("exit code = %d; stderr = %s", exitCode, stderr.String())
	}
	var result struct {
		SchemaVersion     string `json:"schema_version"`
		Rows              uint64 `json:"rows"`
		FractionalAmounts uint64 `json:"fractional_amounts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode command output: %v\n%s", err, stdout.String())
	}
	if result.SchemaVersion != "legal-tender.schedule-e-verification.v1" || result.Rows != 1 || result.FractionalAmounts != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestVerifyScheduleERequiresInput(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "verify-schedule-e"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
}

func TestAuditScheduleAOverlapRequiresInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "audit-schedule-a-overlap", "--period", "2024"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
}

func TestAuditScheduleABAlignmentRequiresInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "audit-schedule-ab-alignment", "--cycle", "2024"}, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "--schedule-a-facts") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestPublishReceiverCommitteeFlowsRequiresInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "publish-receiver-committee-flows", "--cycle", "2024"}, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "--storage-root and --run-id are required") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestAuditReceiverFlowMasterGapsRequiresInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "audit-receiver-flow-master-gaps", "--cycle", "2024"}, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "--storage-root is required") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestPublishReceiverFlowCommitteeIdentitiesRequiresInputs(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "publish-receiver-flow-committee-identities", "--cycle", "2024"}, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestPublishReceiverCommitteeFlowProjectionBundleV2RequiresInputs(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "publish-receiver-committee-flow-projection-bundle-v2", "--cycle", "2024"}, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestProbeArangoReceiverCommitteeFlowsV2RequiresInputs(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run([]string{"pipeline", "fec", "probe-arango-receiver-committee-flows-v2", "--cycle", "2024"}, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestAuditClassicFlowsRequiresInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "audit-classic-flows", "--period", "2024"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--pas2 and --oth are required") {
		t.Fatalf("unexpected diagnostic: %q", stderr.String())
	}
}

func TestBenchmarkScheduleALayoutRequiresInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "benchmark-schedule-a-layout"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--source-total-rows") {
		t.Fatalf("stderr = %q; want required benchmark inputs", stderr.String())
	}
}

func TestPublishScheduleAOccurrencesRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-schedule-a-occurrences"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--release, --cycle, --storage-root, and --run-id are required") {
		t.Fatalf("unexpected diagnostic: %q", stderr.String())
	}
}

func TestPublishScheduleACompactOccurrencesRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-schedule-a-compact-occurrences"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--release, --cycle, --storage-root, and --run-id are required") {
		t.Fatalf("unexpected diagnostic: %q", stderr.String())
	}
}

func TestPublishScheduleAFactsRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-schedule-a-facts"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--occurrences") {
		t.Fatalf("stderr = %q; want required fact inputs", stderr.String())
	}
}

func TestPublishScheduleEOccurrencesRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-schedule-e-occurrences"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--release, --cycle, --storage-root, and --run-id are required") {
		t.Fatalf("unexpected diagnostic: %q", stderr.String())
	}
}

func TestPublishScheduleEFactsRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-schedule-e-facts"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--occurrences") {
		t.Fatalf("stderr = %q; want required fact inputs", stderr.String())
	}
}

func TestPublishScheduleAColumnarFactsRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-schedule-a-columnar-facts"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--occurrences") {
		t.Fatalf("stderr = %q; want required columnar fact inputs", stderr.String())
	}
}

func TestPublishCompactCandidateItemizedReceiptsRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-candidate-itemized-receipts-compact"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--storage-root") {
		t.Fatalf("stderr = %q; want required compact calculation inputs", stderr.String())
	}
}

func TestPublishCandidateItemizedReceiptsFactBundleRequiresCycle(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-candidate-itemized-receipts-fact-bundle"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--cycle") {
		t.Fatalf("stderr = %q; want required fact-bundle inputs", stderr.String())
	}
}

func TestPublishIndependentExpenditureProjectionBundleRequiresCycle(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-independent-expenditure-projection-bundle"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--cycle") {
		t.Fatalf("stderr = %q; want required projection-bundle inputs", stderr.String())
	}
}

func TestPublishResolvedIndependentExpenditureProjectionBundleRequiresCycle(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-resolved-independent-expenditure-projection-bundle"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--cycle") {
		t.Fatalf("stderr = %q; want required resolved projection-bundle inputs", stderr.String())
	}
}

func TestPublishIndependentExpenditureCandidateResolutionRequiresCycle(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-independent-expenditure-candidate-resolution"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--cycle") {
		t.Fatalf("stderr = %q; want required candidate-resolution inputs", stderr.String())
	}
}

func TestPublishResolvedIndependentExpendituresRequiresCycle(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-resolved-independent-expenditures"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--cycle") {
		t.Fatalf("stderr = %q; want required resolved aggregate inputs", stderr.String())
	}
}

func TestProbeArangoCandidateReceiptsRequiresConnectionInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "probe-arango-candidate-receipts"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--endpoint") {
		t.Fatalf("stderr = %q; want required ArangoDB inputs", stderr.String())
	}
}

func TestProbeArangoIndependentExpendituresRequiresConnectionInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "probe-arango-independent-expenditures"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--endpoint") {
		t.Fatalf("stderr = %q; want required ArangoDB inputs", stderr.String())
	}
}

func TestProbeArangoResolvedIndependentExpendituresRequiresConnectionInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "probe-arango-resolved-independent-expenditures"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--endpoint") {
		t.Fatalf("stderr = %q; want required resolved ArangoDB inputs", stderr.String())
	}
}

func TestPublishCandidateItemizedReceiptsRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-candidate-itemized-receipts"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--all-candidates-summary-facts") {
		t.Fatalf("stderr = %q; want required calculation inputs", stderr.String())
	}
}

func TestPublishClassicOccurrencesRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-classic-occurrences"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--dataset") {
		t.Fatalf("stderr = %q; want required classic inputs", stderr.String())
	}
}

func TestPublishClassicFactsRequiresExactInputs(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if exitCode := Run([]string{"pipeline", "fec", "publish-classic-facts"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit code = %d; want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "--occurrences") {
		t.Fatalf("stderr = %q; want required fact inputs", stderr.String())
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../.."))
}
