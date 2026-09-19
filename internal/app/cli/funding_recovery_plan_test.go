package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/recovery/funding"
)

func TestFundingRecoveryPlanCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--hash-blobs"}, {"--arango-endpoint", "forbidden"}, {"--allow-missing-stage"}, {"--inputs", "x", "--expected-inputs-sha256", "x", "--storage-root", "x", "extra"}} {
		var out, stderr bytes.Buffer
		if runFundingRecoveryPlan(context.Background(), args, &out, &stderr) != 2 || out.Len() != 0 {
			t.Fatal("unsafe CLI accepted", args)
		}
	}
	var out, stderr bytes.Buffer
	if runFundingRecoveryPlan(context.Background(), []string{"--help"}, &out, &stderr) != 0 {
		t.Fatal("help failed")
	}
	root := t.TempDir()
	pin := strings.Repeat("a", 64)
	in := funding.Inputs{Version: funding.InputsVersion, Generation: funding.Reference{Kind: "generation", ID: pin, SHA256: pin, Path: "missing.json"}}
	b, _ := json.Marshal(in)
	sha := sha256.Sum256(b)
	file := filepath.Join(root, "inputs.json")
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"pipeline", "fec", "plan-funding-recovery", "--inputs", file, "--expected-inputs-sha256", hex.EncodeToString(sha[:]), "--storage-root", root}
	out.Reset()
	stderr.Reset()
	if RunContext(context.Background(), args, &out, &stderr) != 1 {
		t.Fatal("blocked plan must stay nonzero")
	}
	var p funding.RecoveryPlan
	if json.Unmarshal(out.Bytes(), &p) != nil || p.Version != funding.PlanVersion || p.DependencyPlanComplete || p.ExecutionReady || p.Inventory.Counts["missing"] != 1 {
		t.Fatalf("lost explicit incomplete plan: %s", out.String())
	}
	first := append([]byte{}, out.Bytes()...)
	out.Reset()
	stderr.Reset()
	args = append(args, "--expected-plan-id", p.ID)
	if RunContext(context.Background(), args, &out, &stderr) != 1 || !bytes.Equal(first, out.Bytes()) {
		t.Fatal("blocked replay changed")
	}
	args[len(args)-1] = pin
	out.Reset()
	stderr.Reset()
	if RunContext(context.Background(), args, &out, &stderr) != 1 || !strings.Contains(stderr.String(), "replay identity differs") {
		t.Fatal("wrong expected identity accepted")
	}
	content, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(content, b) {
		t.Fatal("input modified")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatal("planner wrote to storage")
	}
}
