package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/recovery/funding"
)

func TestFundingRecoveryVerificationCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--hash-blobs"}, {"--arango-endpoint", "forbidden"}, {"--allow-missing-stage"}, {"--plan", "untrusted.json"}} {
		var out, stderr bytes.Buffer
		if runFundingRecoveryVerify(context.Background(), args, &out, &stderr) != 2 || out.Len() != 0 {
			t.Fatal("unsafe args accepted", args)
		}
	}
	var out, stderr bytes.Buffer
	if runFundingRecoveryVerify(context.Background(), []string{"--help"}, &out, &stderr) != 0 {
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
	args := []string{"pipeline", "fec", "verify-funding-recovery-files", "--inputs", file, "--expected-inputs-sha256", hex.EncodeToString(sha[:]), "--storage-root", root}
	for _, extra := range [][]string{nil, {"--max-bytes", "10"}, {"--workers", "1"}, {"--max-bytes", "10", "--workers", "9"}} {
		out.Reset()
		stderr.Reset()
		if RunContext(context.Background(), append(slices.Clone(args), extra...), &out, &stderr) != 2 || out.Len() != 0 {
			t.Fatal("missing resource admission accepted")
		}
	}
	args = append(args, "--max-bytes", "10", "--workers", "1")
	out.Reset()
	stderr.Reset()
	if RunContext(context.Background(), args, &out, &stderr) != 1 {
		t.Fatal("missing inputs accepted")
	}
	var v funding.FileVerification
	if json.Unmarshal(out.Bytes(), &v) != nil || v.Version != funding.VerificationVersion || v.Complete || v.RecoveryReady || v.State != "dependency_plan_incomplete" {
		t.Fatal("incomplete result lost")
	}
	first := append([]byte{}, out.Bytes()...)
	args = append(args, "--expected-verification-id", v.ID)
	out.Reset()
	stderr.Reset()
	if RunContext(context.Background(), args, &out, &stderr) != 1 || !bytes.Equal(first, out.Bytes()) {
		t.Fatal("replay differs")
	}
	args[len(args)-1] = pin
	out.Reset()
	stderr.Reset()
	if RunContext(context.Background(), args, &out, &stderr) != 1 || !strings.Contains(stderr.String(), "replay identity differs") {
		t.Fatal("expected identity not enforced")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatal("verification wrote to storage")
	}
	content, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(content, b) {
		t.Fatal("input changed")
	}
}
