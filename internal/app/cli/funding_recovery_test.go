package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/recovery/funding"
)

func TestFundingRecoveryCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--inputs", "missing"}, {"--inputs", "x", "--expected-inputs-sha256", "x", "--storage-root", "x", "extra"}, {"--arango-endpoint", "forbidden"}} {
		var out, err bytes.Buffer
		if runFundingRecovery(context.Background(), args, &out, &err) != 2 || out.Len() != 0 {
			t.Fatalf("accepted invalid args: %v", args)
		}
	}
	var out, stderr bytes.Buffer
	if runFundingRecovery(context.Background(), []string{"--help"}, &out, &stderr) != 0 {
		t.Fatal("help failed")
	}
	root := t.TempDir()
	h := sha256.Sum256([]byte("missing"))
	pin := hex.EncodeToString(h[:])
	in := funding.Inputs{Version: funding.InputsVersion, Generation: funding.Reference{Kind: "generation", ID: pin, SHA256: pin, Path: "missing.json"}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "inputs.json")
	if err = os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
	h = sha256.Sum256(b)
	out.Reset()
	stderr.Reset()
	code := runFundingRecovery(context.Background(), []string{"--inputs", file, "--expected-inputs-sha256", hex.EncodeToString(h[:]), "--storage-root", root}, &out, &stderr)
	var result funding.Result
	if code != 1 || json.Unmarshal(out.Bytes(), &result) != nil || result.Complete || result.Counts["missing"] != 1 {
		t.Fatalf("incomplete report was lost: %d %s %s", code, out.String(), stderr.String())
	}
}
