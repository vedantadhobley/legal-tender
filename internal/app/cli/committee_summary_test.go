package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

func TestVerifyCommitteeSummaryCommand(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "contracts/sources/fec/committee-summary/v1/fixtures/sample.csv")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	args := []string{"pipeline", "fec", "verify-committee-summary", "--input", path, "--cycle", "2024", "--expected-sha256", hex.EncodeToString(sum[:]), "--expected-bytes", strconv.Itoa(len(raw))}
	var stdout, stderr bytes.Buffer
	if code := Run(args, &stdout, &stderr); code != 0 {
		t.Fatal(code, stderr.String())
	}
	var result committeesummary.Verification
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.Rows != 8 || result.TerminalAttributionEligible || result.IssueCounts["invalid_date"] != 1 {
		t.Fatalf("bad result: %+v", result)
	}
	stdout.Reset()
	stderr.Reset()
	args[len(args)-1] = "1"
	if code := Run(args, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatal("failure emitted result", code)
	}
	args[len(args)-1] = strconv.Itoa(len(raw))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code := RunContext(ctx, args, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatal("cancel emitted result", code)
	}
}

func TestVerifyCommitteeSummaryOptions(t *testing.T) {
	for _, tc := range []struct {
		options []string
		want    int
	}{{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--cycle", "2024"}, 2}, {[]string{"--unknown"}, 2}} {
		var out, errout bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "verify-committee-summary"}, tc.options...), &out, &errout)
		if code != tc.want || out.Len() != 0 {
			t.Fatal(code, tc.want, out.String())
		}
	}
}

func TestPublishCommitteeSummaryOptions(t *testing.T) {
	for _, tc := range []struct {
		options []string
		want    int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--unknown"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--release", "absent.json", "--cycle", "2024", "--run-id", "test"}, 1},
	} {
		var out, errout bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "publish-committee-summary"}, tc.options...), &out, &errout); code != tc.want || out.Len() != 0 {
			t.Fatal(code, out.String())
		}
	}
}

func TestSummaryAssertionOptions(t *testing.T) {
	for _, tc := range []struct {
		options []string
		want    int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--unknown"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--summary-facts", "absent.json", "--cycle", "2023"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--summary-facts", "absent.json", "--cycle", "2024"}, 1},
	} {
		var out, errout bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "calculate-committee-summary-assertions"}, tc.options...), &out, &errout); code != tc.want || out.Len() != 0 {
			t.Fatal(code, tc.want, out.String())
		}
	}
}
