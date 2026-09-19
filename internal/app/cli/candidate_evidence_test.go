package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func TestCandidateEvidenceCommandBoundary(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--terminal-policy", "leaf"}, 2},
		{[]string{"--view-version", "v3"}, 2},
		{[]string{"--candidate-master-facts", "/missing"}, 2},
		{[]string{"--storage-root", "/missing", "--basis-result", "/missing/inventory", "--observation-bundle", "/missing/bundle", "--linkage-facts", "/missing/linkage", "--cycle", "2024", "--candidate", "H0AA00001"}, 1},
	} {
		var out, diagnostic bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "build-candidate-evidence"}, tc.args...), &out, &diagnostic); code != tc.code {
			t.Fatal(code, diagnostic.String())
		}
		if out.Len() != 0 {
			t.Fatal("success output on failure")
		}
	}
}

func TestReportRenderFailureRemovesOnlyNewIncompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.md")
	want := errors.New("render failed")
	err := writeNewReport(path, func(w io.Writer) error { _, _ = io.WriteString(w, "partial"); return want })
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial report left behind", err)
	}
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	err = writeNewReport(path, func(w io.Writer) error { t.Fatal("called renderer on existing file"); return nil })
	if err == nil {
		t.Fatal("existing file accepted")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "existing" {
		t.Fatal("existing report changed")
	}
}

func TestEvidenceReportNeverOverwritesAndExecutableIsPinned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "candidate.md")
	if err := writeNewCandidateReport(path, fundingbasis.CandidateEvidence{}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNewCandidateReport(path, fundingbasis.CandidateEvidence{CandidateID: "changed"}); err == nil {
		t.Fatal("overwrote report")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("report changed")
	}
	a, err := executableDigest(context.Background())
	if err != nil || len(a) != 64 {
		t.Fatal(a, err)
	}
	b, err := executableDigest(context.Background())
	if err != nil || a != b {
		t.Fatal("build identity unstable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executableDigest(ctx); err == nil {
		t.Fatal("cancellation ignored")
	}
}
