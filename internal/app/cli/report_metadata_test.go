package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"encoding/json"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

func TestReportMetadataCLI(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--capture", "missing.json"}, 1},
		{[]string{"--capture", "missing.json", "extra"}, 2},
	} {
		var out, err bytes.Buffer
		args := append([]string{"pipeline", "fec", "review-report-metadata"}, tc.args...)
		if got := Run(args, &out, &err); got != tc.code {
			t.Fatalf("code %d, want %d: %s", got, tc.code, err.String())
		}
		if out.Len() != 0 {
			t.Fatal("failure/help emitted result JSON")
		}
	}
}

func TestCaptureReportMetadataCLI(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--request", "missing", "--output-dir", "unused"}, 1},
		{[]string{"--request", "missing", "--output-dir", "unused", "--demo-key", "--api-key-env", "X"}, 2},
	} {
		var out, stderr bytes.Buffer
		if got := Run(append([]string{"pipeline", "fec", "capture-report-metadata"}, tc.args...), &out, &stderr); got != tc.code || out.Len() != 0 {
			t.Fatalf("unexpected CLI result %d: %s", got, stderr.String())
		}
	}
	t.Setenv("LT_METADATA_TEST_MISSING_KEY", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "request.json")
	r := reportmetadata.FetchRequest{Contract: reportmetadata.Contract, SchemaSHA256: reportmetadata.SwaggerSHA256, Endpoint: "/v1/filings/", Query: reportmetadata.Query{FileNumbers: []int64{101}, PerPage: 100}, Limits: reportmetadata.FetchLimits{Pages: 2, Requests: 2, AttemptsPerPage: 1, SourceBytes: 4096}}
	b, _ := json.Marshal(r)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if got := Run([]string{"pipeline", "fec", "capture-report-metadata", "--request", path, "--output-dir", filepath.Join(dir, "capture"), "--api-key-env", "LT_METADATA_TEST_MISSING_KEY"}, &out, &stderr); got != 1 || out.Len() != 0 {
		t.Fatal("missing credential accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "capture")); !os.IsNotExist(err) {
		t.Fatal("missing credential created capture")
	}
}
