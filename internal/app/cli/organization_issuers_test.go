package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestIssuerCLIReplaysWithoutWikimediaOrNetwork(t *testing.T) {
	dir := t.TempDir()
	queries := filepath.Join("..", "..", "..", "tests", "fixtures", "organization-resolution", "capture-v1", "queries.json")
	q, err := os.ReadFile(queries)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile("../../source/sec/testdata/directory.json")
	if err != nil {
		t.Fatal(err)
	}
	m := sec.Manifest{Contract: sec.Contract, BuildSHA256: wikimedia.Hash([]byte("fixture")), UserAgent: "LegalTender/synthetic (test@local)", Response: wikimedia.Response{
		URL: sec.DirectoryURL, ObservedAt: "2026-09-15T00:00:00Z", Status: 200, Headers: map[string]string{"Content-Type": "application/json"}, Bytes: len(body), SHA256: wikimedia.Hash(body), Body: "directory.body"}}
	manifest, _ := json.Marshal(m)
	for name, b := range map[string][]byte{"capture.json": manifest, "directory.body": body} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"pipeline", "entities", "discover-issuer-organizations", "--queries", queries, "--expected-queries-sha256", wikimedia.Hash(q), "--issuer-capture", dir, "--expected-issuer-sha256", wikimedia.Hash(manifest)}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 || stderr.Len() != 0 {
			t.Fatal(code, stderr.String())
		}
		var r org.IssuerResult
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if !r.SourceUsable || len(r.Decisions) != 20 || r.Policy != org.IssuerPolicy || r.IdentityPublicationApproved {
			t.Fatal("offline issuer boundary")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("CLI replay changed")
		}
		first = append([]byte(nil), out.Bytes()...)
	}
	args[len(args)-1] = wikimedia.Hash([]byte("wrong capture"))
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 || stderr.Len() == 0 {
		t.Fatal("wrong pin accepted")
	}
	// Failed source is a structured failed result, never twenty absent entities.
	m.Response.Status, m.Response.Failure = 403, "http_status_not_ok"
	manifest, _ = json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, "capture.json"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	args[len(args)-1] = wikimedia.Hash(manifest)
	out.Reset()
	stderr.Reset()
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || stderr.Len() != 0 {
		t.Fatal("source failure should produce structured output")
	}
	var failed org.IssuerResult
	if err := json.Unmarshal(out.Bytes(), &failed); err != nil || failed.SourceUsable || len(failed.Decisions) != 20 || failed.Decisions[0].State != "source_unusable" {
		t.Fatal("source failure became absence", err)
	}
}

func TestIssuerCLIRequiresPinsAndExplicitCapture(t *testing.T) {
	t.Setenv("SEC_USER_AGENT", "")
	for _, command := range []string{"capture-issuer-directory", "discover-issuer-organizations"} {
		for _, flag := range []string{"--help", "", "--unknown"} {
			args := []string{"pipeline", "entities", command}
			if flag != "" {
				args = append(args, flag)
			}
			want := 2
			if flag == "--help" {
				want = 0
			}
			var out, stderr bytes.Buffer
			if code := RunContext(context.Background(), args, &out, &stderr); code != want || out.Len() != 0 {
				t.Fatal(command, flag, code)
			}
		}
	}
}

func TestIssuerContactEnvironmentAndFlagPrecedence(t *testing.T) {
	t.Setenv("SEC_USER_AGENT", "LegalTender/test (configured@example.invalid)")
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), []string{"pipeline", "entities", "capture-issuer-directory", "--help"}, &out, &stderr); code != 0 || bytes.Contains(stderr.Bytes(), []byte("configured@example.invalid")) {
		t.Fatal("help exposed configured contact")
	}
	for _, tc := range []struct {
		env, flag, message string
		code               int
	}{
		{"LegalTender/test (configured@example.invalid)", "", "file exists", 1},
		{"bad", "LegalTender/test (override@example.invalid)", "file exists", 1},
		{"LegalTender/test (configured@example.invalid)", "bad", "declared user agent with contact required", 1},
		{"", "", "declared user agent", 2},
	} {
		t.Setenv("SEC_USER_AGENT", tc.env)
		// An existing output directory fails after source option validation but
		// before any network request, so this tests real CLI configuration safely.
		args := []string{"pipeline", "entities", "capture-issuer-directory", "--output", t.TempDir()}
		if tc.flag != "" {
			args = append(args, "--user-agent", tc.flag)
		}
		out.Reset()
		stderr.Reset()
		if code := RunContext(context.Background(), args, &out, &stderr); code != tc.code || out.Len() != 0 || !bytes.Contains(stderr.Bytes(), []byte(tc.message)) {
			t.Fatal("contact configuration precedence", code, stderr.String())
		}
	}
}
