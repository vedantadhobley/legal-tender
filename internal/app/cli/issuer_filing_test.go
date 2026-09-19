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

func TestIssuerFilingCLIReplayAndPins(t *testing.T) {
	root := t.TempDir()
	queries := "../../../tests/fixtures/organization-resolution/capture-v1/queries.json"
	raw, err := os.ReadFile(queries)
	if err != nil {
		t.Fatal(err)
	}
	var qs wikimedia.Queries
	if err := json.Unmarshal(raw, &qs); err != nil {
		t.Fatal(err)
	}
	qs.Queries = qs.Queries[:1]
	qs.Queries[0].Text = "EXAMPLE CORPORATION"
	raw, err = json.Marshal(qs)
	if err != nil {
		t.Fatal(err)
	}
	queries = filepath.Join(root, "queries.json")
	if err := os.WriteFile(queries, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ref := sec.FilingReference{CIK: "0000000100", Accession: "0000000100-25-000001", Document: "example.htm"}
	writeCapture := func(dir, fixture, contract, url, filename, media string, status int) string {
		t.Helper()
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		m := sec.Manifest{Contract: contract, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/synthetic (test@local)", Response: wikimedia.Response{URL: url, ObservedAt: "2026-09-15T00:00:00Z", Status: status, Headers: map[string]string{"Content-Type": media}, Bytes: len(body), SHA256: wikimedia.Hash(body), Body: filename}}
		if status != 200 {
			m.Response.Failure = "http_status_not_ok"
		}
		manifest, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		for name, b := range map[string][]byte{"capture.json": manifest, filename: body} {
			if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
				t.Fatal(err)
			}
		}
		return wikimedia.Hash(manifest)
	}
	directory, filing := filepath.Join(root, "directory"), filepath.Join(root, "filing")
	dp := writeCapture(directory, "../../source/sec/testdata/directory.json", sec.Contract, sec.DirectoryURL, "directory.body", "application/json", 200)
	fp := writeCapture(filing, "../../source/sec/testdata/filing.htm", sec.FilingContract, ref.URL(), "filing.body", "text/html", 200)
	args := []string{"pipeline", "entities", "inspect-issuer-filing", "--cik", ref.CIK, "--accession", ref.Accession, "--document", ref.Document, "--queries", queries, "--expected-queries-sha256", wikimedia.Hash(raw), "--issuer-capture", directory, "--expected-issuer-sha256", dp, "--filing-capture", filing, "--expected-filing-sha256", fp}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 || stderr.Len() != 0 {
			t.Fatal(code, stderr.String())
		}
		var r org.FiledIssuerResult
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if len(r.Comparisons) != 1 || r.Comparisons[0].State != "reported_name_matches_filed_registrant" || r.IdentityPublicationApproved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
			t.Fatal("unapproved filed evidence")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("replay changed")
		}
		first = append([]byte(nil), out.Bytes()...)
	}
	for _, flag := range []string{"--expected-queries-sha256", "--expected-issuer-sha256", "--expected-filing-sha256"} {
		bad := append([]string(nil), args...)
		for i := range bad {
			if bad[i] == flag {
				bad[i+1] = wikimedia.Hash([]byte("wrong"))
			}
		}
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), bad, &out, &stderr); code != 1 || out.Len() != 0 || stderr.Len() == 0 {
			t.Fatal("wrong pin", flag, code)
		}
	}
	args[len(args)-1] = writeCapture(filing, "../../source/sec/testdata/filing.htm", sec.FilingContract, ref.URL(), "filing.body", "text/html", 403)
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || stderr.Len() != 0 {
		t.Fatal("structured failed capture", code, stderr.String())
	}
	var r org.FiledIssuerResult
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.Filing.SourceUsable || r.Comparisons[0].State != "filing_source_unusable" {
		t.Fatal("failure became absence", err)
	}
}

func TestIssuerFilingCLIHelpAndContactBoundary(t *testing.T) {
	t.Setenv("SEC_USER_AGENT", "LegalTender/test (private@example.invalid)")
	for _, command := range []string{"capture-issuer-filing", "inspect-issuer-filing"} {
		for _, flags := range [][]string{{"--help"}, nil, {"--unknown"}} {
			var out, stderr bytes.Buffer
			want := 2
			if len(flags) == 1 && flags[0] == "--help" {
				want = 0
			}
			if code := RunContext(context.Background(), append([]string{"pipeline", "entities", command}, flags...), &out, &stderr); code != want || out.Len() != 0 || bytes.Contains(stderr.Bytes(), []byte("private@example.invalid")) {
				t.Fatal("help or validation", code)
			}
		}
	}
	args := []string{"pipeline", "entities", "capture-issuer-filing", "--cik", "0000000100", "--accession", "0000000100-25-000001", "--document", "example.htm", "--output", t.TempDir()}
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || !bytes.Contains(stderr.Bytes(), []byte("file exists")) {
		t.Fatal("environment contact not used before network", code, stderr.String())
	}
	stderr.Reset()
	if code := RunContext(context.Background(), append(args, "--user-agent", "bad"), &out, &stderr); code != 1 || !bytes.Contains(stderr.Bytes(), []byte("declared user agent")) {
		t.Fatal("explicit contact override", code, stderr.String())
	}
}
