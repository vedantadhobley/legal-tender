package sec

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func filingFixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/filing.htm")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFiledRegistrantGrainAndLocators(t *testing.T) {
	body := filingFixture(t)
	facts, err := ParseFilingIdentity(body, "0000000100")
	if err != nil || len(facts) != 2 {
		t.Fatal(err, facts)
	}
	if facts[0].Text != "Example Corporation" || facts[1].Text != "0000000100" {
		t.Fatal(facts)
	}
	for i, f := range facts {
		if f.Issue != "" || f.Ordinal != i+1 || f.Context.StartDate != "2024-01-01" || f.Context.EndDate != "2024-12-31" {
			t.Fatal(f)
		}
		span := body[f.StartByte:f.EndByte]
		if !bytes.HasPrefix(span, []byte("<ix:nonNumeric")) || !bytes.HasSuffix(span, []byte("</ix:nonNumeric>")) || wikimedia.Hash(span) != f.SHA256 {
			t.Fatal("fact locator")
		}
		if !bytes.HasPrefix(body[f.Context.StartByte:f.Context.EndByte], []byte("<xbrli:context")) {
			t.Fatal("context locator")
		}
	}
	// A namespace alias is not a different concept; prose never supplies facts.
	renamed := strings.ReplaceAll(string(body), "dei:", "other:")
	renamed = strings.ReplaceAll(renamed, "xmlns:dei=", "xmlns:other=")
	got, err := ParseFilingIdentity([]byte(renamed), "0000000100")
	if err != nil || len(got) != 2 || got[0].Text != facts[0].Text || got[0].Issue != "" {
		t.Fatal("namespace resolution", err)
	}
}

func TestFiledRegistrantUnsupportedAndConflictingEvidence(t *testing.T) {
	raw := string(filingFixture(t))
	for _, tc := range []struct{ name, from, to, issue string }{
		{"missing_context", `contextRef="annual"`, `contextRef="missing"`, "missing_context"},
		{"format", `id="name"`, `id="name" format="ixt:unknown"`, "fact_transformation_or_attribute_unsupported"},
		{"continuation", `id="name"`, `id="name" continuedAt="next"`, "fact_transformation_or_attribute_unsupported"},
		{"nil", `id="name"`, `id="name" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true"`, "fact_transformation_or_attribute_unsupported"},
		{"exclude", `<span>Corporation</span>`, `<ix:exclude>Corporation</ix:exclude>`, "nested_non_xhtml_content_unsupported"},
		{"context_cik", `scheme="http://www.sec.gov/CIK">0000000100`, `scheme="http://www.sec.gov/CIK">0000000200`, "context_issuer_conflict"},
		{"scheme", `http://www.sec.gov/CIK`, `https://example.invalid/CIK`, "context_unusable"},
		{"date", `2024-12-31`, `2024-02-30`, "context_unusable"},
		{"reversed_period", `2024-01-01`, `2025-01-01`, "context_unusable"},
		{"dimensions", `</xbrli:entity>`, `<xbrli:segment/></xbrli:entity>`, "dimensional_context_not_issuer_wide"},
		{"duplicate_id", `id="cik"`, `id="name"`, "duplicate_fact_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := strings.Replace(raw, tc.from, tc.to, 1)
			if changed == raw {
				t.Fatal("ineffective mutation")
			}
			f, err := ParseFilingIdentity([]byte(changed), "0000000100")
			if err != nil || len(f) != 2 || f[0].Issue != tc.issue {
				t.Fatal(err, f)
			}
		})
	}
	f, err := ParseFilingIdentity([]byte(strings.Replace(raw, `id="cik">0000000100`, `id="cik">0000000200`, 1)), "0000000100")
	if err != nil || f[1].Issue != "reported_identifier_conflict" {
		t.Fatal("reported CIK", err, f)
	}
	for _, bad := range []string{
		"", "<html/>", raw + raw, raw + "trailing", strings.Replace(raw, "<body>", "<body><!DOCTYPE x>", 1),
		strings.Replace(raw, `id="name"`, `id="name" id="name"`, 1),
		strings.Replace(raw, "</xbrli:context>", `</xbrli:context><xbrli:context id="annual"/>`, 1),
		strings.Replace(raw, "UTF-8", "ISO-8859-1", 1),
		strings.Replace(raw, "Example", "\xff", 1),
		strings.Replace(raw, "Example", strings.Repeat("x", 8193), 1),
		strings.Replace(raw, "<body>", "<body>"+strings.Repeat("<div>", 128)+strings.Repeat("</div>", 128), 1),
	} {
		if _, err := ParseFilingIdentity([]byte(bad), "0000000100"); err == nil {
			t.Fatal("unsupported filing accepted")
		}
	}
}

func TestFilingReferenceConfinementAndReplay(t *testing.T) {
	ref := FilingReference{"0000000100", "0000000999-25-000001", "example-20241231.htm"}
	if ref.Validate() != nil || ref.URL() != "https://www.sec.gov/Archives/edgar/data/100/000000099925000001/example-20241231.htm" {
		t.Fatal("filing agent accession need not start with issuer CIK")
	}
	for _, bad := range []FilingReference{
		{"100", ref.Accession, ref.Document}, {"0000000000", ref.Accession, ref.Document},
		{ref.CIK, "bad", ref.Document}, {ref.CIK, ref.Accession, "../x.htm"},
		{ref.CIK, ref.Accession, "x.htm?key=x"}, {ref.CIK, ref.Accession, "https://evil.invalid/x.htm"},
	} {
		if bad.Validate() == nil {
			t.Fatal("unconfined reference")
		}
	}
	for _, status := range []int{200, 403} {
		body := filingFixture(t)
		client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			if r.URL.String() != ref.URL() || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
				t.Fatal("public fixed request")
			}
			return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"text/html; charset=UTF-8"}}, ContentLength: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}, nil
		})}
		dir := filepath.Join(t.TempDir(), "capture")
		pin, err := captureDocument(context.Background(), Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/test (test@local)"}, client, ref.spec())
		if err != nil {
			t.Fatal(err)
		}
		one, err := ReadFiling(dir, pin, ref)
		if err != nil || one.SourceUsable != (status == 200) || one.IdentityPublicationApproved {
			t.Fatal(err, one.Issues)
		}
		two, err := ReadFiling(dir, pin, ref)
		if err != nil || !reflect.DeepEqual(one, two) {
			t.Fatal("replay", err)
		}
		if status == 403 && (len(one.Facts) != 0 || len(one.Issues) != 1 || one.Issues[0] != "http_status_not_ok") {
			t.Fatal("failure became empty success")
		}
		other := ref
		other.Document = "other.htm"
		if _, err := ReadFiling(dir, pin, other); err == nil {
			t.Fatal("reference substitution")
		}
		if _, err := ReadFiling(dir, wikimedia.Hash([]byte("wrong")), ref); err == nil {
			t.Fatal("wrong pin")
		}
		if _, err := Read(dir, pin); err == nil {
			t.Fatal("filing became directory")
		}
		if err := os.WriteFile(filepath.Join(dir, "filing.body"), []byte("corrupt"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFiling(dir, pin, ref); err == nil {
			t.Fatal("changed source")
		}
	}
}

func TestPublicAnnualReportIdentity(t *testing.T) {
	body, err := os.ReadFile("../../../tests/fixtures/organization-resolution/sec-annual-report-v1.htm")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := ParseFilingIdentity(body, "0000034782")
	if err != nil || len(facts) != 2 {
		t.Fatal("real filing", err, len(facts))
	}
	values := map[string]string{}
	for _, f := range facts {
		if f.Issue != "" || f.Context.CIKText != "0000034782" || f.Context.StartDate != "2024-01-01" || f.Context.EndDate != "2024-12-31" || f.SHA256 != wikimedia.Hash(body[f.StartByte:f.EndByte]) {
			t.Fatal("real evidence", f)
		}
		values[f.Concept] = f.Text
	}
	if values["EntityRegistrantName"] != "1st Source Corporation" || values["EntityCentralIndexKey"] != "0000034782" {
		t.Fatal(values)
	}
}
