package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestCompanyPageCLI(t *testing.T) {
	args := []string{"pipeline", "entities", "extract-company-page", "--body", "../../../tests/fixtures/person-affiliation/ridgeline-leadership.html", "--expected-body-sha256", "c91e3c5785ddf31a229ac50c6757dca476d8c5acb5cdaab234369ce892be4151", "--source-url", "https://ridgeline.ai/company/leadership", "--observed-on", "2026-09-15"}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 || stderr.Len() != 0 {
			t.Fatal(code, stderr.String())
		}
		var r struct {
			BuildSHA256 string               `json:"build_sha256"`
			Evidence    companypage.Evidence `json:"evidence"`
			Proposals   json.RawMessage      `json:"proposals"`
		}
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if !wikimedia.Digest(r.BuildSHA256) || r.Evidence.Contract != companypage.Contract || r.Evidence.Source.SHA256 != args[6] || len(r.Evidence.Entries) == 0 || r.Evidence.IdentityApproved || r.Evidence.GraphPublicationApproved || r.Evidence.FinancialAttribution {
			t.Fatal("wrong evidence or interpretation boundary")
		}
		if r.Proposals != nil {
			t.Fatal("prose interpretation enabled without opt-in")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("CLI replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
	for _, tc := range []struct {
		field int
		value string
	}{
		{6, strings.Repeat("a", 64)}, {4, t.TempDir()}, {4, filepath.Join(t.TempDir(), "absent.html")},
	} {
		bad := append([]string(nil), args...)
		bad[tc.field] = tc.value
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), bad, &out, &stderr); code != 1 || out.Len() != 0 {
			t.Fatal("invalid input emitted usable result", code)
		}
	}
	link := filepath.Join(t.TempDir(), "link.html")
	target, err := filepath.Abs(args[4])
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	args[4] = link
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("symlink body accepted")
	}
}

func TestCompanyPageCLIProseOptIn(t *testing.T) {
	args := []string{"pipeline", "entities", "extract-company-page", "--body", "../../../tests/fixtures/person-affiliation/supplementary-v1/rick-reviglio.html", "--expected-body-sha256", "c0a0ed7fc8cd9729aa775c98d423d9ecf70175f8cda3560e94b2fba63902e7ef", "--source-url", "https://goblueteam.com/rick-reviglio/", "--observed-on", "2026-09-17", "--propose-relationships"}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 || stderr.Len() != 0 {
			t.Fatal(code, stderr.String())
		}
		var r struct {
			Evidence  companypage.Evidence          `json:"evidence"`
			Proposals personaffiliation.ProseResult `json:"proposals"`
		}
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Proposals.Policy != personaffiliation.ProsePolicy || r.Proposals.Source != r.Evidence.Source || len(r.Proposals.Roles) != 1 || r.Proposals.Roles[0].Person.Text != "Rick Reviglio" || r.Proposals.IdentityApproved || r.Proposals.GraphPublicationApproved || r.Proposals.FinancialAttribution {
			t.Fatal("opt-in changed source or acceptance boundary", r.Proposals)
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("opt-in replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
	args[6] = strings.Repeat("0", 64)
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("opt-in bypassed body verification", code)
	}
}

func TestCompanyPageCLIFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--unknown"}, 2},
		{[]string{"--body", "unused", "--expected-body-sha256", strings.Repeat("a", 64), "--source-url", "https://user:secret@example.org/", "--observed-on", "2026-09-15"}, 2},
	} {
		var out, stderr bytes.Buffer
		args := append([]string{"pipeline", "entities", "extract-company-page"}, tc.args...)
		if code := RunContext(context.Background(), args, &out, &stderr); code != tc.want {
			t.Fatal(code, tc.want)
		}
		if strings.Contains(stderr.String(), "secret") {
			t.Fatal("invalid URL echoed credentials")
		}
	}
}
