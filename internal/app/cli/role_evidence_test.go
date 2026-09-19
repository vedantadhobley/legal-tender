package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestRoleEvidenceCLI(t *testing.T) {
	name := "a107a21854241e9e5f0d54df09a22aafdea4cfcabb27c969c6d033a07802d109"
	path := filepath.Join("../../../tests/fixtures/organization-resolution/capture-v1", name+".body")
	testRoleEvidenceCLI(t, path, name, 5)
}

func TestRoleEvidenceCLIInteractiveSnapshot(t *testing.T) {
	testRoleEvidenceCLI(t, "../../../tests/fixtures/person-affiliation/wikidata-interactive-1.json", "5683ad0692e124dcd9d87bcdc81b7448def720f632a2e4e4b66b8aae4f490076", 31)
}

func testRoleEvidenceCLI(t *testing.T, path, name string, wantCount int) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var body struct{ Entities map[string]json.RawMessage }
	if err = json.Unmarshal(b, &body); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for id := range body.Entities {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	args := []string{"pipeline", "entities", "extract-role-evidence", "--body", path, "--expected-body-sha256", name, "--ids", strings.Join(ids, ",")}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 || stderr.Len() != 0 {
			t.Fatal(code, stderr.String())
		}
		var r struct {
			BuildSHA256 string                 `json:"build_sha256"`
			Evidence    wikimedia.RoleEvidence `json:"evidence"`
		}
		if err = json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if !wikimedia.Digest(r.BuildSHA256) || r.Evidence.Contract != wikimedia.RoleContract || r.Evidence.BodySHA256 != name || r.Evidence.IdentityApproved || r.Evidence.GraphPublicationApproved || r.Evidence.FinancialAttribution {
			t.Fatal("bad CLI evidence boundary")
		}
		count := 0
		for _, e := range r.Evidence.Entities {
			for _, s := range e.Statements {
				count++
				if wikimedia.Hash(s.Raw) != s.RawSHA256 {
					t.Fatal("raw statement bytes changed through JSON output")
				}
			}
		}
		if count != wantCount {
			t.Fatal("statement count")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
	args[6] = strings.Repeat("a", 64)
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("accepted wrong pin")
	}
}

func TestRoleEvidenceCLIFlagsAndFailure(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--unknown"}, 2}} {
		var out, stderr bytes.Buffer
		args := append([]string{"pipeline", "entities", "extract-role-evidence"}, tc.args...)
		if code := RunContext(context.Background(), args, &out, &stderr); code != tc.want {
			t.Fatal(code, tc.want)
		}
	}
	for _, name := range []string{"wikidata-maxlag-1.json", "wikidata-maxlag-2.json", "wikidata-maxlag-3.json"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("../../../tests/fixtures/person-affiliation", name)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// The source-package tests separately pin these retained response bytes.
			args := []string{"pipeline", "entities", "extract-role-evidence", "--body", path, "--expected-body-sha256", wikimedia.Hash(body), "--ids", "Q1393271,Q173395,Q8034666"}
			var out, stderr bytes.Buffer
			if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 || stderr.Len() == 0 {
				t.Fatal("API backoff converted into empty role evidence")
			}
		})
	}
}
