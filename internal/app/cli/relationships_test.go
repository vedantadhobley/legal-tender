package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func relationshipArgs() []string {
	return []string{"pipeline", "entities", "query-relationships",
		"--body", "../../../tests/fixtures/person-affiliation/wikidata-interactive-1.json",
		"--expected-body-sha256", "5683ad0692e124dcd9d87bcdc81b7448def720f632a2e4e4b66b8aae4f490076",
		"--ids", "Q1393271,Q173395,Q8034666", "--entity", "wikidata:Q1393271",
	}
}

func TestRelationshipQueryCLI(t *testing.T) {
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		args := append(relationshipArgs(), "--observed-at", "2026-09-15T21:46:51Z")
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 || stderr.Len() != 0 {
			t.Fatal(code, stderr.String())
		}
		var result struct {
			Build    string                      `json:"build_sha256"`
			Evidence wikimedia.RelationshipQuery `json:"evidence"`
		}
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		r := result.Evidence
		if !wikimedia.Digest(result.Build) || r.Contract != wikimedia.RelationshipContract || r.Entity.ID != "wikidata:Q1393271" || r.ObservedAt != "2026-09-15T21:46:51Z" || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
			t.Fatal("incorrect query boundary")
		}
		found := false
		for _, m := range r.Matches {
			if wikimedia.Hash(m.Statement.Raw) != m.Statement.RawSHA256 {
				t.Fatal("raw bytes changed")
			}
			if m.Statement.SubjectID != r.Entity.ID && m.Statement.ObjectID != r.Entity.ID {
				t.Fatal("unrelated statement")
			}
			if m.Statement.SubjectID == "wikidata:Q173395" && m.Statement.Property == "P169" {
				found = true
			}
		}
		if !found {
			t.Fatal("inverse CEO evidence absent")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("unstable CLI result")
		}
		first = bytes.Clone(out.Bytes())
	}
}

func TestRelationshipQueryCLIRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--unknown"}, 2},
	} {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), append([]string{"pipeline", "entities", "query-relationships"}, tc.args...), &out, &stderr); code != tc.code {
			t.Fatal(code, tc.code)
		}
	}
	for _, tc := range []struct {
		index int
		value string
	}{
		{6, strings.Repeat("a", 64)}, {8, "Q1393271"}, {10, "fec:candidate:Q1393271"}, {10, "John Chambers"},
	} {
		args := relationshipArgs()
		args[tc.index] = tc.value
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
			t.Fatal("invalid input accepted", tc.index, code)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, stderr bytes.Buffer
	if code := RunContext(ctx, relationshipArgs(), &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("cancellation ignored")
	}
	args := append(relationshipArgs(), "--observed-at", "2026-09-16")
	out.Reset()
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("invalid timestamp accepted")
	}
}

func TestRelationshipQueryCLIFileBoundsAndAPIErrors(t *testing.T) {
	dir := t.TempDir()
	apiError := []byte(`{"error":{"code":"maxlag"}}`)
	apiPath := filepath.Join(dir, "api-error.json")
	if err := os.WriteFile(apiPath, apiError, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "symlink.json")
	if err := os.Symlink(apiPath, link); err != nil {
		t.Fatal(err)
	}
	large := filepath.Join(dir, "large.json")
	f, err := os.Create(large)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(wikimedia.MaxBody + 1)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	for _, path := range []string{dir, apiPath, link, large, filepath.Join(dir, "missing")} {
		args := relationshipArgs()
		args[4], args[6] = path, wikimedia.Hash(apiError)
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 || stderr.Len() == 0 {
			t.Fatal("source error became empty query", path, code)
		}
	}
}
