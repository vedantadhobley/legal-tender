package companypage

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These are source-reading regressions, not accepted donor/role examples. The
// reviewer-selected URLs and text expectations never enter the runtime reader.
func TestSupplementarySourcesPreserveEvidenceAndGaps(t *testing.T) {
	dir := "../../../tests/fixtures/person-affiliation/supplementary-v1"
	rawReview, err := os.ReadFile(filepath.Join(dir, "review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var review struct {
		Observed string `json:"observed_on_utc"`
		Sources  []struct {
			File, URL, SHA256 string
			Bytes             int
		}
		IdentityApproved         bool `json:"identity_approved"`
		GraphPublicationApproved bool `json:"graph_publication_approved"`
		FinancialAttribution     bool `json:"financial_attribution"`
	}
	if err := json.Unmarshal(rawReview, &review); err != nil {
		t.Fatal(err)
	}
	if len(review.Sources) != 5 || review.Observed != "2026-09-17" || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
		t.Fatal("review scope changed")
	}
	want := map[string][]string{
		"rick-reviglio.html":     {"Rick Reviglio, President and General Manager of Western Nevada Supply"},
		"jack-reviglio.html":     {"Richard (Rick) John Reviglio"},
		"moana-culture.html":     {"About Moana Nursery"},
		"gdc-alton-russell.html": {"Alton Russell is a Member at Large for the GDC Board of Corrections."},
	}
	for _, item := range review.Sources {
		t.Run(item.File, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, item.File))
			if err != nil || len(raw) != item.Bytes || digest(raw) != item.SHA256 {
				t.Fatal("retained source differs from review", err)
			}
			source := Source{URL: item.URL, ObservedOn: review.Observed, SHA256: item.SHA256}
			if strings.HasSuffix(item.File, ".pdf") {
				if !bytes.HasPrefix(raw, []byte("%PDF-")) {
					t.Fatal("missing original PDF")
				}
				if _, err := Extract(context.Background(), raw, source); err == nil {
					t.Fatal("PDF silently treated as supported HTML")
				}
				return // PDF role interpretation remains reviewed, not Go-extracted.
			}
			r, err := Extract(context.Background(), raw, source)
			if err != nil {
				t.Fatal(err)
			}
			verifySpans(t, raw, r)
			var texts []string
			ld := 0
			for _, e := range r.Entries {
				texts = append(texts, e.Text)
				if e.Kind == "json_ld" {
					ld++
					if e.JSONState != "valid_json_uninterpreted" {
						t.Fatal("JSON-LD parse state changed")
					}
					var v any
					if err := json.Unmarshal([]byte(e.Raw), &v); err != nil {
						t.Fatal(err)
					}
					assertNoStructuredPersonRoles(t, v)
				}
			}
			if ld != 1 {
				t.Fatal("retained JSON-LD inventory changed")
			}
			joined := strings.Join(texts, "\n")
			for _, expected := range want[item.File] {
				if !strings.Contains(joined, expected) {
					t.Fatal("expected source text missing", expected)
				}
			}
			if item.File == "moana-culture.html" && strings.Contains(strings.ToLower(joined), "gescheider") {
				t.Fatal("an absent full-name role was filled from other evidence")
			}
			if item.File == "gdc-alton-russell.html" && strings.Contains(strings.ToLower(joined), "copaco") {
				t.Fatal("unrelated public role became reported-employer evidence")
			}
			second, err := Extract(context.Background(), raw, source)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := json.Marshal(r)
			b, _ := json.Marshal(second)
			if !bytes.Equal(a, b) {
				t.Fatal("offline extraction changed")
			}
		})
	}
}

// This checks the retained markup's measured limitation; it is not a general
// JSON-LD interpreter or a rule for deciding that a person has no affiliation.
func assertNoStructuredPersonRoles(t *testing.T, v any) {
	t.Helper()
	switch value := v.(type) {
	case []any:
		for _, child := range value {
			assertNoStructuredPersonRoles(t, child)
		}
	case map[string]any:
		for key, child := range value {
			switch key {
			case "jobTitle", "worksFor", "employee", "memberOf", "founder", "owns":
				t.Fatal("new structured role evidence needs review", key)
			case "@type":
				types, ok := child.([]any)
				if !ok {
					types = []any{child}
				}
				for _, typ := range types {
					name, _ := typ.(string)
					if name == "Person" || name == "Role" || name == "EmployeeRole" || name == "OrganizationRole" {
						t.Fatal("new structured person evidence needs review", typ)
					}
				}
			}
			assertNoStructuredPersonRoles(t, child)
		}
	}
}
