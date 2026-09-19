package personaffiliation

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type nameSurface struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}
type expectedNames struct {
	ID, Group string
	Names     []nameSurface
}
type nameSelectionScore struct {
	Matched    []nameSurface `json:"matched"`
	Missing    []nameSurface `json:"missing"`
	Unexpected []nameSurface `json:"unexpected"`
	Duplicates int           `json:"duplicates"`
}

// Exact lexical/type comparison against pre-run annotations, not identity truth.
// The original report stays untouched. Repeated predictions cannot inflate recall.
func scoreNameSelections(want []nameSurface, got nameSelectionReport) nameSelectionScore {
	out := nameSelectionScore{Matched: []nameSurface{}, Missing: []nameSurface{}, Unexpected: []nameSurface{}}
	expected := map[nameSurface]bool{}
	for _, n := range want {
		expected[n] = true
	}
	seen := map[nameSurface]bool{}
	for i, n := range got.Supplied {
		key := nameSurface{Kind: n.Kind, Text: got.Citations.Review.Mentions[i].Evidence.Matched.Text}
		if seen[key] {
			out.Duplicates++
		} else if expected[key] {
			out.Matched = append(out.Matched, key)
		} else {
			out.Unexpected = append(out.Unexpected, key)
		}
		seen[key] = true
	}
	for _, n := range want {
		if !seen[n] {
			out.Missing = append(out.Missing, n)
		}
	}
	return out
}

func proseNameExpectations(t *testing.T, cases []modelCase) []expectedNames {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, nameModelFixture, "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected []expectedNames
	if err := strictjson.Decode(raw, &expected); err != nil || len(expected) != len(cases) {
		t.Fatal("missing pre-run expectations", err)
	}
	for i, want := range expected {
		if want.ID != cases[i].ID || (want.Group != "regression" && want.Group != "fresh") || want.Names == nil {
			t.Fatal("invalid expectation scope")
		}
		cat, err := newCitationCatalog(cases[i])
		if err != nil {
			t.Fatal(err)
		}
		seen := map[nameSurface]bool{}
		for _, n := range want.Names {
			if seen[n] || (n.Kind != "person" && n.Kind != "organization") || n.Text == "" {
				t.Fatal("invalid expected name")
			}
			seen[n] = true
			found := false
			for _, entry := range cat.Entries {
				options, err := cat.literalOptions(entry.Entry, n.Text)
				if err != nil {
					t.Fatal(err)
				}
				for _, match := range options.Matches {
					found = found || match.Range != nil
				}
			}
			if !found {
				t.Fatal("expected name has no exact source-token span", want.ID, n)
			}
		}
	}
	return expected
}
