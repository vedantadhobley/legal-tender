package personaffiliation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type interpretationFixture struct {
	ID               string                     `json:"id"`
	SourceCaseSHA256 string                     `json:"source_case_sha256"`
	Review           string                     `json:"review"`
	Input            mentionInterpretationInput `json:"input"`
	Expected         map[string]string          `json:"expected_states"`
}

func interpretationFixtures(t *testing.T) []interpretationFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, "mention-interpretation-v1/review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Scope string                  `json:"scope"`
		Cases []interpretationFixture `json:"cases"`
	}
	if err := strictjson.Decode(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.Scope == "" || len(f.Cases) != 6 {
		t.Fatal("review scope changed")
	}
	return f.Cases
}

func TestProseInterpretationRetainedBoundary(t *testing.T) {
	cases := map[string]modelCase{}
	for _, c := range proseAttachmentCases(t) {
		cases[c.ID] = c
	}
	for _, f := range interpretationFixtures(t) {
		t.Run(f.ID, func(t *testing.T) {
			c, ok := cases[f.ID]
			if !ok || f.Review == "" {
				t.Fatal("missing case or review")
			}
			raw, err := os.ReadFile(filepath.Join(fixtureDir, attachmentFixture, f.ID+".json"))
			if err != nil || hash(raw) != f.SourceCaseSHA256 {
				t.Fatal("source capture changed", err)
			}
			var capture modelRecord
			if err := json.Unmarshal(raw, &capture); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c, capture.Case) {
				t.Fatal("source not identical to earlier trial")
			}
			before, _ := json.Marshal(c)
			out, err := inspectMentionInterpretations(c, encodeMentionInput(f.Input))
			if err != nil {
				t.Fatal(err)
			}
			again, err := inspectMentionInterpretations(c, encodeMentionInput(f.Input))
			if err != nil || !reflect.DeepEqual(out, again) {
				t.Fatal("offline replay changed", err)
			}
			after, _ := json.Marshal(c)
			if string(before) != string(after) || !reflect.DeepEqual(out.Context, c.Entries) || out.Source != c.Source {
				t.Fatal("source evidence mutated")
			}
			if out.Origin != "reviewed_fixture" || out.IdentityApproved || out.GraphPublicationApproved || out.FinancialAttribution {
				t.Fatal("annotation promoted to truth")
			}
			if len(out.Mentions) != len(f.Input.Mentions) || len(out.Interpretations) != len(f.Input.Interpretations) || len(out.Links) != len(f.Input.Links) {
				t.Fatal("input grain lost")
			}
			if len(out.Interpretations) != len(f.Expected) {
				t.Fatal("incomplete state review")
			}
			for i, v := range out.Interpretations {
				if !reflect.DeepEqual(v.Proposal, f.Input.Interpretations[i]) || v.State != f.Expected[v.Proposal.ID] {
					t.Fatal("interpretation or expected state differs", v.Proposal.ID, v.State)
				}
			}
			for _, m := range out.Mentions {
				found := false
				for _, entry := range c.Entries {
					if entry.ID != m.Evidence.Entry {
						continue
					}
					s := m.Evidence.Matched
					if entry.Span != m.Evidence.HTML || entry.Text[s.Start:s.End] != s.Text {
						t.Fatal("literal occurrence changed")
					}
					found = true
				}
				if !found {
					t.Fatal("missing source entry")
				}
			}
			t.Log(f.Review)
		})
	}
}
