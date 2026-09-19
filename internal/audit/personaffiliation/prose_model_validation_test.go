package personaffiliation

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestProseModelCitationValidation(t *testing.T) {
	c := modelCase{Entries: []modelEntry{{ID: 7, Text: "Alex Example is CEO of North Corp."}}}
	valid := `{"roles":[{"person":"Alex Example","role":"CEO","organization":"North Corp","polarity":"asserted","time_text":"","citations":[{"entry":7,"quote":"Alex Example is CEO of North Corp."}]}],"aliases":[]}`
	if _, err := checkModelAnswer(c, []byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(valid, `"entry":7`, `"entry":8`, 1),
		strings.Replace(valid, `"quote":"Alex Example`, `"quote":"Invented Alex Example`, 1),
		strings.Replace(valid, `"organization":"North Corp"`, `"organization":"South Corp"`, 1),
		strings.Replace(valid, `"time_text":""`, `"time_text":"since 2020"`, 1),
		strings.Replace(valid, `"time_text":""`, `"time_text":null`, 1),
		strings.Replace(valid, `"time_text":"",`, ``, 1),
		strings.Replace(valid, `"polarity":"asserted"`, `"polarity":"accepted"`, 1),
		strings.Replace(valid, `"aliases":[]`, `"aliases":null`, 1),
		strings.Replace(valid, `"aliases":[]`, `"aliases":[],"identity_approved":true`, 1),
		strings.Replace(valid, `"person":"Alex Example"`, `"person":"Alex Example","person":"Alex Example"`, 1),
		valid + valid,
	} {
		if _, err := checkModelAnswer(c, []byte(bad)); err == nil {
			t.Fatal("invalid evidence passed", bad)
		}
	}
	if _, err := checkModelAnswer(c, []byte(`{"roles":[],"aliases":[]}`)); err != nil {
		t.Fatal("abstention rejected", err)
	}
	// Real quotes can still support the wrong interpretation. This is a literal
	// evidence check, not automatic relation truth or negation classification.
	c.Entries[0].Text = "Alex Example is CEO of North Corp. That statement is false."
	if _, err := checkModelAnswer(c, []byte(valid)); err != nil {
		t.Fatal("literal validator unexpectedly claimed semantic validation", err)
	}
	if _, err := modelContent([]byte(`{"choices":[{"finish_reason":"length","message":{"content":"{}"}}]}`)); err == nil {
		t.Fatal("truncated generation accepted")
	}
}

func TestProseModelRetainedReplay(t *testing.T) {
	t.Run("gemma", func(t *testing.T) {
		replayProseModel(t, "prose-model-v1", modelProfile{"gemma-4-12b", "none", 2048}, 9)
	})
	t.Run("gpt-oss", func(t *testing.T) {
		replayProseModel(t, "prose-model-gpt-oss-v1", modelProfile{"gpt-oss-120b", "medium", 4096}, 11)
	})
}

func replayProseModel(t *testing.T, fixture string, profile modelProfile, wantValid int) {
	t.Helper()
	dir := filepath.Join(fixtureDir, fixture)
	raw, err := os.ReadFile(filepath.Join(dir, "review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var review struct {
		Model    string
		Metadata map[string]string
		Cases    []struct {
			ID, SHA256, Review string
			LiteralValid       bool `json:"literal_valid"`
			Roles, Aliases     int
		}
		IdentityApproved         bool `json:"identity_approved"`
		GraphPublicationApproved bool `json:"graph_publication_approved"`
		FinancialAttribution     bool `json:"financial_attribution"`
	}
	if err := json.Unmarshal(raw, &review); err != nil {
		t.Fatal(err)
	}
	if review.Model != profile.Model || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
		t.Fatal("trial promoted to acceptance")
	}
	for name, pin := range review.Metadata {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || hash(raw) != pin {
			t.Fatal("trial metadata changed", name, err)
		}
	}
	discovery, err := os.ReadFile(filepath.Join(dir, "models.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkModelProfile(discovery, profile); err != nil {
		t.Fatal(err)
	}
	cases := proseModelCases(t)
	if len(review.Cases) != len(cases) {
		t.Fatal("review scope changed")
	}
	valid, rejected := 0, 0
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, want.ID+".json"))
			if err != nil || hash(raw) != want.SHA256 {
				t.Fatal("capture changed", err)
			}
			var r modelRecord
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			if r.Case.ID != want.ID || !reflect.DeepEqual(r.Case, cases[i]) || r.HTTPStatus != 200 || want.Review == "" {
				t.Fatal("source window, baseline or review changed")
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, r.Request); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(compact.Bytes(), proseModelRequest(cases[i], profile)) {
				t.Fatal("prompt, schema, controls or input changed; keep old trial")
			}
			content, err := modelContent(r.Response)
			var a modelAnswer
			if err == nil {
				a, err = checkModelAnswer(r.Case, content)
			}
			if (err == nil) != want.LiteralValid || len(a.Roles) != want.Roles || len(a.Aliases) != want.Aliases {
				t.Fatal("citation result or candidate count changed", err)
			}
			if err == nil {
				valid++
				if r.CitationCheck != "literal_citations_valid_semantics_unassessed" || r.Failure != "" {
					t.Fatal("recorded validation differs")
				}
			} else {
				rejected++
				if r.CitationCheck != "rejected" || r.Failure != err.Error() {
					t.Fatal("recorded failure differs")
				}
			}
			t.Logf("literal_valid=%t roles=%d aliases=%d; %s", want.LiteralValid, len(a.Roles), len(a.Aliases), want.Review)
		})
	}
	if valid != wantValid || rejected != len(cases)-wantValid {
		t.Fatal("trial counts changed", valid, rejected)
	}
}

func TestProseModelFixedInputs(t *testing.T) {
	cases := proseModelCases(t)
	if len(cases) != 14 {
		t.Fatal("trial scope changed", len(cases))
	}
	for _, c := range cases {
		raw := proseModelRequest(c, modelProfile{"fixture-model", "none", 2048})
		var request struct {
			Messages []struct{ Content string }
			Model    string
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(c.Entries)
		if len(request.Messages) != 2 || request.Messages[0].Content != proseModelPrompt || !bytes.Equal([]byte(request.Messages[1].Content), want) {
			t.Fatal("labels or other context leaked into source prompt")
		}
		if !bytes.Equal(raw, proseModelRequest(c, modelProfile{"fixture-model", "none", 2048})) {
			t.Fatal("request is nondeterministic")
		}
	}
}
