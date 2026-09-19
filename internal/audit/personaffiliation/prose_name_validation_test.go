package personaffiliation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestProseNameModelContract(t *testing.T) {
	cases := proseNameModelCases(t)
	if len(cases) != 8 {
		t.Fatal("fixed batch changed")
	}
	expected := proseNameExpectations(t, cases)
	regressions := proseReferenceModelCases(t)
	profile := modelProfile{"gpt-oss-120b", "medium", 4096}
	for i, c := range cases {
		if expected[i].Group == "regression" {
			found := false
			for _, old := range regressions {
				found = found || reflect.DeepEqual(c, old)
			}
			if !found {
				t.Fatal("known source input changed", c.ID)
			}
		}
		raw, err := proseNameModelRequest(c, profile)
		if err != nil {
			t.Fatal(err)
		}
		var request struct {
			Model           string
			MaxTokens       int    `json:"max_tokens"`
			ReasoningEffort string `json:"reasoning_effort"`
			Seed            int
			Stream          bool
			Messages        []struct{ Role, Content string }
			ResponseFormat  json.RawMessage `json:"response_format"`
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		cat, _ := newCitationCatalog(c)
		input, _ := json.Marshal(cat.Entries)
		if request.Model != profile.Model || request.MaxTokens != profile.MaxTokens || request.ReasoningEffort != profile.ReasoningEffort || request.Seed != 1 || request.Stream || len(request.Messages) != 2 || request.Messages[0].Content != proseNameModelPrompt || request.Messages[1].Content != string(input) {
			t.Fatal("task controls changed or expected labels entered input")
		}
		if bytes.Contains(request.ResponseFormat, []byte(`"interpretations"`)) || !bytes.Contains(request.ResponseFormat, []byte(`"kind"`)) || !bytes.Contains(request.ResponseFormat, []byte(`"strict":true`)) {
			t.Fatal("names-only schema changed")
		}
		again, err := proseNameModelRequest(c, profile)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("nondeterministic request", err)
		}
	}
}

func TestProseNameSelectionBoundaryAndScoring(t *testing.T) {
	c := citationCase(t, `<p>Zoë Test joined A&amp;B.</p><p>Zoë Test spoke.</p>`)
	raw := []byte(`{"names":[{"id":"n","kind":"person","entry":0,"first_token":0,"last_token":1},{"id":"o","kind":"organization","entry":0,"first_token":3,"last_token":5}]}`)
	out, err := inspectNameSelections(c, raw, "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	r := out.Citations.Review
	if r.Mentions[0].Evidence.Matched.Text != "Zoë Test" || r.Mentions[1].Evidence.Matched.Text != "A&B" || !reflect.DeepEqual(r.Context, c.Entries) || r.Source != c.Source || r.Origin != "synthetic_test" || len(r.Interpretations) != 0 || len(r.Links) != 0 || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
		t.Fatal("citation or no-relationship boundary changed")
	}
	want := []nameSurface{{"person", "Zoë Test"}, {"organization", "A&B"}}
	score := scoreNameSelections(want, out)
	if !reflect.DeepEqual(score.Matched, want) || len(score.Missing) != 0 || len(score.Unexpected) != 0 || score.Duplicates != 0 {
		t.Fatal("exact score failed", score)
	}
	// A valid range is not necessarily a name, and a kind is not verified.
	wrong := strings.Replace(string(raw), `"last_token":1`, `"last_token":2`, 1)
	wrong = strings.Replace(wrong, `"kind":"organization"`, `"kind":"person"`, 1)
	out, err = inspectNameSelections(c, []byte(wrong), "model_proposal")
	if err != nil || out.Citations.Review.Origin != "model_proposal" {
		t.Fatal("literal validity should not claim name semantics", err)
	}
	score = scoreNameSelections(want, out)
	if len(score.Matched) != 0 || len(score.Missing) != 2 || len(score.Unexpected) != 2 || score.Unexpected[0].Text != "Zoë Test joined" {
		t.Fatal("wrong kind/range hidden by scoring", score)
	}
	duplicate := strings.TrimSuffix(string(raw), "]}") + `,{"id":"repeat","kind":"person","entry":1,"first_token":0,"last_token":1}]}`
	out, err = inspectNameSelections(c, []byte(duplicate), "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	score = scoreNameSelections(want, out)
	if len(out.Supplied) != 3 || len(score.Matched) != 2 || score.Duplicates != 1 {
		t.Fatal("duplicate prediction erased or inflated score")
	}
}

func TestProseNameSelectionRejectsInvalidInput(t *testing.T) {
	c := citationCase(t, `<p>Zoë Test joined A&amp;B.</p>`)
	base := `{"names":[{"id":"n","kind":"person","entry":0,"first_token":0,"last_token":1}]}`
	for name, raw := range map[string]string{
		"missing names": `{}`, "null names": `{"names":null}`,
		"unknown kind":       strings.Replace(base, `"person"`, `"company"`, 1),
		"missing kind":       strings.Replace(base, `"kind":"person",`, "", 1),
		"null kind":          strings.Replace(base, `"kind":"person"`, `"kind":null`, 1),
		"missing coordinate": strings.Replace(base, `"entry":0,`, "", 1),
		"null coordinate":    strings.Replace(base, `"first_token":0`, `"first_token":null`, 1),
		"range bounds":       strings.Replace(base, `"last_token":1`, `"last_token":999`, 1),
		"duplicate ID":       strings.Replace(base, `}]}`, `},{"id":"n","kind":"person","entry":0,"first_token":0,"last_token":1}]}`, 1),
		"forged text":        strings.Replace(base, `"kind":"person"`, `"kind":"person","text":"invented"`, 1),
		"approval":           `{"identity_approved":true,` + base[1:],
		"provenance":         `{"interpretation_origin":"reviewed_fixture",` + base[1:],
		"relationships":      `{"interpretations":[],` + base[1:],
		"duplicate key":      strings.Replace(base, `"entry":0`, `"entry":0,"entry":0`, 1),
		"trailing JSON":      base + base,
		"input budget":       base + strings.Repeat(" ", 128<<10),
		"name budget":        `{"names":[` + strings.Repeat(`{"id":"n","kind":"person","entry":0,"first_token":0,"last_token":1},`, 64) + `{"id":"last","kind":"person","entry":0,"first_token":0,"last_token":1}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if raw == base {
				t.Fatal("mutation did not apply")
			}
			out, err := inspectNameSelections(c, []byte(raw), "synthetic_test")
			if err == nil || !reflect.DeepEqual(out, nameSelectionReport{}) {
				t.Fatal("invalid input produced a partial report")
			}
		})
	}
	if _, err := inspectNameSelections(c, []byte(`{"names":[]}`), "synthetic_test"); err != nil {
		t.Fatal("explicit empty answer rejected", err)
	}
}
