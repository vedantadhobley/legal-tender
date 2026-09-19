package personaffiliation

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// A third, fixed research task. It does not replace earlier prompts or captures.
//
//go:embed prose_interpretation_prompt.txt
var proseInterpretationPrompt string

const interpretationModelFixture = "mention-model-v1"

// Origin is deliberately absent: model output cannot claim reviewed provenance.
type modelInterpretations struct {
	Mentions        []mentionProposal        `json:"mentions"`
	Interpretations []interpretationProposal `json:"interpretations"`
	Links           []contextProposal        `json:"context_links"`
}

func inspectModelInterpretations(c modelCase, raw []byte) (json.RawMessage, error) {
	if len(raw) > 128<<10 {
		return nil, fmt.Errorf("model interpretation input budget")
	}
	var a modelInterpretations
	if err := strictjson.Decode(raw, &a); err != nil {
		return nil, err
	}
	report, err := inspectMentionInterpretations(c, encodeMentionInput(mentionInterpretationInput{
		Origin: "model_proposal", Mentions: a.Mentions, Interpretations: a.Interpretations, Links: a.Links,
	}))
	if err != nil {
		return nil, err // No repair, partial salvage or semantic approval.
	}
	return json.Marshal(report)
}

func proseInterpretationRequest(c modelCase, p modelProfile) []byte {
	str := map[string]any{"type": "string"}
	object := func(fields map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "object", "properties": fields, "required": required, "additionalProperties": false}
	}
	array := func(item any, max int) map[string]any {
		return map[string]any{"type": "array", "items": item, "maxItems": max}
	}
	enum := func(values ...string) map[string]any { return map[string]any{"type": "string", "enum": values} }
	mention := object(map[string]any{"id": str, "entry": map[string]any{"type": "integer"}, "text": str,
		"text_start": map[string]any{"type": []string{"integer", "null"}}}, "id", "entry", "text", "text_start")
	interpretation := object(map[string]any{
		"id": str, "clause": str, "kind": enum("role", "activity", "name_form", "correction", "unknown"), "normalized": str,
		"role_kind": enum("", "executive", "board_director", "controlling_owner", "employee", "founder", "owner", "unknown"),
		"subjects":  array(str, 64), "organizations": array(str, 64), "evidence": array(str, 64),
		"binding": enum("direct", "coreference", "ambiguous", "unresolved"), "status": enum("asserted", "denied", "hypothetical", "unclear"),
	}, "id", "clause", "kind", "normalized", "role_kind", "subjects", "organizations", "evidence", "binding", "status")
	link := object(map[string]any{"from": str, "to": str, "kind": enum("retracts", "contradicts")}, "from", "to", "kind")
	schema := object(map[string]any{"mentions": array(mention, 64), "interpretations": array(interpretation, 32), "context_links": array(link, 32)}, "mentions", "interpretations", "context_links")
	input, _ := json.Marshal(c.Entries)
	raw, _ := json.Marshal(map[string]any{
		"model": p.Model, "max_tokens": p.MaxTokens, "reasoning_effort": p.ReasoningEffort, "seed": 1, "stream": false,
		"messages":        []map[string]string{{"role": "system", "content": proseInterpretationPrompt}, {"role": "user", "content": string(input)}},
		"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "mention_interpretation_proposals", "strict": true, "schema": schema}},
	})
	return raw
}

func proseInterpretationCases(t *testing.T) []modelCase {
	return proseInputCases(t, interpretationModelFixture)
}

// Shared source/fixture reader only; prompts, cases and outputs remain separate.
func proseInputCases(t *testing.T, fixture string) []modelCase {
	t.Helper()
	dir := filepath.Join(fixtureDir, fixture)
	raw, err := os.ReadFile(filepath.Join(dir, "inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var inputs []struct {
		ID, File, HTML, Expectation, EffectiveURL string
		Source                                    companypage.Source
		Entries                                   []int
	}
	if err := strictjson.Decode(raw, &inputs); err != nil {
		t.Fatal(err)
	}
	cases := []modelCase{}
	seen := map[string]bool{}
	for _, in := range inputs {
		if in.ID == "" || seen[in.ID] || in.Expectation == "" || (in.File == "") == (in.HTML == "") {
			t.Fatal("invalid research case")
		}
		seen[in.ID] = true
		body := []byte(in.HTML)
		if in.File != "" {
			body, err = os.ReadFile(filepath.Join(dir, in.File))
			if err != nil {
				t.Fatal(err)
			}
		}
		source := in.Source
		if in.HTML != "" {
			source = companypage.Source{URL: "https://example.org/" + in.ID, ObservedOn: "2026-09-17", SHA256: hash(body)}
		}
		e, err := companypage.Extract(context.Background(), body, source)
		if err != nil {
			t.Fatal(in.ID, err)
		}
		c := modelCase{ID: in.ID, Source: source, Entries: []modelEntry{}}
		indexes := in.Entries
		if indexes == nil {
			for i, entry := range e.Entries {
				if entry.Kind == "text" || entry.Kind == "heading" {
					indexes = append(indexes, i)
				}
			}
		}
		previous := -1
		for _, i := range indexes {
			if i <= previous || i >= len(e.Entries) || (e.Entries[i].Kind != "text" && e.Entries[i].Kind != "heading") {
				t.Fatal("invalid excerpt selection", in.ID)
			}
			previous = i
			entry := e.Entries[i]
			c.Entries = append(c.Entries, modelEntry{ID: i, Kind: entry.Kind, Text: entry.Text, Span: entry.Span})
		}
		encoded, _ := json.Marshal(c.Entries)
		if len(c.Entries) == 0 || len(c.Entries) > 32 || len(encoded) > 16000 {
			t.Fatal("excerpt budget", in.ID)
		}
		cases = append(cases, c)
	}
	return cases
}

func TestProseInterpretationModelInputs(t *testing.T) {
	if got := len(proseInterpretationCases(t)); got != 9 {
		t.Fatal("fixed fresh batch changed", got)
	}
}

func TestProseInterpretationModelLive(t *testing.T) {
	runProseModelTrial(t, proseInterpretationPrompt, proseInterpretationCases, proseInterpretationRequest, inspectModelInterpretations)
}
