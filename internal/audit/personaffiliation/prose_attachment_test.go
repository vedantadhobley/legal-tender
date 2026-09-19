package personaffiliation

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Separate research version. Neither this prompt nor these proposals feed the app.
//
//go:embed prose_attachment_prompt.txt
var proseAttachmentPrompt string

const attachmentFixture = "prose-attachment-v1"

type entryRole struct {
	Person       string  `json:"person"`
	Role         string  `json:"role"`
	Organization string  `json:"organization"`
	Polarity     string  `json:"polarity"`
	TimeText     *string `json:"time_text"`
	Entries      []int   `json:"evidence_entries"`
}
type entryAlias struct {
	Name    string `json:"name_text"`
	Alias   string `json:"alias_text"`
	Entries []int  `json:"evidence_entries"`
}
type entryAnswer struct {
	Roles   []entryRole  `json:"roles"`
	Aliases []entryAlias `json:"aliases"`
}
type attachedEvidence struct {
	Method                   string             `json:"method"`
	Source                   companypage.Source `json:"source"`
	Context                  []modelEntry       `json:"context"`
	Proposals                modelAnswer        `json:"proposals"`
	Meaning                  string             `json:"meaning"`
	IdentityApproved         bool               `json:"identity_approved"`
	GraphPublicationApproved bool               `json:"graph_publication_approved"`
	FinancialAttribution     bool               `json:"financial_attribution"`
}

// c must come from the verified lexical reader. Every supplied entry, including
// those the model did not select, stays in Context. Selected IDs supply complete
// text for literal checks; this cannot prove semantic binding or completeness.
func attachModelEvidence(c modelCase, raw []byte) (json.RawMessage, error) {
	var a entryAnswer
	if len(raw) > 128<<10 {
		return nil, fmt.Errorf("answer exceeds budget")
	}
	if err := strictjson.Decode(raw, &a); err != nil {
		return nil, err
	}
	if a.Roles == nil || a.Aliases == nil || len(a.Roles) > 16 || len(a.Aliases) > 8 {
		return nil, fmt.Errorf("invalid answer arrays")
	}
	if err := c.Source.Validate(); err != nil {
		return nil, err
	}
	if len(c.Entries) == 0 || len(c.Entries) > 32 {
		return nil, fmt.Errorf("invalid source window")
	}
	entries := map[int]modelEntry{}
	for _, e := range c.Entries {
		if _, exists := entries[e.ID]; exists || e.ID < 0 || e.Text == "" {
			return nil, fmt.Errorf("invalid or repeated source entry")
		}
		entries[e.ID] = e
	}
	quotes := func(ids []int) ([]modelQuote, error) {
		if len(ids) == 0 || len(ids) > 8 {
			return nil, fmt.Errorf("missing or unbounded entry references")
		}
		out := make([]modelQuote, 0, len(ids))
		seen := map[int]bool{}
		for _, id := range ids {
			e, ok := entries[id]
			if !ok || seen[id] {
				return nil, fmt.Errorf("unknown or repeated entry reference")
			}
			seen[id] = true
			out = append(out, modelQuote{Entry: id, Quote: e.Text})
		}
		return out, nil
	}
	answer := modelAnswer{Roles: []modelRole{}, Aliases: []modelAlias{}}
	for _, r := range a.Roles {
		q, err := quotes(r.Entries)
		if err != nil {
			return nil, err
		}
		answer.Roles = append(answer.Roles, modelRole{Person: r.Person, Role: r.Role, Organization: r.Organization, Polarity: r.Polarity, TimeText: r.TimeText, Citations: q})
	}
	for _, v := range a.Aliases {
		q, err := quotes(v.Entries)
		if err != nil {
			return nil, err
		}
		answer.Aliases = append(answer.Aliases, modelAlias{Name: v.Name, Alias: v.Alias, Citations: q})
	}
	encoded, err := json.Marshal(answer)
	if err != nil {
		return nil, err
	}
	if _, err := checkModelAnswer(c, encoded); err != nil {
		return nil, err // Whole-answer rejection; no field normalization or salvage.
	}
	return json.Marshal(attachedEvidence{Method: "source-entry-attachment.v1", Source: c.Source,
		Context: slices.Clone(c.Entries), Proposals: answer,
		Meaning: "literal_fields_and_source_references_only; semantics_and_identity_unassessed"})
}

func proseAttachmentRequest(c modelCase, p modelProfile) []byte {
	// Start with the unchanged transport controls and original field constraints.
	// A fresh schema is built each time; prior experiments are never mutated.
	schema := proseModelSchema()
	props := schema["properties"].(map[string]any)
	for _, key := range []string{"roles", "aliases"} {
		item := props[key].(map[string]any)["items"].(map[string]any)
		fields := item["properties"].(map[string]any)
		delete(fields, "citations")
		fields["evidence_entries"] = map[string]any{"type": "array", "items": map[string]any{"type": "integer"}, "minItems": 1, "maxItems": 8}
		required := item["required"].([]string)
		for i, field := range required {
			if field == "citations" {
				required[i] = "evidence_entries"
			}
		}
	}
	input, _ := json.Marshal(c.Entries)
	raw, _ := json.Marshal(map[string]any{"model": p.Model, "max_tokens": p.MaxTokens, "reasoning_effort": p.ReasoningEffort,
		"seed": 1, "stream": false, "messages": []map[string]string{{"role": "system", "content": proseAttachmentPrompt}, {"role": "user", "content": string(input)}},
		"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "entry_relationship_proposals", "strict": true, "schema": schema}}})
	return raw
}

func proseAttachmentCases(t *testing.T) []modelCase {
	t.Helper()
	cases := slices.Clone(proseModelCases(t)[:6]) // Existing real-source regressions, not fresh evaluation.
	var controls []struct{ ID, HTML, Expectation string }
	raw, err := os.ReadFile(filepath.Join(fixtureDir, attachmentFixture, "controls.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := strictjson.Decode(raw, &controls); err != nil {
		t.Fatal(err)
	}
	for _, control := range controls {
		body := []byte(control.HTML)
		source := companypage.Source{URL: "https://example.org/" + control.ID, ObservedOn: "2026-09-17", SHA256: hash(body)}
		e, err := companypage.Extract(context.Background(), body, source)
		if err != nil {
			t.Fatal(err)
		}
		c := modelCase{ID: control.ID, Source: source, Entries: []modelEntry{}}
		for i, entry := range e.Entries {
			if entry.Kind == "text" || entry.Kind == "heading" {
				c.Entries = append(c.Entries, modelEntry{ID: i, Kind: entry.Kind, Text: entry.Text, Span: entry.Span})
			}
		}
		input, _ := json.Marshal(c.Entries)
		if control.Expectation == "" || len(c.Entries) == 0 || len(c.Entries) > 32 || len(input) > 16000 {
			t.Fatal("invalid control")
		}
		cases = append(cases, c)
	}
	return cases
}

func TestProseAttachmentLiveComparison(t *testing.T) {
	runProseModelTrial(t, proseAttachmentPrompt, proseAttachmentCases, proseAttachmentRequest, attachModelEvidence)
}
