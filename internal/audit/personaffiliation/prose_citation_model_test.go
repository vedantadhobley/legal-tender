package personaffiliation

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

//go:embed prose_citation_model_prompt.txt
var proseCitationModelPrompt string

const citationModelFixture = "citation-model-v1"

func proseCitationModelRequest(c modelCase, p modelProfile) ([]byte, error) {
	cat, err := newCitationCatalog(c)
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(cat.Entries)
	if err != nil || len(input) > 16000 {
		return nil, fmt.Errorf("token catalog exceeds trial input budget")
	}
	// Reuse the exact interpretation vocabulary and transport controls. Build a
	// new request map; the previous prompt, schema and captures remain unchanged.
	var request map[string]any
	if err := json.Unmarshal(proseInterpretationRequest(c, p), &request); err != nil {
		return nil, err
	}
	format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
	format["name"] = "source_token_interpretations"
	schema := format["schema"].(map[string]any)
	properties := schema["properties"].(map[string]any)
	delete(properties, "mentions")
	properties["selections"] = map[string]any{"type": "array", "maxItems": 64, "items": map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"id", "entry", "first_token", "last_token"},
		"properties": map[string]any{"id": map[string]any{"type": "string"}, "entry": map[string]any{"type": "integer"},
			"first_token": map[string]any{"type": "integer"}, "last_token": map[string]any{"type": "integer"}},
	}}
	schema["required"] = []string{"selections", "interpretations", "context_links"}
	request["messages"] = []map[string]string{{"role": "system", "content": proseCitationModelPrompt}, {"role": "user", "content": string(input)}}
	return json.Marshal(request)
}

func inspectCitationModelAnswer(c modelCase, raw []byte) (json.RawMessage, error) {
	cat, err := newCitationCatalog(c)
	if err != nil {
		return nil, err
	}
	report, err := inspectCitationSelections(cat, raw, "model_proposal")
	if err != nil {
		return nil, err
	}
	return json.Marshal(report)
}

func TestProseCitationModelContract(t *testing.T) {
	p := modelProfile{"gpt-oss-120b", "medium", 4096}
	for _, c := range proseInterpretationCases(t) {
		raw, err := proseCitationModelRequest(c, p)
		if err != nil {
			t.Fatal(err)
		}
		var request struct {
			Model     string
			MaxTokens int `json:"max_tokens"`
			Messages  []struct{ Role, Content string }
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		cat, _ := newCitationCatalog(c)
		input, _ := json.Marshal(cat.Entries)
		if request.Model != p.Model || request.MaxTokens != p.MaxTokens || len(request.Messages) != 2 || request.Messages[0].Content != proseCitationModelPrompt || request.Messages[1].Content != string(input) {
			t.Fatal("task controls changed or reviewed labels entered the request")
		}
		again, err := proseCitationModelRequest(c, p)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("nondeterministic request", err)
		}
	}
	c, annotation := mentionUnitInput(t)
	cat, _ := newCitationCatalog(c)
	raw, _ := json.Marshal(reviewedCitationSelections(t, cat, annotation))
	derived, err := inspectCitationModelAnswer(c, raw)
	if err != nil {
		t.Fatal(err)
	}
	var report citationSelectionReport
	if err := json.Unmarshal(derived, &report); err != nil || report.Review.Origin != "model_proposal" || !reflect.DeepEqual(report.Review.Context, c.Entries) || report.Review.Source != c.Source || report.Review.IdentityApproved || report.Review.GraphPublicationApproved || report.Review.FinancialAttribution {
		t.Fatal("model provenance/context boundary", err)
	}
	for _, field := range []string{`"interpretation_origin":"reviewed_fixture"`, `"identity_approved":true`} {
		if partial, err := inspectCitationModelAnswer(c, []byte("{"+field+","+string(raw[1:]))); err == nil || partial != nil {
			t.Fatal("forged provenance produced a report")
		}
	}
}

func TestProseCitationModelLive(t *testing.T) {
	runProseModelTrial(t, proseCitationModelPrompt, proseInterpretationCases, func(c modelCase, p modelProfile) []byte {
		raw, err := proseCitationModelRequest(c, p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, inspectCitationModelAnswer)
}
