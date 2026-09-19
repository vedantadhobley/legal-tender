package personaffiliation

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

//go:embed prose_binding_grounding_prompt.txt
var proseGroundingPrompt string

const groundingFixture = "binding-model-v2"

func groundingNameInput(t *testing.T, c modelCase) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, groundingFixture, "names", c.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var r modelRecord
	if err := strictjson.Decode(raw, &r); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Case, c) || r.HTTPStatus != 200 || r.Failure != "" {
		t.Fatal("invalid original name capture")
	}
	answer, err := modelContent(r.Response)
	if err != nil {
		t.Fatal(err)
	}
	report, err := inspectNameModelAnswer(c, answer)
	var compact bytes.Buffer
	if err != nil || json.Compact(&compact, r.EvidenceAttachment) != nil || !bytes.Equal(report, compact.Bytes()) || r.CitationCheck != "literal_citations_valid_semantics_unassessed" {
		t.Fatal("name report changed", err)
	}
	request, err := proseNameModelRequest(c, modelProfile{"gpt-oss-120b", "medium", 4096})
	compact.Reset()
	if err != nil || json.Compact(&compact, r.Request) != nil || !bytes.Equal(request, compact.Bytes()) {
		t.Fatal("name producer changed", err)
	}
	return answer
}

func proseGroundingRequest(c modelCase, names []byte, p modelProfile) ([]byte, error) {
	raw, err := proseBindingModelRequest(c, names, p)
	if err != nil {
		return nil, err
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
	format["name"] = "whole_entry_role_grounding"
	item := format["schema"].(map[string]any)["properties"].(map[string]any)["interpretations"].(map[string]any)["items"].(map[string]any)
	fields := item["properties"].(map[string]any)
	delete(fields, "clause")
	entries := []int{}
	for _, e := range c.Entries {
		entries = append(entries, e.ID)
	}
	fields["statement_entry"] = map[string]any{"type": "integer", "enum": entries}
	for _, f := range []string{"focus", "subject_surface", "organization_surface"} {
		fields[f] = map[string]any{"type": "string"}
	}
	item["required"] = []string{"id", "statement_entry", "focus", "kind", "normalized", "role_kind", "subjects", "organizations", "subject_surface", "organization_surface", "additional_evidence", "binding", "status"}
	request["messages"].([]any)[0].(map[string]any)["content"] = proseGroundingPrompt
	return json.Marshal(request)
}

func TestProseGroundingNamesLive(t *testing.T) {
	runProseModelTrial(t, proseNameModelPrompt, func(t *testing.T) []modelCase { return proseInputCases(t, groundingFixture) }, func(c modelCase, p modelProfile) []byte {
		raw, err := proseNameModelRequest(c, p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, inspectNameModelAnswer)
}

func TestProseGroundingModelLive(t *testing.T) {
	if os.Getenv("LT_PROSE_MODEL_URL") == "" {
		t.Skip("opt-in local grounding trial")
	}
	names := map[string][]byte{}
	runProseModelTrial(t, proseGroundingPrompt, func(t *testing.T) []modelCase {
		cases := proseInputCases(t, groundingFixture)
		for _, c := range cases {
			names[c.ID] = groundingNameInput(t, c)
		}
		return cases
	}, func(c modelCase, p modelProfile) []byte {
		raw, err := proseGroundingRequest(c, names[c.ID], p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, func(c modelCase, raw []byte) (json.RawMessage, error) {
		out, err := inspectGroundedRoles(c, names[c.ID], raw, "model_proposal")
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	})
}
