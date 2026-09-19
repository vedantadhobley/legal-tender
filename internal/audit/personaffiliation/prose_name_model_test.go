package personaffiliation

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Name surfaces only: classification is supplied, never identity or affiliation.
type nameSelection struct {
	citationSelection
	Kind string `json:"kind"`
}
type nameSelectionInput struct {
	Names []nameSelection `json:"names"`
}
type nameSelectionReport struct {
	Method    string                  `json:"method"`
	Supplied  []nameSelection         `json:"supplied_names"`
	Citations citationSelectionReport `json:"citations"`
}

//go:embed prose_name_model_prompt.txt
var proseNameModelPrompt string

const nameModelFixture = "name-model-v1"

func proseNameModelCases(t *testing.T) []modelCase {
	return proseInputCases(t, nameModelFixture)
}

func inspectNameSelections(c modelCase, raw []byte, origin string) (nameSelectionReport, error) {
	if len(raw) > 128<<10 {
		return nameSelectionReport{}, fmt.Errorf("name selection input budget")
	}
	var in nameSelectionInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return nameSelectionReport{}, err
	}
	if in.Names == nil || len(in.Names) > 64 {
		return nameSelectionReport{}, fmt.Errorf("missing or unbounded names")
	}
	selected := citationSelectionInput{Selections: []citationSelection{}, Interpretations: []interpretationProposal{}, Links: []contextProposal{}}
	for _, n := range in.Names {
		if n.Kind != "person" && n.Kind != "organization" {
			return nameSelectionReport{}, fmt.Errorf("unsupported name kind")
		}
		selected.Selections = append(selected.Selections, n.citationSelection)
	}
	cat, err := newCitationCatalog(c)
	if err != nil {
		return nameSelectionReport{}, err
	}
	encoded, err := json.Marshal(selected)
	if err != nil {
		return nameSelectionReport{}, err
	}
	out, err := inspectCitationSelections(cat, encoded, origin)
	if err != nil {
		return nameSelectionReport{}, err
	}
	return nameSelectionReport{Method: "source-name-selections.v1", Supplied: in.Names, Citations: out}, nil
}

func inspectNameModelAnswer(c modelCase, raw []byte) (json.RawMessage, error) {
	out, err := inspectNameSelections(c, raw, "model_proposal")
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func proseNameModelRequest(c modelCase, profile modelProfile) ([]byte, error) {
	// Reuse transport controls and the source catalog, not interpretation policy.
	raw, err := proseCitationModelRequest(c, profile)
	if err != nil {
		return nil, err
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
	selection := format["schema"].(map[string]any)["properties"].(map[string]any)["selections"].(map[string]any)
	item := selection["items"].(map[string]any)
	item["properties"].(map[string]any)["kind"] = map[string]any{"type": "string", "enum": []string{"person", "organization"}}
	item["required"] = []string{"id", "kind", "entry", "first_token", "last_token"}
	format["name"] = "source_name_selections"
	format["schema"] = map[string]any{"type": "object", "additionalProperties": false, "required": []string{"names"}, "properties": map[string]any{"names": selection}}
	request["messages"].([]any)[0].(map[string]any)["content"] = proseNameModelPrompt
	return json.Marshal(request)
}

func TestProseNameModelLive(t *testing.T) {
	runProseModelTrial(t, proseNameModelPrompt, proseNameModelCases, func(c modelCase, p modelProfile) []byte {
		raw, err := proseNameModelRequest(c, p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, inspectNameModelAnswer)
}
