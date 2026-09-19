package personaffiliation

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

//go:embed prose_binding_model_prompt.txt
var proseBindingModelPrompt string

const bindingModelFixture = "binding-model-v1"

type bindingSeed struct {
	Case     modelCase
	Names    json.RawMessage
	Capture  string
	SHA256   string
	Expected string
}

// Read original captures; no reviewed labels, normalization or manual names enter
// either the candidate list or the request. Sources are re-read through Go.
func proseBindingSeeds(t *testing.T) []bindingSeed {
	t.Helper()
	root := filepath.Join(fixtureDir, bindingModelFixture)
	raw, err := os.ReadFile(filepath.Join(root, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var inputs []struct{ ID, Names, Expectation string }
	if err := strictjson.Decode(raw, &inputs); err != nil {
		t.Fatal(err)
	}
	cases := map[string]modelCase{}
	for _, c := range append(proseNameModelCases(t), proseInputCases(t, bindingModelFixture)...) {
		cases[c.ID] = c
	}
	seeds := []bindingSeed{}
	seen := map[string]bool{}
	for _, in := range inputs {
		c, ok := cases[in.ID]
		if !ok || seen[in.ID] || in.Expectation == "" {
			t.Fatal("invalid binding trial case")
		}
		seen[in.ID] = true
		raw, err := os.ReadFile(filepath.Join(root, in.Names))
		if err != nil {
			t.Fatal(err)
		}
		var record modelRecord
		if err := strictjson.Decode(raw, &record); err != nil || !reflect.DeepEqual(record.Case, c) || record.HTTPStatus != 200 || record.Failure != "" {
			t.Fatal("invalid or mismatched first-stage capture", in.ID, err)
		}
		content, err := modelContent(record.Response)
		if err != nil {
			t.Fatal("no complete first-stage answer", err)
		}
		report, err := inspectNameModelAnswer(c, content)
		var compact bytes.Buffer
		if err != nil || json.Compact(&compact, record.EvidenceAttachment) != nil || !bytes.Equal(report, compact.Bytes()) || record.CitationCheck != "literal_citations_valid_semantics_unassessed" {
			t.Fatal("first-stage report changed", err)
		}
		request, err := proseNameModelRequest(c, modelProfile{"gpt-oss-120b", "medium", 4096})
		compact.Reset()
		if err != nil || json.Compact(&compact, record.Request) != nil || !bytes.Equal(request, compact.Bytes()) {
			t.Fatal("first-stage prompt or controls changed", err)
		}
		seeds = append(seeds, bindingSeed{Case: c, Names: content, Capture: in.Names, SHA256: hash(raw), Expected: in.Expectation})
	}
	return seeds
}

func proseBindingModelRequest(c modelCase, names []byte, profile modelProfile) ([]byte, error) {
	_, candidates, err := bindingCandidates(c, names, "model_proposal")
	if err != nil {
		return nil, err
	}
	raw, err := proseReferenceModelRequest(c, profile)
	if err != nil {
		return nil, err
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	cat, err := newCitationCatalog(c)
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(struct {
		Entries []citationTokenEntry `json:"entries"`
		Names   []bindingCandidate   `json:"names"`
	}{cat.Entries, candidates})
	if err != nil || len(input) > 24000 {
		return nil, fmt.Errorf("binding input budget")
	}
	format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
	format["name"] = "roles_from_name_candidates"
	fields := format["schema"].(map[string]any)["properties"].(map[string]any)["interpretations"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	fields["kind"] = map[string]any{"type": "string", "enum": []string{"role", "correction", "unknown"}}
	for field, kind := range map[string]string{"subjects": "person", "organizations": "organization"} {
		ids := []string{}
		for _, n := range candidates {
			if n.Kind == kind {
				ids = append(ids, n.ID)
			}
		}
		array := fields[field].(map[string]any)
		if len(ids) == 0 {
			array["maxItems"] = 0 // JSON Schema disallows an empty enum.
		} else {
			array["items"] = map[string]any{"type": "string", "enum": ids}
		}
	}
	request["messages"] = []map[string]string{{"role": "system", "content": proseBindingModelPrompt}, {"role": "user", "content": string(input)}}
	return json.Marshal(request)
}

func TestProseBindingSeedNamesLive(t *testing.T) {
	runProseModelTrial(t, proseNameModelPrompt, func(t *testing.T) []modelCase {
		return proseInputCases(t, bindingModelFixture)
	}, func(c modelCase, p modelProfile) []byte {
		raw, err := proseNameModelRequest(c, p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, inspectNameModelAnswer)
}

func TestProseBindingModelLive(t *testing.T) {
	if os.Getenv("LT_PROSE_MODEL_URL") == "" {
		t.Skip("opt-in local binding trial")
	}
	seeds := map[string]bindingSeed{}
	build := func(t *testing.T) []modelCase {
		cases := []modelCase{}
		for _, s := range proseBindingSeeds(t) {
			seeds[s.Case.ID] = s
			cases = append(cases, s.Case)
		}
		return cases
	}
	runProseModelTrial(t, proseBindingModelPrompt, build, func(c modelCase, p modelProfile) []byte {
		raw, err := proseBindingModelRequest(c, seeds[c.ID].Names, p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, func(c modelCase, raw []byte) (json.RawMessage, error) {
		out, err := inspectBindingSelections(c, seeds[c.ID].Names, raw, "model_proposal")
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	})
}
