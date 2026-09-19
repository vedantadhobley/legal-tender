package personaffiliation

import (
	_ "embed"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

//go:embed prose_referent_model_prompt.txt
var proseReferentPrompt string

const referentModelFixture = "referent-model-v1"

type referentTrialCase struct {
	ID     string   `json:"id"`
	Checks []string `json:"checks"`
}

// Fixed known-case regression set. Review checks never enter a model request;
// prior automatic name captures remain uncorrected, including their mistakes.
func proseReferentSeeds(t *testing.T) ([]bindingSeed, []referentTrialCase) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, referentModelFixture, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []referentTrialCase
	if err := strictjson.Decode(raw, &cases); err != nil || len(cases) != 8 {
		t.Fatal("invalid fixed referent evaluation set", err)
	}
	available := map[string]bindingSeed{}
	for _, seed := range proseBindingSeeds(t) {
		available[seed.Case.ID] = seed
	}
	for _, c := range proseInputCases(t, groundingFixture) {
		available[c.ID] = bindingSeed{Case: c, Names: groundingNameInput(t, c)}
	}
	seeds := []bindingSeed{}
	seen := map[string]bool{}
	for _, c := range cases {
		seed, ok := available[c.ID]
		if !ok || seen[c.ID] || len(c.Checks) == 0 {
			t.Fatal("unknown, duplicate or unreviewed case", c.ID)
		}
		for _, check := range c.Checks {
			if check == "" {
				t.Fatal("empty review check", c.ID)
			}
		}
		seen[c.ID] = true
		seeds = append(seeds, seed)
	}
	return seeds, cases
}

func proseReferentRequest(c modelCase, names []byte, p modelProfile) ([]byte, error) {
	raw, err := proseGroundingRequest(c, names, p)
	if err != nil {
		return nil, err
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	_, candidates, err := bindingCandidates(c, names, "model_proposal")
	if err != nil {
		return nil, err
	}
	object := func(fields map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "object", "properties": fields, "required": required, "additionalProperties": false}
	}
	array := func(item any, max int) map[string]any {
		return map[string]any{"type": "array", "items": item, "maxItems": max}
	}
	str := map[string]any{"type": "string"}
	enum := func(values ...string) map[string]any { return map[string]any{"type": "string", "enum": values} }
	choice := func(kind string) map[string]any {
		ids := []string{}
		for _, n := range candidates {
			if n.Kind == kind {
				ids = append(ids, n.ID)
			}
		}
		list := array(str, len(ids))
		if len(ids) > 0 {
			list["items"] = enum(ids...)
		}
		// State-dependent cardinality and reference coverage are checked in Go,
		// independent of the endpoint's structured-output implementation.
		return object(map[string]any{"state": enum("unassessed", "unresolved", "proposed", "ambiguous"), "candidates": list}, "state", "candidates")
	}
	format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
	grounding := format["schema"].(map[string]any)
	links := grounding["properties"].(map[string]any)["context_links"].(map[string]any)["items"].(map[string]any)
	links["properties"].(map[string]any)["kind"] = enum("contradicts")
	referents := object(map[string]any{"interpretation": str, "subject": choice("person"), "organization": choice("organization")}, "interpretation", "subject", "organization")
	corrections := object(map[string]any{"correction": str, "state": enum("proposed", "ambiguous", "unresolved"), "targets": array(str, 32)}, "correction", "state", "targets")
	format["name"] = "role_referents_and_correction_targets"
	format["schema"] = object(map[string]any{"grounding": grounding, "referents": array(referents, 32), "corrections": array(corrections, 32)}, "grounding", "referents", "corrections")
	request["messages"].([]any)[0].(map[string]any)["content"] = proseReferentPrompt
	return json.Marshal(request)
}

func inspectReferentModelAnswer(c modelCase, names, raw []byte) (json.RawMessage, error) {
	out, err := inspectReferentContract(c, names, raw, "model_proposal", "model_proposal")
	if err != nil {
		return nil, err // No repair, partial salvage or review-origin promotion.
	}
	return json.Marshal(out)
}

func TestProseReferentModelLive(t *testing.T) {
	// A generic opt-in for older experiments must not start this new trial.
	if os.Getenv("LT_PROSE_REFERENT_TRIAL") != "1" || os.Getenv("LT_PROSE_MODEL_URL") == "" {
		t.Skip("explicit opt-in referent trial; offline by default")
	}
	seeds, _ := proseReferentSeeds(t)
	names := map[string][]byte{}
	cases := []modelCase{}
	for _, seed := range seeds {
		cases = append(cases, seed.Case)
		names[seed.Case.ID] = seed.Names
	}
	runProseModelTrial(t, proseReferentPrompt, func(*testing.T) []modelCase { return cases }, func(c modelCase, p modelProfile) []byte {
		raw, err := proseReferentRequest(c, names[c.ID], p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, func(c modelCase, raw []byte) (json.RawMessage, error) {
		return inspectReferentModelAnswer(c, names[c.ID], raw)
	})
}
