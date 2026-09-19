package personaffiliation

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"reflect"
	"testing"
)

//go:embed prose_reference_model_prompt.txt
var proseReferenceModelPrompt string

const referenceModelFixture = "reference-model-v1"

func proseReferenceModelCases(t *testing.T) []modelCase {
	return proseInputCases(t, referenceModelFixture)
}

func proseReferenceModelRequest(c modelCase, p modelProfile) ([]byte, error) {
	raw, err := proseCitationModelRequest(c, p)
	if err != nil {
		return nil, err
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
	format["name"] = "source_token_derived_references"
	properties := format["schema"].(map[string]any)["properties"].(map[string]any)
	item := properties["interpretations"].(map[string]any)["items"].(map[string]any)
	fields := item["properties"].(map[string]any)
	fields["additional_evidence"] = fields["evidence"]
	delete(fields, "evidence")
	item["required"] = []string{"id", "clause", "kind", "normalized", "role_kind", "subjects", "organizations", "additional_evidence", "binding", "status"}
	request["messages"].([]any)[0].(map[string]any)["content"] = proseReferenceModelPrompt
	return json.Marshal(request)
}

func inspectReferenceModelAnswer(c modelCase, raw []byte) (json.RawMessage, error) {
	cat, err := newCitationCatalog(c)
	if err != nil {
		return nil, err
	}
	report, err := inspectDerivedCitationReferences(cat, raw, "model_proposal")
	if err != nil {
		return nil, err
	}
	return json.Marshal(report)
}

func TestProseReferenceModelContract(t *testing.T) {
	profile := modelProfile{"gpt-oss-120b", "medium", 4096}
	cases := proseReferenceModelCases(t)
	if len(cases) != 6 {
		t.Fatal("fixed fresh sample changed")
	}
	for _, c := range cases {
		raw, err := proseReferenceModelRequest(c, profile)
		if err != nil {
			t.Fatal(c.ID, err)
		}
		var request map[string]any
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		old, err := proseCitationModelRequest(c, profile)
		if err != nil {
			t.Fatal(err)
		}
		var baseline map[string]any
		if err := json.Unmarshal(old, &baseline); err != nil {
			t.Fatal(err)
		}
		messages := request["messages"].([]any)
		if messages[0].(map[string]any)["content"] != proseReferenceModelPrompt || !reflect.DeepEqual(messages[1], baseline["messages"].([]any)[1]) {
			t.Fatal("prompt/catalog changed or review labels entered request")
		}
		for _, key := range []string{"model", "max_tokens", "reasoning_effort", "seed", "stream"} {
			if !reflect.DeepEqual(request[key], baseline[key]) {
				t.Fatal("transport control changed", key)
			}
		}
		format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
		fields := format["schema"].(map[string]any)["properties"].(map[string]any)["interpretations"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
		if fields["evidence"] != nil || fields["additional_evidence"] == nil || format["strict"] != true {
			t.Fatal("derived-reference schema boundary changed")
		}
		again, err := proseReferenceModelRequest(c, profile)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("nondeterministic request", err)
		}
	}
	cat, in := derivedReferenceUnitInput(t)
	raw, err := inspectReferenceModelAnswer(cat.Case, encodeReferenceInput(t, in))
	if err != nil {
		t.Fatal(err)
	}
	var out derivedReferenceReport
	if err := json.Unmarshal(raw, &out); err != nil || out.Review.Review.Origin != "model_proposal" || out.Review.Review.IdentityApproved || out.Review.Review.GraphPublicationApproved || out.Review.Review.FinancialAttribution {
		t.Fatal("model origin or acceptance boundary changed", err)
	}
}

func TestProseReferenceModelLive(t *testing.T) {
	runProseModelTrial(t, proseReferenceModelPrompt, proseReferenceModelCases, func(c modelCase, p modelProfile) []byte {
		raw, err := proseReferenceModelRequest(c, p)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, inspectReferenceModelAnswer)
}
