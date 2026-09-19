package personaffiliation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestProseReferentModelRequestsOffline(t *testing.T) {
	seeds, reviews := proseReferentSeeds(t)
	ids := []string{}
	for _, seed := range seeds {
		ids = append(ids, seed.Case.ID)
	}
	if !reflect.DeepEqual(ids, []string{"microsoft-hood", "amd-su", "fresh-mcclure", "grounding-multiple-roles", "grounding-council-scope", "grounding-ambiguous-conditional", "grounding-anonymous-correction", "grounding-nearby-names"}) {
		t.Fatal("fixed regression set changed")
	}
	// Arbitrary model ID, not a live inventory claim. Neither profile makes a call.
	for _, profile := range []modelProfile{{"offline-model", "none", 4096}, {"offline-model", "medium", 8192}} {
		for i, seed := range seeds {
			c := seed.Case
			raw, err := proseReferentRequest(c, seed.Names, profile)
			if err != nil {
				t.Fatal(c.ID, err)
			}
			again, err := proseReferentRequest(c, seed.Names, profile)
			if err != nil || !bytes.Equal(raw, again) {
				t.Fatal("nondeterministic request", c.ID, err)
			}
			var request, baseline map[string]any
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Fatal(err)
			}
			old, err := proseGroundingRequest(c, seed.Names, profile)
			if err != nil || json.Unmarshal(old, &baseline) != nil {
				t.Fatal("invalid unchanged grounding request", err)
			}
			messages := request["messages"].([]any)
			if messages[0].(map[string]any)["content"] != proseReferentPrompt || !reflect.DeepEqual(messages[1], baseline["messages"].([]any)[1]) {
				t.Fatal("source catalog or original name candidates changed")
			}
			for _, check := range reviews[i].Checks {
				for _, message := range messages {
					if strings.Contains(message.(map[string]any)["content"].(string), check) {
						t.Fatal("case review entered request")
					}
				}
			}
			for _, key := range []string{"model", "max_tokens", "reasoning_effort", "seed", "stream"} {
				if !reflect.DeepEqual(request[key], baseline[key]) {
					t.Fatal("transport control changed", key)
				}
			}
			format := request["response_format"].(map[string]any)["json_schema"].(map[string]any)
			schema := format["schema"].(map[string]any)
			if format["strict"] != true || format["name"] != "role_referents_and_correction_targets" || schema["additionalProperties"] != false || !reflect.DeepEqual(schema["required"], []any{"grounding", "referents", "corrections"}) {
				t.Fatal("wrapper schema boundary changed")
			}
			fields := schema["properties"].(map[string]any)
			grounding := fields["grounding"].(map[string]any)
			original := baseline["response_format"].(map[string]any)["json_schema"].(map[string]any)["schema"].(map[string]any)
			original["properties"].(map[string]any)["context_links"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)["kind"] = map[string]any{"type": "string", "enum": []any{"contradicts"}}
			if !reflect.DeepEqual(grounding, original) {
				t.Fatal("grounding schema changed beyond retraction authority")
			}
			_, candidates, err := bindingCandidates(c, seed.Names, "model_proposal")
			if err != nil {
				t.Fatal(err)
			}
			refs := fields["referents"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
			for field, kind := range map[string]string{"subject": "person", "organization": "organization"} {
				choice := refs[field].(map[string]any)["properties"].(map[string]any)
				list := choice["candidates"].(map[string]any)
				want := []any{}
				for _, candidate := range candidates {
					if candidate.Kind == kind {
						want = append(want, candidate.ID)
					}
				}
				if list["maxItems"] != float64(len(want)) || !reflect.DeepEqual(choice["state"].(map[string]any)["enum"], []any{"unassessed", "unresolved", "proposed", "ambiguous"}) {
					t.Fatal("choice bounds or states changed")
				}
				if got := list["items"].(map[string]any)["enum"]; len(want) == 0 && got != nil || len(want) > 0 && !reflect.DeepEqual(got, want) {
					t.Fatal("wrong candidate enum", c.ID, field)
				}
			}
			t.Logf("%s reasoning=%s request_bytes=%d", c.ID, profile.ReasoningEffort, len(raw))
		}
	}
}

func TestProseReferentModelAnswerOffline(t *testing.T) {
	// Simulated response only: this annotation is never a retained model result.
	c, names, in := referentUnitInput(t)
	answer := referentJSON(t, in)
	raw, err := inspectReferentModelAnswer(c, names, answer)
	if err != nil {
		t.Fatal(err)
	}
	var out referentReport
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	review := out.Derived.Review.Derived.Review.Review
	if out.AssessmentOrigin != "model_proposal" || review.Origin != "model_proposal" || !reflect.DeepEqual(out.Proposal, in) || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution || len(out.Derived.Proposal.Links) != 2 {
		t.Fatal("model provenance, preservation or approval boundary changed")
	}
	in.Corrections = []correctionTargets{}
	missing := referentJSON(t, in)
	_, _, in = referentUnitInput(t)
	in.Referents[0].Subject = &referentChoice{State: "proposed", Candidates: []string{"n1"}} // Organization, not person.
	for _, bad := range [][]byte{answer[:len(answer)/2], append(append([]byte{}, answer...), answer...), missing, referentJSON(t, in), []byte(`{"assessment_origin":"reviewed_fixture",` + string(answer[1:]))} {
		if partial, err := inspectReferentModelAnswer(c, names, bad); err == nil || partial != nil {
			t.Fatal("invalid response salvaged or repaired")
		}
	}
}

func TestProseReferentModelEmptyNamesOffline(t *testing.T) {
	c, _, _ := referentUnitInput(t)
	raw, err := proseReferentRequest(c, []byte(`{"names":[]}`), modelProfile{"offline-model", "none", 4096})
	if err != nil {
		t.Fatal(err)
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	refs := request["response_format"].(map[string]any)["json_schema"].(map[string]any)["schema"].(map[string]any)["properties"].(map[string]any)["referents"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []string{"subject", "organization"} {
		list := refs[field].(map[string]any)["properties"].(map[string]any)["candidates"].(map[string]any)
		if list["maxItems"] != float64(0) || list["items"].(map[string]any)["enum"] != nil {
			t.Fatal("empty candidates allow fabricated ID or invalid empty enum")
		}
	}
}

func TestProseReferentModelRequiresSpecificOptIn(t *testing.T) {
	t.Setenv("LT_PROSE_REFERENT_TRIAL", "")
	t.Setenv("LT_PROSE_MODEL_URL", ":invalid-url")
	if !t.Run("generic opt-in is insufficient", TestProseReferentModelLive) {
		t.Fatal("new trial started without its specific opt-in")
	}
}
