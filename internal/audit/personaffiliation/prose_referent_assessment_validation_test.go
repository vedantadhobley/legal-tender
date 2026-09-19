package personaffiliation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

func TestProseReferentAssessmentRequestsOffline(t *testing.T) {
	seeds, manifest := proseReferentAssessmentSeeds(t)
	wantIDs := []string{"microsoft-hood", "amd-su", "fresh-mcclure", "grounding-multiple-roles", "grounding-council-scope", "grounding-ambiguous-conditional", "grounding-anonymous-correction", "grounding-nearby-names", "new-pronoun-employer-change", "new-correction-and-conflict"}
	ids := make([]string, 0, len(seeds))
	profile := modelProfile{"offline-model", "medium", 4096}
	for i, seed := range seeds {
		ids = append(ids, seed.Case.ID)
		raw, err := proseReferentAssessmentRequest(seed, profile)
		if err != nil {
			t.Fatal(seed.Case.ID, err)
		}
		again, err := proseReferentAssessmentRequest(seed, profile)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("nondeterministic assessment request", seed.Case.ID, err)
		}
		var request struct {
			Model           string
			ReasoningEffort string `json:"reasoning_effort"`
			MaxTokens       int    `json:"max_tokens"`
			Seed            int
			Stream          bool
			Messages        []struct{ Role, Content string }
			ResponseFormat  struct {
				Type       string
				JSONSchema struct {
					Name   string
					Strict bool
					Schema map[string]any
				} `json:"json_schema"`
			} `json:"response_format"`
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		if request.Model != profile.Model || request.ReasoningEffort != profile.ReasoningEffort || request.MaxTokens != profile.MaxTokens || request.Seed != 1 || request.Stream || len(request.Messages) != 2 || request.Messages[0].Content != proseReferentAssessmentPrompt || request.ResponseFormat.Type != "json_schema" || request.ResponseFormat.JSONSchema.Name != "referent_and_correction_assessments" || !request.ResponseFormat.JSONSchema.Strict {
			t.Fatal("assessment transport or prompt boundary changed", seed.Case.ID)
		}
		for _, check := range manifest[i].Checks {
			if strings.Contains(request.Messages[1].Content, check) || strings.Contains(request.Messages[0].Content, check) {
				t.Fatal("review check entered assessment request", seed.Case.ID)
			}
		}
		var input struct {
			Source          companypage.Source
			Entries         []modelEntry       `json:"source_entries"`
			Candidates      []bindingCandidate `json:"name_candidates"`
			GroundingSHA256 string             `json:"grounding_sha256"`
			Grounding       groundedRoleInput
		}
		if err := json.Unmarshal([]byte(request.Messages[1].Content), &input); err != nil {
			t.Fatal(err)
		}
		var wantGrounding groundedRoleInput
		if err := json.Unmarshal(seed.Grounding, &wantGrounding); err != nil || input.Source != seed.Case.Source || input.GroundingSHA256 != hash(seed.Grounding) || !reflect.DeepEqual(input.Entries, seed.Case.Entries) || !reflect.DeepEqual(input.Grounding, wantGrounding) {
			t.Fatal("source or immutable grounding changed", seed.Case.ID, err)
		}
		_, wantCandidates, err := bindingCandidates(seed.Case, seed.Names, seed.GroundingOrigin)
		if err != nil || !reflect.DeepEqual(input.Candidates, wantCandidates) {
			t.Fatal("candidate catalog changed", seed.Case.ID, err)
		}
		properties := request.ResponseFormat.JSONSchema.Schema["properties"].(map[string]any)
		if len(properties) != 2 || properties["grounding"] != nil || properties["referents"] == nil || properties["corrections"] == nil {
			t.Fatal("model can recreate grounding", seed.Case.ID)
		}
	}
	if !reflect.DeepEqual(ids, wantIDs) {
		t.Fatal("fixed assessment case order changed", ids)
	}
}

func TestProseReferentAssessmentJoinOffline(t *testing.T) {
	c, names, full := referentUnitInput(t)
	grounding, err := json.Marshal(full.Grounding)
	if err != nil {
		t.Fatal(err)
	}
	seed := referentAssessmentSeed{Case: c, Names: names, Grounding: grounding, GroundingOrigin: "synthetic_test"}
	assessment := referentAssessmentInput{Referents: full.Referents, Corrections: full.Corrections}
	for i := range assessment.Referents {
		assessment.Referents[i].Subject = &referentChoice{State: "unresolved", Candidates: []string{}}
		assessment.Referents[i].Organization = &referentChoice{State: "unresolved", Candidates: []string{}}
	}
	raw, err := json.Marshal(assessment)
	if err != nil {
		t.Fatal(err)
	}
	out, err := inspectReferentAssessment(seed, raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Method != "grounding-bound-referent-assessment.v1" || out.GroundingSHA256 != hash(grounding) || out.GroundingOrigin != "synthetic_test" || !reflect.DeepEqual(out.Assessment, assessment) || !reflect.DeepEqual(out.Joined.Proposal.Grounding, full.Grounding) || out.Joined.AssessmentOrigin != "model_proposal" || len(out.Joined.Derived.Proposal.Links) != 2 {
		t.Fatal("assessment join, provenance or exact grounding changed")
	}
	review := out.Joined.Derived.Review.Derived.Review.Review
	if review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
		t.Fatal("assessment approved downstream action")
	}

	bad := assessment
	bad.Referents = append([]roleReferents{}, assessment.Referents...)
	bad.Referents[0].Subject = &referentChoice{State: "unassessed", Candidates: []string{}}
	badRaw, _ := json.Marshal(bad)
	if partial, err := inspectReferentAssessment(seed, badRaw); err == nil || !reflect.DeepEqual(partial, referentAssessmentReport{}) {
		t.Fatal("unassessed model endpoint accepted")
	}
	for _, malformed := range [][]byte{
		[]byte(`{"referents":[],"corrections":[]}`),
		[]byte(`{"grounding":{},"referents":[],"corrections":[]}`),
		append(append([]byte{}, raw...), raw...),
	} {
		if partial, err := inspectReferentAssessment(seed, malformed); err == nil || !reflect.DeepEqual(partial, referentAssessmentReport{}) {
			t.Fatal("invalid assessment repaired or partially joined")
		}
	}
}

func TestProseReferentAssessmentSchemaUsesOnlySuppliedIDs(t *testing.T) {
	c, names, full := referentUnitInput(t)
	grounding, _ := json.Marshal(full.Grounding)
	seed := referentAssessmentSeed{Case: c, Names: names, Grounding: grounding, GroundingOrigin: "synthetic_test"}
	raw, err := proseReferentAssessmentRequest(seed, modelProfile{"offline-model", "none", 2048})
	if err != nil {
		t.Fatal(err)
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	schema := request["response_format"].(map[string]any)["json_schema"].(map[string]any)["schema"].(map[string]any)
	fields := schema["properties"].(map[string]any)
	refs := fields["referents"].(map[string]any)
	if refs["minItems"] != float64(len(full.Referents)) || refs["maxItems"] != float64(len(full.Referents)) {
		t.Fatal("role coverage not encoded")
	}
	item := refs["items"].(map[string]any)["properties"].(map[string]any)
	states := item["subject"].(map[string]any)["properties"].(map[string]any)["state"].(map[string]any)["enum"]
	if !reflect.DeepEqual(states, []any{"unresolved", "proposed", "ambiguous"}) {
		t.Fatal("assessment states changed")
	}
	if strings.Contains(string(raw), `"unassessed"`) || strings.Contains(string(raw), `"grounding":{"type"`) {
		t.Fatal("response schema permits unassessed or grounding output")
	}
}
