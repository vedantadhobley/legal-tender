package personaffiliation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestProseInterpretationModelTrustBoundary(t *testing.T) {
	c, in := mentionUnitInput(t)
	proposal := modelInterpretations{Mentions: in.Mentions, Interpretations: in.Interpretations, Links: in.Links}
	raw, _ := json.Marshal(proposal)
	bound, err := inspectModelInterpretations(c, raw)
	if err != nil {
		t.Fatal(err)
	}
	var report mentionInterpretationReport
	if err := json.Unmarshal(bound, &report); err != nil {
		t.Fatal(err)
	}
	if report.Origin != "model_proposal" || report.Source != c.Source || !reflect.DeepEqual(report.Context, c.Entries) || report.IdentityApproved || report.GraphPublicationApproved || report.FinancialAttribution || report.Interpretations[0].State != "context_blocked" {
		t.Fatal("model proposal escaped the unverified evidence boundary")
	}
	for _, bad := range [][]byte{
		[]byte(`{"interpretation_origin":"reviewed_fixture",` + string(raw[1:])),
		[]byte(`{"identity_approved":true,` + string(raw[1:])),
		[]byte(`{"mentions":[],` + string(raw[1:])),
		append(bytes.Clone(raw), raw...),
		[]byte(`{"mentions":[],"interpretations":[]}`),
		[]byte(strings.Repeat(" ", 128<<10+1)),
	} {
		if partial, err := inspectModelInterpretations(c, bad); err == nil || partial != nil {
			t.Fatal("forged origin, invalid structure or budget produced a report")
		}
	}
	// It is a proposal validator, not a semantic oracle. Wrong but literal
	// endpoint selections can still pass and must never gain approval.
	proposal.Interpretations[0].Subjects = []string{"company"}
	raw, _ = json.Marshal(proposal)
	bound, err = inspectModelInterpretations(c, raw)
	if err != nil {
		t.Fatal("unexpected semantic claim by structural checker", err)
	}
	if err := json.Unmarshal(bound, &report); err != nil || report.IdentityApproved || report.GraphPublicationApproved {
		t.Fatal("invalid approval")
	}
}

func TestProseInterpretationModelRequest(t *testing.T) {
	cases := proseInterpretationCases(t)
	p := modelProfile{"gpt-oss-120b", "medium", 4096}
	for _, c := range cases {
		var request struct {
			Model     string
			MaxTokens int `json:"max_tokens"`
			Messages  []struct{ Role, Content string }
		}
		raw := proseInterpretationRequest(c, p)
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		input, _ := json.Marshal(c.Entries)
		if request.Model != p.Model || request.MaxTokens != p.MaxTokens || len(request.Messages) != 2 || request.Messages[0].Content != proseInterpretationPrompt || request.Messages[1].Content != string(input) || !bytes.Equal(raw, proseInterpretationRequest(c, p)) {
			t.Fatal("request changed or received reviewed expectations")
		}
	}
}
