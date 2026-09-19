package personaffiliation

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func mentionUnitInput(t *testing.T) (modelCase, mentionInterpretationInput) {
	t.Helper()
	body := []byte(`<p>Zoë Test is CEO of A&amp;B.</p><p>Correction: that statement is false.</p><p>Zoë Test is treasurer of Civic Group.</p><p>Another account says Zoë Test is not CEO of A&amp;B.</p>`)
	source := companypage.Source{URL: "https://example.org/mention-unit", ObservedOn: "2026-09-17", SHA256: hash(body)}
	e, err := companypage.Extract(context.Background(), body, source)
	if err != nil {
		t.Fatal(err)
	}
	c := modelCase{ID: "unit", Source: source}
	for i, entry := range e.Entries {
		c.Entries = append(c.Entries, modelEntry{ID: i, Kind: entry.Kind, Text: entry.Text, Span: entry.Span})
	}
	in := mentionInterpretationInput{Origin: "synthetic_test", Mentions: []mentionProposal{
		{ID: "name", Entry: 0, Text: "Zoë Test"}, {ID: "company", Entry: 0, Text: "A&B"},
		{ID: "ceo", Entry: 0, Text: "Zoë Test is CEO of A&B."}, {ID: "correction", Entry: 1, Text: "Correction: that statement is false."},
		{ID: "treasurer", Entry: 2, Text: "Zoë Test is treasurer of Civic Group."}, {ID: "civic", Entry: 2, Text: "Civic Group"},
		{ID: "denial", Entry: 3, Text: "Another account says Zoë Test is not CEO of A&B."},
	}, Interpretations: []interpretationProposal{
		{ID: "positive", Clause: "ceo", Kind: "role", Normalized: "Chief Executive Officer", Role: "executive", Subjects: []string{"name"}, Organizations: []string{"company"}, Evidence: []string{"ceo", "name", "company"}, Binding: "direct", Status: "asserted"},
		{ID: "notice", Clause: "correction", Kind: "correction", Normalized: "retraction", Subjects: []string{}, Organizations: []string{}, Evidence: []string{"correction"}, Binding: "unresolved", Status: "unclear"},
		{ID: "other-role", Clause: "treasurer", Kind: "role", Normalized: "treasurer", Role: "unknown", Subjects: []string{"name"}, Organizations: []string{"civic"}, Evidence: []string{"treasurer", "name", "civic"}, Binding: "coreference", Status: "asserted"},
		{ID: "negative", Clause: "denial", Kind: "role", Normalized: "CEO", Role: "executive", Subjects: []string{"name"}, Organizations: []string{"company"}, Evidence: []string{"denial", "name", "company"}, Binding: "coreference", Status: "denied"},
	}, Links: []contextProposal{{From: "notice", To: "positive", Kind: "retracts"}}}
	return c, in
}

func TestProseInterpretationContext(t *testing.T) {
	c, in := mentionUnitInput(t)
	out, err := inspectMentionInterpretations(c, encodeMentionInput(in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Interpretations[0].State != "context_blocked" || len(out.Interpretations[0].ContextFlags) != 1 || out.Interpretations[2].State != "unverified_role_interpretation" {
		t.Fatal("correction leaked into unrelated role")
	}
	if out.Mentions[2].Evidence.Matched.Text != "Zoë Test is CEO of A&B." || out.Interpretations[0].Proposal.Normalized != "Chief Executive Officer" {
		t.Fatal("normalization overwrote literal evidence")
	}
	if out.Interpretations[3].State != "nonpositive_denied" || out.IdentityApproved || out.GraphPublicationApproved || out.FinancialAttribution {
		t.Fatal("denial/approval boundary")
	}
	// Contradiction flags both statements. Retraction remains a separate flag.
	in.Links = append(in.Links, contextProposal{From: "negative", To: "positive", Kind: "contradicts"})
	out, err = inspectMentionInterpretations(c, encodeMentionInput(in))
	if err != nil || len(out.Interpretations[0].ContextFlags) != 2 || len(out.Interpretations[3].ContextFlags) != 1 {
		t.Fatal("conflict or retraction disappeared", err)
	}
	if out.Interpretations[0].Proposal.Status != "asserted" || out.Interpretations[3].Proposal.Status != "denied" {
		t.Fatal("input assertion rewritten")
	}
	// Missing context annotation cannot be magically detected by a shape checker.
	in.Links = []contextProposal{}
	in.Interpretations[0].Normalized = "unsubstantiated interpretation"
	out, err = inspectMentionInterpretations(c, encodeMentionInput(in))
	if err != nil || out.Interpretations[0].State != "unverified_role_interpretation" || len(out.Context) != 4 || out.GraphPublicationApproved {
		t.Fatal("literal validity claimed semantic truth", err)
	}
}

func TestProseInterpretationRejectsInvalidReferences(t *testing.T) {
	c, in := mentionUnitInput(t)
	base := encodeMentionInput(in)
	for name, mutate := range map[string]func(*mentionInterpretationInput){
		"stitched mention":     func(v *mentionInterpretationInput) { v.Mentions[0].Text = "Zoe Test" },
		"unknown source entry": func(v *mentionInterpretationInput) { v.Mentions[0].Entry = 99 },
		"duplicate mention":    func(v *mentionInterpretationInput) { v.Mentions = append(v.Mentions, v.Mentions[0]) },
		"duplicate interpretation": func(v *mentionInterpretationInput) {
			v.Interpretations = append(v.Interpretations, v.Interpretations[0])
		},
		"missing antecedent":                func(v *mentionInterpretationInput) { v.Interpretations[0].Subjects = []string{"missing"} },
		"uncited antecedent":                func(v *mentionInterpretationInput) { v.Interpretations[0].Evidence = []string{"ceo", "company"} },
		"missing clause":                    func(v *mentionInterpretationInput) { v.Interpretations[0].Clause = "missing" },
		"role without organization":         func(v *mentionInterpretationInput) { v.Interpretations[0].Organizations = []string{} },
		"multiple resolved subjects":        func(v *mentionInterpretationInput) { v.Interpretations[0].Subjects = []string{"name", "company"} },
		"nonrole executive":                 func(v *mentionInterpretationInput) { v.Interpretations[0].Kind = "activity" },
		"bad role kind":                     func(v *mentionInterpretationInput) { v.Interpretations[0].Role = "billionaire" },
		"assertion acceptance":              func(v *mentionInterpretationInput) { v.Interpretations[0].Status = "accepted" },
		"null subjects":                     func(v *mentionInterpretationInput) { v.Interpretations[0].Subjects = nil },
		"unknown target":                    func(v *mentionInterpretationInput) { v.Links[0].To = "missing" },
		"self link":                         func(v *mentionInterpretationInput) { v.Links[0].To = "notice" },
		"duplicate link":                    func(v *mentionInterpretationInput) { v.Links = append(v.Links, v.Links[0]) },
		"role retracts role":                func(v *mentionInterpretationInput) { v.Links[0].From = "negative" },
		"retracting correction unsupported": func(v *mentionInterpretationInput) { v.Links[0].To = "notice"; v.Links[0].From = "positive" },
		"unknown link kind":                 func(v *mentionInterpretationInput) { v.Links[0].Kind = "proves" },
		"null links":                        func(v *mentionInterpretationInput) { v.Links = nil },
		"unlabeled origin":                  func(v *mentionInterpretationInput) { v.Origin = "" },
	} {
		t.Run(name, func(t *testing.T) {
			var v mentionInterpretationInput
			if err := strictjson.Decode(base, &v); err != nil {
				t.Fatal(err)
			}
			mutate(&v)
			out, err := inspectMentionInterpretations(c, encodeMentionInput(v))
			if err == nil || !reflect.DeepEqual(out, mentionInterpretationReport{}) {
				t.Fatal("invalid input produced usable partial result")
			}
		})
	}
	for _, bad := range []string{string(base) + string(base), strings.Replace(string(base), `"interpretation_origin":"synthetic_test"`, `"interpretation_origin":"synthetic_test","identity_approved":true`, 1), strings.Replace(string(base), `"interpretation_origin":"synthetic_test"`, `"interpretation_origin":"synthetic_test","interpretation_origin":"synthetic_test"`, 1)} {
		if _, err := inspectMentionInterpretations(c, []byte(bad)); err == nil {
			t.Fatal("unknown fields, duplicate keys or trailing JSON accepted")
		}
	}
}

func TestProseInterpretationOccurrenceAndEmptyInput(t *testing.T) {
	c, in := mentionUnitInput(t)
	in.Mentions = []mentionProposal{{ID: "repeat", Entry: 0, Text: "CEO"}}
	in.Interpretations = []interpretationProposal{}
	in.Links = []contextProposal{}
	c.Entries[0].Text = "CEO CEO"
	if _, err := inspectMentionInterpretations(c, encodeMentionInput(in)); err == nil {
		t.Fatal("ambiguous occurrence silently selected")
	}
	start := 4
	in.Mentions[0].Start = &start
	out, err := inspectMentionInterpretations(c, encodeMentionInput(in))
	if err != nil || out.Mentions[0].Evidence.Matched.Start != 4 {
		t.Fatal("explicit occurrence lost", err)
	}
	start = -1
	if _, err := inspectMentionInterpretations(c, encodeMentionInput(in)); err == nil {
		t.Fatal("invalid byte offset accepted")
	}
	start = 1
	c.Entries[0].Text = "éCEO"
	in.Mentions[0].Text = "CEO"
	if _, err := inspectMentionInterpretations(c, encodeMentionInput(in)); err == nil {
		t.Fatal("split UTF-8 offset accepted")
	}
	start = 2
	if _, err := inspectMentionInterpretations(c, encodeMentionInput(in)); err != nil {
		t.Fatal("UTF-8 byte offset rejected", err)
	}
	in.Mentions = []mentionProposal{}
	if _, err := inspectMentionInterpretations(c, encodeMentionInput(in)); err != nil {
		t.Fatal("empty interpretation rejected", err)
	}
	// No interpretation entry is secretly synthesized from the surrounding text.
	raw, _ := json.Marshal(in)
	out, err = inspectMentionInterpretations(c, raw)
	if err != nil || len(out.Interpretations) != 0 || len(out.Context) != 4 {
		t.Fatal("empty does not mean source absent", err)
	}
}
