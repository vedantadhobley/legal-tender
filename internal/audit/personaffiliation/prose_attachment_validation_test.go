package personaffiliation

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

func TestProseAttachmentValidation(t *testing.T) {
	body := []byte(`<p>Riley Example is CEO of A&amp;B.</p><p>Correction: that statement is false.</p>`)
	source := companypage.Source{URL: "https://example.org/unit", ObservedOn: "2026-09-17", SHA256: hash(body)}
	e, err := companypage.Extract(context.Background(), body, source)
	if err != nil {
		t.Fatal(err)
	}
	c := modelCase{ID: "unit", Source: source}
	for i, entry := range e.Entries {
		c.Entries = append(c.Entries, modelEntry{ID: i, Kind: entry.Kind, Text: entry.Text, Span: entry.Span})
	}
	valid := `{"roles":[{"person":"Riley Example","role":"CEO","organization":"A&B","polarity":"denied","time_text":"","evidence_entries":[0,1]}],"aliases":[]}`
	raw, err := attachModelEvidence(c, []byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	var attached attachedEvidence
	if err := json.Unmarshal(raw, &attached); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attached.Context, c.Entries) || attached.Source != source || attached.IdentityApproved || attached.GraphPublicationApproved || attached.FinancialAttribution {
		t.Fatal("source/context or acceptance boundary changed")
	}
	for i, entry := range attached.Context {
		span := entry.Span
		if span.Start < 0 || span.End > len(body) || hash(body[span.Start:span.End]) != span.SHA256 || attached.Proposals.Roles[0].Citations[i].Quote != entry.Text {
			t.Fatal("attachment is not exact reader output")
		}
	}
	for _, bad := range []string{
		strings.Replace(valid, `[0,1]`, `[0,42]`, 1),
		strings.Replace(valid, `[0,1]`, `[0,0]`, 1),
		strings.Replace(valid, `[0,1]`, `[-1]`, 1),
		strings.Replace(valid, `[0,1]`, `[]`, 1),
		strings.Replace(valid, `[0,1]`, `null`, 1),
		strings.Replace(valid, `[0,1]`, `[0,1,2,3,4,5,6,7,8]`, 1),
		strings.Replace(valid, `[0,1]`, `[0.5]`, 1),
		strings.Replace(valid, `"organization":"A&B"`, `"organization":"Other Corp"`, 1),
		strings.Replace(valid, `"time_text":""`, `"time_text":"2020"`, 1),
		strings.Replace(valid, `"time_text":""`, `"time_text":null`, 1),
		strings.Replace(valid, `"time_text":"",`, ``, 1),
		strings.Replace(valid, `"denied"`, `"approved"`, 1),
		strings.Replace(valid, `"aliases":[]`, `"aliases":null`, 1),
		strings.Replace(valid, `"aliases":[]`, `"aliases":[],"identity_approved":true`, 1),
		strings.Replace(valid, `"evidence_entries":[0,1]`, `"evidence_entries":[0,1],"citations":[]`, 1),
		strings.Replace(valid, `"person":"Riley Example"`, `"person":"Riley Example","person":"Riley Example"`, 1),
		valid + valid,
	} {
		if output, err := attachModelEvidence(c, []byte(bad)); err == nil || output != nil {
			t.Fatal("invalid answer escaped whole-answer rejection", bad)
		}
	}
	// Preserving full context is not an automatic check of polarity or selection.
	omittedCorrection := strings.Replace(strings.Replace(valid, `[0,1]`, `[0]`, 1), `"denied"`, `"asserted"`, 1)
	raw, err = attachModelEvidence(c, []byte(omittedCorrection))
	if err != nil {
		t.Fatal("literal check falsely claims semantic validation", err)
	}
	if err := json.Unmarshal(raw, &attached); err != nil {
		t.Fatal(err)
	}
	if len(attached.Context) != 2 || !strings.Contains(attached.Context[1].Text, "false") || len(attached.Proposals.Roles[0].Citations) != 1 {
		t.Fatal("omitted correction lost or silently added to model-selected evidence")
	}
	duplicate := c
	duplicate.Entries = append(append([]modelEntry{}, c.Entries...), c.Entries[0])
	if _, err := attachModelEvidence(duplicate, []byte(valid)); err == nil {
		t.Fatal("duplicate input entry accepted")
	}
	alias := `{"roles":[],"aliases":[{"name_text":"Riley Example","alias_text":"Ri","evidence_entries":[0]}]}`
	// Even a literal substring is not automatically an alias assertion.
	if _, err := attachModelEvidence(c, []byte(alias)); err != nil {
		t.Fatal(err)
	}
	if _, err := attachModelEvidence(c, []byte(strings.Replace(alias, `"Ri"`, `"Riley E"`, 1))); err != nil {
		t.Fatal(err)
	}
	if _, err := attachModelEvidence(c, []byte(strings.Replace(alias, `"Ri"`, `"Invented"`, 1))); err == nil {
		t.Fatal("nonliteral alias accepted")
	}
	if _, err := attachModelEvidence(c, []byte(`{"roles":[],"aliases":[]}`)); err != nil {
		t.Fatal("abstention rejected", err)
	}
}

func TestProseAttachmentFixedInputs(t *testing.T) {
	cases := proseAttachmentCases(t)
	if len(cases) != 15 {
		t.Fatal("trial scope changed")
	}
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.ID] {
			t.Fatal("duplicate case")
		}
		seen[c.ID] = true
		raw := proseAttachmentRequest(c, modelProfile{"fixture-model", "medium", 4096})
		var request struct{ Messages []struct{ Content string } }
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		entries, _ := json.Marshal(c.Entries)
		if len(request.Messages) != 2 || request.Messages[0].Content != proseAttachmentPrompt || !bytes.Equal([]byte(request.Messages[1].Content), entries) {
			t.Fatal("review labels leaked into input")
		}
		if !bytes.Equal(raw, proseAttachmentRequest(c, modelProfile{"fixture-model", "medium", 4096})) {
			t.Fatal("request changed across calls")
		}
	}
}
