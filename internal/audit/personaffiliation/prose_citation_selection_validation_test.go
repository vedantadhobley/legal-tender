package personaffiliation

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

func citationCase(t *testing.T, html string) modelCase {
	t.Helper()
	body := []byte(html)
	source := companypage.Source{URL: "https://example.org/citation-test", ObservedOn: "2026-09-17", SHA256: hash(body)}
	e, err := companypage.Extract(context.Background(), body, source)
	if err != nil {
		t.Fatal(err)
	}
	c := modelCase{Source: source, Entries: []modelEntry{}}
	for i, entry := range e.Entries {
		if entry.Kind == "text" || entry.Kind == "heading" {
			c.Entries = append(c.Entries, modelEntry{ID: i, Kind: entry.Kind, Text: entry.Text, Span: entry.Span})
		}
	}
	return c
}

func TestProseCitationCatalogOccurrences(t *testing.T) {
	c := citationCase(t, `<p>Zoë &amp; Co met Zoë &amp; Co. Banana banana. Co-founder Zoë’s note: é.</p>`)
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range cat.Entries[0].Tokens {
		if token.Text != c.Entries[0].Text[token.Start:token.End] || !utf8.ValidString(c.Entries[0].Text[:token.Start]) || !utf8.ValidString(token.Text) {
			t.Fatal("token is not source-owned UTF-8 text")
		}
		quote, err := cat.selectRange(citationRange{Entry: 0, First: token.ID, Last: token.ID})
		if err != nil || quote.Matched.Text != token.Text || quote.HTML != c.Entries[0].Span {
			t.Fatal("token selection or original HTML span changed", err)
		}
	}
	for _, tc := range []struct {
		text, state string
		count       int
		selectable  bool
	}{
		{"Zoë & Co", "ambiguous", 2, true},
		{"ana", "ambiguous", 4, false}, // Include overlapping substrings, but never round to token boundaries.
		{"Zoë’s", "unique", 1, true},
		{"é", "unique", 1, true},
		{"Zoë & Co", "not_found", 0, false}, // NBSP is not an ordinary space.
		{"Zoe & Co", "not_found", 0, false},
		{"Zoë &amp; Co", "not_found", 0, false}, // Decoded text and HTML are distinct coordinate systems.
		{"é", "not_found", 0, false},            // Do not silently compose the source's combining marks.
	} {
		t.Run(tc.text, func(t *testing.T) {
			out, err := cat.literalOptions(0, tc.text)
			if err != nil || out.State != tc.state || len(out.Matches) != tc.count {
				t.Fatal("literal options", out, err)
			}
			last := -1
			for _, match := range out.Matches {
				if match.Evidence.Matched.Text != tc.text || match.Evidence.Matched.Start <= last || (match.Range != nil) != tc.selectable {
					t.Fatal("occurrence omitted, rounded or selected implicitly")
				}
				last = match.Evidence.Matched.Start
				if match.Range != nil {
					selected, err := cat.selectRange(*match.Range)
					if err != nil || selected != match.Evidence {
						t.Fatal("Go-selected text differs from the occurrence", err)
					}
				}
			}
		})
	}
	view, _ := json.Marshal(cat.Entries)
	if strings.Contains(string(view), `"start"`) || strings.Contains(string(view), `"end"`) {
		t.Fatal("producer catalog asks for byte offsets")
	}
	for _, ref := range []citationRange{{Entry: 99}, {First: -1}, {First: 1, Last: 0}, {Last: len(cat.Entries[0].Tokens)}} {
		if _, err := cat.selectRange(ref); err == nil {
			t.Fatal("invalid range accepted", ref)
		}
	}
}

// Convert existing manually reviewed annotations, NOT saved model responses.
// A unique literal lookup supplies test coordinates; ambiguity is an error here,
// never an instruction to use the first match.
func reviewedCitationSelections(t *testing.T, cat citationCatalog, in mentionInterpretationInput) citationSelectionInput {
	t.Helper()
	out := citationSelectionInput{Selections: []citationSelection{}, Interpretations: in.Interpretations, Links: in.Links}
	for _, mention := range in.Mentions {
		options, err := cat.literalOptions(mention.Entry, mention.Text)
		if err != nil || options.State != "unique" || options.Matches[0].Range == nil {
			t.Fatal("reviewed coordinate is not uniquely token-aligned", mention.ID, err)
		}
		ref := options.Matches[0].Range
		out.Selections = append(out.Selections, citationSelection{ID: mention.ID, Entry: &ref.Entry, First: &ref.First, Last: &ref.Last})
	}
	return out
}

func TestProseCitationSelectionContext(t *testing.T) {
	c, annotated := mentionUnitInput(t)
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	in := reviewedCitationSelections(t, cat, annotated)
	raw, _ := json.Marshal(in)
	out, err := inspectCitationSelections(cat, raw, "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out.Review.Context, c.Entries) || out.Review.Source != c.Source || out.Review.Origin != "synthetic_test" || out.Review.IdentityApproved || out.Review.GraphPublicationApproved || out.Review.FinancialAttribution {
		t.Fatal("context, provenance or approvals changed")
	}
	if out.Review.Interpretations[0].State != "context_blocked" || out.Review.Interpretations[2].State != "unverified_role_interpretation" || out.Review.Interpretations[3].State != "nonpositive_denied" {
		t.Fatal("correction leaked into unrelated role or changed denial")
	}
	if !reflect.DeepEqual(out.Contexts[0].Entries, c.Entries[:1]) || !reflect.DeepEqual(out.Contexts[1].Entries, c.Entries[1:2]) {
		t.Fatal("full selected entries or correction context missing")
	}
	again, err := inspectCitationSelections(cat, raw, "synthetic_test")
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("nondeterministic source attachment")
	}
	// Wrong but valid source selections still cannot be detected semantically.
	in.Interpretations[0].Clause = "name"
	in.Interpretations[0].Subjects = []string{"company"}
	raw, _ = json.Marshal(in)
	wrong, err := inspectCitationSelections(cat, raw, "synthetic_test")
	if err != nil || wrong.Review.IdentityApproved || wrong.Review.GraphPublicationApproved || !reflect.DeepEqual(wrong.Contexts[0].Entries, c.Entries[:1]) {
		t.Fatal("citation validity claimed semantic validation", err)
	}
	// The caller cannot mutate the catalog through a returned full-entry slice.
	wrong.Contexts[0].Entries[0].Text = "changed"
	if cat.Case.Entries[0].Text != c.Entries[0].Text {
		t.Fatal("report aliases source context")
	}
}

func TestProseCitationSelectionRejectsForgedInput(t *testing.T) {
	c, annotated := mentionUnitInput(t)
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	in := reviewedCitationSelections(t, cat, annotated)
	raw, _ := json.Marshal(in)
	for _, bad := range []string{
		string(raw) + string(raw),
		`{"interpretation_origin":"reviewed_fixture",` + string(raw[1:]),
		`{"identity_approved":true,` + string(raw[1:]),
		`{"selections":[],` + string(raw[1:]),
		strings.Replace(string(raw), `"first_token":0`, `"first_token":null`, 1),
		strings.Replace(string(raw), `"first_token":0,`, "", 1),
		strings.Replace(string(raw), `"first_token":0`, `"first_token":-1`, 1),
		strings.Replace(string(raw), `"first_token":0`, `"first_token":0,"text":"forged"`, 1),
		strings.Replace(string(raw), `"first_token":0`, `"first_token":0,"text_start":0`, 1),
		strings.Replace(string(raw), `"entry":0`, `"entry":999`, 1),
		strings.Replace(string(raw), `"id":"name"`, `"id":"company"`, 1),
		strings.Replace(string(raw), `"clause":"ceo"`, `"clause":"missing"`, 1),
		`{"selections":[],"interpretations":[]}`,
		strings.Repeat(" ", 128<<10+1),
	} {
		out, err := inspectCitationSelections(cat, []byte(bad), "synthetic_test")
		if err == nil || !reflect.DeepEqual(out, citationSelectionReport{}) {
			t.Fatal("invalid input produced a usable partial report")
		}
	}
	if _, err := inspectCitationSelections(cat, raw, "invented_origin"); err == nil {
		t.Fatal("unsupported caller provenance")
	}
	empty, err := inspectCitationSelections(cat, []byte(`{"selections":[],"interpretations":[],"context_links":[]}`), "synthetic_test")
	if err != nil || len(empty.Review.Mentions) != 0 || !reflect.DeepEqual(empty.Review.Context, c.Entries) {
		t.Fatal("empty proposal loses source context", err)
	}
}

func TestProseCitationCatalogRejectsInvalidSource(t *testing.T) {
	c := citationCase(t, "<p>Some source text.</p>")
	for _, mutate := range []func(*modelCase){
		func(c *modelCase) { c.Source.SHA256 = "invalid" },
		func(c *modelCase) { c.Entries = nil },
		func(c *modelCase) { c.Entries[0].ID = -1 },
		func(c *modelCase) { c.Entries[0].Kind = "excluded" },
		func(c *modelCase) { c.Entries[0].Text = "\xff" },
		func(c *modelCase) { c.Entries[0].Text = " " },
		func(c *modelCase) { c.Entries[0].Text = strings.Repeat("a", 64<<10+1) },
		func(c *modelCase) { c.Entries[0].Text = strings.Repeat("a ", 4097) },
		func(c *modelCase) { c.Entries = append(c.Entries, c.Entries[0]) },
	} {
		bad := c
		bad.Entries = slices.Clone(c.Entries)
		mutate(&bad)
		out, err := newCitationCatalog(bad)
		if err == nil || !reflect.DeepEqual(out, citationCatalog{}) {
			t.Fatal("invalid source produced a catalog")
		}
	}
	cat, err := newCitationCatalog(citationCase(t, "<p>"+strings.Repeat("x ", 65)+"</p>"))
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		entry int
		text  string
	}{{0, "x"}, {0, ""}, {99, "x"}, {0, "\xff"}} {
		out, err := cat.literalOptions(query.entry, query.text)
		if err == nil || !reflect.DeepEqual(out, citationOptions{}) {
			t.Fatal("invalid or over-budget query produced partial matches")
		}
	}
}
