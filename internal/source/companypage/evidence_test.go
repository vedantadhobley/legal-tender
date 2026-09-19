package companypage

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func extractTest(t *testing.T, body string) Evidence {
	t.Helper()
	raw := []byte(body)
	r, err := Extract(context.Background(), raw, Source{"https://example.org/team", "2026-09-15", digest(raw)})
	if err != nil {
		t.Fatal(err)
	}
	verifySpans(t, raw, r)
	return r
}

func verifySpans(t *testing.T, raw []byte, r Evidence) {
	t.Helper()
	end := 0
	for i, e := range r.Entries {
		if e.Span.Start < end || e.Span.End > len(raw) || e.Span.End < e.Span.Start || digest(raw[e.Span.Start:e.Span.End]) != e.Span.SHA256 {
			t.Fatal("incorrect or overlapping source span", i, e.Span)
		}
		end = e.Span.End
		if e.Payload != nil {
			s := e.Payload
			if s.Start < e.Span.Start || s.End > e.Span.End || string(raw[s.Start:s.End]) != e.Raw || digest(raw[s.Start:s.End]) != s.SHA256 {
				t.Fatal("incorrect JSON-LD payload span")
			}
		}
		for _, idx := range e.PrecedingHeadings {
			if idx < 0 || idx >= i || r.Entries[idx].Kind != "heading" {
				t.Fatal("heading reference not prior evidence")
			}
		}
	}
	if r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
		t.Fatal("text extraction approved an interpretation")
	}
}

func TestGenericExtraction(t *testing.T) {
	r := extractTest(t, `<!doctype html><head><meta charset="UTF-8"><title>A &amp; B</title><meta name="description" content="A company"><meta name="description" content="Another claim"><link rel="alternate CANONICAL" href="https://elsewhere.invalid/"></head><body><h2>Board</h2><h3>Alex <em>Quinn</em></h3><p>Founder &amp; <b>Chairman</b><br>formerly CEO</p><p>Founder &amp; Chairman</p><style>hidden</style><script>fake person</script><template><template>hidden</template>hidden</template><noscript>hidden</noscript><svg><text>hidden</text></svg><script type="application/ld+json"> { "@context": "https://do-not-fetch.invalid", "@type": "Person", "name": "Alex Quinn" } </script><script type="application/ld+json">{"name":"one","name":"two"}</script></body>`)
	var text []string
	metadata, excluded, ld := 0, 0, []Entry{}
	for _, e := range r.Entries {
		if e.Text != "" {
			text = append(text, e.Text)
		}
		switch e.Kind {
		case "metadata":
			metadata++
		case "excluded":
			excluded++
		case "json_ld":
			ld = append(ld, e)
		}
		if e.Text == "Founder & Chairman formerly CEO" {
			if len(e.PrecedingHeadings) != 2 || r.Entries[e.PrecedingHeadings[0]].Text != "Board" || r.Entries[e.PrecedingHeadings[1]].Text != "Alex Quinn" {
				t.Fatal("lost preceding source headings")
			}
		}
	}
	if strings.Join(text, "|") != "A & B|Board|Alex Quinn|Founder & Chairman formerly CEO|Founder & Chairman" || metadata != 4 || excluded != 5 || len(ld) != 2 {
		t.Fatal(text, metadata, excluded, len(ld))
	}
	if ld[0].JSONState != "valid_json_uninterpreted" || ld[1].JSONState != "invalid_json" {
		t.Fatal("invalid JSON promoted or valid raw block lost")
	}
}

func TestLexicalNotBrowserVisibilityOrRoleInterpretation(t *testing.T) {
	r := extractTest(t, `<h2>Board</h2><div hidden><p>Alex Quinn</p></div><h2>Employees</h2><p>Jo Li</p><p>Engineer</p><p>Jo Li</p><p>Engineer</p><p>counter<em>example</em></p><p>left</p><p>right</p><script>{unclosed`)
	var text []string
	for _, e := range r.Entries {
		if e.Text != "" {
			text = append(text, e.Text)
		}
		if e.Text == "Jo Li" && len(e.PrecedingHeadings) != 1 {
			t.Fatal("stale same-level heading was retained")
		}
	}
	if strings.Join(text, "|") != "Board|Alex Quinn|Employees|Jo Li|Engineer|Jo Li|Engineer|counterexample|left|right" {
		t.Fatal(text)
	}
	last := r.Entries[len(r.Entries)-1]
	if last.Kind != "excluded" || last.Issue != "unclosed_region" {
		t.Fatal("truncated script disappeared")
	}
}

func TestInputAndResourceRejection(t *testing.T) {
	for name, body := range map[string]string{
		"empty": "", "NUL": "<p>\x00</p>", "encoding": "<p>\xff</p>",
		"charset":      `<meta charset="windows-1252">`,
		"http_charset": `<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">`,
		"bytes":        strings.Repeat("x", MaxBody+1),
		"tokens":       strings.Repeat("<a></a>", maxTokens),
		"entries":      strings.Repeat("<p>x</p>", maxEntries+1),
		"depth":        strings.Repeat("<template>", 130),
	} {
		t.Run(name, func(t *testing.T) {
			raw := []byte(body)
			if r, err := Extract(context.Background(), raw, Source{"https://example.org/", "2026-09-15", digest(raw)}); err == nil || r.Contract != "" {
				t.Fatal("invalid input produced evidence")
			}
		})
	}
	raw := []byte("<p>text</p>")
	for _, source := range []Source{
		{"https://example.org/", "2026-09-15", strings.Repeat("a", 64)},
		{"https://user:secret@example.org/", "2026-09-15", digest(raw)},
		{"https://example.org/?token=secret", "2026-09-15", digest(raw)},
		{"file:///tmp/page", "2026-09-15", digest(raw)},
		{"https://example.org/", "2026-02-30", digest(raw)},
	} {
		if _, err := Extract(context.Background(), raw, source); err == nil {
			t.Fatal("bad source metadata accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Extract(ctx, raw, Source{"https://example.org/", "2026-09-15", digest(raw)}); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestDuplicateAttributesRetainOriginalBytes(t *testing.T) {
	body := `<meta NAME="description" content="first" CONTENT="second">`
	r := extractTest(t, body)
	if len(r.Entries) != 1 || r.Entries[0].Span.End != len(body) || len(r.Entries[0].Attributes) != 2 || r.Entries[0].Attributes[1].Value != "first" {
		t.Fatal("HTML5 first-attribute view or original tag binding changed")
	}
}

func TestRetainedCompanyPages(t *testing.T) {
	for _, tc := range []struct {
		file, pin, url string
		want           []string
	}{
		{"jc2-about.html", "45fe693cfae5b459c92df0947f0b33efa386b56fc68fa160dd3c5c6332e3e391", "https://www.jc2ventures.com/about", []string{"John Chambers is the founder and CEO of JC2 Ventures.", "JOHN J. CHAMBERS", "Head of Growth", "Chairman Emeritus"}},
		{"jc2-bio.html", "9bce701416c1345470ef55d1555f7875f0d4c4449f1c3a81f549d49cab717857", "https://www.jc2ventures.com/john-chambers", []string{"Prior to founding JC2 Ventures in January 2018", "when he stepped down as CEO in 2015."}},
		{"ridgeline-leadership.html", "c91e3c5785ddf31a229ac50c6757dca476d8c5acb5cdaab234369ce892be4151", "https://ridgeline.ai/company/leadership", []string{"Board of Directors", "Dave Duffield", "Founder and Chairman"}},
	} {
		t.Run(tc.file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("../../../tests/fixtures/person-affiliation", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			source := Source{tc.url, "2026-09-15", tc.pin}
			r, err := Extract(context.Background(), raw, source)
			if err != nil {
				t.Fatal(err)
			}
			verifySpans(t, raw, r)
			for _, want := range tc.want {
				found := false
				for _, e := range r.Entries {
					if strings.Contains(e.Text, want) {
						found = true
					}
				}
				if !found {
					t.Error("missing source text:", want)
				}
			}
			second, err := Extract(context.Background(), raw, source)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := json.Marshal(r)
			b, _ := json.Marshal(second)
			if !bytes.Equal(a, b) {
				t.Fatal("replay changed")
			}
			t.Logf("%d source bytes; %d evidence entries; result SHA-256 %s", len(raw), len(r.Entries), digest(a))
		})
	}
}

func FuzzPinnedHTML(f *testing.F) {
	f.Add([]byte(`<p>A <em>person</em></p><script type="application/ld+json">{}</script>`))
	f.Add([]byte(`<h2>Board</h2><div><h3>Name</h3><p>Title</p></div>`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		r, err := Extract(context.Background(), raw, Source{"https://example.org/", "2026-09-15", digest(raw)})
		if err == nil {
			verifySpans(t, raw, r)
		}
	})
}
