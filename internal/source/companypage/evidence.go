// Package companypage extracts lexical evidence from retained HTML. It neither
// renders a browser DOM nor interprets text as an accepted affiliation.
package companypage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
	"golang.org/x/net/html"
)

const Contract = "company/html-evidence@1.0.0"
const MaxBody = 2 << 20
const maxTokens = 100_000
const maxEntries = 10_000

type Source struct {
	URL        string `json:"url"`
	ObservedOn string `json:"observed_on"`
	SHA256     string `json:"sha256"`
}

// Span uses zero-based, half-open offsets into the original, pinned HTML bytes.
type Span struct {
	Start  int    `json:"start"`
	End    int    `json:"end"`
	SHA256 string `json:"sha256"`
}

type Attribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Entry struct {
	Kind       string      `json:"kind"`
	Tag        string      `json:"tag,omitempty"`
	Span       Span        `json:"span"`
	Text       string      `json:"text,omitempty"`
	Attributes []Attribute `json:"attributes,omitempty"`
	// PrecedingHeadings are entry indexes, not semantic ownership or role links.
	PrecedingHeadings []int  `json:"preceding_headings,omitempty"`
	Payload           *Span  `json:"payload,omitempty"`
	Raw               string `json:"raw,omitempty"`
	JSONState         string `json:"json_state,omitempty"`
	Issue             string `json:"issue,omitempty"`
}

type Evidence struct {
	Contract                 string  `json:"contract"`
	Source                   Source  `json:"source"`
	Bytes                    int     `json:"bytes"`
	Entries                  []Entry `json:"entries"`
	Interpretation           string  `json:"interpretation"`
	IdentityApproved         bool    `json:"identity_approved"`
	GraphPublicationApproved bool    `json:"graph_publication_approved"`
	FinancialAttribution     bool    `json:"financial_attribution"`
}

func digest(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func (s Source) Validate() error {
	u, err := url.Parse(s.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
		return errors.New("company page requires an explicit HTTPS source URL without credentials, query or fragment")
	}
	d, err := time.Parse("2006-01-02", s.ObservedOn)
	if err != nil || d.Format("2006-01-02") != s.ObservedOn {
		return errors.New("company page requires an observation day, not role validity")
	}
	b, err := hex.DecodeString(s.SHA256)
	if err != nil || len(b) != sha256.Size || hex.EncodeToString(b) != s.SHA256 {
		return errors.New("company page requires an exact lowercase body SHA-256")
	}
	return nil
}

// Extract has no network, matching, reviewed-label or company-selector input.
func Extract(ctx context.Context, raw []byte, source Source) (Evidence, error) {
	if err := source.Validate(); err != nil {
		return Evidence{}, err
	}
	if len(raw) == 0 || len(raw) > MaxBody || digest(raw) != source.SHA256 || !utf8.Valid(raw) || bytes.IndexByte(raw, 0) >= 0 {
		return Evidence{}, errors.New("company page body is unbounded, unpinned or not supported UTF-8")
	}
	p := extractor{raw: raw, result: Evidence{Contract: Contract, Source: source, Bytes: len(raw), Entries: []Entry{}, Interpretation: "lexical_source_evidence_only; identity_role_and_historical_validity_unassessed"}}
	z := html.NewTokenizer(bytes.NewReader(raw))
	z.SetMaxBuf(MaxBody)
	pos := 0
	for tokens := 0; ; tokens++ {
		if err := ctx.Err(); err != nil {
			return Evidence{}, err
		}
		if tokens >= maxTokens {
			return Evidence{}, errors.New("company page token budget exceeded")
		}
		tt := z.Next()
		start := pos
		pos += len(z.Raw()) // Read Raw before Token; decoding can change its buffer.
		if tt == html.ErrorToken {
			if z.Err() != io.EOF || pos != len(raw) {
				return Evidence{}, errors.New("company page tokenization failed")
			}
			if p.region != nil {
				p.endRegion(len(raw), len(raw), "unclosed_region")
			}
			p.flush()
			if len(p.result.Entries) > maxEntries {
				return Evidence{}, errors.New("company page entry budget exceeded")
			}
			return p.result, nil
		}
		if err := p.token(z.Token(), start, pos); err != nil {
			return Evidence{}, err
		}
		if len(p.result.Entries) > maxEntries {
			return Evidence{}, errors.New("company page entry budget exceeded")
		}
	}
}

type region struct {
	tag                     string
	start, bodyStart, depth int
	jsonLD                  bool
}

type extractor struct {
	raw        []byte
	result     Evidence
	text       strings.Builder
	start, end int
	kind, tag  string
	headings   []int
	region     *region
}

func (p *extractor) span(start, end int) Span {
	return Span{Start: start, End: end, SHA256: digest(p.raw[start:end])}
}

func (p *extractor) flush() {
	text := strings.Join(strings.Fields(p.text.String()), " ")
	if text != "" {
		kind := p.kind
		if kind == "" {
			kind = "text"
		}
		e := Entry{Kind: kind, Tag: p.tag, Text: text, Span: p.span(p.start, p.end), PrecedingHeadings: append([]int(nil), p.headings...)}
		if kind == "heading" {
			for len(p.headings) > 0 && p.result.Entries[p.headings[len(p.headings)-1]].Tag >= p.tag {
				p.headings = p.headings[:len(p.headings)-1]
			}
			e.PrecedingHeadings = append([]int(nil), p.headings...)
			p.headings = append(p.headings, len(p.result.Entries))
		}
		p.result.Entries = append(p.result.Entries, e)
	}
	p.text.Reset()
	p.kind, p.tag = "", ""
}

func (p *extractor) endRegion(bodyEnd, end int, issue string) {
	r := p.region
	e := Entry{Kind: "excluded", Tag: r.tag, Span: p.span(r.start, end), Issue: issue}
	if r.jsonLD {
		body := p.raw[r.bodyStart:bodyEnd]
		s := p.span(r.bodyStart, bodyEnd)
		e.Kind, e.Payload, e.Raw, e.JSONState = "json_ld", &s, string(body), "valid_json_uninterpreted"
		if strictjson.Decode(body, nil) != nil {
			e.JSONState = "invalid_json"
		}
	}
	p.result.Entries = append(p.result.Entries, e)
	p.region = nil
}

func (p *extractor) token(t html.Token, start, end int) error {
	if p.region != nil {
		r := p.region
		if t.Data == r.tag {
			if t.Type == html.StartTagToken {
				r.depth++
				if r.depth > 128 {
					return errors.New("company page excluded-region depth exceeded")
				}
			} else if t.Type == html.EndTagToken {
				r.depth--
				if r.depth == 0 {
					p.endRegion(start, end, "")
				}
			}
		}
		return nil
	}
	if t.Type == html.TextToken {
		if p.text.Len() == 0 {
			p.start = start
		}
		p.text.WriteString(t.Data)
		p.end = end
		return nil
	}
	if t.Type != html.StartTagToken && t.Type != html.SelfClosingTagToken && t.Type != html.EndTagToken {
		return nil
	}
	opening := t.Type != html.EndTagToken
	if opening {
		attrs := attributes(t)
		if excluded(t.Data) {
			p.flush()
			p.region = &region{tag: t.Data, start: start, bodyStart: end, depth: 1, jsonLD: t.Data == "script" && strings.EqualFold(strings.TrimSpace(attrs["type"]), "application/ld+json")}
			return nil
		}
		if t.Data == "meta" || (t.Data == "link" && hasWord(attrs["rel"], "canonical")) {
			p.flush()
			if t.Data == "meta" {
				if err := charset(attrs); err != nil {
					return err
				}
			}
			e := Entry{Kind: "metadata", Tag: t.Data, Span: p.span(start, end)}
			for _, a := range t.Attr {
				e.Attributes = append(e.Attributes, Attribute{Name: a.Key, Value: a.Val})
			}
			p.result.Entries = append(p.result.Entries, e)
			return nil
		}
	}
	if t.Data == "br" {
		if opening && p.text.Len() > 0 {
			p.text.WriteByte('\n')
			p.end = end
		}
		return nil
	}
	if boundary(t.Data) {
		p.flush()
		if opening {
			if heading(t.Data) {
				p.kind, p.tag = "heading", t.Data
			} else if t.Data == "title" {
				p.kind, p.tag = "title", t.Data
			}
		}
	}
	return nil
}

// The pinned HTML5 tokenizer exposes the first occurrence of duplicate
// attributes. This decoded view is not a lossless replacement for the tag span.
func attributes(t html.Token) map[string]string {
	a := make(map[string]string, len(t.Attr))
	for _, v := range t.Attr {
		a[v.Key] = v.Val
	}
	return a
}

func charset(a map[string]string) error {
	c, exists := a["charset"]
	if strings.EqualFold(strings.TrimSpace(a["http-equiv"]), "content-type") {
		_, params, err := mime.ParseMediaType(a["content"])
		if err != nil {
			return errors.New("company page has invalid content-type metadata")
		}
		if other, ok := params["charset"]; ok {
			if !utf8Charset(other) {
				return errors.New("company page declares unsupported encoding")
			}
		}
	}
	if exists && !utf8Charset(c) {
		return errors.New("company page declares unsupported encoding")
	}
	return nil
}

func utf8Charset(s string) bool { return strings.EqualFold(strings.TrimSpace(s), "utf-8") }
func heading(s string) bool     { return len(s) == 2 && s[0] == 'h' && s[1] >= '1' && s[1] <= '6' }
func hasWord(s, word string) bool {
	for _, v := range strings.Fields(s) {
		if strings.EqualFold(v, word) {
			return true
		}
	}
	return false
}

func excluded(tag string) bool {
	switch tag {
	case "script", "style", "template", "noscript", "svg", "math":
		return true
	}
	return false
}

func boundary(tag string) bool {
	if heading(tag) {
		return true
	}
	switch tag {
	case "address", "article", "aside", "blockquote", "body", "dd", "div", "dl", "dt", "fieldset", "figcaption", "figure", "footer", "form", "head", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table", "tbody", "td", "th", "thead", "tr", "ul", "title":
		return true
	}
	return false
}
