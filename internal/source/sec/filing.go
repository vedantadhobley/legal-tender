package sec

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const FilingContract = "sec/inline-registrant-identity@1.0.0"
const inlineNS = "http://www.xbrl.org/2013/inlineXBRL"
const instanceNS = "http://www.xbrl.org/2003/instance"
const xhtmlNS = "http://www.w3.org/1999/xhtml"

var accessionPattern = regexp.MustCompile(`^[0-9]{10}-[0-9]{2}-[0-9]{6}$`)
var documentPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,199}\.html?$`)
var deiPattern = regexp.MustCompile(`^https?://xbrl\.sec\.gov/dei/20[0-9]{2}$`)

type FilingReference struct {
	CIK       string `json:"cik"`
	Accession string `json:"accession"`
	Document  string `json:"document"`
}

func canonicalCIK(s string) string {
	if len(s) < 1 || len(s) > 10 {
		return ""
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return ""
		}
	}
	if strings.TrimLeft(s, "0") == "" {
		return ""
	}
	return strings.Repeat("0", 10-len(s)) + s
}

func (r FilingReference) Validate() error {
	if canonicalCIK(r.CIK) != r.CIK || r.CIK == "" || !accessionPattern.MatchString(r.Accession) || !documentPattern.MatchString(r.Document) || strings.Contains(r.Document, "..") {
		return fmt.Errorf("invalid SEC filing reference")
	}
	return nil
}

func (r FilingReference) URL() string {
	return "https://www.sec.gov/Archives/edgar/data/" + strings.TrimLeft(r.CIK, "0") + "/" + strings.ReplaceAll(r.Accession, "-", "") + "/" + r.Document
}

func (r FilingReference) spec() documentSpec {
	return documentSpec{FilingContract, r.URL(), "filing.body", "text/html"}
}

func CaptureFiling(ctx context.Context, o Options, r FilingReference) (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return capturePublicDocument(ctx, o, r.spec())
}

type FilingContext struct {
	ID            string `json:"id"`
	CIKText       string `json:"identifier_text"`
	Scheme        string `json:"identifier_scheme"`
	StartDate     string `json:"start_date,omitempty"`
	EndDate       string `json:"end_date,omitempty"`
	Instant       string `json:"instant,omitempty"`
	HasDimensions bool   `json:"has_dimensions"`
	StartByte     int64  `json:"start_byte"`
	EndByte       int64  `json:"end_byte"`
	Issue         string `json:"issue,omitempty"`
}

type RegistrantFact struct {
	Ordinal   int           `json:"ordinal"`
	ID        string        `json:"id"`
	Concept   string        `json:"concept"`
	Namespace string        `json:"namespace"`
	ContextID string        `json:"context_ref"`
	Text      string        `json:"text"`
	StartByte int64         `json:"start_byte"`
	EndByte   int64         `json:"end_byte"`
	SHA256    string        `json:"sha256"`
	Context   FilingContext `json:"context"`
	Issue     string        `json:"issue,omitempty"`
}

type FilingIdentity struct {
	Reference                   FilingReference  `json:"reference"`
	CaptureSHA256               string           `json:"capture_sha256"`
	Source                      Manifest         `json:"source"`
	Facts                       []RegistrantFact `json:"facts"`
	Issues                      []string         `json:"issues"`
	SourceUsable                bool             `json:"source_usable"`
	IdentityPublicationApproved bool             `json:"identity_publication_approved"`
}

func ReadFiling(directory, pin string, ref FilingReference) (FilingIdentity, error) {
	out := FilingIdentity{Reference: ref, CaptureSHA256: pin, Facts: []RegistrantFact{}, Issues: []string{}}
	if err := ref.Validate(); err != nil {
		return out, err
	}
	r, body, err := readDocument(directory, pin, ref.spec())
	if err != nil {
		return out, err
	}
	out.Source = r.Manifest
	if r.Issue != "" {
		out.Issues = append(out.Issues, r.Issue)
		return out, nil
	}
	facts, err := ParseFilingIdentity(body, ref.CIK)
	if err != nil {
		out.Issues = append(out.Issues, "source_schema_not_accepted")
		return out, nil
	}
	out.SourceUsable, out.Facts = true, facts
	return out, nil
}

type pendingContext struct {
	FilingContext
	depth, entities, identifiers, periods int
	unsupported                           bool
	leaves                                map[string]int
}

// ParseFilingIdentity reads only tagged DEI registrant names/CIKs and their
// referenced contexts. Other filing contents remain opaque source bytes. This
// is strict XML, not forgiving HTML scraping or a full XBRL processor.
func ParseFilingIdentity(body []byte, cik string) ([]RegistrantFact, error) {
	if canonicalCIK(cik) != cik || cik == "" || len(body) > MaxBody {
		return nil, fmt.Errorf("filing identity input/budget")
	}
	d := xml.NewDecoder(bytes.NewReader(body))
	d.CharsetReader = func(charset string, in io.Reader) (io.Reader, error) {
		if !strings.EqualFold(charset, "ASCII") && !strings.EqualFold(charset, "US-ASCII") {
			return nil, fmt.Errorf("unsupported filing charset")
		}
		for _, b := range body {
			if b >= 128 {
				return nil, fmt.Errorf("non-ASCII byte under ASCII declaration")
			}
		}
		return in, nil
	}
	var path []xml.Name
	namespaces := []map[string]string{{}}
	facts := []RegistrantFact{}
	active := map[int]int{} // selected fact index -> element depth
	contexts := map[string]FilingContext{}
	var ctx *pendingContext
	roots := 0
	for {
		start := d.InputOffset()
		token, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid filing XML")
		}
		switch t := token.(type) {
		case xml.Directive:
			return nil, fmt.Errorf("XML directives/DTDs are unsupported")
		case xml.StartElement:
			if len(path) == 0 {
				roots++
				if roots != 1 || t.Name != (xml.Name{Space: xhtmlNS, Local: "html"}) {
					return nil, fmt.Errorf("one XHTML filing root required")
				}
			}
			path = append(path, t.Name)
			if len(path) > 128 {
				return nil, fmt.Errorf("filing XML depth budget")
			}
			ns := namespaces[len(namespaces)-1]
			copied := false
			seen := map[xml.Name]bool{}
			attrs := map[string]string{}
			for _, a := range t.Attr {
				if seen[a.Name] {
					return nil, fmt.Errorf("duplicate filing attribute")
				}
				seen[a.Name] = true
				if a.Name.Space == "xmlns" || a.Name == (xml.Name{Local: "xmlns"}) {
					if !copied {
						clone := make(map[string]string, len(ns)+1)
						for k, v := range ns {
							clone[k] = v
						}
						ns, copied = clone, true
					}
					prefix := a.Name.Local
					if a.Name.Space == "" {
						prefix = ""
					}
					ns[prefix] = a.Value
				} else if a.Name.Space == "" {
					attrs[a.Name.Local] = a.Value
				}
			}
			namespaces = append(namespaces, ns)
			for i := range active {
				if t.Name.Space != xhtmlNS {
					facts[i].Issue = "nested_non_xhtml_content_unsupported"
				}
			}
			if t.Name == (xml.Name{Space: instanceNS, Local: "context"}) {
				if ctx != nil || attrs["id"] == "" || len(contexts) >= 50000 {
					return nil, fmt.Errorf("filing context shape/budget")
				}
				if _, exists := contexts[attrs["id"]]; exists {
					return nil, fmt.Errorf("duplicate filing context")
				}
				ctx = &pendingContext{FilingContext: FilingContext{ID: attrs["id"], StartByte: start}, depth: len(path), leaves: map[string]int{}}
			} else if ctx != nil {
				ctx.start(path, attrs)
			}
			if t.Name == (xml.Name{Space: inlineNS, Local: "nonNumeric"}) {
				parts := strings.Split(attrs["name"], ":")
				if len(parts) != 2 || !deiPattern.MatchString(ns[parts[0]]) || (parts[1] != "EntityRegistrantName" && parts[1] != "EntityCentralIndexKey") {
					continue
				}
				if len(facts) >= 1024 {
					return nil, fmt.Errorf("registrant fact budget")
				}
				f := RegistrantFact{Ordinal: len(facts) + 1, ID: attrs["id"], Concept: parts[1], Namespace: ns[parts[0]], ContextID: attrs["contextRef"], StartByte: start}
				for _, a := range t.Attr {
					if a.Name.Space == "xmlns" || a.Name == (xml.Name{Local: "xmlns"}) {
						continue
					}
					if a.Name.Space != "" || (a.Name.Local != "id" && a.Name.Local != "name" && a.Name.Local != "contextRef") {
						f.Issue = "fact_transformation_or_attribute_unsupported"
					}
				}
				active[len(facts)] = len(path)
				facts = append(facts, f)
			}
		case xml.CharData:
			if len(path) == 0 && strings.TrimSpace(string(t)) != "" {
				return nil, fmt.Errorf("text outside filing root")
			}
			for i := range active {
				if len(facts[i].Text)+len(t) > 8192 {
					return nil, fmt.Errorf("registrant text budget")
				}
				facts[i].Text += string(t)
			}
			if ctx != nil {
				ctx.text(path, string(t))
			}
		case xml.EndElement:
			for i, depth := range active {
				if depth == len(path) {
					facts[i].EndByte = d.InputOffset()
					facts[i].SHA256 = wikimedia.Hash(body[facts[i].StartByte:facts[i].EndByte])
					delete(active, i)
				}
			}
			if ctx != nil && len(path) == ctx.depth {
				ctx.EndByte = d.InputOffset()
				ctx.finish()
				contexts[ctx.ID], ctx = ctx.FilingContext, nil
			}
			path, namespaces = path[:len(path)-1], namespaces[:len(namespaces)-1]
		}
	}
	if roots != 1 || len(path) != 0 {
		return nil, fmt.Errorf("incomplete filing XML")
	}
	ids := map[string]int{}
	for _, f := range facts {
		if f.ID != "" {
			ids[f.ID]++
		}
	}
	for i := range facts {
		f := &facts[i]
		c, exists := contexts[f.ContextID]
		f.Context = c
		switch {
		case f.Issue != "":
		case !exists:
			f.Issue = "missing_context"
		case c.Issue != "":
			f.Issue = "context_unusable"
		case canonicalCIK(c.CIKText) != cik:
			f.Issue = "context_issuer_conflict"
		case c.HasDimensions:
			f.Issue = "dimensional_context_not_issuer_wide"
		case strings.TrimSpace(f.Text) == "":
			f.Issue = "empty_registrant_value"
		case f.ID != "" && ids[f.ID] != 1:
			f.Issue = "duplicate_fact_id"
		case f.Concept == "EntityCentralIndexKey" && canonicalCIK(f.Text) != cik:
			f.Issue = "reported_identifier_conflict"
		}
	}
	return facts, nil
}

func (c *pendingContext) start(path []xml.Name, attrs map[string]string) {
	rel := path[c.depth:]
	for _, n := range rel {
		if n.Space != instanceNS {
			c.HasDimensions = true
			return
		}
	}
	if len(rel) == 1 {
		switch rel[0].Local {
		case "entity":
			c.entities++
		case "period":
			c.periods++
		case "scenario":
			c.HasDimensions = true
		default:
			c.unsupported = true
		}
	} else if len(rel) == 2 {
		switch rel[0].Local + "/" + rel[1].Local {
		case "entity/identifier":
			c.identifiers++
			c.Scheme = attrs["scheme"]
		case "entity/segment":
			c.HasDimensions = true
		case "period/startDate", "period/endDate", "period/instant":
			c.leaves[rel[1].Local]++
		default:
			c.unsupported = true
		}
	} else {
		c.unsupported = true
	}
}

func (c *pendingContext) text(path []xml.Name, text string) {
	rel := path[c.depth:]
	if len(rel) != 2 || rel[0].Space != instanceNS || rel[1].Space != instanceNS {
		return
	}
	switch rel[0].Local + "/" + rel[1].Local {
	case "entity/identifier":
		c.CIKText += text
	case "period/startDate":
		c.StartDate += text
	case "period/endDate":
		c.EndDate += text
	case "period/instant":
		c.Instant += text
	}
}

func (c *pendingContext) finish() {
	if c.unsupported || c.entities != 1 || c.identifiers != 1 || c.periods != 1 || c.Scheme != "http://www.sec.gov/CIK" || canonicalCIK(c.CIKText) == "" {
		c.Issue = "unsupported_context_shape"
		return
	}
	dates := []string{c.Instant}
	if c.leaves["instant"] == 0 && c.leaves["startDate"] == 1 && c.leaves["endDate"] == 1 {
		dates = []string{c.StartDate, c.EndDate}
		if c.StartDate > c.EndDate {
			c.Issue = "invalid_context_period"
		}
	} else if c.leaves["instant"] != 1 || c.leaves["startDate"] != 0 || c.leaves["endDate"] != 0 {
		c.Issue = "unsupported_context_period"
	}
	for _, date := range dates {
		if _, err := time.Parse("2006-01-02", date); err != nil {
			c.Issue = "invalid_context_date"
		}
	}
}
