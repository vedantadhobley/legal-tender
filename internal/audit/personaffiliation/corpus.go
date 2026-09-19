// Package personaffiliation replays a small reviewed corpus. Its annotations are
// test evidence, never a production person directory or automatic prose parser.
package personaffiliation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/identityassertions"
	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const Version = "person-affiliation-corpus.v1"
const layoutPin = "dd15056f52cb49d947874a14f2a83b47deac02ebb5851f9f8478990ae921575f"
const maxArtifact = 4 << 20

type Artifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}

type Source struct {
	ID         string   `json:"id"`
	URL        string   `json:"url"`
	ObservedOn string   `json:"observed_on"`
	Body       Artifact `json:"body"`
}

// RoleAnnotation is a human-reviewed interpretation, not an extracted fact.
// Person IDs are corpus-local source subjects; they are not resolved donor IDs.
type RoleAnnotation struct {
	ID       string       `json:"id"`
	SourceID string       `json:"source_id"`
	Excerpts []string     `json:"excerpts"`
	Reason   string       `json:"reason"`
	Claim    screen.Claim `json:"claim"`
}

type Case struct {
	ID      string   `json:"id"`
	Ordinal int      `json:"ordinal"`
	RoleIDs []string `json:"role_ids"`
	Review  string   `json:"review"`
}

type Corpus struct {
	Version string           `json:"version"`
	Scope   string           `json:"scope"`
	Layout  Artifact         `json:"layout"`
	Filing  Source           `json:"filing"`
	Headers Artifact         `json:"headers"`
	Sources []Source         `json:"sources"`
	Roles   []RoleAnnotation `json:"reviewed_roles"`
	Cases   []Case           `json:"cases"`
}

type CaseResult struct {
	ID            string        `json:"id"`
	TransactionID string        `json:"as_filed_transaction_id"`
	RawDate       string        `json:"as_filed_date"`
	Review        string        `json:"review_annotation"`
	Assessment    screen.Result `json:"assessment"`
}

type Result struct {
	Version      string       `json:"version"`
	CorpusSHA256 string       `json:"corpus_sha256"`
	Scope        string       `json:"scope"`
	Cases        []CaseResult `json:"cases"`
}

// Run verifies pinned local files and exact excerpt spans, then calls the real
// screening evaluator. It does not use review labels to change its decisions.
// This verifies byte provenance, not the truth of the reviewer's interpretation.
func Run(ctx context.Context, directory, corpusPin string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	raw, err := read(directory, Artifact{Path: "corpus.json", SHA256: corpusPin}, 1<<20)
	if err != nil {
		return Result{}, err
	}
	var c Corpus
	if err := strictjson.Decode(raw, &c); err != nil {
		return Result{}, err
	}
	if c.Version != Version || c.Scope == "" || len(c.Sources) == 0 || len(c.Sources) > 16 || len(c.Roles) > 64 || len(c.Cases) == 0 || len(c.Cases) > 64 || c.Layout.SHA256 != layoutPin {
		return Result{}, fmt.Errorf("unsupported corpus scope, layout or budget")
	}
	layout, err := read(directory, c.Layout, maxArtifact)
	if err != nil {
		return Result{}, err
	}
	var spec struct {
		Fields []struct {
			Name     string
			Sequence int
		}
	}
	if err := json.Unmarshal(layout, &spec); err != nil {
		return Result{}, err
	}
	positions := map[string]int{}
	for _, f := range spec.Fields {
		positions[f.Name] = f.Sequence - 1
	}
	if err := validateSource(c.Filing); err != nil {
		return Result{}, err
	}
	if _, err := read(directory, c.Filing.Body, maxArtifact); err != nil {
		return Result{}, err
	}
	if _, err := read(directory, c.Headers, 128<<10); err != nil {
		return Result{}, err
	}
	filing, err := reportscope.AssessElectronic(ctx, reportscope.Request{
		SourceURL: c.Filing.URL, BodyPath: filepath.Join(directory, c.Filing.Body.Path), BodySHA256: c.Filing.Body.SHA256,
		HeadersPath: filepath.Join(directory, c.Headers.Path), HeadersSHA256: c.Headers.SHA256,
	})
	if err != nil {
		return Result{}, err
	}
	if filing.Disposition != "electronic_cover_parsed" || filing.CaptureExtent != "complete_response" {
		return Result{}, fmt.Errorf("unsupported or incomplete filing")
	}
	sourceBodies := map[string][]byte{}
	sources := map[string]Source{}
	for _, s := range c.Sources {
		if err := validateSource(s); err != nil {
			return Result{}, err
		}
		if _, exists := sources[s.ID]; exists {
			return Result{}, fmt.Errorf("duplicate source")
		}
		b, err := read(directory, s.Body, maxArtifact)
		if err != nil {
			return Result{}, err
		}
		sources[s.ID], sourceBodies[s.ID] = s, b
	}
	claims := map[string]screen.Claim{}
	for _, a := range c.Roles {
		s, found := sources[a.SourceID]
		if _, duplicate := claims[a.ID]; duplicate || !found || a.ID == "" || a.Reason == "" || len(a.Excerpts) == 0 || len(a.Excerpts) > 8 || a.Claim.Source != (screen.Reference{}) || !strings.HasPrefix(a.Claim.PersonID, "review:") || !strings.HasPrefix(a.Claim.OrganizationID, "review:") {
			return Result{}, fmt.Errorf("invalid or duplicate reviewed role")
		}
		spans := []string{}
		for _, excerpt := range a.Excerpts {
			b := sourceBodies[a.SourceID]
			if excerpt == "" || bytes.Count(b, []byte(excerpt)) != 1 {
				return Result{}, fmt.Errorf("role excerpt missing or nonunique: %s", a.ID)
			}
			start := bytes.Index(b, []byte(excerpt))
			spans = append(spans, fmt.Sprintf("bytes[%d,%d)", start, start+len(excerpt)))
		}
		claim := a.Claim
		claim.Source = screen.Reference{SHA256: s.Body.SHA256, Locator: strings.Join(spans, ";") + ";review=" + a.ID}
		claims[a.ID] = claim
	}
	out := Result{Version: Version, CorpusSHA256: corpusPin, Scope: c.Scope, Cases: []CaseResult{}}
	caseIDs, ordinals := map[string]bool{}, map[int]bool{}
	usedClaims := map[string]bool{}
	for _, tc := range c.Cases {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if tc.ID == "" || tc.Review == "" || caseIDs[tc.ID] || ordinals[tc.Ordinal] || tc.Ordinal < 3 || tc.Ordinal > len(filing.Records) || len(tc.RoleIDs) > 64 {
			return Result{}, fmt.Errorf("invalid or duplicate case")
		}
		caseIDs[tc.ID], ordinals[tc.Ordinal] = true, true
		rec := filing.Records[tc.Ordinal-1]
		f, err := selectedFields(rec, positions, filing.Cover)
		if err != nil {
			return Result{}, err
		}
		selected, seen := []screen.Claim{}, map[string]bool{}
		for _, id := range tc.RoleIDs {
			claim, exists := claims[id]
			if !exists || seen[id] {
				return Result{}, fmt.Errorf("missing or duplicate case role")
			}
			seen[id] = true
			usedClaims[id] = true
			selected = append(selected, claim)
		}
		a := appearance(f, rec, c.Filing.Body.SHA256)
		result, err := screen.Assess(a, selected)
		if err != nil {
			return Result{}, err
		}
		out.Cases = append(out.Cases, CaseResult{ID: tc.ID, TransactionID: f["transaction_id"], RawDate: f["contribution_date"], Review: tc.Review, Assessment: result})
	}
	if len(usedClaims) != len(claims) {
		return Result{}, fmt.Errorf("unused role annotation was not evaluated")
	}
	return out, nil
}

// Only selected complete, ASCII, exact-width 8.4 individual rows are mapped.
// Other records remain retained bytes. This is not a general e-filing adapter.
func selectedFields(rec reportscope.Record, positions map[string]int, cover *reportscope.Cover) (map[string]string, error) {
	if !rec.Complete {
		return nil, fmt.Errorf("incomplete selected occurrence")
	}
	b := bytes.TrimSuffix(bytes.TrimSuffix(rec.Raw, []byte{'\n'}), []byte{'\r'})
	for _, v := range b {
		if (v < 32 && v != 0x1c) || v > 126 {
			return nil, fmt.Errorf("unsupported selected occurrence encoding")
		}
	}
	fields := strings.Split(string(b), "\x1c")
	if len(fields) != 45 {
		return nil, fmt.Errorf("selected occurrence width")
	}
	m := map[string]string{}
	for name, pos := range positions {
		m[name] = fields[pos]
	}
	form := strings.TrimRight(cover.Form, "NAT")
	if m["entity_type"] != "IND" || m["filer_committee_id_number"] != cover.CommitteeID || !strings.HasPrefix(m["form_type"], "SA") || !slices.Contains(reportscope.ScheduleALines(form), strings.TrimPrefix(m["form_type"], "SA")) {
		return nil, fmt.Errorf("selected occurrence outside individual Schedule A profile")
	}
	if raw := m["contribution_date"]; raw != "" {
		if _, err := time.Parse("20060102", raw); err != nil || len(raw) != 8 {
			return nil, fmt.Errorf("selected occurrence has invalid as-filed date")
		}
	}
	return m, nil
}

func appearance(f map[string]string, rec reportscope.Record, sourceHash string) screen.Appearance {
	p := func(key string) *string { v := f[key]; return &v }
	parts := []string{}
	for _, k := range []string{"contributor_prefix", "contributor_first_name", "contributor_middle_name", "contributor_last_name", "contributor_suffix"} {
		if f[k] != "" {
			parts = append(parts, f[k])
		}
	}
	// The display name is derived from explicit components, not a surname-order
	// guess. Keep each original component and the raw date alongside this view.
	name, date := strings.Join(parts, " "), f["contribution_date"]
	if t, err := time.Parse("20060102", date); err == nil && len(date) == 8 {
		date = t.Format(time.DateOnly)
	}
	return screen.Appearance{Source: screen.Reference{SHA256: sourceHash, Locator: fmt.Sprintf("record:%d;bytes[%d,%d);sha256:%s", rec.Ordinal, rec.Offset, rec.Offset+rec.Bytes, rec.SHA256)}, Receipt: identityassertions.Receipt{
		Ordinal: int64(rec.Ordinal), Normalization: "audit_efile84_component_name_and_iso_day.v1", Name: &name,
		Recipient: p("filer_committee_id_number"), Entity: p("entity_type"), First: p("contributor_first_name"), Middle: p("contributor_middle_name"), Last: p("contributor_last_name"), Prefix: p("contributor_prefix"), Suffix: p("contributor_suffix"),
		Employer: p("contributor_employer"), Occupation: p("contributor_occupation"), ReceiptDate: &date,
	}}
}

func validateSource(s Source) error {
	u, err := url.Parse(s.URL)
	_, dateErr := time.Parse(time.DateOnly, s.ObservedOn)
	if err != nil || s.ID == "" || u.Scheme != "https" || u.Host == "" || u.User != nil || dateErr != nil || len(s.ObservedOn) != 10 {
		return fmt.Errorf("invalid source provenance")
	}
	return nil
}

func read(directory string, a Artifact, limit int64) ([]byte, error) {
	if a.Path == "" || a.Path == "." || filepath.Base(a.Path) != a.Path || len(a.SHA256) != 64 {
		return nil, fmt.Errorf("invalid artifact reference")
	}
	path := filepath.Join(directory, a.Path)
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > limit || (a.Bytes != 0 && int64(a.Bytes) != st.Size()) {
		return nil, fmt.Errorf("artifact unavailable or outside byte budget")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	h := sha256.Sum256(b)
	if err != nil || int64(len(b)) > limit || hex.EncodeToString(h[:]) != a.SHA256 || (a.Bytes != 0 && a.Bytes != len(b)) {
		return nil, fmt.Errorf("artifact digest/size mismatch")
	}
	return b, nil
}
