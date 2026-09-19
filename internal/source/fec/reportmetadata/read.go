package reportmetadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

var committeePattern = regexp.MustCompile(`^C[0-9]{8}$`)

// ReadCapture verifies a local capture descriptor and all referenced bytes.
// os.Root prevents relative paths and symlinks from escaping the capture root.
// The caller retains source files, including rejected bytes; this reader writes nothing.
func ReadCapture(ctx context.Context, path string) (Review, error) {
	var c Capture
	f, err := os.Open(path)
	if err != nil {
		return Review{}, errors.New("cannot open capture descriptor")
	}
	raw, err := io.ReadAll(io.LimitReader(f, (256<<10)+1))
	f.Close()
	if err != nil || len(raw) > 256<<10 {
		return Review{}, errors.New("capture descriptor exceeds read budget")
	}
	if err := strictJSON(raw, &c); err != nil {
		return Review{}, err
	}
	if err := validateCapture(c); err != nil {
		return Review{}, err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return Review{}, errors.New("cannot open capture root")
	}
	defer root.Close()
	out := Review{Version: ParserVersion, Contract: Contract, SchemaSHA256: SwaggerSHA256,
		CaptureSHA256: digest(raw), Endpoint: c.Endpoint, Query: c.Query,
		State: "validated_observations", PaginationState: "partial", Pages: []PageReview{},
		MissingRequestedFiles: []int64{}, Issues: []Issue{}}
	seen := map[string]bool{}
	var initial Pagination
	for i, page := range c.Pages {
		if err := ctx.Err(); err != nil {
			return Review{}, err
		}
		body, err := readArtifact(root, page.Body, MaxPageBytes)
		if err != nil {
			return Review{}, fmt.Errorf("page %d body: %w", page.Page, err)
		}
		headers, err := readArtifact(root, page.Headers, 128<<10)
		if err != nil {
			return Review{}, fmt.Errorf("page %d headers: %w", page.Page, err)
		}
		if err := validateHeaders(headers, page); err != nil {
			return Review{}, fmt.Errorf("page %d: %w", page.Page, err)
		}
		pr, issues, err := reviewPage(body, c, page)
		if err != nil {
			return Review{}, fmt.Errorf("page %d: %w", page.Page, err)
		}
		out.Issues = append(out.Issues, issues...)
		p := pr.Pagination
		if p.IsCountExact {
			expectedPages := p.Count / int64(p.PerPage)
			if p.Count%int64(p.PerPage) != 0 {
				expectedPages++
			}
			if p.Pages != expectedPages && !(p.Count == 0 && p.Pages == 1) {
				out.issue(page.Page, 0, "pagination", "inconsistent_exact_page_count")
			}
		}
		if i == 0 {
			initial = p
		} else if p.Count != initial.Count || p.IsCountExact != initial.IsCountExact || p.Pages != initial.Pages {
			out.issue(page.Page, 0, "pagination", "changing_pagination_assertions")
		}
		if len(pr.Records) == 0 && i != len(c.Pages)-1 {
			out.issue(page.Page, 0, "pagination", "pages_after_empty_response")
		}
		for _, record := range pr.Records {
			if record.FileNumber == "" {
				continue
			}
			if seen[record.FileNumber] {
				out.issue(page.Page, record.Ordinal, "file_number", "repeated_file_number")
			}
			seen[record.FileNumber] = true
		}
		out.Rows += len(pr.Records)
		out.Pages = append(out.Pages, pr)
	}
	last := out.Pages[len(out.Pages)-1]
	if initial.IsCountExact && int64(out.Rows) > initial.Count {
		out.issue(last.Capture.Page, 0, "pagination", "exact_count_exceeded")
	}
	if len(last.Records) == 0 {
		out.PaginationState = "empty_page_observed"
		if initial.IsCountExact && int64(out.Rows) != initial.Count {
			out.issue(last.Capture.Page, 0, "pagination", "exact_count_not_conserved")
		}
	} else if initial.IsCountExact && int64(out.Rows) == initial.Count {
		out.PaginationState = "exact_count_satisfied"
	}
	for _, n := range c.Query.FileNumbers {
		if !seen[strconv.FormatInt(n, 10)] {
			out.MissingRequestedFiles = append(out.MissingRequestedFiles, n)
		}
	}
	slices.Sort(out.MissingRequestedFiles)
	if len(out.Issues) > 0 {
		out.State = "blocked"
	}
	return out, nil
}

func (r *Review) issue(page, ordinal int, field, code string) {
	r.Issues = append(r.Issues, Issue{page, ordinal, field, code})
}

func validateCapture(c Capture) error {
	if c.Contract != Contract || c.SchemaSHA256 != SwaggerSHA256 || shapes[c.Endpoint] == nil {
		return errors.New("unsupported metadata contract, schema, or endpoint")
	}
	if len(c.Pages) == 0 || len(c.Pages) > MaxPages {
		return errors.New("invalid capture bounds")
	}
	if err := validateQuery(c.Endpoint, c.Query); err != nil {
		return err
	}
	var total int64
	paths := map[string]bool{}
	for i, p := range c.Pages {
		if p.Page != i+1 || (p.TimeBasis != "client_clock" && p.TimeBasis != "http_date") {
			return errors.New("noncontiguous pages or unknown observation time basis")
		}
		if _, err := time.Parse(time.RFC3339Nano, p.ObservedAt); err != nil {
			return errors.New("invalid observation timestamp")
		}
		for _, a := range []Artifact{p.Body, p.Headers} {
			if !fs.ValidPath(a.Path) || paths[a.Path] || len(a.SHA256) != 64 || a.Bytes <= 0 || a.Bytes > MaxPageBytes {
				return errors.New("invalid artifact descriptor")
			}
			if _, err := hex.DecodeString(a.SHA256); err != nil || strings.ToLower(a.SHA256) != a.SHA256 {
				return errors.New("invalid artifact digest")
			}
			paths[a.Path] = true
			total += a.Bytes
		}
	}
	if total > MaxCaptureBytes {
		return errors.New("capture exceeds total byte budget")
	}
	return nil
}

func validateQuery(endpoint string, q Query) error {
	if shapes[endpoint] == nil || q.PerPage < 1 || q.PerPage > 100 {
		return errors.New("unsupported metadata endpoint or page size")
	}
	if len(q.FileNumbers) > 0 {
		if endpoint != "/v1/filings/" || len(q.FileNumbers) > 100 || q.CommitteeID != "" || q.Cycle != 0 {
			return errors.New("invalid exact-file scope")
		}
		seen := map[int64]bool{}
		for _, n := range q.FileNumbers {
			if n <= 0 || seen[n] {
				return errors.New("invalid or repeated requested file number")
			}
			seen[n] = true
		}
	} else if !committeePattern.MatchString(q.CommitteeID) || q.Cycle < 1976 || q.Cycle%2 != 0 || q.Cycle > 9998 {
		return errors.New("require an exact committee and two-year cycle")
	}
	return nil
}

func readArtifact(root *os.Root, a Artifact, limit int64) ([]byte, error) {
	if a.Bytes > limit {
		return nil, errors.New("artifact exceeds byte budget")
	}
	f, err := root.Open(a.Path)
	if err != nil {
		return nil, errors.New("cannot open scoped artifact")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != a.Bytes {
		return nil, errors.New("artifact size or file type mismatch")
	}
	b, err := io.ReadAll(io.LimitReader(f, a.Bytes+1))
	if err != nil || int64(len(b)) != a.Bytes || digest(b) != a.SHA256 {
		return nil, errors.New("artifact byte identity mismatch")
	}
	return b, nil
}

func validateHeaders(raw []byte, p PageCapture) error {
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
	if len(lines) < 3 || !regexp.MustCompile(`^HTTP/(1\.[01]|[23](\.0)?) 200( |$)`).MatchString(lines[0]) {
		return errors.New("require one successful HTTP response header block")
	}
	h := map[string][]string{}
	ended := false
	for _, line := range lines[1:] {
		if line == "" {
			ended = true
			continue
		}
		if ended {
			return errors.New("multiple or incomplete response header blocks")
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok || name == "" || strings.TrimSpace(name) != name {
			return errors.New("invalid response header")
		}
		h[strings.ToLower(name)] = append(h[strings.ToLower(name)], strings.TrimSpace(value))
	}
	if !ended || len(h["content-type"]) != 1 {
		return errors.New("missing or repeated content type")
	}
	media, _, err := mime.ParseMediaType(h["content-type"][0])
	if err != nil || media != "application/json" {
		return errors.New("unexpected response media type")
	}
	if lengths := h["content-length"]; len(lengths) > 0 {
		if len(lengths) != 1 || lengths[0] != strconv.FormatInt(p.Body.Bytes, 10) {
			return errors.New("response content length mismatch")
		}
	}
	if p.TimeBasis == "http_date" {
		if len(h["date"]) != 1 {
			return errors.New("missing or repeated HTTP date")
		}
		d, err := http.ParseTime(h["date"][0])
		observed, _ := time.Parse(time.RFC3339Nano, p.ObservedAt)
		if err != nil || !observed.Equal(d) {
			return errors.New("HTTP date does not match declared time evidence")
		}
	}
	return nil
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func reviewPage(body []byte, capture Capture, page PageCapture) (PageReview, []Issue, error) {
	var envelope struct {
		APIVersion string          `json:"api_version"`
		Pagination json.RawMessage `json:"pagination"`
		Results    json.RawMessage `json:"results"`
	}
	if err := strictJSON(body, &envelope); err != nil {
		return PageReview{}, nil, err
	}
	var p Pagination
	var paginationFields map[string]json.RawMessage
	if strictJSON(envelope.Pagination, &p) != nil || json.Unmarshal(envelope.Pagination, &paginationFields) != nil || len(paginationFields) != 5 || envelope.APIVersion != "1.0" {
		return PageReview{}, nil, errors.New("unsupported API envelope")
	}
	for _, v := range paginationFields {
		if bytes.Equal(v, []byte("null")) {
			return PageReview{}, nil, errors.New("null pagination field")
		}
	}
	var rows []json.RawMessage
	if json.Unmarshal(envelope.Results, &rows) != nil || rows == nil || p.Page != page.Page || p.PerPage != capture.Query.PerPage || p.Count < 0 || p.Pages < 0 || len(rows) > p.PerPage {
		return PageReview{}, nil, errors.New("invalid result/pagination bounds")
	}
	out := PageReview{Capture: page, Pagination: p, Records: []Record{}}
	issues := []Issue{}
	add := func(i int, field, code string) { issues = append(issues, Issue{page.Page, i, field, code}) }
	shape := shapes[capture.Endpoint]
	for i, raw := range rows {
		r := Record{Ordinal: i + 1, SHA256: digest(raw), Raw: raw}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields == nil {
			add(i+1, "", "nonobject_record")
			out.Records = append(out.Records, r)
			continue
		}
		// Sorted field traversal makes error evidence reproducible.
		keys := make([]string, 0, len(fields)+len(shape))
		for k := range fields {
			keys = append(keys, k)
		}
		for k := range shape {
			if _, ok := fields[k]; !ok {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, key := range keys {
			v, exists := fields[key]
			kind, known := shape[key]
			switch {
			case !exists:
				add(i+1, key, "missing_field")
			case !known:
				add(i+1, key, "unreviewed_field")
			case !matches(v, kind):
				add(i+1, key, "unreviewed_type")
			}
		}
		var number int64
		if json.Unmarshal(fields["file_number"], &number) == nil && number > 0 {
			r.FileNumber = strconv.FormatInt(number, 10)
		} else {
			add(i+1, "file_number", "invalid_file_number")
		}
		var committee string
		if json.Unmarshal(fields["committee_id"], &committee) == nil && committeePattern.MatchString(committee) {
			r.CommitteeID = committee
		} else {
			add(i+1, "committee_id", "invalid_committee_id")
		}
		q := capture.Query
		if len(q.FileNumbers) > 0 && !slices.Contains(q.FileNumbers, number) {
			add(i+1, "file_number", "outside_request_scope")
		}
		if q.CommitteeID != "" {
			var cycle int
			if committee != q.CommitteeID {
				add(i+1, "committee_id", "outside_request_scope")
			}
			if json.Unmarshal(fields["cycle"], &cycle) != nil || cycle != q.Cycle {
				add(i+1, "cycle", "outside_request_scope")
			}
		}
		out.Records = append(out.Records, r)
	}
	return out, issues, nil
}

func matches(raw json.RawMessage, kind string) bool {
	if bytes.Equal(raw, []byte("null")) {
		return true
	}
	if strings.HasSuffix(kind, "[]") {
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil || items == nil {
			return false
		}
		for _, item := range items {
			if bytes.Equal(item, []byte("null")) || !matches(item, strings.TrimSuffix(kind, "[]")) {
				return false
			}
		}
		return true
	}
	for _, alternative := range strings.Split(kind, "|") {
		switch alternative {
		case "string":
			if len(raw) > 0 && raw[0] == '"' {
				return true
			}
		case "boolean":
			if bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false")) {
				return true
			}
		case "number", "integer":
			if len(raw) == 0 || (raw[0] != '-' && (raw[0] < '0' || raw[0] > '9')) {
				continue
			}
			if alternative == "number" {
				return true
			}
			// Source integer slots must be integral JSON lexemes. No float conversion.
			if !bytes.ContainsAny(raw, ".eE") {
				return true
			}
		}
	}
	return false
}
