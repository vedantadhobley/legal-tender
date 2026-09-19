package reportscope

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

var documentURL = regexp.MustCompile(`^https://docquery\.fec\.gov/(paper|dcdev)/posted/([1-9][0-9]{0,9})\.fec$`)
var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var statusPattern = regexp.MustCompile(`^HTTP/(?:1\.[01]|[23](?:\.0)?) (200|206)(?: |$)`)
var rangePattern = regexp.MustCompile(`^bytes 0-([0-9]+)/([0-9]+)$`)

// Assess verifies all requested evidence before returning a deterministic review.
// It never takes a metadata Review from the caller: each capture is revalidated.
// Unsupported representations remain source-located unresolved observations.
func Assess(ctx context.Context, request Request) (Assessment, error) {
	out, err := readDocument(ctx, request)
	if err != nil {
		return Assessment{}, err
	}
	out.assessPaper(documentURL.FindStringSubmatch(request.SourceURL)[1])
	if err := out.metadata(ctx, request.MetadataCaptures); err != nil {
		return Assessment{}, err
	}
	slices.Sort(out.Issues)
	out.Issues = slices.Compact(out.Issues)
	return out, nil
}

// Shared byte verification and framing only. Representation readers remain separate.
func readDocument(ctx context.Context, request Request) (Assessment, error) {
	if err := ctx.Err(); err != nil {
		return Assessment{}, err
	}
	u := documentURL.FindStringSubmatch(request.SourceURL)
	if u == nil || len(request.MetadataCaptures) > 4 {
		return Assessment{}, errors.New("require a supported public document URL and at most four metadata captures")
	}
	body, b, err := readPinned(request.BodyPath, request.BodySHA256, MaxBodyBytes)
	if err != nil {
		return Assessment{}, fmt.Errorf("document body: %w", err)
	}
	headers, h, err := readPinned(request.HeadersPath, request.HeadersSHA256, 128<<10)
	if err != nil {
		return Assessment{}, fmt.Errorf("document headers: %w", err)
	}
	out := Assessment{Version: Version, SourceURL: request.SourceURL, FileNumber: u[2],
		Body: b, Headers: h, Representation: "unsupported", Disposition: "unresolved",
		Basis: "retained_machine_representation_only", Records: []Record{}, Metadata: []Assertion{}, MetadataInputs: []MetadataInput{},
		Differences: []Difference{}, Issues: []string{}}
	if err := out.transport(headers, len(body)); err != nil {
		return Assessment{}, err
	}
	if err := out.records(ctx, body); err != nil {
		return Assessment{}, err
	}
	return out, nil
}

func readPinned(path, expected string, cap int64) ([]byte, reportmetadata.Artifact, error) {
	var a reportmetadata.Artifact
	if !hashPattern.MatchString(expected) {
		return nil, a, errors.New("require a lowercase SHA-256 pin")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, a, errors.New("cannot open pinned artifact")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > cap {
		return nil, a, errors.New("invalid artifact type or size budget")
	}
	b, err := io.ReadAll(io.LimitReader(f, cap+1))
	if err != nil || len(b) == 0 || int64(len(b)) > cap || digest(b) != expected {
		return nil, a, errors.New("artifact byte identity mismatch")
	}
	return b, reportmetadata.Artifact{Path: path, SHA256: expected, Bytes: int64(len(b))}, nil
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func (a *Assessment) transport(raw []byte, bodyBytes int) error {
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
	status := statusPattern.FindStringSubmatch(lines[0])
	if status == nil {
		return errors.New("require one HTTP 200 or 206 response")
	}
	h := http.Header{}
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
		if !ok || name == "" || strings.TrimSpace(name) != name || strings.ContainsAny(line, "\r\x00") {
			return errors.New("invalid response header")
		}
		h.Add(textproto.CanonicalMIMEHeaderKey(name), strings.TrimSpace(value))
	}
	for _, field := range []string{"Content-Type", "Content-Length", "Date"} {
		if !ended || len(h.Values(field)) != 1 {
			return errors.New("missing or repeated required response header")
		}
	}
	media, _, err := mime.ParseMediaType(h.Get("Content-Type"))
	if err != nil || (media != "binary/octet-stream" && media != "application/octet-stream" && media != "text/plain") {
		return errors.New("unsupported document media type")
	}
	if h.Get("Content-Length") != strconv.Itoa(bodyBytes) || len(h.Values("Content-Encoding")) != 0 || len(h.Values("Transfer-Encoding")) != 0 {
		return errors.New("unsupported or mismatched response framing")
	}
	date, err := http.ParseTime(h.Get("Date"))
	if err != nil {
		return errors.New("invalid HTTP date")
	}
	a.HTTPDate = date.UTC().Format("2006-01-02T15:04:05Z")
	a.PublisherBytes = int64(bodyBytes)
	a.CaptureExtent = "complete_response"
	if status[1] == "200" {
		if len(h.Values("Content-Range")) != 0 {
			return errors.New("range assertion on complete response")
		}
		return nil
	}
	r := rangePattern.FindStringSubmatch(h.Get("Content-Range"))
	if len(h.Values("Content-Range")) != 1 || r == nil {
		return errors.New("require an exact zero-origin prefix range")
	}
	end, e1 := strconv.ParseInt(r[1], 10, 64)
	total, e2 := strconv.ParseInt(r[2], 10, 64)
	if e1 != nil || e2 != nil || end != int64(bodyBytes)-1 || total <= int64(bodyBytes) {
		return errors.New("inconsistent prefix range")
	}
	a.CaptureExtent = "prefix"
	a.PublisherBytes = total
	a.Issues = append(a.Issues, "partial_document_capture")
	return nil
}

func (a *Assessment) records(ctx context.Context, b []byte) error {
	for offset := 0; offset < len(b); {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(a.Records) >= MaxRecords {
			return errors.New("record count budget exceeded")
		}
		n := bytes.IndexByte(b[offset:], '\n')
		complete := n >= 0 || a.CaptureExtent == "complete_response"
		if n < 0 {
			n = len(b) - offset
		} else {
			n++
		}
		if n > 64<<10 {
			return errors.New("record byte budget exceeded")
		}
		raw := b[offset : offset+n]
		a.Records = append(a.Records, Record{len(a.Records) + 1, offset, n, digest(raw), complete, raw})
		offset += n
	}
	return nil
}
