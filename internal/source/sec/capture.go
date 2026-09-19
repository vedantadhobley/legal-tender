package sec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type Manifest struct {
	Contract    string             `json:"contract"`
	BuildSHA256 string             `json:"build_sha256"`
	UserAgent   string             `json:"user_agent"`
	Response    wikimedia.Response `json:"response"`
}

type Replay struct {
	CaptureSHA256 string   `json:"capture_sha256"`
	Manifest      Manifest `json:"manifest"`
	Rows          []Row    `json:"rows"`
	Issue         string   `json:"issue,omitempty"`
}

type Options struct{ Directory, BuildSHA256, UserAgent string }

type documentSpec struct{ Contract, URL, Body, Media string }

func directorySpec() documentSpec {
	return documentSpec{Contract, DirectoryURL, "directory.body", "application/json"}
}

func validAgent(s string) bool {
	return len(s) >= 10 && len(s) <= 256 && !strings.ContainsFunc(s, unicode.IsControl)
}

// Capture makes one fixed-origin bulk request; it transmits no FEC query text.
// The operator supplies the declared user agent and reachable contact required
// by SEC fair-access guidance. No retry, redirect, auth or proxy is inherited.
func Capture(ctx context.Context, o Options) (string, error) {
	return capturePublicDocument(ctx, o, directorySpec())
}

func capturePublicDocument(ctx context.Context, o Options, spec documentSpec) (string, error) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DisableCompression = true
	t.MaxResponseHeaderBytes = 128 << 10
	t.ResponseHeaderTimeout = 20 * time.Second
	t.TLSHandshakeTimeout = 10 * time.Second
	defer t.CloseIdleConnections()
	c := &http.Client{Transport: t, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return captureDocument(ctx, o, c, spec)
}

func capture(ctx context.Context, o Options, c *http.Client) (string, error) {
	return captureDocument(ctx, o, c, directorySpec())
}

func captureDocument(ctx context.Context, o Options, c *http.Client, spec documentSpec) (string, error) {
	if !wikimedia.Digest(o.BuildSHA256) || !validAgent(o.UserAgent) {
		return "", fmt.Errorf("SEC build and declared user agent with contact required")
	}
	if err := os.Mkdir(o.Directory, 0700); err != nil {
		return "", err
	}
	r := wikimedia.Response{URL: spec.URL, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Headers: map[string]string{}, Body: spec.Body}
	var body []byte
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, spec.URL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", o.UserAgent)
	req.Header.Set("Accept", spec.Media)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.Do(req)
	if err != nil {
		r.Failure = "transport_failure"
	} else {
		r.Status = resp.StatusCode
		for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Length", "ETag", "Last-Modified", "Date", "Retry-After"} {
			if v := resp.Header.Get(k); v != "" {
				r.Headers[k] = v
			}
		}
		body, err = io.ReadAll(io.LimitReader(resp.Body, MaxBody+1))
		closeErr := resp.Body.Close()
		switch {
		case len(body) > MaxBody:
			body = body[:MaxBody]
			r.Failure = "body_budget_exceeded"
		case err != nil || closeErr != nil:
			r.Failure = "body_read_failure"
		case resp.ContentLength >= 0 && resp.ContentLength != int64(len(body)):
			r.Failure = "content_length_mismatch"
		default:
			r.Failure = responseFailureMedia(r, spec.Media)
		}
	}
	r.Bytes, r.SHA256 = len(body), wikimedia.Hash(body)
	if err = writeNew(filepath.Join(o.Directory, r.Body), body); err != nil {
		return "", err
	}
	m := Manifest{Contract: spec.Contract, BuildSHA256: o.BuildSHA256, UserAgent: o.UserAgent, Response: r}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if err = writeNew(filepath.Join(o.Directory, "capture.json"), b); err != nil {
		return "", err
	}
	return wikimedia.Hash(b), nil
}

func responseFailure(r wikimedia.Response) string {
	return responseFailureMedia(r, "application/json")
}

func responseFailureMedia(r wikimedia.Response, expected string) string {
	if r.Status != http.StatusOK {
		return "http_status_not_ok"
	}
	media, _, err := mime.ParseMediaType(r.Headers["Content-Type"])
	if err != nil || media != expected {
		return "unexpected_content_type"
	}
	if v := r.Headers["Content-Encoding"]; v != "" && v != "identity" {
		return "unexpected_encoding"
	}
	return ""
}

func writeNew(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// Read is offline. Failed HTTP/schema observations remain failures, never an
// empty valid directory or evidence that an organization does not exist.
func Read(directory, pin string) (Replay, error) {
	out, b, err := readDocument(directory, pin, directorySpec())
	if err != nil || out.Issue != "" {
		return out, err
	}
	rows, err := Parse(b)
	if err != nil {
		out.Issue = "source_schema_not_accepted"
	} else {
		out.Rows = rows
	}
	return out, nil
}

func readDocument(directory, pin string, spec documentSpec) (out Replay, body []byte, err error) {
	if !wikimedia.Digest(pin) {
		return out, nil, fmt.Errorf("SEC capture digest required")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return out, nil, err
	}
	defer root.Close()
	read := func(name string, limit int64) ([]byte, error) {
		info, err := root.Lstat(name)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() > limit {
			return nil, fmt.Errorf("SEC file type/size")
		}
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, limit+1))
		if err != nil || int64(len(b)) > limit {
			return nil, fmt.Errorf("SEC bounded file read failed")
		}
		return b, nil
	}
	b, err := read("capture.json", 1<<20)
	if err != nil {
		return out, nil, err
	}
	if wikimedia.Hash(b) != pin {
		return out, nil, fmt.Errorf("SEC manifest digest mismatch")
	}
	if err = strictjson.Decode(b, &out.Manifest); err != nil {
		return out, nil, err
	}
	m, r := out.Manifest, out.Manifest.Response
	if m.Contract != spec.Contract || !wikimedia.Digest(m.BuildSHA256) || !validAgent(m.UserAgent) || r.URL != spec.URL || r.Body != spec.Body || r.Bytes < 0 || r.Bytes > MaxBody || !wikimedia.Digest(r.SHA256) {
		return out, nil, fmt.Errorf("SEC manifest contract/locator mismatch")
	}
	if _, err = time.Parse(time.RFC3339Nano, r.ObservedAt); err != nil {
		return out, nil, fmt.Errorf("SEC observation timestamp")
	}
	b, err = read(r.Body, MaxBody)
	if err != nil {
		return out, nil, err
	}
	if len(b) != r.Bytes || wikimedia.Hash(b) != r.SHA256 {
		return out, nil, fmt.Errorf("SEC body digest/size mismatch")
	}
	out.CaptureSHA256, out.Issue, out.Rows = pin, r.Failure, []Row{}
	if r.Failure != "" {
		switch r.Failure {
		case "transport_failure", "body_budget_exceeded", "body_read_failure", "content_length_mismatch", "http_status_not_ok", "unexpected_content_type", "unexpected_encoding":
			return out, b, nil
		default:
			return out, nil, fmt.Errorf("unknown SEC response failure")
		}
	}
	if responseFailureMedia(r, spec.Media) != "" {
		return out, nil, fmt.Errorf("SEC response status/headers")
	}
	if v := r.Headers["Content-Length"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n != len(b) {
			return out, nil, fmt.Errorf("SEC response content length")
		}
	}
	return out, b, nil
}
