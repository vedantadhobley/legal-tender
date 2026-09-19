package gleif

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type RequestSet struct {
	WikimediaCaptureSHA256 string   `json:"wikimedia_capture_sha256"`
	SelectionPolicy        string   `json:"selection_policy"`
	LEIs                   []string `json:"leis"`
}
type Entry struct {
	LEI      string             `json:"lei"`
	Response wikimedia.Response `json:"response"`
}
type Manifest struct {
	Contract    string     `json:"contract"`
	BuildSHA256 string     `json:"build_sha256"`
	UserAgent   string     `json:"user_agent"`
	Requests    RequestSet `json:"requests"`
	Entries     []Entry    `json:"entries"`
}
type Observation struct {
	LEI         string             `json:"lei"`
	Response    wikimedia.Response `json:"response"`
	Record      *Record            `json:"record,omitempty"`
	Issue       string             `json:"issue,omitempty"`
	ShapeSHA256 string             `json:"shape_sha256"`
}
type Replay struct {
	CaptureSHA256 string        `json:"capture_sha256"`
	Manifest      Manifest      `json:"manifest"`
	Observations  []Observation `json:"observations"`
}
type Options struct{ Directory, BuildSHA256, UserAgent string }

func (r RequestSet) Validate() error {
	if !wikimedia.Digest(r.WikimediaCaptureSHA256) || r.SelectionPolicy != "organization-registry-requests.v1" || r.LEIs == nil || len(r.LEIs) > MaxRecords || !slices.IsSorted(r.LEIs) {
		return fmt.Errorf("invalid registry request scope")
	}
	for i, lei := range r.LEIs {
		if !ValidLEI(lei) || (i > 0 && r.LEIs[i-1] == lei) {
			return fmt.Errorf("invalid or duplicate requested LEI")
		}
	}
	return nil
}

func Capture(ctx context.Context, requests RequestSet, o Options) (string, error) {
	c, closeClient := registryClient()
	defer closeClient()
	return capture(ctx, requests, o, c, time.Second)
}

func registryClient() (*http.Client, func()) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DisableCompression = true
	t.MaxResponseHeaderBytes = 128 << 10
	t.MaxConnsPerHost = 1
	t.ResponseHeaderTimeout = 20 * time.Second
	t.TLSHandshakeTimeout = 10 * time.Second
	c := &http.Client{Transport: t, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, t.CloseIdleConnections
}

func capture(ctx context.Context, requests RequestSet, o Options, c *http.Client, spacing time.Duration) (string, error) {
	if err := requests.Validate(); err != nil {
		return "", err
	}
	if !wikimedia.Digest(o.BuildSHA256) || len(o.UserAgent) < 10 || len(o.UserAgent) > 256 || strings.ContainsAny(o.UserAgent, "\r\n") {
		return "", fmt.Errorf("registry build and descriptive user agent required")
	}
	if err := os.Mkdir(o.Directory, 0700); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	m := Manifest{Contract: Contract, BuildSHA256: o.BuildSHA256, UserAgent: o.UserAgent, Requests: requests, Entries: []Entry{}}
	stopped := false
	for i, lei := range requests.LEIs {
		if i > 0 && !stopped {
			timer := time.NewTimer(spacing)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		r := wikimedia.Response{URL: RecordURL(lei), ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Headers: map[string]string{}}
		var body []byte
		switch {
		case stopped:
			r.Failure = "capture_stopped"
		case ctx.Err() != nil:
			r.Failure = "capture_cancelled"
		default:
			body = fetchRegistry(ctx, c, o.UserAgent, &r)
		}
		r.Bytes = len(body)
		r.SHA256 = wikimedia.Hash(body)
		r.Body = fmt.Sprintf("%02d.body", i)
		if err := writeNew(filepath.Join(o.Directory, r.Body), body); err != nil {
			return "", err
		}
		if r.Failure != "" {
			stopped = true
		} else if _, err := Parse(body, lei); err != nil {
			stopped = true
		}
		m.Entries = append(m.Entries, Entry{LEI: lei, Response: r})
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if err = writeNew(filepath.Join(o.Directory, "capture.json"), b); err != nil {
		return "", err
	}
	return wikimedia.Hash(b), nil
}

// The callers construct fixed-origin URLs; this does not accept caller endpoints.
func fetchRegistry(ctx context.Context, c *http.Client, agent string, r *wikimedia.Response) []byte {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		r.Failure = "invalid_request"
		return nil
	}
	req.Header.Set("User-Agent", agent)
	req.Header.Set("Accept", "application/vnd.api+json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.Do(req)
	if err != nil {
		r.Failure = "transport_failure"
		return nil
	}
	r.Status = resp.StatusCode
	for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Length", "ETag", "Last-Modified", "Date", "Retry-After"} {
		if v := resp.Header.Get(k); v != "" {
			r.Headers[k] = v
		}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBody+1))
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
		r.Failure = responseFailure(*r)
	}
	return body
}

func responseFailure(r wikimedia.Response) string {
	if r.Status != 200 {
		return "http_status_not_ok"
	}
	media, _, err := mime.ParseMediaType(r.Headers["Content-Type"])
	if err != nil || (media != "application/json" && media != "application/vnd.api+json") {
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

// Read reconstructs every exact-identifier request. No links are followed and
// HTTP 404 is a failed observation, not permission to erase an identity.
func Read(directory, pin string) (Replay, error) {
	var out Replay
	if !wikimedia.Digest(pin) {
		return out, fmt.Errorf("registry capture digest required")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return out, err
	}
	defer root.Close()
	read := func(name string) ([]byte, error) {
		info, err := root.Lstat(name)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() > MaxBody {
			return nil, fmt.Errorf("registry file type/size")
		}
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, MaxBody+1))
		if err != nil {
			return nil, err
		}
		if len(b) > MaxBody {
			return nil, fmt.Errorf("registry read budget")
		}
		return b, nil
	}
	b, err := read("capture.json")
	if err != nil {
		return out, err
	}
	if wikimedia.Hash(b) != pin {
		return out, fmt.Errorf("registry manifest digest mismatch")
	}
	if err = strictjson.Decode(b, &out.Manifest); err != nil {
		return out, err
	}
	m := out.Manifest
	if m.Contract != Contract || !wikimedia.Digest(m.BuildSHA256) || len(m.UserAgent) < 10 || len(m.UserAgent) > 256 || strings.ContainsAny(m.UserAgent, "\r\n") {
		return out, fmt.Errorf("registry manifest contract")
	}
	if err = m.Requests.Validate(); err != nil {
		return out, err
	}
	if len(m.Entries) != len(m.Requests.LEIs) {
		return out, fmt.Errorf("registry request conservation")
	}
	out.CaptureSHA256 = pin
	out.Observations = []Observation{}
	stopped := false
	for i, e := range m.Entries {
		r := e.Response
		if e.LEI != m.Requests.LEIs[i] || r.URL != RecordURL(e.LEI) || r.Body != fmt.Sprintf("%02d.body", i) || r.Bytes < 0 || r.Bytes > MaxBody || !wikimedia.Digest(r.SHA256) {
			return out, fmt.Errorf("registry request/locator mismatch")
		}
		if _, err = time.Parse(time.RFC3339Nano, r.ObservedAt); err != nil {
			return out, fmt.Errorf("registry observation time")
		}
		b, err = read(r.Body)
		if err != nil {
			return out, err
		}
		if len(b) != r.Bytes || wikimedia.Hash(b) != r.SHA256 {
			return out, fmt.Errorf("registry body digest/size mismatch")
		}
		if stopped && (r.Failure != "capture_stopped" || r.Status != 0 || len(b) != 0) {
			return out, fmt.Errorf("registry request after stop")
		}
		if !stopped && r.Failure == "capture_stopped" {
			return out, fmt.Errorf("unexplained registry stop")
		}
		o := Observation{LEI: e.LEI, Response: r, Issue: r.Failure, ShapeSHA256: wikimedia.ShapeFingerprint(b)}
		if r.Failure == "" {
			if responseFailure(r) != "" {
				return out, fmt.Errorf("registry response status/headers")
			}
			if v := r.Headers["Content-Length"]; v != "" {
				n, err := strconv.Atoi(v)
				if err != nil || n != len(b) {
					return out, fmt.Errorf("registry content length")
				}
			}
			record, err := Parse(b, e.LEI)
			if err != nil {
				o.Issue = err.Error()
			} else {
				o.Record = &record
			}
		}
		if o.Issue != "" {
			stopped = true
		}
		out.Observations = append(out.Observations, o)
	}
	return out, nil
}
