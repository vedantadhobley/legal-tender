package wikimedia

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type CaptureOptions struct {
	Directory, UserAgent, BuildSHA256 string
}

// Capture makes at most two serial requests per query. Errors are retained, not
// retried or translated into empty search results. There is no mutable pointer.
func Capture(ctx context.Context, queries []byte, o CaptureOptions) (string, error) {
	client, closeClient := captureClient()
	defer closeClient()
	return capture(ctx, queries, o, client, time.Second)
}

func captureClient() (*http.Client, func()) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 128 << 10
	transport.MaxConnsPerHost = 1
	transport.ResponseHeaderTimeout = 20 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return client, transport.CloseIdleConnections
}

func capture(ctx context.Context, raw []byte, o CaptureOptions, client *http.Client, spacing time.Duration) (string, error) {
	var queries Queries
	if err := strictjson.Decode(raw, &queries); err != nil {
		return "", err
	}
	if err := queries.Validate(); err != nil {
		return "", err
	}
	texts := make([]string, len(queries.Queries))
	for i, q := range queries.Queries {
		texts[i] = q.Text
	}
	return captureSearches(ctx, raw, o, client, spacing, Contract, texts)
}

// Only validated, versioned planners call this shared transport. It neither
// interprets names nor changes the existing background request/backoff policy.
func captureSearches(ctx context.Context, raw []byte, o CaptureOptions, client *http.Client, spacing time.Duration, contract string, texts []string) (string, error) {
	if len(raw) > MaxBody || !Digest(o.BuildSHA256) || o.Directory == "" || len(o.UserAgent) < 10 || len(o.UserAgent) > 256 || strings.ContainsAny(o.UserAgent, "\r\n") {
		return "", fmt.Errorf("capture requires bounded inputs, build digest, new directory and descriptive user agent")
	}
	if err := os.Mkdir(o.Directory, 0700); err != nil {
		return "", err
	}
	if err := writeNew(filepath.Join(o.Directory, "queries.json"), raw); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	m := Manifest{Contract: contract, BuildSHA256: o.BuildSHA256, UserAgent: o.UserAgent, QueriesSHA256: Hash(raw), Entries: []Entry{}}
	used := 0
	var last time.Time
	stopped := false
	fetch := func(url string) (Response, error) {
		r := Response{URL: url, Headers: map[string]string{}}
		var body []byte
		if stopped || used >= MaxCapture {
			r.Failure = "capture_stopped_or_byte_budget"
		} else {
			if wait := spacing - time.Since(last); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
				case <-timer.C:
				}
			}
			if ctx.Err() != nil {
				r.Failure = "capture_deadline_or_cancelled"
			} else {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				if err != nil {
					return r, err
				}
				req.Header.Set("User-Agent", o.UserAgent)
				req.Header.Set("Accept", "application/json")
				req.Header.Set("Accept-Encoding", "identity")
				r.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
				resp, err := client.Do(req)
				last = time.Now()
				if err != nil {
					r.Failure = "transport_failure"
				} else {
					r.Status = resp.StatusCode
					for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Length", "ETag", "Last-Modified", "Date", "Retry-After"} {
						if s := resp.Header.Get(k); s != "" {
							r.Headers[k] = s
						}
					}
					limit := min(MaxBody, MaxCapture-used)
					body, err = io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
					closeErr := resp.Body.Close()
					if len(body) > limit {
						body = body[:limit]
						r.Failure = "body_budget_exceeded"
					} else if err != nil || closeErr != nil {
						r.Failure = "body_read_failure"
					} else if resp.ContentLength >= 0 && resp.ContentLength != int64(len(body)) {
						r.Failure = "content_length_mismatch"
					}
					if r.Failure == "" {
						r.Failure = responseFailure(r)
					}
				}
			}
		}
		if r.ObservedAt == "" {
			r.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		used += len(body)
		r.Bytes = len(body)
		r.SHA256 = Hash(body)
		r.Body = r.SHA256 + ".body"
		if err := writeBody(o.Directory, r.Body, body); err != nil {
			return r, err
		}
		if r.Failure != "" {
			stopped = true
		}
		return r, nil
	}
	for i, text := range texts {
		e := Entry{Query: i}
		var err error
		e.Search, err = fetch(SearchURL(text))
		if err != nil {
			return "", err
		}
		if e.Search.Failure == "" {
			body, err := os.ReadFile(filepath.Join(o.Directory, e.Search.Body))
			if err != nil {
				return "", err
			}
			pages, err := ParseSearch(body)
			if err != nil {
				stopped = true
			}
			if err == nil {
				if ids := PageIDs(pages); len(ids) > 0 {
					r, err := fetch(EntityURL(ids))
					if err != nil {
						return "", err
					}
					e.Entities = &r
					if r.Failure == "" {
						entityBody, e := os.ReadFile(filepath.Join(o.Directory, r.Body))
						if e != nil {
							return "", e
						}
						if _, e = ParseEntities(entityBody, ids); e != nil {
							stopped = true
						}
					}
				}
			}
		}
		m.Entries = append(m.Entries, e)
		// Preserve completed attempt metadata even if a later write/process fails.
		b, err := json.Marshal(e)
		if err != nil {
			return "", err
		}
		if err = writeNew(filepath.Join(o.Directory, fmt.Sprintf("attempt-%02d.json", i)), b); err != nil {
			return "", err
		}
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	if err = writeNew(filepath.Join(o.Directory, "capture.json"), b); err != nil {
		return "", err
	}
	return Hash(b), nil
}

func responseFailure(r Response) string {
	if r.Status != http.StatusOK {
		return "http_status_not_ok"
	}
	media, params, err := mime.ParseMediaType(r.Headers["Content-Type"])
	if err != nil || media != "application/json" || (params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8")) {
		return "unexpected_content_type"
	}
	if e := r.Headers["Content-Encoding"]; e != "" && e != "identity" {
		return "unexpected_content_encoding"
	}
	return ""
}

func writeNew(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
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

func writeBody(dir, name string, b []byte) error {
	err := writeNew(filepath.Join(dir, name), b)
	if os.IsExist(err) {
		previous, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			return e
		}
		if Hash(previous) == Hash(b) {
			return nil
		}
	}
	return err
}
