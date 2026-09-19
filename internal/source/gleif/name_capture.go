package gleif

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type NameEntry struct {
	Query    int                `json:"query_index"`
	Response wikimedia.Response `json:"response"`
}
type NameManifest struct {
	Contract    string      `json:"contract"`
	BuildSHA256 string      `json:"build_sha256"`
	UserAgent   string      `json:"user_agent"`
	Plan        NamePlan    `json:"plan"`
	Entries     []NameEntry `json:"entries"`
}
type NameObservation struct {
	Query       NameQuery          `json:"query"`
	Response    wikimedia.Response `json:"response"`
	Page        *NamePage          `json:"page,omitempty"`
	Issue       string             `json:"issue,omitempty"`
	ShapeSHA256 string             `json:"shape_sha256"`
}
type NameReplay struct {
	CaptureSHA256 string            `json:"capture_sha256"`
	Manifest      NameManifest      `json:"manifest"`
	Observations  []NameObservation `json:"observations"`
	CaptureUsable bool              `json:"capture_usable"`
}

func CaptureNames(ctx context.Context, plan NamePlan, o Options) (string, error) {
	c, closeClient := registryClient()
	defer closeClient()
	return captureNames(ctx, plan, o, c, time.Second)
}

func captureNames(ctx context.Context, plan NamePlan, o Options, c *http.Client, spacing time.Duration) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	if !wikimedia.Digest(o.BuildSHA256) || len(o.UserAgent) < 10 || len(o.UserAgent) > 256 || strings.ContainsAny(o.UserAgent, "\r\n") {
		return "", fmt.Errorf("registry name capture provenance")
	}
	if err := os.Mkdir(o.Directory, 0700); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	m := NameManifest{Contract: NameContract, BuildSHA256: o.BuildSHA256, UserAgent: o.UserAgent, Plan: plan, Entries: []NameEntry{}}
	stopped := false
	for i, q := range plan.Queries {
		if i > 0 && !stopped {
			timer := time.NewTimer(spacing)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		r := wikimedia.Response{URL: NameURL(q.Text, 1), ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Headers: map[string]string{}}
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
		} else if _, err := ParseNames(body, q.Text); err != nil {
			stopped = true
		}
		m.Entries = append(m.Entries, NameEntry{Query: i, Response: r})
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

// ReadNames verifies the capture before use. Pagination links are evidence only;
// replay has no network path and never manufactures a complete company census.
func ReadNames(directory, pin string) (NameReplay, error) {
	out := NameReplay{CaptureSHA256: pin, CaptureUsable: true, Observations: []NameObservation{}}
	if !wikimedia.Digest(pin) {
		return out, fmt.Errorf("registry name capture pin")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return out, err
	}
	defer root.Close()
	read := func(name string) ([]byte, error) {
		st, err := root.Lstat(name)
		if err != nil {
			return nil, err
		}
		if !st.Mode().IsRegular() || st.Size() > MaxBody {
			return nil, fmt.Errorf("registry capture file type/size")
		}
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, MaxBody+1))
		if len(b) > MaxBody {
			return nil, fmt.Errorf("registry capture read budget")
		}
		return b, err
	}
	b, err := read("capture.json")
	if err != nil {
		return out, err
	}
	if wikimedia.Hash(b) != pin {
		return out, fmt.Errorf("registry name manifest digest mismatch")
	}
	if err = strictjson.Decode(b, &out.Manifest); err != nil {
		return out, err
	}
	m := out.Manifest
	if m.Contract != NameContract || !wikimedia.Digest(m.BuildSHA256) || len(m.UserAgent) < 10 || len(m.UserAgent) > 256 || strings.ContainsAny(m.UserAgent, "\r\n") {
		return out, fmt.Errorf("registry name manifest contract")
	}
	if err = m.Plan.Validate(); err != nil {
		return out, err
	}
	if len(m.Entries) != len(m.Plan.Queries) {
		return out, fmt.Errorf("registry name request conservation")
	}
	stopped := false
	for i, e := range m.Entries {
		r := e.Response
		q := m.Plan.Queries[i]
		if e.Query != i || r.URL != NameURL(q.Text, 1) || r.Body != fmt.Sprintf("%02d.body", i) || r.Bytes < 0 || r.Bytes > MaxBody || !wikimedia.Digest(r.SHA256) {
			return out, fmt.Errorf("registry name request identity")
		}
		if _, err = time.Parse(time.RFC3339Nano, r.ObservedAt); err != nil {
			return out, fmt.Errorf("registry name request time")
		}
		b, err = read(r.Body)
		if err != nil {
			return out, err
		}
		if len(b) != r.Bytes || wikimedia.Hash(b) != r.SHA256 {
			return out, fmt.Errorf("registry name body digest/size")
		}
		if stopped && (r.Failure != "capture_stopped" || r.Status != 0 || len(b) != 0) {
			return out, fmt.Errorf("registry name request after stop")
		}
		if !stopped && r.Failure == "capture_stopped" {
			return out, fmt.Errorf("unexplained registry name stop")
		}
		o := NameObservation{Query: q, Response: r, Issue: r.Failure, ShapeSHA256: wikimedia.ShapeFingerprint(b)}
		if r.Failure == "" {
			if responseFailure(r) != "" {
				return out, fmt.Errorf("registry name response status/headers")
			}
			if v := r.Headers["Content-Length"]; v != "" {
				n, err := strconv.Atoi(v)
				if err != nil || n != len(b) {
					return out, fmt.Errorf("registry name content length")
				}
			}
			page, err := ParseNames(b, q.Text)
			if err != nil {
				o.Issue = err.Error()
			} else {
				o.Page = &page
			}
		}
		if o.Issue != "" {
			stopped = true
			out.CaptureUsable = false
		}
		out.Observations = append(out.Observations, o)
	}
	return out, nil
}
