package sec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/directory.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDirectoryGrainAndStrictShape(t *testing.T) {
	raw := fixture(t)
	rows, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 || rows[0].CIK != "0000000100" || rows[0].Source.CIK != 100 || rows[0].Source.Ticker == rows[1].Source.Ticker || rows[0].CIK != rows[1].CIK || rows[3].Key != "10" {
		t.Fatal("source row/identifier conservation", rows)
	}
	for _, bad := range []string{
		`null`, `{}`, `[]`, `{"0":null}`, `{"01":{"cik_str":1,"title":"X","ticker":"X"}}`,
		strings.Replace(string(raw), `"cik_str": 100`, `"cik_str": "100"`, 1),
		strings.Replace(string(raw), `"cik_str": 100`, `"cik_str": 1.0`, 1),
		strings.Replace(string(raw), `"cik_str": 100`, `"cik_str": 1e2`, 1),
		strings.Replace(string(raw), `"cik_str": 100`, `"cik_str": 0`, 1),
		strings.Replace(string(raw), `"cik_str": 100`, `"cik_str": 10000000000`, 1),
		strings.Replace(string(raw), `"cik_str": 100`, `"cik_str": null`, 1),
		strings.Replace(string(raw), `"cik_str": 100`, `"CIK_STR": 100`, 1),
		strings.Replace(string(raw), `"ticker": "EXA"`, `"ticker": null`, 1),
		strings.Replace(string(raw), `"title": "EXAMPLE CORP"`, `"title": " "`, 1),
		strings.Replace(string(raw), `"title": "EXAMPLE CORP"`, `"title": "X","extra":true`, 1),
		strings.Replace(string(raw), `"ticker": "EXA"`, `"ticker": "X","ticker":"X"`, 1),
		strings.Replace(string(raw), `"1":`, `"0":`, 1),
		strings.Replace(string(raw), `EXAMPLE CORP`, `\ud800`, 1),
		strings.Replace(string(raw), `EXAMPLE CORP`, "\xff", 1),
		string(raw) + "{}",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatal("unreviewed directory shape accepted")
		}
	}
	if _, err := Parse(bytes.Repeat([]byte("x"), MaxBody+1)); err == nil {
		t.Fatal("byte budget")
	}
	var many bytes.Buffer
	many.WriteByte('{')
	for i := range MaxRows + 1 {
		if i != 0 {
			many.WriteByte(',')
		}
		fmt.Fprintf(&many, `"%d":{"cik_str":1,"ticker":"X","title":"X"}`, i)
	}
	many.WriteByte('}')
	if _, err := Parse(many.Bytes()); err == nil {
		t.Fatal("row budget")
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCaptureRequiresDeclaredOptionsBeforeRequest(t *testing.T) {
	c := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
		t.Fatal("request before validating capture options")
		return nil, nil
	})}
	for _, agent := range []string{"", "short", "LegalTender\r\nAuthorization: x", "LegalTender\x00contact", strings.Repeat("x", 257)} {
		dir := filepath.Join(t.TempDir(), "capture")
		if _, err := capture(context.Background(), Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: agent}, c); err == nil {
			t.Fatal("invalid declared user agent accepted")
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("invalid options created a capture")
		}
	}
}

func save(t *testing.T, status int, body []byte, change func(*http.Response)) (string, string) {
	t.Helper()
	calls := 0
	c := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "GET" || r.URL.String() != DirectoryURL || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Accept-Encoding") != "identity" || r.Header.Get("User-Agent") == "" {
			t.Fatal("fixed public request boundary")
		}
		resp := &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, ContentLength: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}
		if change != nil {
			change(resp)
		}
		return resp, nil
	})}
	dir := filepath.Join(t.TempDir(), "capture")
	options := Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/test (test@local)"}
	sha, err := capture(context.Background(), options, c)
	if err != nil || calls != 1 {
		t.Fatal("one request", err, calls)
	}
	if _, err := capture(context.Background(), options, c); err == nil || calls != 1 {
		t.Fatal("existing directory overwritten or requested again")
	}
	return dir, sha
}

func TestCaptureReplayAndCorruption(t *testing.T) {
	dir, sha := save(t, 200, fixture(t), nil)
	r, err := Read(dir, sha)
	if err != nil || r.Issue != "" || len(r.Rows) != 4 {
		t.Fatal("valid source", err)
	}
	again, err := Read(dir, sha)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("offline replay changed", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "directory.body"))
	if err != nil || !bytes.Equal(b, fixture(t)) {
		t.Fatal("raw source changed", err)
	}
	if _, err := Read(dir, wikimedia.Hash([]byte("wrong"))); err == nil {
		t.Fatal("wrong manifest pin")
	}
	original, _ := json.Marshal(r.Manifest)
	for _, mutate := range []func(*Manifest){
		func(m *Manifest) { m.Contract = "unknown" },
		func(m *Manifest) { m.Response.URL = "https://example.org" },
		func(m *Manifest) { m.Response.Body = "../outside" },
		func(m *Manifest) { m.Response.Bytes++ },
		func(m *Manifest) { m.Response.SHA256 = wikimedia.Hash([]byte("wrong")) },
		func(m *Manifest) { m.Response.Status = 404 },
		func(m *Manifest) { m.Response.ObservedAt = "invalid" },
		func(m *Manifest) { m.Response.Headers["Content-Length"] = "10" },
		func(m *Manifest) { m.Response.Failure = "invented" },
	} {
		var m Manifest
		_ = json.Unmarshal(original, &m)
		mutate(&m)
		b, _ := json.Marshal(m)
		if err := os.WriteFile(filepath.Join(dir, "capture.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(dir, wikimedia.Hash(b)); err == nil {
			t.Fatal("corrupt capture accepted")
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "capture.json"), original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "directory.body"), filepath.Join(dir, "real.body")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real.body", filepath.Join(dir, "directory.body")); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(dir, sha); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestFailedCapturesAreNotEmptyDirectories(t *testing.T) {
	for _, tc := range []struct {
		name, issue string
		status      int
		body        []byte
		change      func(*http.Response)
	}{
		{"forbidden", "http_status_not_ok", 403, []byte("blocked"), nil},
		{"missing", "http_status_not_ok", 404, nil, nil},
		{"throttled", "http_status_not_ok", 429, nil, nil},
		{"redirect", "http_status_not_ok", 302, nil, nil},
		{"schema", "source_schema_not_accepted", 200, []byte(`{"new_shape":true}`), nil},
		{"empty", "source_schema_not_accepted", 200, []byte(`{}`), nil},
		{"budget", "body_budget_exceeded", 200, bytes.Repeat([]byte("x"), MaxBody+1), nil},
		{"length", "content_length_mismatch", 200, fixture(t), func(r *http.Response) { r.ContentLength++ }},
		{"media", "unexpected_content_type", 200, fixture(t), func(r *http.Response) { r.Header.Set("Content-Type", "text/html") }},
		{"encoding", "unexpected_encoding", 200, fixture(t), func(r *http.Response) { r.Header.Set("Content-Encoding", "gzip") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, pin := save(t, tc.status, tc.body, tc.change)
			r, err := Read(dir, pin)
			if err != nil || r.Issue != tc.issue || len(r.Rows) != 0 {
				t.Fatal("failed source yielded rows", err, r.Issue)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "cancelled")
	pin, err := Capture(ctx, Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/test (test@local)"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Read(dir, pin)
	if err != nil || r.Issue != "transport_failure" || len(r.Rows) != 0 {
		t.Fatal("cancelled request interpreted as absence", err)
	}
}
