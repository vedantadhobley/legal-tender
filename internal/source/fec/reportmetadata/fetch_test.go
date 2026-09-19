package reportmetadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testKey = "secret_fixture_key_123456"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fetchRequest() FetchRequest {
	return FetchRequest{Contract: Contract, SchemaSHA256: SwaggerSHA256, Endpoint: "/v1/filings/",
		Query: Query{FileNumbers: []int64{101}, PerPage: 100}, Limits: FetchLimits{Pages: 3, Requests: 5, AttemptsPerPage: 2, SourceBytes: 1 << 20}}
}

func pageBody(page int, rows ...map[string]any) []byte {
	if rows == nil {
		rows = []map[string]any{}
	}
	return jsonBytes(map[string]any{"api_version": "1.0", "pagination": Pagination{Count: 1, IsCountExact: true, Page: page, Pages: 1, PerPage: 100}, "results": rows})
}

func response(status int, body []byte) *http.Response {
	return &http.Response{StatusCode: status, Proto: "HTTP/1.1", Header: http.Header{"Content-Type": []string{"application/json"}}, ContentLength: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}
}

func noWait(context.Context, time.Duration) error { return nil }

func testFetch(t *testing.T, r FetchRequest, rt roundTripFunc) (FetchResult, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "capture")
	client := metadataClient()
	client.Transport = rt
	out, err := fetch(context.Background(), dir, r, testKey, client, noWait)
	if err != nil {
		t.Fatal(err)
	}
	if out.HistoryComplete || out.FinancialSelectionReady {
		t.Fatal("capture promoted financial/history completeness")
	}
	assertNoKey(t, dir, testKey)
	var saved FetchResult
	b, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil || strictJSON(b, &saved) != nil || !bytes.Equal(jsonBytes(out), jsonBytes(saved)) {
		t.Fatal("final result differs", err)
	}
	return out, dir
}

func assertNoKey(t *testing.T, dir, key string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if credentialIn(b, key) {
			t.Fatal("credential persisted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFetchCapturesUntilObservedEmptyAndReplays(t *testing.T) {
	r := fetchRequest()
	calls := 0
	out, dir := testFetch(t, r, func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Scheme != "https" || req.URL.Host != "api.open.fec.gov" || req.URL.Path != r.Endpoint || req.URL.Query().Get("page") != fmt.Sprint(calls) || len(req.URL.Query()) != 3 || req.Header.Get("X-Api-Key") != testKey || req.Header.Get("Accept-Encoding") != "identity" {
			t.Fatal("unsafe or incorrect request")
		}
		if calls == 1 {
			resp := response(200, pageBody(1, testRecord(r.Endpoint, 101)))
			resp.Proto = "HTTP/2.0" // Go's live HTTP/2 representation, unlike curl's HTTP/2.
			return resp, nil
		}
		return response(200, pageBody(2)), nil
	})
	if calls != 2 || out.State != "captured" || out.Capture == nil || out.Review == nil {
		t.Fatalf("unexpected capture: %+v", out)
	}
	review, err := ReadCapture(context.Background(), filepath.Join(dir, out.Capture.Path))
	if err != nil || review.Rows != 1 || review.PaginationState != "empty_page_observed" {
		t.Fatal("bad replay", err)
	}
	if _, err := Fetch(context.Background(), dir, r, testKey); err == nil {
		t.Fatal("existing directory accepted")
	}
}

func TestFetchBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		change       func(*FetchRequest)
		body         func(int) []byte
	}{
		{"page", "page_budget", func(r *FetchRequest) { r.Limits.Pages = 1 }, func(p int) []byte { return pageBody(p, testRecord("/v1/filings/", 101)) }},
		{"request", "request_budget", func(r *FetchRequest) { r.Limits.Requests = 1 }, func(p int) []byte { return pageBody(p, testRecord("/v1/filings/", 101)) }},
		{"bytes", "body_or_byte_budget", func(r *FetchRequest) { r.Limits.SourceBytes = 1024 }, func(int) []byte { return bytes.Repeat([]byte("x"), 4096) }},
		{"page_bytes", "body_or_byte_budget", func(r *FetchRequest) { r.Limits.SourceBytes = 8 << 20 }, func(int) []byte { return bytes.Repeat([]byte("x"), MaxPageBytes+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := fetchRequest()
			tc.change(&r)
			calls := 0
			out, dir := testFetch(t, r, func(*http.Request) (*http.Response, error) { calls++; return response(200, tc.body(calls)), nil })
			if out.State != "incomplete" || out.Reason != tc.reason || out.BytesRead > r.Limits.SourceBytes || out.Capture != nil {
				t.Fatalf("budget escaped: %+v", out)
			}
			if _, err := os.Stat(filepath.Join(dir, "capture.json")); !os.IsNotExist(err) {
				t.Fatal("incomplete capture published")
			}
		})
	}
}

func TestFetchRetryAndRejection(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		status                    int
		retryAfter, state, reason string
		calls                     int
	}{
		{"rate_limit", 429, "1", "captured", "query_empty_page_observed", 3},
		{"unavailable", 503, "", "captured", "query_empty_page_observed", 3},
		{"long_delay", 429, "3600", "incomplete", "retry_deferred", 1},
		{"unknown_delay", 503, "not-a-delay", "incomplete", "retry_deferred", 1},
		{"unauthorized", 401, "", "blocked", "rejected_http_status", 1},
		{"redirect", 302, "", "blocked", "rejected_http_status", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			out, _ := testFetch(t, fetchRequest(), func(req *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					resp := response(tc.status, []byte(`{"error":"test"}`))
					resp.Header.Set("Retry-After", tc.retryAfter)
					resp.Header.Set("Location", "https://example.invalid/steal")
					return resp, nil
				}
				if req.URL.Query().Get("page") == "1" {
					return response(200, pageBody(1, testRecord("/v1/filings/", 101))), nil
				}
				return response(200, pageBody(2)), nil
			})
			if calls != tc.calls || out.State != tc.state || out.Reason != tc.reason {
				t.Fatalf("unexpected result: %+v", out)
			}
			if out.Attempts[0].Body == nil {
				t.Fatal("error body not retained")
			}
		})
	}
}

func TestFetchPreservesRejectedSourceBytes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   []byte
		reason string
	}{
		{"invalid_json", []byte(`{"broken":`), "capture_reader_rejected"},
		{"schema_drift", bytes.Replace(pageBody(1, testRecord("/v1/filings/", 101)), []byte(`"is_amended":false`), []byte(`"is_amended":"false"`), 1), "source_review_issues"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, dir := testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) { return response(200, tc.body), nil })
			if out.State != "blocked" || out.Reason != tc.reason || out.Capture != nil {
				t.Fatalf("bad source accepted: %+v", out)
			}
			b, err := os.ReadFile(filepath.Join(dir, out.Attempts[0].Body.Path))
			if err != nil || !bytes.Equal(b, tc.body) {
				t.Fatal("rejected bytes lost")
			}
		})
	}
}

func TestFetchChangingAndRepeatedPagination(t *testing.T) {
	for _, changed := range []bool{false, true} {
		calls := 0
		out, _ := testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) {
			calls++
			b := pageBody(calls, testRecord("/v1/filings/", 101))
			if changed && calls == 2 {
				b = bytes.Replace(b, []byte(`"count":1`), []byte(`"count":2`), 1)
			}
			return response(200, b), nil
		})
		if out.State != "blocked" || out.Reason != "source_review_issues" || calls != 2 {
			t.Fatal("pagination drift accepted")
		}
	}
}

func TestFetchNeverRetainsEchoedCredential(t *testing.T) {
	for _, body := range []string{testKey, `{"echo":"\u0073` + testKey[1:] + `"}`, "%73" + url.QueryEscape(testKey[1:])} {
		out, _ := testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) { return response(500, []byte(body)), nil })
		if out.Reason != "credential_echo_suppressed" || out.Attempts[0].Body != nil || out.Attempts[0].Headers != nil {
			t.Fatal("credential echo retained")
		}
	}
	out, _ := testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) { return nil, errors.New("transport echoed " + testKey) })
	if out.Reason != "attempt_budget" || len(out.Attempts) != 2 {
		t.Fatal("transport retry not bounded")
	}
	out, _ = testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) {
		resp := response(401, []byte(`{}`))
		resp.Header.Set("Set-Cookie", testKey)
		return resp, nil
	})
	if len(out.Attempts[0].OmittedHeaders) != 1 || out.Attempts[0].OmittedHeaders[0] != "set-cookie" {
		t.Fatal("cookie not omitted")
	}
	out, _ = testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) {
		resp := response(401, []byte(`{}`))
		resp.Header.Set("X-Error", testKey)
		return resp, nil
	})
	if out.Reason != "credential_echo_suppressed" {
		t.Fatal("header echo not suppressed")
	}
}

func TestFetchCancellationDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := metadataClient()
	client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { return response(503, []byte(`{}`)), nil })
	dir := filepath.Join(t.TempDir(), "capture")
	out, err := fetch(ctx, dir, fetchRequest(), testKey, client, func(context.Context, time.Duration) error { cancel(); return ctx.Err() })
	if err != nil || out.State != "incomplete" || out.Reason != "canceled_or_deadline" || len(out.Attempts) != 1 {
		t.Fatal("cancellation not retained", err)
	}
}

func TestFetchResponseFramingAndConfiguration(t *testing.T) {
	for name, change := range map[string]func(*http.Response){
		"content_length_mismatch":      func(r *http.Response) { r.ContentLength++ },
		"unsupported_content_encoding": func(r *http.Response) { r.Header.Set("Content-Encoding", "gzip") },
		"header_or_byte_budget":        func(r *http.Response) { r.Header.Set("X-Large", strings.Repeat("a", maxHeaderBytes)) },
		"rejected_transport":           func(r *http.Response) { r.Header.Set("Content-Type", "text/html") },
	} {
		t.Run(name, func(t *testing.T) {
			out, _ := testFetch(t, fetchRequest(), func(*http.Request) (*http.Response, error) {
				resp := response(200, pageBody(1, testRecord("/v1/filings/", 101)))
				change(resp)
				return resp, nil
			})
			if out.State == "captured" || out.Attempts[0].Outcome != name {
				t.Fatalf("framing accepted: %+v", out)
			}
		})
	}
	for _, change := range []func(*FetchRequest){
		func(r *FetchRequest) { r.Endpoint = "https://example.invalid/" }, func(r *FetchRequest) { r.Limits.Requests = 49 },
		func(r *FetchRequest) { r.Limits.Pages = 0 }, func(r *FetchRequest) { r.Limits.SourceBytes = MaxCaptureBytes + 1 },
		func(r *FetchRequest) { r.Query.FileNumbers = append(r.Query.FileNumbers, 101) },
	} {
		r := fetchRequest()
		change(&r)
		if ValidateFetchRequest(r) == nil {
			t.Fatal("invalid scope accepted")
		}
	}
	r := fetchRequest()
	r.Query = Query{CommitteeID: "C12345678", Cycle: 2024, PerPage: 100}
	if ValidateFetchRequest(r) != nil || !strings.Contains(requestURL(r, 1), "cycle=2024") {
		t.Fatal("committee scope failed")
	}
	for _, raw := range [][]byte{[]byte(`{"api_key":"hidden"}`), []byte(`{"contract":"a","contract":"b"}`), bytes.Repeat([]byte(" "), (256<<10)+1)} {
		path := filepath.Join(t.TempDir(), "request.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFetchRequest(path); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
}

func TestMetadataClientAndDelay(t *testing.T) {
	c := metadataClient()
	defer c.CloseIdleConnections()
	tr := c.Transport.(*http.Transport)
	if tr.Proxy != nil || !tr.DisableKeepAlives || !tr.DisableCompression || c.Timeout != 30*time.Second {
		t.Fatal("unsafe transport defaults")
	}
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		input string
		want  time.Duration
	}{{"", time.Second}, {"-1", time.Second}, {"2", 2 * time.Second}, {"99999999999999999999999999", 31 * time.Second}, {now.Add(4 * time.Second).Format(http.TimeFormat), 4 * time.Second}} {
		if retryDelay(tc.input, now) != tc.want {
			t.Fatal("incorrect retry delay")
		}
	}
}
