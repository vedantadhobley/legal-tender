package reportmetadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testRecord(endpoint string, file int) map[string]any {
	r := map[string]any{}
	for k := range shapes[endpoint] {
		r[k] = nil
	}
	r["file_number"] = file
	r["committee_id"] = "C12345678"
	r["cycle"] = 2024
	r["previous_file_number"] = -11
	r["is_amended"] = false
	return r
}

func jsonBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func testCapture(t *testing.T, endpoint string, rows ...map[string]any) (Capture, string) {
	t.Helper()
	dir := t.TempDir()
	c := Capture{Contract: Contract, SchemaSHA256: SwaggerSHA256, Endpoint: endpoint,
		Query: Query{CommitteeID: "C12345678", Cycle: 2024, PerPage: 100}}
	body := jsonBytes(map[string]any{"api_version": "1.0", "pagination": Pagination{Count: int64(len(rows)), IsCountExact: true, Page: 1, Pages: 1, PerPage: 100}, "results": rows})
	c.Pages = []PageCapture{writeTestPage(t, dir, 1, body)}
	return c, filepath.Join(dir, "capture.json")
}

func writeTestPage(t *testing.T, dir string, page int, body []byte) PageCapture {
	t.Helper()
	headers := []byte(fmt.Sprintf("HTTP/2 200 \r\ncontent-type: application/json\r\ncontent-length: %d\r\ndate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(body)))
	b := Artifact{Path: fmt.Sprintf("page-%d.json", page), SHA256: digest(body), Bytes: int64(len(body))}
	h := Artifact{Path: fmt.Sprintf("page-%d.headers", page), SHA256: digest(headers), Bytes: int64(len(headers))}
	if err := os.WriteFile(filepath.Join(dir, b.Path), body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, h.Path), headers, 0600); err != nil {
		t.Fatal(err)
	}
	return PageCapture{Page: page, ObservedAt: "2026-09-10T07:00:00Z", TimeBasis: "http_date", Body: b, Headers: h}
}

func readTest(t *testing.T, c Capture, path string) (Review, error) {
	t.Helper()
	if err := os.WriteFile(path, jsonBytes(c), 0600); err != nil {
		t.Fatal(err)
	}
	return ReadCapture(context.Background(), path)
}

func TestRawAssertionsAndReplay(t *testing.T) {
	for endpoint := range shapes {
		t.Run(endpoint, func(t *testing.T) {
			r := testRecord(endpoint, 101)
			if endpoint != "/v1/filings/" {
				r["amendment_chain"] = []any{"101", json.Number("102.0")}
				r["individual_itemized_contributions_period"] = "0.00"
			}
			c, path := testCapture(t, endpoint, r)
			out, err := readTest(t, c, path)
			if err != nil {
				t.Fatal(err)
			}
			if out.State != "validated_observations" || out.Rows != 1 || out.PaginationState != "exact_count_satisfied" || out.HistoryComplete || out.FinancialSelectionReady {
				t.Fatalf("unexpected result: %+v", out)
			}
			got := out.Pages[0].Records[0]
			if got.FileNumber != "101" || !bytes.Equal(got.Raw, jsonBytes(r)) || got.SHA256 != digest(jsonBytes(r)) {
				t.Fatal("record lost raw type/identity")
			}
			again, err := ReadCapture(context.Background(), path)
			if err != nil || !bytes.Equal(jsonBytes(out), jsonBytes(again)) {
				t.Fatal("nondeterministic replay", err)
			}
		})
	}
}

func TestDriftAndDuplicatesRemainOccurrences(t *testing.T) {
	endpoint := "/v1/filings/"
	for name, change := range map[string]func(map[string]any){
		"unreviewed_field":      func(r map[string]any) { r["new_publisher_field"] = nil },
		"missing_field":         func(r map[string]any) { delete(r, "coverage_start_date") },
		"unreviewed_type":       func(r map[string]any) { r["is_amended"] = "false" },
		"invalid_file_number":   func(r map[string]any) { r["file_number"] = nil },
		"invalid_committee_id":  func(r map[string]any) { r["committee_id"] = "" },
		"outside_request_scope": func(r map[string]any) { r["cycle"] = 2026 },
	} {
		t.Run(name, func(t *testing.T) {
			r := testRecord(endpoint, 101)
			change(r)
			c, path := testCapture(t, endpoint, r)
			out, err := readTest(t, c, path)
			if err != nil {
				t.Fatal(err)
			}
			if out.State != "blocked" || out.Rows != 1 || !bytes.Equal(out.Pages[0].Records[0].Raw, jsonBytes(r)) {
				t.Fatal("drift lost occurrence")
			}
			found := false
			for _, i := range out.Issues {
				found = found || i.Code == name
			}
			if !found {
				t.Fatalf("missing %s: %+v", name, out.Issues)
			}
		})
	}
	r := testRecord(endpoint, 101)
	c, path := testCapture(t, endpoint, r, r)
	out, err := readTest(t, c, path)
	if err != nil || out.Rows != 2 || out.State != "blocked" || out.Issues[0].Code != "repeated_file_number" {
		t.Fatal("duplicate discarded", err, out)
	}
}

func TestPaginationEvidence(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			c, path := testCapture(t, "/v1/filings/", testRecord("/v1/filings/", 101))
			first := map[string]any{"api_version": "1.0", "pagination": Pagination{Count: 1, IsCountExact: exact, Page: 1, Pages: 1, PerPage: 100}, "results": []any{testRecord(c.Endpoint, 101)}}
			c.Pages[0] = writeTestPage(t, filepath.Dir(path), 1, jsonBytes(first))
			out, err := readTest(t, c, path)
			if err != nil {
				t.Fatal(err)
			}
			if !exact && out.PaginationState != "partial" {
				t.Fatal("approximate count closed scope")
			}
			second := map[string]any{"api_version": "1.0", "pagination": Pagination{Count: 1, IsCountExact: exact, Page: 2, Pages: 1, PerPage: 100}, "results": []any{}}
			c.Pages = append(c.Pages, writeTestPage(t, filepath.Dir(path), 2, jsonBytes(second)))
			out, err = readTest(t, c, path)
			if err != nil || out.State != "validated_observations" || out.PaginationState != "empty_page_observed" || out.HistoryComplete {
				t.Fatal("bad traversal state", err, out)
			}
			second["pagination"] = Pagination{Count: 2, IsCountExact: exact, Page: 2, Pages: 1, PerPage: 100}
			c.Pages[1] = writeTestPage(t, filepath.Dir(path), 2, jsonBytes(second))
			out, err = readTest(t, c, path)
			if err != nil || out.State != "blocked" {
				t.Fatal("changing count accepted", err)
			}
		})
	}
}

func TestCaptureFailClosed(t *testing.T) {
	for name, change := range map[string]func(*Capture, string){
		"digest":         func(c *Capture, _ string) { c.Pages[0].Body.SHA256 = strings.Repeat("0", 64) },
		"page_gap":       func(c *Capture, _ string) { c.Pages[0].Page = 2 },
		"time":           func(c *Capture, _ string) { c.Pages[0].ObservedAt = "2026-09-11T07:00:00Z" },
		"size":           func(c *Capture, _ string) { c.Pages[0].Body.Bytes = MaxPageBytes + 1 },
		"escape":         func(c *Capture, _ string) { c.Pages[0].Body.Path = "../page-1.json" },
		"unknown_schema": func(c *Capture, _ string) { c.SchemaSHA256 = strings.Repeat("0", 64) },
		"endpoint":       func(c *Capture, _ string) { c.Endpoint = "https://example.com/?api_key=secret" },
		"file_filter_on_reports": func(c *Capture, _ string) {
			c.Endpoint = "/v1/reports/house-senate/"
			c.Query = Query{FileNumbers: []int64{101}, PerPage: 100}
		},
		"symlink": func(c *Capture, path string) {
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(filepath.Dir(path), "escape")); err != nil {
				t.Fatal(err)
			}
			c.Pages[0].Body = Artifact{Path: "escape", Bytes: 6, SHA256: digest([]byte("secret"))}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, path := testCapture(t, "/v1/filings/", testRecord("/v1/filings/", 101))
			change(&c, path)
			_, err := readTest(t, c, path)
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatal("unsafe acceptance/error", err)
			}
		})
	}
}

func TestStrictJSON(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`{"x":1,"x":2}`), []byte(`{"x":1,"\u0078":2}`), []byte(`{"a":{"x":1,"x":2}}`),
		[]byte(`{"x":"\ud800"}`), []byte(`{"x":"\udc00"}`), []byte(`{"x":"\ud800\u0041"}`),
		{'"', 0xff, '"'}, []byte(`{} {}`), []byte(strings.Repeat("[", 34) + strings.Repeat("]", 34)),
	} {
		if strictJSON(raw, nil) == nil {
			t.Fatalf("accepted unsafe JSON: %q", raw)
		}
	}
	for _, raw := range []string{`{"x":"\ud83d\ude00"}`, `{"x":"\\ud800"}`, `{"x":9007199254740993,"b":null}`} {
		if err := strictJSON([]byte(raw), nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCancellationAndCredentialRejection(t *testing.T) {
	c, path := testCapture(t, "/v1/filings/", testRecord("/v1/filings/", 101))
	if _, err := readTest(t, c, path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadCapture(ctx, path); err != context.Canceled {
		t.Fatal(err)
	}
	bad := bytes.Replace(jsonBytes(c), []byte(`"per_page":100`), []byte(`"per_page":100,"api_key":"DO_NOT_ECHO"`), 1)
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadCapture(context.Background(), path)
	if err == nil || strings.Contains(err.Error(), "DO_NOT_ECHO") {
		t.Fatal("credential accepted/leaked", err)
	}
}

func TestMissingFilesAreObservationNotAbsence(t *testing.T) {
	c, path := testCapture(t, "/v1/filings/", testRecord("/v1/filings/", 101))
	c.Query = Query{FileNumbers: []int64{102, 101}, PerPage: 100}
	out, err := readTest(t, c, path)
	if err != nil || len(out.MissingRequestedFiles) != 1 || out.MissingRequestedFiles[0] != 102 || out.HistoryComplete {
		t.Fatal(err, out)
	}
}
