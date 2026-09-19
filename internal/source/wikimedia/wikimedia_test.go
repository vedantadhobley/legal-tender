package wikimedia

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, e := os.ReadFile("testdata/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func testQueries() Queries {
	return Queries{Version: "organization-queries.v1", Selection: "fixture", BuildSHA256: Hash([]byte("build")), Queries: []Query{{Text: "Example Corp.", References: []Reference{{FactSetID: "facts", ManifestSHA256: Hash([]byte("facts")), FactID: "fact", Field: "CONNECTED_ORG_NM"}}}}}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (r roundTrip) RoundTrip(q *http.Request) (*http.Response, error) { return r(q) }
func testClient(t *testing.T, search, entities []byte, count *int) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		*count++
		if r.Header.Get("User-Agent") != "LegalTender/test (test@local)" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("request headers")
		}
		var b []byte
		switch r.URL.String() {
		case SearchURL("Example Corp."):
			b = search
		case EntityURL([]string{"Q100"}):
			b = entities
		default:
			t.Fatalf("unexpected request: %s", r.URL)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}}, Body: io.NopCloser(strings.NewReader(string(b))), ContentLength: int64(len(b))}, nil
	})}
}

func TestCaptureAndOfflineReplay(t *testing.T) {
	q := testQueries()
	raw, _ := json.Marshal(q)
	n := 0
	dir := filepath.Join(t.TempDir(), "capture")
	sha, err := capture(context.Background(), raw, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: q.BuildSHA256}, testClient(t, fixture(t, "search"), fixture(t, "entities"), &n), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatal(n)
	}
	first, err := Read(dir, sha)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Read(dir, sha)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("replay changed", err)
	}
	o := first.Observations[0]
	if o.Issue != "" || len(o.Entities["Q100"].Claims["P31"]) == 0 || o.SearchShapeSHA256 == "" || o.EntitiesShapeSHA256 == "" {
		t.Fatalf("lost evidence: %+v", o)
	}
	if _, err = capture(context.Background(), raw, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: q.BuildSHA256}, testClient(t, nil, nil, &n), 0); err == nil || n != 2 {
		t.Fatal("existing directory reused")
	}
	if _, err = Read(dir, Hash([]byte("wrong"))); err == nil {
		t.Fatal("wrong digest accepted")
	}
	path := filepath.Join(dir, first.Manifest.Entries[0].Search.Body)
	if err = os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Read(dir, sha); err == nil {
		t.Fatal("body corruption accepted")
	}
}

func TestMalformedSearchStaysEvidence(t *testing.T) {
	for _, raw := range []string{`{"batchcomplete":true,"batchcomplete":true}`, `{"batchcomplete":true,"new_field":42}`, `{"error":{"code":"maxlag"}}`, `{"batchcomplete":false}`, `{"batchcomplete":true,"query":{}}`, `{"batchcomplete":true,"query":{"pages":null}}`, `{"batchcomplete":true,"warnings":{}}`, "{\"bad\":\"\xff\"}"} {
		t.Run(Hash([]byte(raw))[:8], func(t *testing.T) {
			n := 0
			q := testQueries()
			b, _ := json.Marshal(q)
			dir := filepath.Join(t.TempDir(), "capture")
			sha, err := capture(context.Background(), b, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: q.BuildSHA256}, testClient(t, []byte(raw), nil, &n), 0)
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Fatal("fetched entities after bad search")
			}
			r, err := Read(dir, sha)
			if err != nil || r.Observations[0].Issue == "" {
				t.Fatal("bad search not blocked", err)
			}
			body, err := os.ReadFile(filepath.Join(dir, r.Manifest.Entries[0].Search.Body))
			if err != nil || string(body) != raw {
				t.Fatal("raw error body lost", err)
			}
		})
	}
}

func TestHTTPFailuresAndBudgets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		length    int64
		transport bool
	}{
		{"rate_limit", 429, `{"error":"busy"}`, 16, false},
		{"redirect", 302, "", 0, false},
		{"transport", 0, "", 0, true},
		{"oversized", 200, strings.Repeat("x", MaxBody+10), MaxBody + 10, false},
		{"truncated", 200, `{}`, 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := testQueries()
			other := q.Queries[0]
			other.Text = "Another Corp"
			q.Queries = append(q.Queries, other)
			b, _ := json.Marshal(q)
			calls := 0
			dir := filepath.Join(t.TempDir(), "capture")
			client := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				if tc.transport {
					return nil, errors.New("fixture transport failure")
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentLength: tc.length, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			sha, err := capture(context.Background(), b, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: q.BuildSHA256}, client, 0)
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("continued after source failure", calls)
			}
			r, err := Read(dir, sha)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range r.Observations {
				if o.Issue == "" {
					t.Fatal("failure lost")
				}
			}
			if r.Manifest.Entries[0].Search.Bytes > MaxBody {
				t.Fatal("body over budget")
			}
		})
	}
}

func TestParsers(t *testing.T) {
	if p, e := ParseSearch([]byte(`{"batchcomplete":true}`)); e != nil || len(p) != 0 {
		t.Fatal(p, e)
	}
	b := fixture(t, "search")
	for _, raw := range [][]byte{
		[]byte(strings.Replace(string(b), `"Q100"`, `"not-a-qid"`, 1)),
		[]byte(strings.Replace(string(b), `"pageid":1`, `"pageid":0`, 1)),
		[]byte(strings.Replace(string(b), `"revid":10`, `"revid":null`, 1)),
	} {
		if _, err := ParseSearch(raw); err == nil {
			t.Fatal("invalid page accepted")
		}
	}
	b = fixture(t, "entities")
	if _, err := ParseEntities(b, []string{"Q100"}); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		strings.Replace(string(b), `"success":1`, `"success":0`, 1),
		strings.Replace(string(b), `"id":"Q100"`, `"id":"Q200"`, 1),
		strings.Replace(string(b), `"type":"item"`, `"type":"lexeme"`, 1),
		strings.Replace(string(b), `"labels":`, `"unreviewed":true,"labels":`, 1),
		`{"entities":{"Q100":{"id":"Q100","missing":true}},"success":1}`,
	} {
		e, err := ParseEntities([]byte(raw), []string{"Q100"})
		if strings.Contains(raw, `"missing":true`) {
			if err != nil || e["Q100"].Missing == nil {
				t.Fatal("missing not retained", err)
			}
		} else if err == nil {
			t.Fatal("invalid entity accepted")
		}
	}
}

func TestQueryValidation(t *testing.T) {
	q := testQueries()
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Queries){
		func(q *Queries) { q.Queries = append(q.Queries, q.Queries[0]) },
		func(q *Queries) { q.Queries[0].Text = "\nExample" },
		func(q *Queries) { q.Queries[0].References[0].Field = "donor_address" },
		func(q *Queries) { q.Queries[0].References[0].ManifestSHA256 = "" },
		func(q *Queries) {
			q.Queries[0].References = append(q.Queries[0].References, q.Queries[0].References[0])
		},
	} {
		q := testQueries()
		change(&q)
		if q.Validate() == nil {
			t.Fatal("invalid queries accepted")
		}
	}
}

func TestCancelledCaptureAndConfinedReplay(t *testing.T) {
	q := testQueries()
	b, _ := json.Marshal(q)
	n := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "capture")
	sha, err := capture(ctx, b, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: q.BuildSHA256}, testClient(t, nil, nil, &n), time.Second)
	if err != nil || n != 0 {
		t.Fatal("cancel issued request", err, n)
	}
	r, err := Read(dir, sha)
	if err != nil || r.Observations[0].Issue == "" {
		t.Fatal("cancel not retained", err)
	}
	body := filepath.Join(dir, r.Manifest.Entries[0].Search.Body)
	if err = os.Remove(body); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("queries.json", body); err != nil {
		t.Fatal(err)
	}
	if _, err = Read(dir, sha); err == nil {
		t.Fatal("symlink accepted")
	}
}
