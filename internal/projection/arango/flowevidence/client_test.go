package flowevidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func mockClient(t *testing.T, f transportFunc) *client {
	t.Helper()
	c, err := newClient(Options{Endpoint: "http://arango", Username: "test", Password: "private-value"}, "lt_flow_evidence_test")
	if err != nil {
		t.Fatal(err)
	}
	c.http.Transport = f
	return c
}
func response(body string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}
func TestCursorStreamsAllPagesWithCapsAndCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			calls, rows, deletes := 0, 0, 0
			c := mockClient(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == "DELETE" {
					deletes++
					return response(`{}`), nil
				}
				calls++
				if calls == 1 {
					var request map[string]any
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Fatal(err)
					}
					options := request["options"].(map[string]any)
					if request["memoryLimit"] != float64(128<<20) || options["stream"] != true || options["maxRuntime"] != float64(5) || options["failOnWarning"] != true {
						t.Fatal("unbounded query", request)
					}
					return response(`{"result":[1],"hasMore":true,"id":"next"}`), nil
				}
				if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/cursor/next") {
					t.Fatal("bad continuation")
				}
				return response(`{"result":[2],"hasMore":false}`), nil
			})
			err := c.query(context.Background(), "RETURN 1", nil, 5, func(json.RawMessage) error {
				rows++
				if fail {
					return errors.New("consumer failed")
				}
				return nil
			})
			if fail {
				if err == nil || rows != 1 || deletes != 1 {
					t.Fatal(err, rows, deletes)
				}
			} else if err != nil || rows != 2 || calls != 2 || deletes != 0 {
				t.Fatal(err, rows, calls, deletes)
			}
		})
	}
}
func TestReadbackRejectsAlteredDocumentsAndMissingRows(t *testing.T) {
	for _, body := range []string{`{"result":[],"hasMore":false}`, `{"result":[{"_key":"x","amount":"2"}],"hasMore":false}`} {
		c := mockClient(t, func(*http.Request) (*http.Response, error) { return response(body), nil })
		if err := readback(context.Background(), c, entities, []map[string]string{{"_key": "x", "amount": "1"}}); err == nil {
			t.Fatal("accepted corrupt readback")
		}
	}
}
func TestCredentialErrorsAreRedactedAndRedirectsDisabled(t *testing.T) {
	for _, endpoint := range []string{"http://test:private-value@arango", "http://arango?token=private-value", "http://[private-value"} {
		_, err := newClient(Options{Endpoint: endpoint, Username: "test", Password: "private-value"}, "lt_flow_evidence_test")
		if err == nil || strings.Contains(err.Error(), "private-value") {
			t.Fatal("unsafe endpoint error", err)
		}
	}
	c := mockClient(t, func(*http.Request) (*http.Response, error) { return nil, errors.New("private-value") })
	if err := c.json(context.Background(), "GET", "/_api/version", nil, nil); err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatal("transport error leaked", err)
	}
	if c.http.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("redirect enabled")
	}
	c.http.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		r := response(`{"errorNum":123,"errorMessage":"private-value"}`)
		r.StatusCode = 400
		return r, nil
	})
	if err := c.json(context.Background(), "GET", "/_api/version", nil, nil); err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatal("API error leaked", err)
	}
}
func TestProjectionLockCancellation(t *testing.T) {
	root := t.TempDir()
	unlock, err := projectionLock(context.Background(), root, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := projectionLock(ctx, root, "test"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
