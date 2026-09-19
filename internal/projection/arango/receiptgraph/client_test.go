package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSafeTransport(t *testing.T) {
	for _, endpoint := range []string{"http://user:SECRET@example.com", "ftp://example.com", "http://example.com?token=SECRET", "http://example.com/#SECRET", "%SECRET"} {
		if _, e := newClient(Options{Endpoint: endpoint, Username: "u", Password: "SECRET"}, "lt_receipt_sample_test"); e == nil || strings.Contains(e.Error(), "SECRET") {
			t.Fatal("unsafe endpoint validation")
		}
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "SECRET", 500) }))
	defer s.Close()
	c, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "SECRET"}, "lt_receipt_sample_test")
	e := c.json(context.Background(), "GET", "/_api/version", nil, nil)
	if e == nil || strings.Contains(e.Error(), "SECRET") {
		t.Fatal("server body leaked")
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, s.URL+"/SECRET", 302) }))
	defer redirect.Close()
	c, _ = newClient(Options{Endpoint: redirect.URL, Username: "u", Password: "SECRET"}, "lt_receipt_sample_test")
	if e = c.json(context.Background(), "GET", "/_api/version", nil, nil); e != apiError(302) {
		t.Fatal("redirect followed")
	}
}
func TestImportConservation(t *testing.T) {
	for _, body := range []string{`{"created":2,"updated":0,"ignored":0,"errors":0,"empty":0}`, `{"created":1,"updated":0,"ignored":1,"errors":0,"empty":0}`, `{"created":0,"updated":0,"ignored":0,"errors":1,"empty":0}`} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("complete") != "true" || r.URL.Query().Get("onDuplicate") != "replace" {
				t.Error("unsafe import options")
			}
			fmt.Fprint(w, body)
		}))
		c, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p"}, "lt_receipt_sample_test")
		e := c.importBatch(context.Background(), appearances, batch{keys: []string{"1", "2"}, data: []byte("{}\n{}\n")})
		s.Close()
		if (e == nil) != (strings.Contains(body, `"created":2`)) {
			t.Fatalf("incorrect conservation: %v", e)
		}
	}
}
func TestFullReadbackDetectsNullFalseAmountsAndExtraFields(t *testing.T) {
	want := `{"_key":"1","amount":"9007199254740993","known":false,"nullable":null}`
	for _, got := range []string{want, `{"_key":"1","amount":"9007199254740992","known":false,"nullable":null}`, `{"_key":"1","amount":"9007199254740993","nullable":null}`, `{"_key":"1","amount":"9007199254740993","known":false,"nullable":""}`, `{"_key":"1","amount":"9007199254740993","known":false,"nullable":null,"extra":0}`, `null`} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"hasMore": false, "result": []json.RawMessage{json.RawMessage(got)}})
		}))
		c, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p"}, "lt_receipt_sample_test")
		e := c.verifyBatch(context.Background(), appearances, batch{keys: []string{"1"}, data: []byte(want + "\n")})
		s.Close()
		if (e == nil) != (got == want) {
			t.Fatalf("readback accepted altered field: %v", e)
		}
	}
}
func TestCursorCleanupOnFailedReadback(t *testing.T) {
	deleted := false
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			deleted = true
			fmt.Fprint(w, `{}`)
			return
		}
		fmt.Fprint(w, `{"result":[null],"hasMore":true,"id":"123"}`)
	}))
	defer s.Close()
	c, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p"}, "lt_receipt_sample_test")
	e := c.verifyBatch(context.Background(), appearances, batch{keys: []string{"1"}, data: []byte("{}\n")})
	if e == nil || !deleted {
		t.Fatal("failed cursor left open")
	}
}

func TestCycleImportSyncAndReadOnlySchema(t *testing.T) {
	requests := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if strings.HasSuffix(r.URL.Path, "/_api/import") {
			if r.URL.Query().Get("waitForSync") != "true" {
				t.Error("checkpointable import did not request durable sync")
			}
			fmt.Fprint(w, `{"created":1}`)
			return
		}
		if r.Method != "GET" {
			t.Error("read-only schema validation wrote metadata")
		}
		http.NotFound(w, r)
	}))
	defer s.Close()
	c, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p", FullCycle: true}, "lt_receipt_cycle_test")
	if err := c.importBatch(context.Background(), appearances, batch{keys: []string{"1"}, data: []byte("{}\n")}); err != nil {
		t.Fatal(err)
	}
	if err := c.ensureSchema(context.Background(), false); err == nil {
		t.Fatal("accepted missing schema")
	}
	if requests != 2 {
		t.Fatalf("unexpected requests: %d", requests)
	}
}
