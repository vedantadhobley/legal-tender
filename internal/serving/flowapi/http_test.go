package flowapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	graph "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

type stub struct {
	calls   int
	queries []graph.ReadQuery
	err     error
}

func (s *stub) View() graph.View {
	return graph.View{SchemaVersion: graph.ReadVersion, ProjectionID: strings.Repeat("a", 64), Cycle: "2026", State: "partial", EconomicFlowStatus: "not_established"}
}
func (s *stub) Entity(context.Context, string) (json.RawMessage, error) {
	s.calls++
	return json.RawMessage(`{"terminal_attribution_eligible":false}`), s.err
}
func (s *stub) Component(ctx context.Context, key string) (json.RawMessage, error) {
	return s.Entity(ctx, key)
}
func (s *stub) Source(context.Context, graph.Ledger, string) (flow.SourceExample, error) {
	s.calls++
	return flow.SourceExample{Fields: map[string]any{"raw_amount": "-1.00", "empty": "", "unknown": nil}}, s.err
}
func (s *stub) Query(_ context.Context, q graph.ReadQuery) (graph.ReadPage, error) {
	s.calls++
	s.queries = append(s.queries, q)
	return graph.ReadPage{Items: []json.RawMessage{json.RawMessage(`{"signed_amount_minor_units":"9007199254740993"}`)}, HasMore: q.After == "", Last: strings.Repeat("b", 64)}, s.err
}
func request(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}
func TestHTTPPaginationBindsProjectionQueryAndPageSize(t *testing.T) {
	s := &stub{}
	h, err := NewHandler(s)
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/committee-flow/" + s.View().ProjectionID + "/queries/observations?ledger=schedule_a&committee=C00000001&direction=any&limit=1"
	w := request(t, h, "GET", base)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var body struct {
		Projection graph.View `json:"projection"`
		Data       page       `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Projection.State != "partial" || body.Projection.TerminalEligible || body.Data.Next == nil || !strings.Contains(w.Body.String(), `"9007199254740993"`) {
		t.Fatal(w.Body.String())
	}
	token := url.QueryEscape(*body.Data.Next)
	w = request(t, h, "GET", base+"&cursor="+token)
	if w.Code != 200 || s.queries[1].After != strings.Repeat("b", 64) {
		t.Fatal(w.Code, s.queries)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Data.Next != nil || body.Data.HasMore {
		t.Fatal(w.Body.String(), err)
	}
	before := s.calls
	for _, path := range []string{
		strings.Replace(base, "schedule_a", "schedule_b", 1) + "&cursor=" + token,
		strings.Replace(base, "limit=1", "limit=2", 1) + "&cursor=" + token,
		base + "&cursor=x" + token, base + "&ledger=schedule_b", base + "&limit=0", base + "&typo=true",
	} {
		if w := request(t, h, "GET", path); w.Code != 400 {
			t.Fatal("bad continuation/filter accepted", w.Code, path)
		}
	}
	if s.calls != before {
		t.Fatal("invalid request reached evidence reader")
	}
	w = request(t, h, "GET", strings.Replace(base, s.View().ProjectionID, strings.Repeat("c", 64), 1))
	if w.Code != 409 {
		t.Fatal(w.Code)
	}
	restarted, _ := NewHandler(s)
	if w := request(t, restarted, "GET", base+"&cursor="+token); w.Code != 400 {
		t.Fatal("cursor survived signing-key restart")
	}
}

func TestHTTPReadOnlyRedactionAndRequestBounds(t *testing.T) {
	s := &stub{}
	h, _ := NewHandler(s)
	prefix := "/v1/committee-flow/" + s.View().ProjectionID
	for _, method := range []string{"POST", "DELETE", "PUT", "PATCH"} {
		if w := request(t, h, method, prefix+"/entities/C00000001"); w.Code != 405 {
			t.Fatal(w.Code)
		}
	}
	for _, suffix := range []string{"/queries/paths?ledger=schedule_a&committee=C00000001&target=C00000002&max_depth=9", "/queries/observations?committee=C00000001&direction=any", "/queries/paths?ledger=schedule_a&committee=C00000001&target=C00000002&max_depth=2&limit=26", "/entities/C00000001?file=/etc/passwd"} {
		if w := request(t, h, "GET", prefix+suffix); w.Code != 400 {
			t.Fatal(w.Code, suffix)
		}
	}
	for _, pair := range []struct {
		err  error
		code int
	}{{errors.New("secret-internal-value"), 503}, {graph.ErrNotFound, 404}, {context.DeadlineExceeded, 504}} {
		s.err = pair.err
		w := request(t, h, "GET", prefix+"/entities/C00000001")
		if w.Code != pair.code || strings.Contains(w.Body.String(), "secret-internal-value") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if s.calls != 3 {
		t.Fatal("invalid input executed")
	}
	s.err = nil
	w := request(t, h, "GET", prefix+"/observations/schedule_b/"+strings.Repeat("b", 64)+"/source")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"unknown":null`) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	var b boundedBuffer
	if _, err := b.Write(make([]byte, (8<<20)+1)); err == nil {
		t.Fatal("unbounded response")
	}
}

func TestHTTPConcurrencyGate(t *testing.T) {
	s := &stub{}
	h := &handler{evidence: s, slots: make(chan struct{}, 4)}
	// The production wrapper uses non-blocking admission, not an unbounded
	// waiting queue. Exercise it with four blocked provider calls.
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	provider := blockingEvidence{stub: s, entered: entered, release: release}
	wrapped, _ := NewHandler(provider)
	done := make(chan struct{}, 4)
	for range cap(h.slots) {
		go func() {
			request(t, wrapped, "GET", "/v1/committee-flow/"+s.View().ProjectionID+"/entities/C00000001")
			done <- struct{}{}
		}()
	}
	for range cap(h.slots) {
		<-entered
	}
	if w := request(t, wrapped, "GET", "/healthz"); w.Code != 429 {
		t.Fatal(w.Code)
	}
	close(release)
	for range cap(h.slots) {
		<-done
	}
}

type blockingEvidence struct {
	*stub
	entered chan struct{}
	release chan struct{}
}

func (s blockingEvidence) Entity(context.Context, string) (json.RawMessage, error) {
	s.entered <- struct{}{}
	<-s.release
	return json.RawMessage(`{}`), nil
}
