// Package flowapi serves one immutable observation projection. It contains HTTP
// validation and pagination only; the Go graph/calculation packages own evidence.
package flowapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	graph "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

type Evidence interface {
	View() graph.View
	Entity(context.Context, string) (json.RawMessage, error)
	Component(context.Context, string) (json.RawMessage, error)
	Source(context.Context, graph.Ledger, string) (flow.SourceExample, error)
	Query(context.Context, graph.ReadQuery) (graph.ReadPage, error)
}

type handler struct {
	evidence Evidence
	key      [32]byte
	slots    chan struct{}
}
type envelope struct {
	Projection graph.View `json:"projection"`
	Data       any        `json:"data"`
}
type page struct {
	Query   graph.ReadQuery   `json:"query"`
	Items   []json.RawMessage `json:"items"`
	HasMore bool              `json:"has_more"`
	Next    *string           `json:"next_cursor"`
	Scope   string            `json:"coverage_scope"`
}
type continuation struct{ Projection, Query, After string }

func NewHandler(e Evidence) (http.Handler, error) {
	h := &handler{evidence: e, slots: make(chan struct{}, 4)}
	if _, err := rand.Read(h.key[:]); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("GET /v1/committee-flow", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			fail(w, graph.ErrInvalidQuery)
			return
		}
		writeJSON(w, 200, envelope{h.evidence.View(), nil})
	})
	mux.HandleFunc("GET /v1/committee-flow/{projection}/entities/{key}", h.single)
	mux.HandleFunc("GET /v1/committee-flow/{projection}/components/{key}", h.single)
	mux.HandleFunc("GET /v1/committee-flow/{projection}/observations/{ledger}/{key}/source", h.source)
	mux.HandleFunc("GET /v1/committee-flow/{projection}/queries/{kind}", h.query)
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) { fail(w, graph.ErrNotFound) })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			writeJSON(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		if len(r.URL.RequestURI()) > 8192 {
			fail(w, graph.ErrInvalidQuery)
			return
		}
		select {
		case h.slots <- struct{}{}:
			defer func() { <-h.slots }()
		default:
			w.Header().Set("Retry-After", "1")
			writeJSON(w, 429, map[string]string{"error": "request_capacity_exceeded"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	}), nil
}

func (h *handler) pinned(w http.ResponseWriter, r *http.Request) bool {
	if r.PathValue("projection") != h.evidence.View().ProjectionID {
		writeJSON(w, 409, map[string]string{"error": "projection_mismatch"})
		return false
	}
	return true
}
func (h *handler) single(w http.ResponseWriter, r *http.Request) {
	if !h.pinned(w, r) {
		return
	}
	if r.URL.RawQuery != "" {
		fail(w, graph.ErrInvalidQuery)
		return
	}
	var data json.RawMessage
	var err error
	if strings.Contains(r.URL.Path, "/entities/") {
		data, err = h.evidence.Entity(r.Context(), r.PathValue("key"))
	} else {
		data, err = h.evidence.Component(r.Context(), r.PathValue("key"))
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, envelope{h.evidence.View(), data})
}
func (h *handler) source(w http.ResponseWriter, r *http.Request) {
	if !h.pinned(w, r) {
		return
	}
	if r.URL.RawQuery != "" {
		fail(w, graph.ErrInvalidQuery)
		return
	}
	data, err := h.evidence.Source(r.Context(), graph.Ledger(r.PathValue("ledger")), r.PathValue("key"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, envelope{h.evidence.View(), data})
}

func parseQuery(kind string, v url.Values) (graph.ReadQuery, error) {
	allowed := map[string]bool{"ledger": true, "committee": true, "direction": true, "component": true, "target": true, "max_depth": true, "limit": true, "cursor": true}
	for k, values := range v {
		if !allowed[k] || len(values) != 1 || values[0] == "" {
			return graph.ReadQuery{}, graph.ErrInvalidQuery
		}
	}
	q := graph.ReadQuery{Kind: kind, Ledger: graph.Ledger(v.Get("ledger")), Committee: v.Get("committee"), Direction: v.Get("direction"), Component: v.Get("component"), Target: v.Get("target"), Limit: 25}
	if kind == "shortest" {
		q.Limit = 1
	}
	for name, dest := range map[string]*int{"max_depth": &q.Depth, "limit": &q.Limit} {
		if value := v.Get(name); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || strconv.Itoa(n) != value {
				return q, graph.ErrInvalidQuery
			}
			*dest = n
		}
	}
	return q, q.Validate()
}

func (h *handler) query(w http.ResponseWriter, r *http.Request) {
	if !h.pinned(w, r) {
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fail(w, graph.ErrInvalidQuery)
		return
	}
	q, err := parseQuery(r.PathValue("kind"), values)
	if err != nil {
		fail(w, err)
		return
	}
	queryJSON, _ := json.Marshal(q)
	digest := sha256.Sum256(queryJSON)
	fingerprint := base64.RawURLEncoding.EncodeToString(digest[:])
	if token := values.Get("cursor"); token != "" {
		after, err := h.decode(token, fingerprint)
		if err != nil {
			fail(w, err)
			return
		}
		q.After = after
		if err := q.Validate(); err != nil {
			fail(w, err)
			return
		}
	}
	result, err := h.evidence.Query(r.Context(), q)
	if err != nil {
		fail(w, err)
		return
	}
	out := page{Query: q, Items: result.Items, HasMore: result.HasMore, Scope: "selected_observations_in_pinned_projection"}
	if q.Kind == "entities" {
		out.Scope = "referenced_committee_identities_in_pinned_projection"
	}
	if q.Depth > 0 {
		out.Scope = "paths_within_requested_depth_not_global_graph_coverage"
	}
	if q.Kind == "shortest" {
		out.Scope = "one_hop_shortest_path_within_requested_depth"
	}
	if result.HasMore {
		next := h.encode(continuation{h.evidence.View().ProjectionID, fingerprint, result.Last})
		out.Next = &next
	}
	writeJSON(w, 200, envelope{h.evidence.View(), out})
}

func (h *handler) encode(c continuation) string {
	b, _ := json.Marshal(c)
	mac := hmac.New(sha256.New, h.key[:])
	_, _ = mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func (h *handler) decode(token, fingerprint string) (string, error) {
	if len(token) > 4096 {
		return "", graph.ErrInvalidQuery
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return "", graph.ErrInvalidQuery
	}
	b, e1 := base64.RawURLEncoding.DecodeString(parts[0])
	sig, e2 := base64.RawURLEncoding.DecodeString(parts[1])
	mac := hmac.New(sha256.New, h.key[:])
	_, _ = mac.Write(b)
	if e1 != nil || e2 != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		return "", graph.ErrInvalidQuery
	}
	var c continuation
	if json.Unmarshal(b, &c) != nil || c.Projection != h.evidence.View().ProjectionID || c.Query != fingerprint || c.After == "" {
		return "", graph.ErrInvalidQuery
	}
	return c.After, nil
}

func fail(w http.ResponseWriter, err error) {
	status, code := 503, "evidence_unavailable"
	if errors.Is(err, graph.ErrInvalidQuery) {
		status, code = 400, "invalid_query"
	}
	if errors.Is(err, graph.ErrNotFound) {
		status, code = 404, "not_found"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		status, code = 504, "query_timeout"
	}
	writeJSON(w, status, map[string]string{"error": code})
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 8<<20 {
		return 0, errors.New("response exceeds byte limit")
	}
	return b.Buffer.Write(p)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	var b boundedBuffer
	if err := json.NewEncoder(&b).Encode(value); err != nil {
		w.WriteHeader(503)
		_, _ = w.Write([]byte("{\"error\":\"response_limit_or_encoding_failure\"}\n"))
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}
