package receiptgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"time"
)

// This transport is deliberately restricted to this isolated projection. Never
// return credentials, endpoint text, transport errors or server error bodies.
type client struct {
	base               *url.URL
	db, user, password string
	http               *http.Client
	durable            bool
}

func newClient(o Options, db string) (*client, error) {
	u, e := url.Parse(o.Endpoint)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || o.Username == "" || o.Password == "" || !(strings.HasPrefix(db, "lt_receipt_sample_") || strings.HasPrefix(db, "lt_receipt_cycle_") || strings.HasPrefix(db, "lt_receipt_shared_")) || strings.ContainsAny(db, "/\\.") {
		return nil, fmt.Errorf("valid isolated endpoint and separate credentials required")
	}
	return &client{u, db, o.Username, o.Password, &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, o.FullCycle}, nil
}

type apiError int

func (e apiError) Error() string { return fmt.Sprintf("Arango HTTP %d", int(e)) }
func (c *client) request(ctx context.Context, db, method, api string, q url.Values, body io.Reader, out any) error {
	u := *c.base
	u.Path = path.Join(u.Path, "_db", db, api)
	u.RawQuery = q.Encode()
	r, e := http.NewRequestWithContext(ctx, method, u.String(), body)
	if e != nil {
		return fmt.Errorf("cannot construct Arango request")
	}
	r.SetBasicAuth(c.user, c.password)
	r.Header.Set("Content-Type", "application/json")
	v, e := c.http.Do(r)
	if e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("Arango transport failed")
	}
	defer v.Body.Close()
	if v.StatusCode < 200 || v.StatusCode >= 300 {
		return apiError(v.StatusCode)
	}
	if out == nil {
		_, e = io.Copy(io.Discard, io.LimitReader(v.Body, 32<<20))
		return e
	}
	if e = json.NewDecoder(io.LimitReader(v.Body, 32<<20)).Decode(out); e != nil {
		return fmt.Errorf("invalid Arango response")
	}
	return nil
}
func (c *client) json(ctx context.Context, method, api string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return e
		}
		r = bytes.NewReader(b)
	}
	return c.request(ctx, c.db, method, api, nil, r, out)
}

type edgeDefinition struct {
	Collection string   `json:"collection"`
	From       []string `json:"from"`
	To         []string `json:"to"`
}
type graph struct {
	Name    string           `json:"name"`
	Edges   []edgeDefinition `json:"edgeDefinitions"`
	Orphans []string         `json:"orphanCollections"`
}

func graphDefinition() graph {
	return graph{"receipt_evidence", []edgeDefinition{{receipts, []string{appearances}, []string{entities}}, {conduits, []string{appearances}, []string{entities}}, {authorizations, []string{entities}, []string{entities}}}, []string{}}
}
func (c *client) ensure(ctx context.Context) error {
	return c.ensureSchema(ctx, true)
}

func (c *client) ensureSchema(ctx context.Context, create bool) error {
	e := c.json(ctx, "GET", "/_api/version", nil, nil)
	if e == apiError(404) && create {
		b, _ := json.Marshal(map[string]string{"name": c.db})
		e = c.request(ctx, "_system", "POST", "/_api/database", nil, bytes.NewReader(b), nil)
	}
	if e != nil {
		return e
	}
	for _, name := range collections {
		kind := 2
		if name == receipts || name == conduits || name == authorizations {
			kind = 3
		}
		var r struct {
			Name string `json:"name"`
			Type int    `json:"type"`
		}
		e = c.json(ctx, "GET", "/_api/collection/"+name, nil, &r)
		if e == apiError(404) && create {
			e = c.json(ctx, "POST", "/_api/collection", map[string]any{"name": name, "type": kind}, &r)
		}
		if e != nil {
			return e
		}
		if r.Name != name || r.Type != kind {
			return fmt.Errorf("incompatible collection schema")
		}
	}
	var r struct {
		Graph graph `json:"graph"`
	}
	e = c.json(ctx, "GET", "/_api/gharial/receipt_evidence", nil, &r)
	if e == apiError(404) && create {
		e = c.json(ctx, "POST", "/_api/gharial", graphDefinition(), &r)
	}
	if e != nil {
		return e
	}
	want := graphDefinition()
	// Gharial may return edge definitions in collection-name order.
	if r.Graph.Name != want.Name || len(r.Graph.Orphans) != 0 || len(r.Graph.Edges) != len(want.Edges) {
		return fmt.Errorf("incompatible named graph")
	}
	for _, x := range want.Edges {
		n := 0
		for _, y := range r.Graph.Edges {
			if reflect.DeepEqual(x, y) {
				n++
			}
		}
		if n != 1 {
			return fmt.Errorf("incompatible named graph edges")
		}
	}
	return nil
}
func (c *client) importBatch(ctx context.Context, name string, b batch) error {
	var r struct{ Created, Updated, Ignored, Errors, Empty int }
	q := url.Values{"collection": {name}, "type": {"documents"}, "onDuplicate": {"replace"}, "complete": {"true"}}
	if c.durable {
		q.Set("waitForSync", "true")
	}
	e := c.request(ctx, c.db, "POST", "/_api/import", q, bytes.NewReader(b.data), &r)
	if e != nil {
		return e
	}
	if r.Errors != 0 || r.Ignored != 0 || r.Empty != 0 || r.Created+r.Updated != len(b.keys) {
		return fmt.Errorf("batch import did not conserve rows")
	}
	return nil
}
func (c *client) query(ctx context.Context, q string, bind map[string]any, visit func(json.RawMessage) error) error {
	type cursor struct {
		Result []json.RawMessage `json:"result"`
		More   bool              `json:"hasMore"`
		ID     string            `json:"id"`
	}
	var p cursor
	e := c.json(ctx, "POST", "/_api/cursor", map[string]any{"query": q, "bindVars": bind, "batchSize": 1000, "ttl": 60, "memoryLimit": 128 << 20, "options": map[string]any{"stream": true, "maxRuntime": 30, "failOnWarning": true}}, &p)
	if e != nil {
		return e
	}
	defer func() {
		if p.More && p.ID != "" {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = c.json(cleanup, "DELETE", "/_api/cursor/"+url.PathEscape(p.ID), nil, nil)
		}
	}()
	for {
		for _, v := range p.Result {
			if e := visit(v); e != nil {
				return e
			}
		}
		if !p.More {
			return nil
		}
		if p.ID == "" {
			return fmt.Errorf("missing cursor ID")
		}
		var next cursor
		if e := c.json(ctx, "POST", "/_api/cursor/"+url.PathEscape(p.ID), nil, &next); e != nil {
			return e
		}
		p = next
	}
}
func equalJSON(a, b []byte) bool {
	decode := func(b []byte) (any, error) {
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		var v any
		e := d.Decode(&v)
		if e == nil && d.Decode(new(any)) != io.EOF {
			e = fmt.Errorf("trailing JSON")
		}
		return v, e
	}
	x, e := decode(a)
	y, f := decode(b)
	return e == nil && f == nil && reflect.DeepEqual(x, y)
}
func (c *client) verifyBatch(ctx context.Context, name string, b batch) error {
	rows := bytes.Split(bytes.TrimSuffix(b.data, []byte{'\n'}), []byte{'\n'})
	var source [][]byte
	if len(b.evidence) > 0 {
		if name != appearances {
			return fmt.Errorf("unsupported reconstructed collection")
		}
		source = bytes.Split(bytes.TrimSuffix(b.evidence, []byte{'\n'}), []byte{'\n'})
		if len(source) != len(rows) {
			return fmt.Errorf("source proof batch membership mismatch")
		}
	}
	n := 0
	e := c.query(ctx, "FOR k IN @keys LET d = DOCUMENT(CONCAT(@collection, '/', k)) RETURN d == null ? null : UNSET(d, '_id', '_rev')", map[string]any{"keys": b.keys, "collection": name}, func(raw json.RawMessage) error {
		if n >= len(rows) || !equalJSON(raw, rows[n]) {
			return fmt.Errorf("full document readback mismatch in %s", name)
		}
		if source != nil {
			if err := verifyReconstruction(raw, source[n]); err != nil {
				return err
			}
		}
		n++
		return nil
	})
	if e != nil {
		return e
	}
	if n != len(b.keys) {
		return fmt.Errorf("readback count mismatch")
	}
	return nil
}
func (c *client) count(ctx context.Context, name string) (uint64, error) {
	var r struct {
		Count uint64 `json:"count"`
	}
	e := c.json(ctx, "GET", "/_api/collection/"+name+"/count", nil, &r)
	return r.Count, e
}
