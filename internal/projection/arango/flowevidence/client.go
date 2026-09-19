package flowevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"
)

type client struct {
	base                         *url.URL
	database, username, password string
	http                         *http.Client
}
type apiError struct{ status, number int }

func (e *apiError) Error() string {
	return fmt.Sprintf("ArangoDB HTTP %d error %d", e.status, e.number)
}
func statusIs(err error, code int) bool {
	var e *apiError
	return errors.As(err, &e) && e.status == code
}

func newClient(o Options, database string) (*client, error) {
	u, err := url.Parse(o.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || o.Username == "" || o.Password == "" {
		return nil, fmt.Errorf("Arango endpoint must have scheme/host only (optional base path); credentials required separately")
	}
	if !strings.HasPrefix(database, "lt_flow_evidence_") || strings.ContainsAny(database, "/\\.") {
		return nil, fmt.Errorf("refusing non-evidence database")
	}
	return &client{u, database, o.Username, o.Password, &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Never include endpoint, credentials, transport error text, or server error
// bodies in diagnostics. All operations are bound to the derived database.
func (c *client) request(ctx context.Context, database, method, api string, query url.Values, body io.Reader, out any) error {
	u := *c.base
	u.Path = path.Join(u.Path, "_db", database, api)
	u.RawQuery = query.Encode()
	r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return fmt.Errorf("cannot construct Arango request")
	}
	r.SetBasicAuth(c.username, c.password)
	r.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("Arango transport request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var e struct {
			Number int `json:"errorNum"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&e)
		return &apiError{response.StatusCode, e.Number}
	}
	if out == nil {
		_, err = io.Copy(io.Discard, response.Body)
		return err
	}
	return json.NewDecoder(response.Body).Decode(out)
}
func (c *client) json(ctx context.Context, method, api string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	return c.request(ctx, c.database, method, api, nil, reader, out)
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
	return graph{GraphName, []edgeDefinition{{receivers, []string{entities}, []string{entities}}, {senders, []string{entities}, []string{entities}}}, []string{}}
}
func equalGraph(a, b graph) bool {
	sort.Slice(a.Edges, func(i, j int) bool { return a.Edges[i].Collection < a.Edges[j].Collection })
	sort.Slice(b.Edges, func(i, j int) bool { return b.Edges[i].Collection < b.Edges[j].Collection })
	return reflect.DeepEqual(a, b)
}
func (c *client) ensureSchema(ctx context.Context) error {
	err := c.json(ctx, "GET", "/_api/version", nil, nil)
	if statusIs(err, 404) {
		body, _ := json.Marshal(map[string]string{"name": c.database})
		err = c.request(ctx, "_system", "POST", "/_api/database", nil, bytes.NewReader(body), nil)
	}
	if err != nil {
		return err
	}
	for _, def := range []struct {
		name string
		kind int
	}{{entities, 2}, {receivers, 3}, {senders, 3}, {components, 2}, {metadata, 2}} {
		var v struct {
			Name string `json:"name"`
			Type int    `json:"type"`
		}
		err := c.json(ctx, "GET", "/_api/collection/"+def.name, nil, &v)
		if statusIs(err, 404) {
			err = c.json(ctx, "POST", "/_api/collection", map[string]any{"name": def.name, "type": def.kind}, &v)
		}
		if err != nil {
			return err
		}
		if v.Name != def.name || v.Type != def.kind {
			return fmt.Errorf("incompatible collection %s", def.name)
		}
	}
	var existing struct {
		Graph graph `json:"graph"`
	}
	err = c.json(ctx, "GET", "/_api/gharial/"+GraphName, nil, &existing)
	if statusIs(err, 404) {
		err = c.json(ctx, "POST", "/_api/gharial", graphDefinition(), &existing)
	}
	if err != nil {
		return err
	}
	if !equalGraph(existing.Graph, graphDefinition()) {
		return fmt.Errorf("incompatible evidence graph definition")
	}
	for _, collection := range []string{receivers, senders, components} {
		fields := []string{"component_id"}
		if collection == components {
			fields = []string{"sender_committee_id", "reported_recipient_committee_id"}
		}
		body, _ := json.Marshal(map[string]any{"type": "persistent", "fields": fields, "unique": false, "sparse": false})
		if err := c.request(ctx, c.database, "POST", "/_api/index", url.Values{"collection": {collection}}, bytes.NewReader(body), nil); err != nil {
			return err
		}
	}
	return nil
}

func importRows[T any](ctx context.Context, c *client, collection string, rows []T, batch int) error {
	for start := 0; start < len(rows); start += batch {
		end := min(start+batch, len(rows))
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		for _, v := range rows[start:end] {
			if err := enc.Encode(v); err != nil {
				return err
			}
		}
		var r struct{ Created, Updated, Ignored, Errors, Empty int }
		if err := c.request(ctx, c.database, "POST", "/_api/import", url.Values{"collection": {collection}, "type": {"documents"}, "onDuplicate": {"replace"}, "complete": {"true"}}, &b, &r); err != nil {
			return err
		}
		if r.Errors != 0 || r.Empty != 0 || r.Ignored != 0 || r.Created+r.Updated != end-start {
			return fmt.Errorf("%s import did not conserve documents", collection)
		}
	}
	return nil
}

type cursor struct {
	Result []json.RawMessage `json:"result"`
	More   bool              `json:"hasMore"`
	ID     string            `json:"id"`
}

func (c *client) query(ctx context.Context, q string, bind map[string]any, seconds int, visit func(json.RawMessage) error) error {
	var page cursor
	request := map[string]any{"query": q, "bindVars": bind, "batchSize": 1000, "ttl": 60, "memoryLimit": 128 << 20, "options": map[string]any{"stream": true, "maxRuntime": seconds, "failOnWarning": true}}
	if err := c.json(ctx, "POST", "/_api/cursor", request, &page); err != nil {
		return err
	}
	defer func() {
		if page.More && page.ID != "" {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = c.json(cleanup, "DELETE", "/_api/cursor/"+url.PathEscape(page.ID), nil, nil)
		}
	}()
	for {
		for _, raw := range page.Result {
			if err := visit(raw); err != nil {
				return err
			}
		}
		if !page.More {
			return nil
		}
		if page.ID == "" {
			return fmt.Errorf("cursor omitted continuation identity")
		}
		var next cursor
		if err := c.json(ctx, "POST", "/_api/cursor/"+url.PathEscape(page.ID), nil, &next); err != nil {
			return err
		}
		page = next
	}
}

// Compare every field, including amounts/locators and explicit false values.
// Comparing normalized JSON also catches omitted fields and extra attributes.
func equalDocument(raw json.RawMessage, expected any) bool {
	b, err := json.Marshal(expected)
	if err != nil {
		return false
	}
	decode := func(b []byte) (any, error) {
		var v any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		err := d.Decode(&v)
		return v, err
	}
	a, e1 := decode(raw)
	v, e2 := decode(b)
	return e1 == nil && e2 == nil && reflect.DeepEqual(a, v)
}
func readback[T any](ctx context.Context, c *client, collection string, rows []T) error {
	i := 0
	err := c.query(ctx, "FOR d IN @@collection SORT d._key RETURN UNSET(d, '_id', '_rev')", map[string]any{"@collection": collection}, 600, func(raw json.RawMessage) error {
		if i >= len(rows) || !equalDocument(raw, rows[i]) {
			return fmt.Errorf("%s full document readback mismatch at %d", collection, i)
		}
		i++
		return nil
	})
	if err != nil {
		return err
	}
	if i != len(rows) {
		return fmt.Errorf("%s readback count mismatch", collection)
	}
	return nil
}
