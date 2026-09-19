package flowevidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

const ReadVersion = "legal-tender.committee-flow-api.v1"

var ErrNotFound = errors.New("evidence not found in this projection")
var ErrInvalidQuery = errors.New("invalid evidence query")

// Reader is a pinned, read-only projection. It never creates schema, imports
// documents, advances pointers, or repairs a completed projection.
type Reader struct {
	m       model
	c       *client
	sources *flow.SourceReader
	proof   completion
}

type View struct {
	SchemaVersion      string        `json:"schema_version"`
	ProjectionID       string        `json:"projection_id"`
	Cycle              string        `json:"cycle"`
	State              string        `json:"state"`
	BundleID           string        `json:"bundle_id"`
	BundleSHA          string        `json:"bundle_sha256"`
	CalculationID      string        `json:"calculation_set_id"`
	Inputs             flow.Inputs   `json:"inputs"`
	Entities           int           `json:"entities"`
	Unresolved         int           `json:"unresolved_same_cycle_masters"`
	A                  flow.Measures `json:"schedule_a"`
	B                  flow.Measures `json:"schedule_b"`
	EconomicFlowStatus string        `json:"economic_flow_status"`
	TerminalEligible   bool          `json:"terminal_attribution_eligible"`
}

// Physical routing is owned by the projection, not copied into its consumers.
func (r *Reader) Database() string       { return r.m.database }
func (r *Reader) ComponentCount() uint64 { return uint64(len(r.m.components)) }

func (r *Reader) View() View {
	state := "ready"
	if r.m.unresolved > 0 {
		state = "partial"
	}
	return View{ReadVersion, r.m.id, r.m.bundle.Cycle, state, r.m.bundle.BundleID, r.m.bundleSHA, r.m.calculation.CalculationSetID, r.m.calculation.Input, len(r.m.entities), r.m.unresolved, r.m.calculation.A.Selected, r.m.calculation.B.Selected, "not_established", false}
}

func OpenReader(ctx context.Context, o Options) (*Reader, error) {
	if o.StorageRoot == "" || o.Bundle == "" || o.Cycle == "" {
		return nil, fmt.Errorf("storage root, exact bundle and cycle required")
	}
	if _, err := newClient(o, "lt_flow_evidence_validation"); err != nil {
		return nil, err
	}
	m, err := loadModel(ctx, o)
	if err != nil {
		return nil, err
	}
	c, err := newClient(o, m.database)
	if err != nil {
		return nil, err
	}
	var g struct {
		Graph graph `json:"graph"`
	}
	if err := c.json(ctx, "GET", "/_api/gharial/"+GraphName, nil, &g); err != nil {
		return nil, err
	}
	if !equalGraph(g.Graph, graphDefinition()) {
		return nil, fmt.Errorf("incompatible read graph")
	}
	var prior completion
	n := 0
	if err := c.query(ctx, "FOR d IN projection_metadata RETURN UNSET(d, '_id', '_rev')", map[string]any{}, 5, func(raw json.RawMessage) error {
		n++
		if n > 1 {
			return fmt.Errorf("extra completion metadata")
		}
		return json.Unmarshal(raw, &prior)
	}); err != nil {
		return nil, err
	}
	if n != 1 || prior.Key != m.id || prior.SchemaVersion != Version || prior.State != "complete" || prior.BundleSHA != m.bundleSHA || !reflect.DeepEqual(prior.Bundle, m.bundle) || !reflect.DeepEqual(prior.Checks, checks()) {
		return nil, fmt.Errorf("completed projection with exact ancestry required")
	}
	if err := readback(ctx, c, entities, m.entities); err != nil {
		return nil, err
	}
	if err := readback(ctx, c, receivers, m.a); err != nil {
		return nil, err
	}
	if err := readback(ctx, c, senders, m.b); err != nil {
		return nil, err
	}
	if err := readback(ctx, c, components, m.components); err != nil {
		return nil, err
	}
	queries, err := queryGate(ctx, c, &m)
	if err != nil {
		return nil, err
	}
	want := completion{Key: m.id, SchemaVersion: Version, State: "complete", Bundle: m.bundle, BundleSHA: m.bundleSHA, Checks: checks(), QueryChecks: []string{}, SourceChecks: []flow.SourceLocator{}}
	for _, q := range queries {
		want.QueryChecks = append(want.QueryChecks, string(q.Ledger)+":"+q.Kind+":"+q.Status)
	}
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		if es := m.edges(side); len(es) > 0 {
			want.SourceChecks = append(want.SourceChecks, flow.SourceLocator{Side: string(side), FactSetID: es[0].FactSetID, Ordinal: es[0].Ordinal})
		}
	}
	if err := readback(ctx, c, metadata, []completion{want}); err != nil {
		return nil, err
	}
	sources, err := flow.OpenSourceReader(ctx, o.StorageRoot, filepath.Join(o.StorageRoot, flow.PublicationBase, "manifests", m.calculation.CalculationSetID+".json"))
	if err != nil {
		return nil, err
	}
	if sources.CalculationID() != m.calculation.CalculationSetID {
		return nil, fmt.Errorf("source reader identity mismatch")
	}
	return &Reader{m: m, c: c, sources: sources, proof: want}, nil
}

// Recipients exposes membership in the already verified selected ledger for
// deterministic validation-case selection. It adds no financial interpretation.
func (r *Reader) Recipients(side Ledger) (map[string]bool, error) {
	if _, err := edgeCollection(side); err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, e := range r.m.edges(side) {
		out[e.Recipient] = true
	}
	return out, nil
}

// VerifyCompletion checks that the pinned completion is still present without
// reopening the source tree or silently repairing changed graph metadata.
func (r *Reader) VerifyCompletion(ctx context.Context) error {
	if r.proof.Key != r.m.id || r.proof.State != "complete" {
		return fmt.Errorf("reader has no verified completion")
	}
	_, err := r.document(ctx, metadata, r.m.id, r.proof)
	return err
}

func committeeKey(s string) bool {
	return len(s) == 9 && s[0] == 'C' && strings.Trim(s[1:], "0123456789") == ""
}
func digestKey(s string) bool { return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == "" }

func (r *Reader) expectedEntity(key string) (entity, bool) {
	i := sort.Search(len(r.m.entities), func(i int) bool { return r.m.entities[i].Key >= key })
	if i == len(r.m.entities) || r.m.entities[i].Key != key {
		return entity{}, false
	}
	return r.m.entities[i], true
}
func (r *Reader) expectedEdge(side Ledger, key string) (observation, bool) {
	es := r.m.edges(side)
	i := sort.Search(len(es), func(i int) bool { return es[i].Key >= key })
	if i == len(es) || es[i].Key != key {
		return observation{}, false
	}
	return es[i], true
}
func (r *Reader) expectedComponent(key string) (component, bool) {
	i := sort.Search(len(r.m.components), func(i int) bool { return r.m.components[i].Key >= key })
	if i == len(r.m.components) || r.m.components[i].Key != key {
		return component{}, false
	}
	return r.m.components[i], true
}

func (r *Reader) document(ctx context.Context, collection, key string, expected any) (json.RawMessage, error) {
	var out json.RawMessage
	n := 0
	err := r.c.query(ctx, "FOR d IN @@collection FILTER d._key == @key RETURN UNSET(d, '_id', '_rev')", map[string]any{"@collection": collection, "key": key}, 5, func(raw json.RawMessage) error {
		n++
		if n > 1 || !equalDocument(raw, expected) {
			return fmt.Errorf("evidence document differs from verified publication")
		}
		out = raw
		return nil
	})
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, fmt.Errorf("verified evidence document is missing")
	}
	return out, nil
}

func (r *Reader) Entity(ctx context.Context, key string) (json.RawMessage, error) {
	if !committeeKey(key) {
		return nil, ErrInvalidQuery
	}
	e, ok := r.expectedEntity(key)
	if !ok {
		return nil, ErrNotFound
	}
	return r.document(ctx, entities, key, e)
}
func (r *Reader) Component(ctx context.Context, key string) (json.RawMessage, error) {
	if !digestKey(key) {
		return nil, ErrInvalidQuery
	}
	e, ok := r.expectedComponent(key)
	if !ok {
		return nil, ErrNotFound
	}
	raw, err := r.document(ctx, components, key, e)
	if err != nil {
		return nil, err
	}
	var summary map[string]json.RawMessage
	if err := json.Unmarshal(raw, &summary); err != nil {
		return nil, err
	}
	delete(summary, "schedule_a_ordinals")
	delete(summary, "schedule_b_ordinals")
	summary["schedule_a_members"], _ = json.Marshal(len(e.A))
	summary["schedule_b_members"], _ = json.Marshal(len(e.B))
	return json.Marshal(summary)
}
func (r *Reader) Source(ctx context.Context, side Ledger, key string) (flow.SourceExample, error) {
	collection, err := edgeCollection(side)
	if err != nil || !digestKey(key) {
		return flow.SourceExample{}, ErrInvalidQuery
	}
	e, ok := r.expectedEdge(side, key)
	if !ok {
		return flow.SourceExample{}, ErrNotFound
	}
	if _, err := r.document(ctx, collection, key, e); err != nil {
		return flow.SourceExample{}, err
	}
	rows, err := r.sources.Lookup(ctx, []flow.SourceLocator{{Side: string(side), FactSetID: e.FactSetID, Ordinal: e.Ordinal}})
	if err != nil {
		return flow.SourceExample{}, err
	}
	if len(rows) != 1 {
		return flow.SourceExample{}, fmt.Errorf("source lookup cardinality mismatch")
	}
	return rows[0], nil
}

// ObservationAt joins a source occurrence to this exact projection. Callers do
// not duplicate its edge-key policy or accept an ordinal from another fact set.
func (r *Reader) ObservationAt(ctx context.Context, side Ledger, ordinal uint64) (json.RawMessage, error) {
	collection, err := edgeCollection(side)
	if err != nil || ordinal == 0 {
		return nil, ErrInvalidQuery
	}
	fact := r.m.calculation.Input.A.FactSetID
	if side == ScheduleB {
		fact = r.m.calculation.Input.B.FactSetID
	}
	key := edgeKey(side, fact, ordinal)
	e, ok := r.expectedEdge(side, key)
	if !ok {
		return nil, ErrNotFound
	}
	if _, err := r.document(ctx, collection, key, e); err != nil {
		return nil, err
	}
	// Canonical verified values, not backend property order, define downstream
	// connection identities. Verification happens before returning these bytes.
	return json.Marshal(e)
}
