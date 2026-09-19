package flowevidence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

type completion struct {
	Key           string               `json:"_key"`
	SchemaVersion string               `json:"schema_version"`
	State         string               `json:"state"`
	Bundle        flow.Bundle          `json:"bundle"`
	BundleSHA     string               `json:"bundle_sha256"`
	Checks        []string             `json:"checks"`
	QueryChecks   []string             `json:"query_checks"`
	SourceChecks  []flow.SourceLocator `json:"source_checks"`
}

func checks() []string {
	return []string{"exact_readiness_ancestry", "separate_ledger_conservation", "complete_component_membership", "explicit_identity_coverage", "full_document_readback", "single_ledger_bounded_queries", "one_sided_evidence_lookup", "full_source_row_drilldown", "storage_measurement"}
}

func Run(ctx context.Context, o Options) (Result, error) {
	start := time.Now()
	if o.StorageRoot == "" || o.Bundle == "" || o.Cycle == "" {
		return Result{}, fmt.Errorf("storage root, readiness bundle, and expected cycle required")
	}
	if o.BatchSize == 0 {
		o.BatchSize = 5000
	}
	if o.BatchSize < 1 || o.BatchSize > 50000 {
		return Result{}, fmt.Errorf("batch size must be 1..50000")
	}
	if o.Progress == nil {
		o.Progress = func(string) {}
	}
	// Validate credentials before the expensive backing verification.
	if _, err := newClient(o, "lt_flow_evidence_validation"); err != nil {
		return Result{}, err
	}
	o.Progress("validating exact readiness and building occurrence-grain model")
	m, err := loadModel(ctx, o)
	if err != nil {
		return Result{}, err
	}
	c, err := newClient(o, m.database)
	if err != nil {
		return Result{}, err
	}
	unlock, err := projectionLock(ctx, o.StorageRoot, m.id)
	if err != nil {
		return Result{}, err
	}
	defer unlock()
	o.Progress(fmt.Sprintf("model ready: %d A, %d B, %d components; ensuring isolated schema", len(m.a), len(m.b), len(m.components)))
	if err := c.ensureSchema(ctx); err != nil {
		return Result{}, err
	}
	want := completion{Key: m.id, SchemaVersion: Version, State: "complete", Bundle: m.bundle, BundleSHA: m.bundleSHA, Checks: checks(), QueryChecks: []string{}, SourceChecks: []flow.SourceLocator{}}
	var prior completion
	exists := false
	if err := c.query(ctx, "FOR d IN projection_metadata RETURN UNSET(d, '_id', '_rev')", map[string]any{}, 5, func(raw json.RawMessage) error {
		if exists {
			return fmt.Errorf("unexpected extra completion metadata")
		}
		exists = true
		if err := json.Unmarshal(raw, &prior); err != nil {
			return err
		}
		if prior.Key != want.Key || prior.SchemaVersion != want.SchemaVersion || prior.State != want.State || prior.BundleSHA != want.BundleSHA || !reflect.DeepEqual(prior.Bundle, want.Bundle) || !reflect.DeepEqual(prior.Checks, want.Checks) || !equalDocument(raw, prior) {
			return fmt.Errorf("incompatible completion metadata")
		}
		return nil
	}); err != nil {
		return Result{}, err
	}
	if !exists {
		o.Progress("importing entities and separate ledger observations")
		if err := importRows(ctx, c, entities, m.entities, o.BatchSize); err != nil {
			return Result{}, err
		}
		if err := importRows(ctx, c, receivers, m.a, o.BatchSize); err != nil {
			return Result{}, err
		}
		if err := importRows(ctx, c, senders, m.b, o.BatchSize); err != nil {
			return Result{}, err
		}
		if err := importRows(ctx, c, components, m.components, o.BatchSize); err != nil {
			return Result{}, err
		}
	} else {
		o.Progress("completed projection exists; revalidating without replacing documents")
	}
	o.Progress("reading every imported field back, including exact signed amounts and source locators")
	if err := readback(ctx, c, entities, m.entities); err != nil {
		return Result{}, err
	}
	if err := readback(ctx, c, receivers, m.a); err != nil {
		return Result{}, err
	}
	if err := readback(ctx, c, senders, m.b); err != nil {
		return Result{}, err
	}
	if err := readback(ctx, c, components, m.components); err != nil {
		return Result{}, err
	}
	o.Progress("checking bounded single-ledger paths, cycles, shortest paths, and one-sided evidence")
	queries, err := queryGate(ctx, c, &m)
	if err != nil {
		return Result{}, err
	}
	for _, q := range queries {
		want.QueryChecks = append(want.QueryChecks, string(q.Ledger)+":"+q.Kind+":"+q.Status)
	}
	refs := []flow.SourceLocator{}
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		if e := m.edges(side); len(e) > 0 {
			refs = append(refs, flow.SourceLocator{Side: string(side), FactSetID: e[0].FactSetID, Ordinal: e[0].Ordinal})
		}
	}
	proofs := []SourceProof{}
	var verified uint64
	if len(refs) > 0 {
		o.Progress("verifying complete source backing and seeking full physical rows from graph locators")
		// Locators are read from the database, not substituted from local inputs.
		for i, ref := range refs {
			collection, _ := edgeCollection(Ledger(ref.Side))
			key := edgeKey(Ledger(ref.Side), ref.FactSetID, ref.Ordinal)
			n := 0
			err := c.query(ctx, "FOR d IN @@collection FILTER d._key == @key RETURN {side:d.ledger, fact_set_id:d.fact_set_id, source_row_ordinal:d.source_row_ordinal}", map[string]any{"@collection": collection, "key": key}, 5, func(raw json.RawMessage) error {
				n++
				var got flow.SourceLocator
				if err := json.Unmarshal(raw, &got); err != nil {
					return err
				}
				if got != ref {
					return fmt.Errorf("graph source locator mismatch")
				}
				refs[i] = got
				return nil
			})
			if err != nil {
				return Result{}, err
			}
			if n != 1 {
				return Result{}, fmt.Errorf("graph source lookup missing")
			}
		}
		rows, n, err := flow.LookupSources(ctx, o.StorageRoot, filepath.Join(o.StorageRoot, flow.PublicationBase, "manifests", m.calculation.CalculationSetID+".json"), refs)
		if err != nil {
			return Result{}, err
		}
		verified = n
		if len(rows) != len(refs) {
			return Result{}, fmt.Errorf("source drilldown count mismatch")
		}
		for _, row := range rows {
			collection, _ := edgeCollection(Ledger(row.Side))
			proofs = append(proofs, SourceProof{collection + "/" + edgeKey(Ledger(row.Side), row.FactSetID, row.Ordinal), row})
		}
	}
	want.SourceChecks = refs
	state := "ready"
	if m.unresolved > 0 {
		state = "partial"
	}
	result := Result{SchemaVersion: Version, ProjectionID: m.id, Database: m.database, State: state, Reused: exists, Cycle: m.bundle.Cycle, BundleID: m.bundle.BundleID, BundleSHA256: m.bundleSHA, Entities: len(m.entities), Unresolved: m.unresolved, A: m.calculation.A.Selected, B: m.calculation.B.Selected, Components: len(m.components), Sources: proofs, VerifiedSourceShards: verified, Queries: queries, Checks: checks()}
	for _, name := range []string{entities, receivers, senders, components} {
		var figures struct {
			Figures struct {
				Documents uint64 `json:"documentsSize"`
				Cache     uint64 `json:"cacheSize"`
				Indexes   struct {
					Size uint64 `json:"size"`
				} `json:"indexes"`
			} `json:"figures"`
		}
		if err := c.json(ctx, "GET", "/_api/collection/"+name+"/figures", nil, &figures); err != nil {
			return Result{}, err
		}
		result.Storage = append(result.Storage, Storage{name, figures.Figures.Documents, figures.Figures.Indexes.Size, figures.Figures.Cache})
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return Result{}, err
	}
	result.ProcessPeakRSSBytes = uint64(usage.Maxrss) * 1024
	// Completion is last: failures in readback, queries, sources, or figures must
	// never produce a consumer-ready marker. Replays cannot repair completed data.
	if exists {
		if !reflect.DeepEqual(prior, want) {
			return Result{}, fmt.Errorf("completion checks changed on replay")
		}
	} else {
		if err := importRows(ctx, c, metadata, []completion{want}, 1); err != nil {
			return Result{}, err
		}
	}
	if err := readback(ctx, c, metadata, []completion{want}); err != nil {
		return Result{}, err
	}
	result.ElapsedSeconds = time.Since(start).Seconds()
	result.CompletedAt = time.Now().UTC()
	return result, nil
}

func projectionLock(ctx context.Context, root, id string) (func(), error) {
	path := filepath.Join(root, "projections/arango/committee-flow-evidence/v1", ".publish-"+id+".lock")
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
