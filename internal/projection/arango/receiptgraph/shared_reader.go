package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const SharedFamily = "shared_conduit_association"

// SharedReader never invokes a publisher. Opening proves complete calculation
// membership and expected graph payload hashes; selected documents are checked
// against source evidence on each query. It is not a fresh all-graph field scan.
type SharedReader struct {
	base        *CycleReader
	cl          *client
	view        SharedView
	raw         []byte
	calculation string
	updated     c.Result
	ids         []string
}

func (r *CycleReader) OpenShared(ctx context.Context, manifest, calculation string, view SharedView) (*SharedReader, error) {
	raw, err := manifestBytes(manifest)
	if err != nil {
		return nil, err
	}
	var v sharedCompletion
	if digest(raw) != view.Projection.SHA256 || strictjson.Decode(raw, &v) != nil {
		return nil, fmt.Errorf("shared completion bytes differ")
	}
	d := v.sharedDefinition
	id := d.Key
	d.Key = ""
	b, _ := json.Marshal(d)
	if !validDigest(id) || digest(b) != id || v.Version != SharedVersion || v.Policy != policy.GroupPolicy || !validDigest(v.Build) || v.Base != r.View().Projection || v.FactSet != r.completion.Inputs.Facts.ID || v.Cycle != r.completion.Inputs.Cycle || v.AdditionalAmount != "0" || v.FinancialEligibility || v.TerminalEligible {
		return nil, fmt.Errorf("unsupported shared completion identity or ancestry")
	}
	b, err = manifestBytes(calculation)
	if err != nil {
		return nil, err
	}
	updated, err := c.DecodeManifest(b, v.Calculation.ID)
	if err != nil {
		return nil, err
	}
	if digest(b) != v.Calculation.SHA256 || updated.Groups == nil || updated.Groups.ChangedRows != v.Added || !reflect.DeepEqual(sharedView(v, raw, updated), view) {
		return nil, fmt.Errorf("shared calculation or generation binding differs")
	}
	// Stream both complete artifacts, never retain a population-sized member map.
	batcher := newBatches(2000, func(batch) error { return nil })
	base := r.View()
	ids := map[string]bool{}
	err = c.ReadAdditions(ctx, r.options.Conduits, r.loaded.c, calculation, updated, func(decision c.Decision) error {
		a, e, err := sharedDocuments(base, decision)
		if err != nil {
			return err
		}
		if len(ids) >= 100000 && !ids[*decision.ConduitID] {
			return fmt.Errorf("shared entity context exceeds bound")
		}
		ids[*decision.ConduitID] = true
		if err := batcher.add(appearances, a.Key, a); err != nil {
			return err
		}
		return batcher.add(conduits, e.Key, e)
	})
	if err != nil {
		return nil, err
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	slices.Sort(ordered)
	for _, id := range ordered {
		if err = batcher.add(entities, id, r.loaded.entity(id)); err != nil {
			return nil, err
		}
	}
	if err = batcher.finish(); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(v.Counts, batcher.counts) || !reflect.DeepEqual(v.Digests, batcher.digests()) {
		return nil, fmt.Errorf("shared graph manifest differs from complete additive source membership")
	}
	cl, err := newClient(r.options, view.Database)
	if err != nil {
		return nil, err
	}
	if err = cl.ensureSchema(ctx, false); err != nil {
		return nil, err
	}
	out := &SharedReader{base: r, cl: cl, view: view, raw: raw, calculation: calculation, updated: updated, ids: ordered}
	if err = out.VerifyCompletion(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *SharedReader) VerifyCompletion(ctx context.Context) error {
	for _, name := range collections {
		n, err := r.cl.count(ctx, name)
		if err != nil {
			return err
		}
		want := r.view.Counts[name]
		if name == metadata {
			want = 1
		}
		if n != want {
			return fmt.Errorf("shared live count differs: %s", name)
		}
	}
	_, err := verifyConnectionDocument(ctx, r.cl, metadata, r.view.Projection.ID, json.RawMessage(r.raw))
	return err
}

func (r *SharedReader) WitnessID() string {
	if len(r.ids) == 0 {
		return ""
	}
	return r.ids[0]
}
