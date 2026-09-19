package receiptgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const SharedVersion = "legal-tender.arango.shared-conduit-extension.v1"

type SharedOptions struct {
	Calculation, CalculationID, BuildSHA256, PublicationDirectory, LockDirectory, ArangoDataDirectory string
	Workers, BatchSize                                                                                int
	ReserveFreeBytes, MaxFilesystemGrowthBytes, MaxEncodedBytes                                       uint64
	Progress                                                                                          func(string)
}

type sharedDefinition struct {
	Key                  string    `json:"_key"`
	Version              string    `json:"schema_version"`
	Policy               string    `json:"association_policy"`
	Build                string    `json:"executable_sha256"`
	Base                 Reference `json:"base_receipt_projection"`
	Calculation          Reference `json:"conduit_calculation"`
	FactSet              string    `json:"fact_set_id"`
	Cycle                string    `json:"acquisition_cycle"`
	Added                uint64    `json:"new_associations"`
	AdditionalAmount     string    `json:"additional_amount_minor_units"`
	FinancialEligibility bool      `json:"financial_eligibility"`
	TerminalEligible     bool      `json:"terminal_attribution_eligible"`
}

type sharedCompletion struct {
	sharedDefinition
	Counts  map[string]uint64 `json:"counts"`
	Digests map[string]string `json:"ordered_document_values_sha256"`
}

type SharedView struct {
	Projection      Reference         `json:"projection"`
	Database        string            `json:"database"`
	Base            Reference         `json:"base_receipt_projection"`
	Calculation     Reference         `json:"conduit_calculation"`
	FactSet         string            `json:"fact_set_id"`
	Policy          string            `json:"association_policy"`
	Added           uint64            `json:"new_associations"`
	RemainingShared uint64            `json:"remaining_shared_degree_exclusions"`
	Counts          map[string]uint64 `json:"collection_counts"`
	Digests         map[string]string `json:"ordered_document_values_sha256"`
	EndpointJoin    string            `json:"endpoint_join_contract"`
}

type SharedPublication struct {
	View         SharedView        `json:"view"`
	Manifest     string            `json:"manifest"`
	Reused       bool              `json:"read_only_replay"`
	EncodedBytes map[string]uint64 `json:"encoded_document_bytes"`
	Storage      StorageEnvelope   `json:"storage_envelope"`
}

// This node is a source-occurrence reference, not a second donor or receipt.
type sharedAppearance struct {
	Key              string `json:"_key"`
	FactSet          string `json:"fact_set_id"`
	Ordinal          uint64 `json:"source_row_ordinal"`
	BaseProjection   string `json:"base_receipt_projection_id"`
	IdentityResolved bool   `json:"identity_resolved"`
}

func sharedDocuments(base CycleView, d c.Decision) (sharedAppearance, conduit, error) {
	if d.State != policy.SharedAssociation || d.ConduitID == nil || !committeePattern.MatchString(*d.ConduitID) || d.Ordinal == 0 || d.Ordinal > base.SourceRows || d.Related == 0 || d.Related > base.SourceRows || d.Related == d.Ordinal {
		return sharedAppearance{}, conduit{}, fmt.Errorf("invalid shared association")
	}
	key, err := p.AppearanceID(base.Inputs.Facts.ID, d.Ordinal)
	if err != nil {
		return sharedAppearance{}, conduit{}, err
	}
	a := sharedAppearance{Key: key, FactSet: base.Inputs.Facts.ID, Ordinal: d.Ordinal, BaseProjection: base.Projection.ID}
	e := conduit{Key: key, From: appearances + "/" + key, To: entities + "/" + *d.ConduitID, FactSet: a.FactSet, Decision: d, AdditionalAmount: "0"}
	return a, e, nil
}

// PublishShared reuses a verified base graph through exact occurrence references.
// It writes only an isolated extension database. Old graph documents and all
// monetary ledgers stay unchanged. Every new edge is fully read back.
func (r *CycleReader) PublishShared(ctx context.Context, o SharedOptions) (SharedPublication, error) {
	var out SharedPublication
	if !validDigest(o.BuildSHA256) || o.Workers < 1 || o.Workers > 8 || o.BatchSize < 1 || o.BatchSize > 5000 || o.PublicationDirectory == "" || o.LockDirectory == "" || o.ArangoDataDirectory == "" || o.ReserveFreeBytes == 0 || o.MaxFilesystemGrowthBytes == 0 || o.MaxEncodedBytes == 0 || o.MaxFilesystemGrowthBytes > ^uint64(0)-o.ReserveFreeBytes {
		return out, fmt.Errorf("bounded shared publisher and explicit storage envelope required")
	}
	if o.Progress == nil {
		o.Progress = func(string) {}
	}
	b, err := manifestBytes(o.Calculation)
	if err != nil {
		return out, err
	}
	updated, err := c.DecodeManifest(b, o.CalculationID)
	if err != nil {
		return out, err
	}
	base := r.View()
	if updated.Groups == nil || updated.Groups.BaselineID != base.Inputs.Conduits.ID || updated.Groups.BaselineSHA256 != base.Inputs.Conduits.SHA256 || updated.ParticipantID != base.Inputs.Participants.ID || updated.ParticipantSHA256 != base.Inputs.Participants.SHA256 || updated.FactSetID != base.Inputs.Facts.ID || updated.FactManifestSHA256 != base.Inputs.Facts.SHA256 || updated.SourceRows != base.SourceRows || updated.Cycle != base.Inputs.Cycle {
		return out, fmt.Errorf("shared projection requires exact base receipt ancestry")
	}
	if err = c.VerifyGroupBacking(ctx, o.Calculation, updated); err != nil {
		return out, err
	}
	if err = r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	d := sharedDefinition{Version: SharedVersion, Policy: policy.GroupPolicy, Build: o.BuildSHA256, Base: base.Projection, Calculation: Reference{updated.CalculationID, digest(b)}, FactSet: updated.FactSetID, Cycle: updated.Cycle, Added: updated.Groups.ChangedRows, AdditionalAmount: "0"}
	b, _ = json.Marshal(d)
	d.Key = digest(b)
	unlock, err := lock(o.LockDirectory, d.Key)
	if err != nil {
		return out, err
	}
	defer unlock()
	dir := filepath.Join(o.PublicationDirectory, d.Key)
	guard, err := sharedStorage(o, dir, d)
	if err != nil {
		return out, err
	}
	out.Storage = guard.envelope
	db := "lt_receipt_shared_" + updated.Cycle + "_" + d.Key[:32]
	cl, err := newClient(Options{Endpoint: r.options.Endpoint, Username: r.options.Username, Password: r.options.Password, FullCycle: true}, db)
	if err != nil {
		return out, err
	}
	if err = guard.check(); err != nil {
		return out, err
	}
	published, publishedErr := manifestBytes(filepath.Join(dir, "manifest.json"))
	if publishedErr != nil && !os.IsNotExist(publishedErr) {
		return out, publishedErr
	}
	if err = cl.ensureSchema(ctx, os.IsNotExist(publishedErr)); err != nil {
		return out, err
	}
	var prior json.RawMessage
	err = cl.query(ctx, "FOR d IN projection_metadata RETURN UNSET(d, '_id', '_rev')", map[string]any{}, func(raw json.RawMessage) error {
		var v sharedCompletion
		if prior != nil || strictjson.Decode(raw, &v) != nil || !reflect.DeepEqual(v.sharedDefinition, d) {
			return fmt.Errorf("incompatible shared completion")
		}
		prior = append(json.RawMessage(nil), raw...)
		return nil
	})
	if err != nil {
		return out, err
	}
	out.Reused = prior != nil
	if publishedErr == nil && !equalJSON(published, prior) {
		return out, fmt.Errorf("shared manifest/live completion differ")
	}
	var encoded uint64
	var batchesOut *batches
	o.Progress(fmt.Sprintf("streaming %d new associations; old graph read-only; replay=%t", d.Added, out.Reused))
	err = withWorkers(ctx, o.Workers, func(ctx context.Context, b batch) error {
		if b.collection == conduits {
			if err := r.verifySharedBaseMembers(ctx, b); err != nil {
				return err
			}
		}
		if !out.Reused {
			if err := guard.check(); err != nil {
				return err
			}
			if err := cl.importBatch(ctx, b.collection, b); err != nil {
				return err
			}
		}
		return cl.verifyBatch(ctx, b.collection, b)
	}, func(send func(batch) error) error {
		batchesOut = newBatches(o.BatchSize, func(b batch) error {
			n := uint64(len(b.data))
			if n > o.MaxEncodedBytes-encoded {
				return fmt.Errorf("shared encoded-document budget exceeded")
			}
			encoded += n
			return send(b)
		})
		needed := map[string]bool{}
		err := c.ReadAdditions(ctx, r.options.Conduits, r.loaded.c, o.Calculation, updated, func(d c.Decision) error {
			a, e, err := sharedDocuments(base, d)
			if err != nil {
				return err
			}
			if len(needed) >= 100000 && !needed[*d.ConduitID] {
				return fmt.Errorf("shared entity context exceeds bound")
			}
			needed[*d.ConduitID] = true
			if err = batchesOut.add(appearances, a.Key, a); err != nil {
				return err
			}
			return batchesOut.add(conduits, e.Key, e)
		})
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(needed))
		for id := range needed {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			if err = batchesOut.add(entities, id, r.loaded.entity(id)); err != nil {
				return err
			}
		}
		return batchesOut.finish()
	})
	if err != nil {
		return out, err
	}
	if batchesOut.counts[appearances] != d.Added || batchesOut.counts[conduits] != d.Added {
		return out, fmt.Errorf("shared projection membership mismatch")
	}
	for _, name := range collections {
		n, err := cl.count(ctx, name)
		if err != nil {
			return out, err
		}
		want := batchesOut.counts[name]
		if name == metadata && out.Reused {
			want = 1
		}
		if n != want {
			return out, fmt.Errorf("shared collection count differs: %s", name)
		}
	}
	complete := sharedCompletion{d, batchesOut.counts, batchesOut.digests()}
	raw, _ := json.Marshal(complete)
	if out.Reused && !equalJSON(raw, prior) {
		return out, fmt.Errorf("shared full replay changed")
	}
	if err = r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	if !out.Reused {
		if err = guard.check(); err != nil {
			return out, err
		}
		if err = cl.importBatch(ctx, metadata, batch{collection: metadata, keys: []string{d.Key}, data: append(raw, '\n')}); err != nil {
			return out, err
		}
	}
	if err = cl.verifyBatch(ctx, metadata, batch{keys: []string{d.Key}, data: append(raw, '\n')}); err != nil {
		return out, err
	}
	out.Manifest = filepath.Join(dir, "manifest.json")
	if err = writePublicationFile(out.Manifest, raw, true); err != nil {
		return out, err
	}
	out.EncodedBytes = batchesOut.bytes
	out.View = sharedView(complete, raw, updated)
	return out, nil
}

func sharedView(v sharedCompletion, raw []byte, updated c.Result) SharedView {
	return SharedView{Projection: Reference{v.Key, digest(raw)}, Database: "lt_receipt_shared_" + v.Cycle + "_" + v.Key[:32], Base: v.Base, Calculation: v.Calculation, FactSet: v.FactSet, Policy: v.Policy, Added: v.Added, RemainingShared: updated.States["shared_related_record_unresolved"], Counts: v.Counts, Digests: v.Digests, EndpointJoin: "appearance=(fact_set_id,source_row_ordinal); committee=exact_reported_FEC_ID; base-generation facets retained"}
}

func (r *CycleReader) verifySharedBaseMembers(ctx context.Context, b batch) error {
	var edges []conduit
	for _, line := range bytes.Split(bytes.TrimSuffix(b.data, []byte{'\n'}), []byte{'\n'}) {
		var e conduit
		if strictjson.Decode(line, &e) != nil {
			return fmt.Errorf("invalid shared edge batch")
		}
		edges = append(edges, e)
	}
	index := 0
	err := r.cl.query(ctx, "FOR k IN @keys LET d = DOCUMENT(CONCAT(@collection, '/', k)) RETURN d == null ? null : UNSET(d, '_id', '_rev')", map[string]any{"keys": b.keys, "collection": appearances}, func(raw json.RawMessage) error {
		if index >= len(edges) {
			return fmt.Errorf("extra base appearance")
		}
		var a compactAppearance
		if strictjson.Decode(raw, &a) != nil {
			return fmt.Errorf("missing or invalid base appearance")
		}
		e := edges[index]
		want := e.Decision
		want.State = "shared_related_record_unresolved"
		want.ConduitID = nil
		if a.Key != e.Key || a.FactSet != e.FactSet || uint64(a.Ordinal) != e.Decision.Ordinal || a.IdentityResolved || a.ConduitState != want.State || !reflect.DeepEqual(a.Conduit, &want) {
			return fmt.Errorf("base appearance does not retain the exact prior decision")
		}
		index++
		return nil
	})
	if err != nil {
		return err
	}
	if index != len(edges) {
		return fmt.Errorf("base appearance membership incomplete")
	}
	return nil
}

func sharedStorage(o SharedOptions, dir string, d sharedDefinition) (*storageGuard, error) {
	info, err := os.Stat(filepath.Join(o.ArangoDataDirectory, "ENGINE"))
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("actual Arango ENGINE file required")
	}
	sample, err := filesystemSample(o.ArangoDataDirectory)
	if err != nil {
		return nil, err
	}
	var saved struct {
		Definition sharedDefinition `json:"definition"`
		Storage    StorageEnvelope  `json:"storage"`
	}
	saved.Definition = d
	saved.Storage = StorageEnvelope{sample, o.ReserveFreeBytes, o.MaxFilesystemGrowthBytes, o.MaxEncodedBytes}
	path := filepath.Join(dir, "storage.json")
	if b, err := manifestBytes(path); err == nil {
		if strictjson.Decode(b, &saved) != nil || !reflect.DeepEqual(saved.Definition, d) || saved.Storage.ReserveFreeBytes != o.ReserveFreeBytes || saved.Storage.MaxGrowthBytes != o.MaxFilesystemGrowthBytes || saved.Storage.MaxEncodedBytes != o.MaxEncodedBytes {
			return nil, fmt.Errorf("shared storage retry envelope differs")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	} else {
		if sample.Available < o.ReserveFreeBytes+o.MaxFilesystemGrowthBytes {
			return nil, fmt.Errorf("shared storage admission requires reserve plus growth budget")
		}
		if err = os.MkdirAll(dir, 0750); err != nil {
			return nil, err
		}
		b, _ := json.Marshal(saved)
		if err = writePublicationFile(path, b, true); err != nil {
			return nil, err
		}
	}
	g := &storageGuard{directory: o.ArangoDataDirectory, envelope: saved.Storage, sample: filesystemSample}
	if err = g.check(); err != nil {
		return nil, err
	}
	return g, nil
}
