package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

type completion struct {
	definition
	Counts               map[string]uint64 `json:"counts"`
	Digests              map[string]string `json:"ordered_document_values_sha256"`
	Unrouted             uint64            `json:"unrouted_receipts"`
	States               map[string]uint64 `json:"conduit_states"`
	SourceEvidenceSHA256 map[string]string `json:"source_evidence_document_values_sha256,omitempty"`
}

func Run(ctx context.Context, o Options) (Result, error) {
	start := time.Now()
	var result Result
	if o.Layout == "" {
		o.Layout = ExpandedLayout
	}
	if o.FullCycle {
		if e := validateCycleOptions(o); e != nil {
			return result, e
		}
	}
	if (o.Layout != ExpandedLayout && o.Layout != CompactLayout) || (o.CompareResult == "") != (o.CompareSHA256 == "") || (o.CompareResult != "" && (o.Layout != CompactLayout || !validDigest(o.CompareSHA256))) {
		return result, fmt.Errorf("supported layout and exact compact-v2 comparison path/digest required")
	}
	if (!o.FullCycle && (o.First == 0 || o.Rows == 0 || o.Rows > 1000000)) || o.Workers < 1 || o.Workers > 8 || o.BatchSize < 1 || o.BatchSize > 5000 || !validDigest(o.BuildSHA256) || o.StorageRoot == "" || o.LockDirectory == "" {
		return result, fmt.Errorf("sample range 1..1000000, workers 1..8, batch 1..5000, build and lock/storage roots required")
	}
	if o.Progress == nil {
		o.Progress = func(string) {}
	}
	if _, e := newClient(o, "lt_receipt_sample_validation"); e != nil {
		return result, e
	}
	o.Progress("validating exact participant, conduit, source and authorization ancestry")
	l, e := load(ctx, o)
	if e != nil {
		return result, e
	}
	if o.FullCycle {
		o.First, o.Rows = 1, l.p.SourceRows
	}
	comparison, e := loadBaseline(ctx, o, l)
	if e != nil {
		return result, e
	}
	result.Definition = l.definition
	result.Database = "lt_receipt_sample_" + l.p.Cycle + "_" + l.definition.Key[:32]
	if o.FullCycle {
		result.Database = "lt_receipt_cycle_" + l.p.Cycle + "_" + l.definition.Key[:32]
	}
	result.Workers = o.Workers
	result.BatchSize = o.BatchSize
	cl, e := newClient(o, result.Database)
	if e != nil {
		return result, e
	}
	unlock, e := lock(o.LockDirectory, l.definition.Key)
	if e != nil {
		return result, e
	}
	defer unlock()
	var cycle *cycleRun
	create := true
	if o.FullCycle {
		cycle, e = openCycle(o, l.definition, l.p.Files)
		if e != nil {
			return result, e
		}
		create = cycle.resumeRows == 0 && cycle.published == nil
		if create {
			if e = cycle.beforeWrite(); e != nil {
				return result, e
			}
		}
		o.Progress(fmt.Sprintf("cycle publication %s; checkpoint %d/%d rows; reserve %d bytes; net filesystem-growth cap %d bytes", l.definition.Key, cycle.resumeRows, o.Rows, o.ReserveFreeBytes, o.MaxFilesystemGrowthBytes))
	}
	if e = cl.ensureSchema(ctx, create); e != nil {
		return result, e
	}
	var prior json.RawMessage
	e = cl.query(ctx, "FOR d IN projection_metadata RETURN UNSET(d, '_id', '_rev')", map[string]any{}, func(raw json.RawMessage) error {
		if prior != nil {
			return fmt.Errorf("extra completion metadata")
		}
		var v completion
		if e := json.Unmarshal(raw, &v); e != nil {
			return e
		}
		a, _ := json.Marshal(v.definition)
		b, _ := json.Marshal(l.definition)
		if !equalJSON(a, b) {
			return fmt.Errorf("incompatible completion ancestry")
		}
		prior = append(json.RawMessage{}, raw...)
		return nil
	})
	if e != nil {
		return result, e
	}
	result.Reused = prior != nil
	if cycle != nil && cycle.published != nil && !equalJSON(prior, cycle.published) {
		return result, fmt.Errorf("published manifest and graph completion differ")
	}
	result.ConduitStates = map[string]uint64{}
	needed := map[string]bool{}
	var pathOrdinal, conduitOrdinal, relatedOrdinal uint64
	authorized := map[string]bool{}
	for _, a := range l.links {
		if a.State == "authorized" {
			authorized[strings.TrimPrefix(a.From, entities+"/")] = true
		}
	}
	var batchesOut *batches
	streamStart := time.Now()
	o.Progress(fmt.Sprintf("streaming at most %d occurrences with %d bounded import/readback workers; completed replay is read-only", o.Rows, o.Workers))
	e = withWorkerBarrier(ctx, o.Workers, func(ctx context.Context, b batch) error {
		if !result.Reused && !b.readOnly {
			if e := cycle.beforeWrite(); e != nil {
				return e
			}
			if e := cl.importBatch(ctx, b.collection, b); e != nil {
				return e
			}
		}
		if e := cl.verifyBatch(ctx, b.collection, b); e != nil {
			return e
		}
		if comparison != nil {
			return comparison.verify(ctx, b)
		}
		return nil
	}, func(send func(batch) error, barrier func() error) error {
		readOnly := false
		b := newBatches(o.BatchSize, func(v batch) error {
			v.readOnly = readOnly
			if e := cycle.admit(v); e != nil {
				return e
			}
			return send(v)
		})
		batchesOut = b
		addID := func(id string) error {
			if len(needed) >= 100000 && !needed[id] {
				return fmt.Errorf("referenced entity context exceeds 100000 cap")
			}
			needed[strings.Clone(id)] = true
			return nil
		}
		e := streamWith(ctx, o, l, func(f p.File, visit func(p.Row) error) error {
			readOnly = cycle != nil && f.Last <= cycle.resumeRows
			_, err := p.ReadShard(ctx, participantDir(o), f, visit)
			if err != nil {
				return err
			}
			if cycle == nil {
				return nil
			}
			if err = b.finish(); err != nil {
				return err
			}
			if err = barrier(); err != nil {
				return err
			}
			if err = cycle.mark(f.Last, result, b, result.Reused); err != nil {
				return err
			}
			o.Progress(fmt.Sprintf("verified source rows %d/%d; elapsed %s; checkpointed=%t; read_only=%t", f.Last, o.Rows, time.Since(start).Round(time.Second), !result.Reused, result.Reused || readOnly))
			return nil
		}, func(row p.Row, d *c.Decision) error {
			a, r, v, e := project(l.p.FactSetID, row, d)
			if e != nil {
				return e
			}
			result.ConduitStates[a.ConduitState]++
			var proof any
			if o.Layout == CompactLayout {
				proof = a
			}
			if e = b.addEvidence(appearances, a.Key, physicalAppearance(o.Layout, a), proof); e != nil {
				return e
			}
			if r == nil {
				result.Unrouted++
			} else {
				if e = addID(*row.Recipient); e != nil {
					return e
				}
				if pathOrdinal == 0 && !row.Memo && authorized[*row.Recipient] {
					pathOrdinal = uint64(row.Ordinal)
				}
				if e = b.add(receipts, r.Key, r); e != nil {
					return e
				}
			}
			if v != nil {
				if e = addID(*v.Decision.ConduitID); e != nil {
					return e
				}
				if conduitOrdinal == 0 {
					conduitOrdinal = v.Decision.Ordinal
					relatedOrdinal = v.Decision.Related
				}
				if e = b.add(conduits, v.Key, v); e != nil {
					return e
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
		readOnly = false // Context is checkpointed by final graph completion only.
		// Authorization is context, never an extra money ledger. Preserve unresolved
		// and unauthorized states too; positive traversal filters the shared policy.
		for _, a := range l.links {
			if e = addID(strings.TrimPrefix(a.From, entities+"/")); e != nil {
				return e
			}
			if e = addID(strings.TrimPrefix(a.To, entities+"/")); e != nil {
				return e
			}
			if e = b.add(authorizations, a.Key, a); e != nil {
				return e
			}
		}
		ids := make([]string, 0, len(needed))
		for id := range needed {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			v := l.entity(id)
			if v.State == "missing_same_cycle_master" {
				result.MissingMasters++
			}
			if e = b.add(entities, id, v); e != nil {
				return e
			}
		}
		return b.finish()
	})
	if e != nil {
		return result, e
	}
	result.ImportReadbackMS = time.Since(streamStart).Milliseconds()
	result.Counts = batchesOut.counts
	result.PayloadSHA256 = batchesOut.digests()
	result.PayloadBytes = batchesOut.bytes
	if o.Layout == CompactLayout {
		result.SourceEvidenceSHA256 = batchesOut.sourceDigests()
	}
	if result.Counts[appearances] != o.Rows || result.Counts[receipts]+result.Unrouted != o.Rows {
		return result, fmt.Errorf("selected occurrence conservation failed")
	}
	if o.FullCycle && result.Counts[conduits] != l.c.Qualified {
		return result, fmt.Errorf("complete-cycle qualified conduit conservation failed")
	}
	for _, name := range collections {
		if name == metadata {
			continue
		}
		n, e := cl.count(ctx, name)
		if e != nil {
			return result, e
		}
		if n != result.Counts[name] {
			return result, fmt.Errorf("extra or missing %s documents", name)
		}
	}
	o.Progress("checking typed candidate paths, source drilldown and storage before completion")
	if e = queryAndSourceGate(ctx, cl, o, l, &result, pathOrdinal, conduitOrdinal, relatedOrdinal); e != nil {
		return result, e
	}
	if comparison != nil {
		result.Comparison, e = comparison.finish(result)
		if e != nil {
			return result, e
		}
	}
	result.Storage = map[string]json.RawMessage{}
	for _, name := range collections {
		if name == metadata {
			continue
		}
		var v json.RawMessage
		if e = cl.json(ctx, "GET", "/_api/collection/"+name+"/figures", nil, &v); e != nil {
			return result, e
		}
		result.Storage[name] = v
	}
	want := completion{definition: l.definition, Counts: result.Counts, Digests: result.PayloadSHA256, Unrouted: result.Unrouted, States: result.ConduitStates, SourceEvidenceSHA256: result.SourceEvidenceSHA256}
	raw, _ := json.Marshal(want)
	if result.Reused {
		if !equalJSON(prior, raw) {
			return result, fmt.Errorf("completion readback checks changed")
		}
	} else {
		b := batch{collection: metadata, keys: []string{l.definition.Key}, data: append(raw, '\n')}
		if e = cycle.beforeWrite(); e != nil {
			return result, e
		}
		if e = cl.importBatch(ctx, metadata, b); e != nil {
			return result, e
		}
	}
	if e = cl.verifyBatch(ctx, metadata, batch{keys: []string{l.definition.Key}, data: append(raw, '\n')}); e != nil {
		return result, e
	}
	if n, e := cl.count(ctx, metadata); e != nil || n != 1 {
		if e != nil {
			return result, e
		}
		return result, fmt.Errorf("completion count mismatch")
	}
	result.Publication, e = cycle.publish(raw)
	if e != nil {
		return result, e
	}
	var u syscall.Rusage
	if e = syscall.Getrusage(syscall.RUSAGE_SELF, &u); e != nil {
		return result, e
	}
	result.PeakRSSBytes = uint64(u.Maxrss) * 1024
	result.ElapsedMS = time.Since(start).Milliseconds()
	return result, nil
}

func lock(dir, id string) (func(), error) {
	if e := os.MkdirAll(dir, 0750); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(dir, "receipt-sample-"+id+".lock"), os.O_CREATE|os.O_RDWR, 0640)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("projection already has a publisher")
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}

func queryAndSourceGate(ctx context.Context, cl *client, o Options, l loaded, r *Result, pathOrdinal, conduitOrdinal, relatedOrdinal uint64) error {
	if pathOrdinal != 0 {
		key, _ := p.AppearanceID(l.p.FactSetID, pathOrdinal)
		e := cl.query(ctx, "FOR committee, receipt IN 1..1 OUTBOUND @start reported_receipts FOR candidate, authorization IN 1..1 OUTBOUND committee candidate_authorization_context FILTER authorization.authorization_state == 'authorized' LIMIT 5 RETURN {appearance:@start, committee:committee._id, candidate:candidate._id, receipt:UNSET(receipt, '_id', '_rev'), authorization:UNSET(authorization, '_id', '_rev')}", map[string]any{"start": appearances + "/" + key}, func(raw json.RawMessage) error {
			r.Queries = append(r.Queries, append(json.RawMessage{}, raw...))
			return nil
		})
		if e != nil {
			return e
		}
		if len(r.Queries) == 0 {
			return fmt.Errorf("expected candidate path absent")
		}
	}
	if conduitOrdinal != 0 {
		key, _ := p.AppearanceID(l.p.FactSetID, conduitOrdinal)
		n := 0
		e := cl.query(ctx, "FOR v, e IN 1..1 OUTBOUND @start reported_conduit_associations RETURN UNSET(e, '_id', '_rev')", map[string]any{"start": appearances + "/" + key}, func(raw json.RawMessage) error {
			n++
			var v conduit
			if e := json.Unmarshal(raw, &v); e != nil {
				return e
			}
			if v.Decision.Ordinal != conduitOrdinal || v.Decision.Related != relatedOrdinal || v.AdditionalAmount != "0" || v.FinancialEligibility {
				return fmt.Errorf("conduit path evidence mismatch")
			}
			r.ConduitWitness = append(json.RawMessage{}, raw...)
			return nil
		})
		if e != nil {
			return e
		}
		if n != 1 {
			return fmt.Errorf("conduit path absent or duplicated")
		}
	}
	ordinals := []uint64{o.First, pathOrdinal, conduitOrdinal, relatedOrdinal}
	seen := map[uint64]bool{}
	for _, n := range ordinals {
		if n == 0 || seen[n] {
			continue
		}
		seen[n] = true
		proof, e := l.inspector.Inspect(ctx, n)
		if e != nil {
			return e
		}
		// A related memo may be outside this sample. Its exact source locator remains
		// valid without inventing a dangling memo vertex or claiming it was imported.
		if n >= o.First && n-o.First < o.Rows {
			key, _ := p.AppearanceID(l.p.FactSetID, n)
			count := 0
			e = cl.query(ctx, "FOR d IN contributor_appearances FILTER d._key == @key RETURN UNSET(d, '_id', '_rev')", map[string]any{"key": key}, func(raw json.RawMessage) error {
				count++
				var a appearance
				if o.Layout == CompactLayout {
					var compactRow compactAppearance
					if e := json.Unmarshal(raw, &compactRow); e != nil {
						return e
					}
					var err error
					a, err = expand(compactRow, proof.Participant)
					if err != nil {
						return err
					}
				} else if e := json.Unmarshal(raw, &a); e != nil {
					return e
				}
				x, _ := json.Marshal(a.Row)
				y, _ := json.Marshal(proof.Participant)
				if a.FactSet != proof.FactSetID || a.Key != proof.AppearanceID || !equalJSON(x, y) {
					return fmt.Errorf("graph source drilldown mismatch")
				}
				return nil
			})
			if e != nil {
				return e
			}
			if count != 1 {
				return fmt.Errorf("missing source appearance")
			}
		}
		r.SourceChecks = append(r.SourceChecks, proof)
	}
	return nil
}
