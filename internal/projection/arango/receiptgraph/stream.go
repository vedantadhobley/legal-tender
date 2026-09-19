package receiptgraph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"reflect"
	"sync"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	xs "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

type batch struct {
	collection string
	keys       []string
	data       []byte
	evidence   []byte
	readOnly   bool
}

// Encode while the source callback owns borrowed Parquet strings. Workers never
// retain Row values or depend on the reader's next buffer lifetime.
type batches struct {
	pending      map[string]batch
	counts       map[string]uint64
	bytes        map[string]uint64
	hashes       map[string]hash.Hash
	sourceHashes map[string]hash.Hash
	size         int
	send         func(batch) error
}

func newBatches(size int, send func(batch) error) *batches {
	return &batches{pending: map[string]batch{}, counts: map[string]uint64{}, bytes: map[string]uint64{}, hashes: map[string]hash.Hash{}, sourceHashes: map[string]hash.Hash{}, size: size, send: send}
}
func (b *batches) add(collection, key string, v any) error {
	return b.addEvidence(collection, key, v, nil)
}

// A compact appearance carries its source-backed expanded counterpart only in
// the bounded worker batch, never in the database. The combined payload is capped.
func (b *batches) addEvidence(collection, key string, v, evidence any) error {
	raw, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(raw) > 65536 {
		return fmt.Errorf("graph document exceeds 64KiB cap")
	}
	raw = append(raw, '\n')
	var source []byte
	if evidence != nil {
		source, e = json.Marshal(evidence)
		if e != nil {
			return e
		}
		if len(source) > 65536 {
			return fmt.Errorf("source evidence document exceeds 64KiB cap")
		}
		source = append(source, '\n')
	}
	pending := b.pending[collection]
	if len(pending.keys) > 0 && (len(pending.evidence) > 0) != (source != nil) {
		return fmt.Errorf("mixed source-proof shape in batch")
	}
	if len(pending.data)+len(pending.evidence)+len(raw)+len(source) > 8<<20 {
		if e = b.flush(collection); e != nil {
			return e
		}
		pending = batch{}
	}
	pending.collection = collection
	pending.keys = append(pending.keys, key)
	pending.data = append(pending.data, raw...)
	pending.evidence = append(pending.evidence, source...)
	b.pending[collection] = pending
	if b.hashes[collection] == nil {
		b.hashes[collection] = sha256.New()
	}
	_, _ = b.hashes[collection].Write(raw)
	if b.sourceHashes[collection] == nil {
		b.sourceHashes[collection] = sha256.New()
	}
	if source == nil {
		source = raw
	}
	_, _ = b.sourceHashes[collection].Write(source)
	b.counts[collection]++
	b.bytes[collection] += uint64(len(raw))
	if len(pending.keys) >= b.size {
		return b.flush(collection)
	}
	return nil
}
func (b *batches) flush(collection string) error {
	v := b.pending[collection]
	if len(v.keys) == 0 {
		return nil
	}
	if e := b.send(v); e != nil {
		return e
	}
	delete(b.pending, collection)
	return nil
}
func (b *batches) finish() error {
	for _, name := range collections {
		if e := b.flush(name); e != nil {
			return e
		}
	}
	return nil
}
func (b *batches) digests() map[string]string {
	return documentDigests(b.hashes)
}
func (b *batches) sourceDigests() map[string]string { return documentDigests(b.sourceHashes) }
func documentDigests(hashes map[string]hash.Hash) map[string]string {
	m := map[string]string{}
	for _, name := range collections {
		if h := hashes[name]; h != nil {
			m[name] = hex.EncodeToString(h.Sum(nil))
		} else {
			m[name] = digest(nil)
		}
	}
	return m
}

func withWorkers(ctx context.Context, n int, consume func(context.Context, batch) error, produce func(func(batch) error) error) error {
	return withWorkerBarrier(ctx, n, consume, func(send func(batch) error, _ func() error) error { return produce(send) })
}

// The producer calls barrier only between source shards, after flushing all
// collection buffers. No new jobs are submitted while that barrier waits.
func withWorkerBarrier(ctx context.Context, n int, consume func(context.Context, batch) error, produce func(func(batch) error, func() error) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan batch, 1)
	var wg sync.WaitGroup
	var pending sync.WaitGroup
	var first error
	var once sync.Once
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range jobs {
				if ctx.Err() != nil {
					pending.Done()
					continue
				}
				if e := consume(ctx, b); e != nil {
					once.Do(func() { first = e; cancel() })
				}
				pending.Done()
			}
		}()
	}
	e := produce(func(b batch) error {
		pending.Add(1)
		select {
		case jobs <- b:
			return nil
		case <-ctx.Done():
			pending.Done()
			return ctx.Err()
		}
	}, func() error {
		pending.Wait()
		return ctx.Err()
	})
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	if e != nil {
		return e
	}
	return ctx.Err()
}

// Scan the complete disposition stream for physical/value/census validation.
// Read every intersecting participant shard to EOF, but emit only the declared
// ordinal range. No range-size map and no whole-report buffering is used.
func stream(ctx context.Context, o Options, l loaded, visit func(p.Row, *c.Decision) error) error {
	return streamWith(ctx, o, l, func(f p.File, visit func(p.Row) error) error {
		_, e := p.ReadShard(ctx, participantDir(o), f, visit)
		return e
	}, visit)
}

func streamWith(ctx context.Context, o Options, l loaded, read func(p.File, func(p.Row) error) error, visit func(p.Row, *c.Decision) error) error {
	r, e := xs.Open(ctx, filepath.Join(filepath.Dir(o.Conduits), "data"), l.c.Decisions)
	if e != nil {
		return e
	}
	defer r.Close()
	var d c.Decision
	var end bool
	var previous, total uint64
	states := map[string]uint64{}
	amounts := map[string]uint64{}
	advance := func() error {
		v, e := r.Next()
		if e == io.EOF {
			end = true
			return nil
		}
		if e != nil {
			return e
		}
		d, e = c.DecodeDecision(v, l.p.SourceRows)
		if e != nil {
			return e
		}
		if d.Ordinal <= previous {
			return fmt.Errorf("duplicate or unordered conduit decision")
		}
		previous = d.Ordinal
		total++
		states[d.State]++
		amounts[d.AmountComparison]++
		return nil
	}
	if e = advance(); e != nil {
		return e
	}
	last := o.First + o.Rows - 1
	var selected uint64
	for _, f := range l.p.Files {
		if f.Last < o.First || f.First > last {
			continue
		}
		e = read(f, func(row p.Row) error {
			n := uint64(row.Ordinal)
			if n < o.First || n > last {
				return nil
			}
			for !end && d.Ordinal < n {
				if e := advance(); e != nil {
					return e
				}
			}
			applies := policy.Applies(policy.Evidence{Memo: row.Memo, ReceiptType: row.ReceiptType})
			has := !end && d.Ordinal == n
			if has != applies {
				return fmt.Errorf("conduit/participant membership mismatch")
			}
			var decision *c.Decision
			if has {
				decision = &d
			}
			if e := visit(row, decision); e != nil {
				return e
			}
			selected++
			if has {
				return advance()
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	for !end {
		if e = advance(); e != nil {
			return e
		}
	}
	if selected != o.Rows || total != l.c.EligibleRoleRows || !reflect.DeepEqual(states, l.c.States) || !reflect.DeepEqual(amounts, l.c.Amounts) {
		return fmt.Errorf("complete disposition or selected participant census mismatch")
	}
	return nil
}
