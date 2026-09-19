package receiptgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func cycleFixture(t *testing.T) (Options, definition, []p.File) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ENGINE"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	o := Options{FullCycle: true, Layout: CompactLayout, PublicationDirectory: filepath.Join(dir, "publications"), ArangoDataDirectory: dir, ReserveFreeBytes: 1, MaxFilesystemGrowthBytes: 1 << 30, MaxEncodedBytes: 1 << 20}
	d := definition{Key: digest([]byte("cycle fixture")), Version: CycleVersion, State: CycleState, First: 1, Rows: 12, SourceRows: 12}
	return o, d, []p.File{{First: 1, Last: 4}, {First: 5, Last: 8}, {First: 9, Last: 12}}
}

func TestCycleOptionsDoNotAdmitSamplesOrMissingStorageLimits(t *testing.T) {
	o, _, _ := cycleFixture(t)
	if err := validateCycleOptions(o); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Options){func(v *Options) { v.First = 1 }, func(v *Options) { v.Rows = 12 }, func(v *Options) { v.Layout = ExpandedLayout }, func(v *Options) { v.CompareResult = "old.json" }, func(v *Options) { v.ReserveFreeBytes = 0 }, func(v *Options) { v.MaxFilesystemGrowthBytes = 0 }, func(v *Options) { v.MaxEncodedBytes = 0 }, func(v *Options) { v.PublicationDirectory = "" }, func(v *Options) { v.ArangoDataDirectory = "" }, func(v *Options) { v.MaxFilesystemGrowthBytes = ^uint64(0) }} {
		v := o
		mutate(&v)
		if validateCycleOptions(v) == nil {
			t.Fatalf("accepted invalid cycle request: %+v", v)
		}
	}
}

// Exercise the same shard barrier/checkpoint/batch path with a small physical
// store. Failure leaves verified shards immutable; retry replaces only the
// incomplete suffix. Worker and batch layout do not affect evidence.
func TestCycleCheckpointResumeAndCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			o, d, files := cycleFixture(t)
			var mu sync.Mutex
			store := map[string]string{}
			writes := map[string]int{}
			interrupted := errors.New("injected interruption")
			run := func(workers, size int, fail, completed bool) (*cycleRun, *batches, error) {
				cycle, err := openCycle(o, d, files)
				if err != nil {
					return nil, nil, err
				}
				r := Result{Definition: d, ConduitStates: map[string]uint64{}}
				var out *batches
				err = withWorkerBarrier(context.Background(), workers, func(_ context.Context, b batch) error {
					mu.Lock()
					defer mu.Unlock()
					if !b.readOnly && !completed {
						store[b.keys[0]] = string(b.data)
						writes[b.keys[0]]++
					}
					if store[b.keys[0]] != string(b.data) {
						return errors.New("corrupt stored batch")
					}
					return nil
				}, func(send func(batch) error, barrier func() error) error {
					readOnly := false
					// Use keyed individual values in the fake store so real batch sizes
					// can vary independently of its physical shape.
					b := newBatches(size, func(v batch) error {
						if err := cycle.admit(v); err != nil {
							return err
						}
						var rows []json.RawMessage
						dec := json.NewDecoder(bytes.NewReader(v.data))
						for range v.keys {
							var row json.RawMessage
							if err := dec.Decode(&row); err != nil {
								return err
							}
							rows = append(rows, row)
						}
						for i, k := range v.keys {
							if err := send(batch{collection: v.collection, keys: []string{k}, data: rows[i], readOnly: readOnly}); err != nil {
								return err
							}
						}
						return nil
					})
					out = b
					for _, f := range files {
						readOnly = f.Last <= cycle.resumeRows
						for n := f.First; n <= f.Last; n++ {
							if fail && n == 7 {
								return interrupted
							}
							r.Unrouted++
							r.ConduitStates["unresolved"]++
							if err := b.add(appearances, fmt.Sprint(n), map[string]any{"_key": fmt.Sprint(n), "ordinal": n}); err != nil {
								return err
							}
						}
						if err := b.finish(); err != nil {
							return err
						}
						if err := barrier(); err != nil {
							return err
						}
						if err := cycle.mark(f.Last, r, b, completed); err != nil {
							return err
						}
					}
					return nil
				})
				return cycle, out, err
			}
			cycle, _, err := run(4, 1, true, false)
			if !errors.Is(err, interrupted) {
				t.Fatal(err)
			}
			if _, err = os.Stat(filepath.Join(cycle.dir, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("partial run published")
			}
			checkpointBytes, err := os.ReadFile(filepath.Join(cycle.dir, "progress.json"))
			if err != nil {
				t.Fatal(err)
			}
			var cp checkpoint
			if err = decodeCheckpoint(checkpointBytes, d, files, &cp); err != nil || cp.Last != 4 {
				t.Fatalf("checkpoint: %v %+v", err, cp)
			}
			before := map[string]int{}
			for k, v := range writes {
				before[k] = v
			}
			if corrupt {
				store["2"] = "corrupt"
			}
			cycle, b, err := run(1, 3, false, false)
			if corrupt {
				if err == nil {
					t.Fatal("repaired checkpoint corruption")
				}
				if writes["2"] != before["2"] {
					t.Fatal("overwrote checkpointed row")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cycle.resumeRows != 4 || cycle.prior.Last != 12 || len(store) != 12 {
				t.Fatal("resume lost membership")
			}
			for _, k := range []string{"1", "2", "3", "4"} {
				if writes[k] != before[k] {
					t.Fatal("reimported verified prefix")
				}
			}
			physical, source := b.digests(), b.sourceDigests()
			beforeReplay := map[string]int{}
			for k, v := range writes {
				beforeReplay[k] = v
			}
			_, b, err = run(8, 2, false, true)
			if err != nil || !reflect.DeepEqual(physical, b.digests()) || !reflect.DeepEqual(source, b.sourceDigests()) {
				t.Fatalf("replay changed values: %v", err)
			}
			if !reflect.DeepEqual(beforeReplay, writes) {
				t.Fatal("completed replay wrote data")
			}
		})
	}
}

func TestCheckpointRejectsTamperingAncestryAndPartialShard(t *testing.T) {
	o, d, files := cycleFixture(t)
	v, err := openCycle(o, d, files)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(v.dir, "progress.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*checkpoint){func(c *checkpoint) { c.Last = 3 }, func(c *checkpoint) { c.Definition.Build = "different" }, func(c *checkpoint) { c.Bytes = map[string]uint64{"changed": 1} }} {
		var c checkpoint
		if err = json.Unmarshal(b, &c); err != nil {
			t.Fatal(err)
		}
		mutate(&c)
		raw, _ := json.Marshal(c)
		if decodeCheckpoint(raw, d, files, &checkpoint{}) == nil {
			t.Fatal("accepted altered checkpoint")
		}
	}
	v.prior.Last = 3
	if err = v.store(); err != nil {
		t.Fatal(err)
	}
	if _, err = openCycle(o, d, files); err == nil {
		t.Fatal("accepted hashed partial-shard checkpoint")
	}
}

func TestImmutableCompletionAndEncodedCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := writePublicationFile(path, []byte(`{"complete":true}`), true); err != nil {
		t.Fatal(err)
	}
	if err := writePublicationFile(path, []byte(`{"complete":true}`), true); err != nil {
		t.Fatal(err)
	}
	if err := writePublicationFile(path, []byte(`{"complete":false}`), true); err == nil {
		t.Fatal("overwrote completion")
	}
	v := &cycleRun{prior: checkpoint{Storage: StorageEnvelope{MaxEncodedBytes: 5}}}
	if err := v.admit(batch{data: []byte("12345")}); err != nil {
		t.Fatal(err)
	}
	if err := v.admit(batch{data: []byte("6")}); err == nil {
		t.Fatal("exceeded encoded budget")
	}
}
