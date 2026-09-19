package receiptconduits

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

type sourceRead func(context.Context, int, func(participants.Row) error) (participants.Census, error)
type work struct {
	ctx   context.Context
	space *xsort.Workspace
	p     participants.Result
	t     refs.TopologyResult
	read  sourceRead
	o     Options
}

func parallel(ctx context.Context, workers, count int, visit func(context.Context, int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for range workers {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= count || ctx.Err() != nil {
					return
				}
				if err := visit(ctx, i); err != nil {
					mu.Lock()
					if first == nil || errors.Is(first, context.Canceled) {
						first = err
					}
					mu.Unlock()
					cancel()
					return
				}
			}
		})
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

// split preserves ordinal order without sorting or buffering a complete group.
// Requests split by peer ordinal; topology splits by its own ordinal.
func (w work) split(dir string, f xsort.File, endpoint bool) ([]xsort.File, error) {
	r, err := xsort.Open(w.ctx, dir, f)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	next, nextErr := r.Next()
	out := make([]xsort.File, len(w.p.Files))
	var previous, seen, exact, unsafe uint64
	for i, s := range w.p.Files {
		writer, err := w.space.Writer(w.ctx)
		if err != nil {
			return nil, err
		}
		finish := func() (xsort.File, error) {
			defer writer.Abort()
			for nextErr == nil {
				if len(next.Key) != 8 {
					return xsort.File{}, fmt.Errorf("invalid ordinal split key")
				}
				ordinal := binary.BigEndian.Uint64([]byte(next.Key))
				if ordinal > s.Last {
					break
				}
				if ordinal < s.First {
					return xsort.File{}, fmt.Errorf("split ordinal outside dense input")
				}
				if endpoint {
					e, err := refs.DecodeEndpoint(next, w.p.SourceRows)
					if err != nil {
						return xsort.File{}, err
					}
					if e.Ordinal <= previous {
						return xsort.File{}, fmt.Errorf("repeated topology endpoint")
					}
					previous = e.Ordinal
					if e.Peers > 0 {
						exact++
					}
					if e.UnsafeReasons != 0 {
						unsafe++
					}
				}
				if err := writer.Add(next); err != nil {
					return xsort.File{}, err
				}
				seen++
				next, nextErr = r.Next()
			}
			if nextErr != nil && nextErr != io.EOF {
				return xsort.File{}, nextErr
			}
			return writer.Finish()
		}
		out[i], err = finish()
		if err != nil {
			return nil, err
		}
	}
	if nextErr != io.EOF || seen != f.Rows {
		return nil, fmt.Errorf("unconsumed split input")
	}
	if endpoint && (exact != w.t.ExactEndpointRows || unsafe != w.t.UnsafeEndpointRows) {
		return nil, fmt.Errorf("topology census mismatch")
	}
	return out, nil
}

// scan aligns an immutable participant shard with its sparse endpoint stream.
// It always exhausts both, including occurrences with no association role.
func (w work) scan(ctx context.Context, i int, top xsort.File, visit func(participants.Row, refs.Endpoint) error) (participants.Census, error) {
	r, err := xsort.Open(ctx, w.space.Dir, top)
	if err != nil {
		return participants.Census{}, err
	}
	defer r.Close()
	next, nextErr := r.Next()
	var previous uint64
	c, err := w.read(ctx, i, func(row participants.Row) error {
		ordinal := uint64(row.Ordinal)
		e := refs.Endpoint{Ordinal: ordinal}
		if nextErr == nil {
			v, err := refs.DecodeEndpoint(next, w.p.SourceRows)
			if err != nil {
				return err
			}
			if v.Ordinal < ordinal || v.Ordinal <= previous {
				return fmt.Errorf("misaligned topology endpoint")
			}
			if v.Ordinal == ordinal {
				e = v
				previous = v.Ordinal
				next, nextErr = r.Next()
			}
		}
		if nextErr != nil && nextErr != io.EOF {
			return nextErr
		}
		return visit(row, e)
	})
	if err != nil {
		return c, err
	}
	if nextErr != io.EOF {
		return c, fmt.Errorf("trailing topology endpoint")
	}
	return c, nil
}

func (w work) collect(i int, ctx context.Context, top xsort.File) (xsort.File, xsort.File, participants.Census, uint64, error) {
	requests, err := xsort.New(ctx, w.space, w.o.RunRows, w.o.FanIn)
	if err != nil {
		return xsort.File{}, xsort.File{}, participants.Census{}, 0, err
	}
	immediate, err := w.space.Writer(ctx)
	if err != nil {
		return xsort.File{}, xsort.File{}, participants.Census{}, 0, err
	}
	defer immediate.Abort()
	var eligible uint64
	c, err := w.scan(ctx, i, top, func(row participants.Row, e refs.Endpoint) error {
		if !policy.Applies(evidence(row)) {
			return nil
		}
		eligible++
		if e.Peers == 1 {
			r, err := request(row, e)
			if err != nil {
				return err
			}
			return requests.Add(r)
		}
		d, err := decide(row, e, nil)
		if err != nil {
			return err
		}
		r, err := d.record()
		if err != nil {
			return err
		}
		return immediate.Add(r)
	})
	if err != nil {
		return xsort.File{}, xsort.File{}, c, 0, err
	}
	a, err := requests.Finish()
	if err != nil {
		return a, xsort.File{}, c, 0, err
	}
	b, err := immediate.Finish()
	if err == nil && a.Rows+b.Rows != eligible {
		err = fmt.Errorf("earmark membership mismatch")
	}
	return a, b, c, eligible, err
}

func (w work) qualify(ctx context.Context, i int, top, requests xsort.File) (xsort.File, error) {
	r, err := xsort.Open(ctx, w.space.Dir, requests)
	if err != nil {
		return xsort.File{}, err
	}
	defer r.Close()
	out, err := xsort.New(ctx, w.space, w.o.RunRows, w.o.FanIn)
	if err != nil {
		return xsort.File{}, err
	}
	var groupOutput *xsort.Writer
	if w.o.groups != nil {
		groupOutput, err = w.space.Writer(ctx)
		if err != nil {
			return xsort.File{}, err
		}
		defer groupOutput.Abort()
	}
	next, nextErr := r.Next()
	var count uint64
	_, err = w.scan(ctx, i, top, func(peer participants.Row, e refs.Endpoint) error {
		var group sharedGroup
		var peerRequests uint64
		var accumulator *policy.Group
		var sharedRows uint64
		for nextErr == nil && next.Key <= key(uint64(peer.Ordinal)) {
			original, originTop, err := decodeRequest(next, w.p.SourceRows)
			if err != nil {
				return err
			}
			if originTop.OnlyPeer != uint64(peer.Ordinal) || e.Peers == 0 || (e.Peers == 1 && e.OnlyPeer != uint64(original.Ordinal)) || !reflect.DeepEqual(original.Recipient, peer.Recipient) || !policy.Applies(evidence(original)) {
				return fmt.Errorf("peer membership/recipient mismatch")
			}
			d, err := decide(original, originTop, &policy.Related{Evidence: evidence(peer), Topology: topology(e)})
			if err != nil {
				return err
			}
			if w.o.profile != nil && d.State == sharedRejection {
				group.observe(original, originTop, peer)
			}
			if groupOutput != nil && e.Peers >= 2 {
				if accumulator == nil {
					accumulator, err = policy.NewGroup(uint64(peer.Ordinal), evidence(peer), topology(e))
					if err != nil {
						return err
					}
				}
				if err = accumulator.Observe(uint64(original.Ordinal), evidence(original), topology(originTop), originTop.OnlyPeer); err != nil {
					return err
				}
				if d.State == sharedRejection {
					sharedRows++
				}
			}
			record, err := d.record()
			if err != nil {
				return err
			}
			if err = out.Add(record); err != nil {
				return err
			}
			count++
			peerRequests++
			next, nextErr = r.Next()
		}
		if peerRequests > e.Peers {
			return fmt.Errorf("sole-peer request population exceeds distinct peers")
		}
		if w.o.profile != nil && group.rows != 0 {
			if err := w.o.profile.add(peer, e, peerRequests, group); err != nil {
				return err
			}
		}
		if sharedRows > 0 {
			g := GroupRecord{Related: uint64(peer.Ordinal), SharedRows: sharedRows, Decision: accumulator.Decide()}
			b, err := json.Marshal(g)
			if err != nil {
				return err
			}
			if err = groupOutput.Add(xsort.Record{Key: key(g.Related), Ordinal: g.Related, Data: b}); err != nil {
				return err
			}
		}
		if nextErr != nil && nextErr != io.EOF {
			return nextErr
		}
		return nil
	})
	if err != nil {
		return xsort.File{}, err
	}
	if nextErr != io.EOF || count != requests.Rows {
		return xsort.File{}, fmt.Errorf("unmatched peer request")
	}
	if groupOutput != nil {
		groups, err := groupOutput.Finish()
		if err != nil {
			return xsort.File{}, err
		}
		changes, err := w.groupChanges(ctx, requests, groups)
		if err != nil {
			return xsort.File{}, err
		}
		w.o.groups.groups[i], w.o.groups.changes[i] = groups, changes
	}
	return out.Finish()
}

func sameCensus(a, b participants.Census) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
func sumCensus(cs []participants.Census) participants.Census {
	r := participants.Census{Components: map[string]uint64{}, Routes: map[string]uint64{}, Conduits: map[string]uint64{}, Earmarks: map[string]uint64{}}
	for _, c := range cs {
		r.Rows += c.Rows
		r.MemoRows += c.MemoRows
		r.UnknownAmounts += c.UnknownAmounts
		r.PositiveAmounts += c.PositiveAmounts
		r.NegativeAmounts += c.NegativeAmounts
		r.ZeroAmounts += c.ZeroAmounts
		for j, m := range []map[string]uint64{c.Components, c.Routes, c.Conduits, c.Earmarks} {
			target := []map[string]uint64{r.Components, r.Routes, r.Conduits, r.Earmarks}[j]
			for k, n := range m {
				target[k] += n
			}
		}
	}
	return r
}

// mergeLevel parallelizes disjoint bounded-fan-in merges; the final merge is
// streaming. The shared workspace owns and caps every intermediate artifact.
func (w work) mergeLevel(runs []xsort.File) (xsort.File, error) {
	for len(runs) > 1 {
		count := (len(runs) + w.o.FanIn - 1) / w.o.FanIn
		next := make([]xsort.File, count)
		err := parallel(w.ctx, w.o.Workers, count, func(ctx context.Context, i int) error {
			var err error
			next[i], err = xsort.MergeRuns(ctx, w.space, runs[i*w.o.FanIn:min((i+1)*w.o.FanIn, len(runs))], w.o.FanIn)
			return err
		})
		if err != nil {
			return xsort.File{}, err
		}
		runs = next
	}
	if len(runs) == 0 {
		return xsort.File{}, fmt.Errorf("empty merge input")
	}
	return runs[0], nil
}
