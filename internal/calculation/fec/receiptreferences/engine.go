package receiptreferences

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportreference"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"io"
)

type engine struct {
	ctx                        context.Context
	space                      *xsort.Workspace
	events, pieces, incidences *xsort.Sorter
	filter                     filter
	out                        Result
}

func newEngine(ctx context.Context, s *xsort.Workspace, o Options) (*engine, error) {
	e := &engine{ctx: ctx, space: s, filter: make(filter, o.FilterBytes), out: Result{SchemaVersion: Version, Policy: reportreference.Policy, States: map[string]uint64{}, RunRows: o.RunRows, FanIn: o.FanIn, FilterBytes: o.FilterBytes, Scope: "published_schedule_a_cycle_report"}}
	var err error
	e.events, err = xsort.New(ctx, s, o.RunRows, o.FanIn)
	if err != nil {
		return nil, err
	}
	e.pieces, _ = xsort.New(ctx, s, o.RunRows, o.FanIn)
	e.incidences, _ = xsort.New(ctx, s, o.RunRows, o.FanIn)
	return e, nil
}
func (e *engine) request(r Row) error {
	e.out.SourceRows++
	if !present(r.BackReference) && !present(r.BackSchedule) {
		return nil
	}
	e.out.ReferenceRows++
	ord := uint64(r.Ordinal)
	if err := e.pieces.Add(xsort.Record{Key: ordinalKey(ord), Ordinal: ord, Data: marshal(r)}); err != nil {
		return err
	}
	if !scope(r) {
		return nil
	}
	for i, tx := range []*string{r.Transaction, r.BackReference} {
		if !present(tx) {
			continue
		}
		k := key(r.Recipient, r.File, tx)
		e.filter.visit(k, true)
		if err := e.events.Add(xsort.Record{Key: k, Tag: byte(i + 1), Ordinal: ord}); err != nil {
			return err
		}
	}
	return nil
}
func (e *engine) membership(r Row) error {
	if !scope(r) || !present(r.Transaction) {
		return nil
	}
	k := key(r.Recipient, r.File, r.Transaction)
	if !e.filter.visit(k, false) {
		return nil
	}
	e.out.LookupMemberRows++
	return e.events.Add(xsort.Record{Key: k, Ordinal: uint64(r.Ordinal), Data: marshal(member{uint64(r.Ordinal), r.Schedule, r.Line})})
}
func (e *engine) join() error {
	f, err := e.events.Finish()
	if err != nil {
		return err
	}
	e.out.LookupEvidence = f
	r, err := xsort.Open(e.ctx, e.space.Dir, f)
	if err != nil {
		return err
	}
	defer r.Close()
	var previous string
	var group lookup
	first := true
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if first || v.Key != previous {
			group = lookup{}
			previous = v.Key
			first = false
		}
		if v.Tag == 0 {
			group.Count++
			if group.Count == 1 {
				if err = json.Unmarshal(v.Data, &group.First); err != nil {
					return err
				}
			}
			continue
		}
		if v.Tag != 1 && v.Tag != 2 {
			return fmt.Errorf("invalid lookup tag")
		}
		if err = e.pieces.Add(xsort.Record{Key: ordinalKey(v.Ordinal), Tag: v.Tag, Ordinal: v.Ordinal, Data: marshal(group)}); err != nil {
			return err
		}
	}
	return nil
}
func (e *engine) decisions() error {
	f, err := e.pieces.Finish()
	if err != nil {
		return err
	}
	r, err := xsort.Open(e.ctx, e.space.Dir, f)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := e.space.Writer(e.ctx)
	if err != nil {
		return err
	}
	defer w.Abort()
	var current uint64
	var source *Row
	var own, target *lookup
	var seen uint64
	emit := func() error {
		if source == nil {
			return fmt.Errorf("missing base reference record")
		}
		v := *source
		if scope(v) && ((present(v.Transaction) && own == nil) || (present(v.BackReference) && target == nil)) {
			return fmt.Errorf("missing full-scope lookup")
		}
		in := reportreference.Input{Ordinal: current, ScopeValid: scope(v), Transaction: v.Transaction, BackReference: v.BackReference, BackSchedule: v.BackSchedule}
		d := Decision{Source: v}
		if own != nil {
			if own.Count == 0 {
				return fmt.Errorf("source occurrence absent from its own lookup")
			}
			in.SourceCount = own.Count
			d.SourceMultiplicity = &own.Count
		}
		if target != nil {
			in.TargetCount = target.Count
			in.TargetOrdinal = target.First.Ordinal
			in.TargetSchedule = target.First.Schedule
			in.TargetLine = target.First.Line
			d.TargetMultiplicity = &target.Count
		}
		var to uint64
		d.State, to = reportreference.Decide(in)
		if to != 0 {
			d.Target = &to
			if err := e.incidences.Add(xsort.Record{Key: ordinalKey(current) + ordinalKey(to), Tag: 0, Ordinal: current}); err != nil {
				return err
			}
			if err := e.incidences.Add(xsort.Record{Key: ordinalKey(to) + ordinalKey(current), Tag: 1, Ordinal: current}); err != nil {
				return err
			}
		}
		e.out.States[d.State]++
		seen++
		return w.Add(xsort.Record{Key: ordinalKey(current), Ordinal: current, Data: marshal(d)})
	}
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if v.Key != ordinalKey(v.Ordinal) {
			return fmt.Errorf("piece key mismatch")
		}
		if current != 0 && v.Ordinal != current {
			if err = emit(); err != nil {
				return err
			}
			source = nil
			own = nil
			target = nil
		}
		current = v.Ordinal
		switch v.Tag {
		case 0:
			if source != nil {
				return fmt.Errorf("duplicate base")
			}
			source = new(Row)
			if err = json.Unmarshal(v.Data, source); err != nil {
				return err
			}
			if uint64(source.Ordinal) != current {
				return fmt.Errorf("base ordinal mismatch")
			}
		case 1:
			if own != nil {
				return fmt.Errorf("duplicate source lookup")
			}
			own = new(lookup)
			if err = json.Unmarshal(v.Data, own); err != nil {
				return err
			}
		case 2:
			if target != nil {
				return fmt.Errorf("duplicate target lookup")
			}
			target = new(lookup)
			if err = json.Unmarshal(v.Data, target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid piece tag")
		}
	}
	if current != 0 {
		if err = emit(); err != nil {
			return err
		}
	}
	if seen != e.out.ReferenceRows {
		return fmt.Errorf("reference conservation")
	}
	e.out.States["no_report_reference"] = e.out.SourceRows - e.out.ReferenceRows
	d, err := w.Finish()
	if err != nil {
		return err
	}
	e.out.Decisions = d
	r.Close()
	return e.space.Remove(f)
}
