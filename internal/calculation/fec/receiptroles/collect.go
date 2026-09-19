package receiptroles

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

type accumulator struct {
	Profile
	groups map[Key]*Counts
}
type worker struct {
	rows                uint64
	profiles            map[string]*accumulator
	sources             map[string]Counts
	outside, unresolved Counts
}
type Collector struct {
	ids     []string
	scope   map[string]bool
	masters map[string]string
	workers []worker
	cells   atomic.Uint64
	limit   uint64
}

func New(ids []string, masters map[string]string, workers int) (*Collector, error) {
	if workers < 1 || workers > 8 || len(ids) > 100000 || len(masters) > 100000 {
		return nil, fmt.Errorf("bounded committee scope and 1..8 workers required")
	}
	c := &Collector{ids: append([]string{}, ids...), scope: map[string]bool{}, masters: map[string]string{}, workers: make([]worker, workers), limit: MaxCells}
	sort.Strings(c.ids)
	for _, id := range c.ids {
		if !committeeflows.ValidCommitteeID(&id) || c.scope[id] {
			return nil, fmt.Errorf("invalid/duplicate profile scope")
		}
		c.scope[id] = true
	}
	for id, fact := range masters {
		if !committeeflows.ValidCommitteeID(&id) || len(fact) != 64 || strings.Trim(fact, "0123456789abcdef") != "" {
			return nil, fmt.Errorf("invalid same-cycle master reference")
		}
		c.masters[id] = fact
	}
	for i := range c.workers {
		c.workers[i] = worker{profiles: map[string]*accumulator{}, sources: map[string]Counts{}}
	}
	return c, nil
}

func IdentityState(id *string, masters map[string]string) string {
	if id == nil {
		return "no_routed_source_committee_assertion"
	}
	if _, ok := masters[*id]; ok {
		return "same_cycle_master_fact"
	}
	return "not_in_pinned_same_cycle_master"
}

// Classify only joins the exact routed committee assertion to master facts.
// All role decisions come unchanged from the participant publication.
func Classify(r p.Row, masters map[string]string) (Key, error) {
	if r.Ordinal <= 0 || r.ReportedSourceID != nil && !committeeflows.ValidCommitteeID(r.ReportedSourceID) {
		return Key{}, fmt.Errorf("invalid occurrence/source committee assertion")
	}
	k := Key{Route: r.SourceRoute, Component: r.Component, IndividualDecision: r.IndividualDecision, CommitteeDecision: r.CommitteeDecision, ReceiptRole: r.ReceiptRole, Memo: r.Memo, Conflict: r.EntityConflict, Overlap: r.IndividualOverlap, SourceIdentity: IdentityState(r.ReportedSourceID, masters)}
	if r.Entity != nil {
		k.EntityPresent = true
		k.Entity = *r.Entity
	}
	return k, nil
}
func count(r p.Row) Counts {
	c := Counts{Rows: 1, First: uint64(r.Ordinal)}
	switch {
	case r.Amount == nil:
		c.Unknown = 1
	case *r.Amount > 0:
		c.Positive = 1
	case *r.Amount < 0:
		c.Negative = 1
	default:
		c.Zero = 1
	}
	return c
}
func (c *Collector) cell() error {
	if c.cells.Add(1) > c.limit {
		return fmt.Errorf("receipt role profile map-entry cap exceeded; no partial result")
	}
	return nil
}
func newAccumulator(id string) *accumulator {
	return &accumulator{Profile: Profile{CommitteeID: id, Groups: []Group{}, Earmarks: map[string]uint64{}, Conduits: map[string]uint64{}, References: map[string]uint64{}}, groups: map[Key]*Counts{}}
}
func (c *Collector) increment(m map[string]uint64, k string) error {
	if _, ok := m[k]; !ok {
		if err := c.cell(); err != nil {
			return err
		}
		k = strings.Clone(k)
	}
	m[k]++
	return nil
}

// Observe borrows the row. One serial callback stream owns each worker slot.
func (c *Collector) Observe(workerID int, r p.Row) error {
	if workerID < 0 || workerID >= len(c.workers) || r.Ordinal <= 0 {
		return fmt.Errorf("invalid profile worker or occurrence")
	}
	w := &c.workers[workerID]
	w.rows++
	n := count(r)
	if !committeeflows.ValidCommitteeID(r.Recipient) {
		w.unresolved.merge(n)
		return nil
	}
	if !c.scope[*r.Recipient] {
		w.outside.merge(n)
		return nil
	}
	k, err := Classify(r, c.masters)
	if err != nil {
		return err
	}
	a := w.profiles[*r.Recipient]
	if a == nil {
		if err := c.cell(); err != nil {
			return err
		}
		id := strings.Clone(*r.Recipient)
		a = newAccumulator(id)
		w.profiles[id] = a
	}
	g := a.groups[k]
	if g == nil {
		if err := c.cell(); err != nil {
			return err
		}
		g = &Counts{}
		a.groups[k.owned()] = g
	}
	g.merge(n)
	a.Counts.merge(n)
	for i, k := range []string{r.EarmarkState, r.ConduitState, r.ReferenceState} {
		if err := c.increment([]map[string]uint64{a.Earmarks, a.Conduits, a.References}[i], k); err != nil {
			return err
		}
	}
	if r.ReportedSourceID != nil {
		id := *r.ReportedSourceID
		prior, exists := w.sources[id]
		if !exists {
			if err := c.cell(); err != nil {
				return err
			}
			id = strings.Clone(id)
		}
		prior.merge(n)
		w.sources[id] = prior
	}
	return nil
}

// Finish must only follow a successful, fully verified participant scan.
func (c *Collector) Finish(expected uint64) (Result, error) {
	r := Result{Policy: Policy, Profiles: []Profile{}, Sources: []SourceIdentity{}}
	all := map[string]*accumulator{}
	sources := map[string]Counts{}
	for _, w := range c.workers {
		r.Rows += w.rows
		r.Outside.merge(w.outside)
		r.Unresolved.merge(w.unresolved)
		for id, a := range w.profiles {
			target := all[id]
			if target == nil {
				target = newAccumulator(id)
				all[id] = target
			}
			target.Counts.merge(a.Counts)
			for k, n := range a.groups {
				if target.groups[k] == nil {
					target.groups[k] = &Counts{}
				}
				target.groups[k].merge(*n)
			}
			for i, m := range []map[string]uint64{a.Earmarks, a.Conduits, a.References} {
				dst := []map[string]uint64{target.Earmarks, target.Conduits, target.References}[i]
				for k, n := range m {
					dst[k] += n
				}
			}
		}
		for id, n := range w.sources {
			v := sources[id]
			v.merge(n)
			sources[id] = v
		}
	}
	for _, id := range c.ids {
		a := all[id]
		if a == nil {
			a = newAccumulator(id)
		}
		a.State = "reported_occurrences_present"
		if a.Counts.Rows == 0 {
			a.State = "no_reported_occurrences_in_exact_participant_publication"
		}
		var total uint64
		for k, n := range a.groups {
			a.Groups = append(a.Groups, Group{k, *n})
			total += n.Rows
		}
		sort.Slice(a.Groups, func(i, j int) bool {
			x, _ := json.Marshal(a.Groups[i].Key)
			y, _ := json.Marshal(a.Groups[j].Key)
			return string(x) < string(y)
		})
		if total != a.Counts.Rows || a.Counts.Rows != a.Counts.Positive+a.Counts.Negative+a.Counts.Zero+a.Counts.Unknown {
			return Result{}, fmt.Errorf("role/sign population conservation failed")
		}
		for _, m := range []map[string]uint64{a.Earmarks, a.Conduits, a.References} {
			var n uint64
			for _, v := range m {
				n += v
			}
			if n != total {
				return Result{}, fmt.Errorf("annotation marginal conservation failed")
			}
		}
		r.Scoped.merge(a.Counts)
		r.Profiles = append(r.Profiles, a.Profile)
	}
	sourceIDs := make([]string, 0, len(sources))
	for id := range sources {
		sourceIDs = append(sourceIDs, id)
	}
	sort.Strings(sourceIDs)
	for _, id := range sourceIDs {
		n := sources[id]
		v := SourceIdentity{ID: id, State: IdentityState(&id, c.masters), Rows: n.Rows, First: n.First}
		if fact, ok := c.masters[id]; ok {
			v.MasterFactID = &fact
		}
		r.Sources = append(r.Sources, v)
	}
	if r.Rows != expected || r.Scoped.Rows+r.Outside.Rows+r.Unresolved.Rows != expected || c.cells.Load() > c.limit {
		return Result{}, fmt.Errorf("complete profile population or resource cap failed")
	}
	return r, nil
}
