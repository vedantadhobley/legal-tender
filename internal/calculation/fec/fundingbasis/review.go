package fundingbasis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// ComponentEvidence is complete source evidence for a small inventory
// component, not a capped sample presented as a complete population.
type ComponentEvidence struct {
	SchemaVersion string             `json:"schema_version"`
	InventoryID   string             `json:"inventory_calculation_id"`
	Cycle         string             `json:"cycle"`
	Input         Input              `json:"input"`
	Component     string             `json:"component"`
	Measures      Measures           `json:"measures"`
	Receipts      []Receipt          `json:"receipts"`
	Policy        string             `json:"policy"`
	Decisions     []EvidenceDecision `json:"decisions"`
}

// ReviewComponent reads each member shard once. Larger populations must use
// streaming calculations rather than increasing this source-review limit.
func (r *Reader) ReviewComponent(ctx context.Context, component string, progress func(string)) (ComponentEvidence, error) {
	if !validComponent(component) {
		return ComponentEvidence{}, fmt.Errorf("invalid review component")
	}
	out := ComponentEvidence{SchemaVersion: "legal-tender.fec.receipt-component-evidence.v1", InventoryID: r.result.CalculationID, Cycle: r.result.Cycle, Input: r.result.Input, Component: component, Receipts: []Receipt{}}
	bits := make([]byte, (len(r.manifest.Shards)+7)/8)
	want := make(map[Key]Measures)
	for _, b := range r.result.Buckets {
		if b.Key.Component == component {
			if err := out.Measures.merge(b.Measures); err != nil {
				return out, err
			}
			want[b.Key] = b.Measures
			for i, v := range b.Shards {
				bits[i] |= v
			}
		}
	}
	if out.Measures.Rows > 10000 {
		return ComponentEvidence{}, fmt.Errorf("component has %d rows; complete review limit is 10000", out.Measures.Rows)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	type result struct {
		rows []Receipt
		err  error
	}
	results := make(chan result, 4)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for i := range jobs {
				rows, err := r.queryShard(ctx, i, Query{Component: component}, int(out.Measures.Rows)+1)
				select {
				case results <- result{rows, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for i := range r.manifest.Shards {
			if bits[i/8]&(1<<uint(i%8)) != 0 {
				select {
				case jobs <- i:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	var firstError error
	completed := 0
	for result := range results {
		if firstError != nil {
			continue
		}
		if result.err != nil {
			firstError = result.err
			cancel()
			continue
		}
		out.Receipts = append(out.Receipts, result.rows...)
		if uint64(len(out.Receipts)) > out.Measures.Rows {
			firstError = fmt.Errorf("component exceeds inventory membership")
			cancel()
			continue
		}
		completed++
		if progress != nil && completed%16 == 0 {
			progress(fmt.Sprintf("reviewed %d component-bearing shards", completed))
		}
	}
	if firstError != nil {
		return ComponentEvidence{}, firstError
	}
	if err := ctx.Err(); err != nil {
		return ComponentEvidence{}, err
	}
	sort.Slice(out.Receipts, func(i, j int) bool { return out.Receipts[i].Ordinal < out.Receipts[j].Ordinal })
	got := make(map[Key]Measures)
	var last uint64
	for _, row := range out.Receipts {
		if row.Ordinal <= last {
			return ComponentEvidence{}, fmt.Errorf("duplicate component occurrence")
		}
		last = row.Ordinal
		var amount *int64
		if value := row.Fields["lt_receipt_amount_minor_units"]; value != nil {
			s, ok := value.(string)
			if !ok {
				return ComponentEvidence{}, fmt.Errorf("invalid source amount")
			}
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return ComponentEvidence{}, err
			}
			amount = &n
		}
		var conduit *string
		if s, ok := row.Fields["conduit_cmte_id"].(string); ok {
			conduit = &s
		}
		m := got[row.Key]
		if err := m.observe(receiptRow{Amount: amount, ConduitID: conduit}); err != nil {
			return ComponentEvidence{}, err
		}
		got[row.Key] = m
	}
	if len(got) != len(want) {
		return ComponentEvidence{}, fmt.Errorf("component bucket membership mismatch")
	}
	for k, m := range want {
		if got[k] != m {
			return ComponentEvidence{}, fmt.Errorf("component measures differ from inventory")
		}
	}
	decisions, err := annotate(out.Receipts)
	if err != nil {
		return ComponentEvidence{}, err
	}
	out.Policy, out.Decisions = EvidencePolicy, decisions
	return out, nil
}
