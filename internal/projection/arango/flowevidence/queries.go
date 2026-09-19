package flowevidence

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

func edgeCollection(side Ledger) (string, error) {
	switch side {
	case ScheduleA:
		return receivers, nil
	case ScheduleB:
		return senders, nil
	}
	return "", fmt.Errorf("explicit schedule_a or schedule_b ledger required")
}

type pathRow struct {
	Vertices []string `json:"vertices"`
	Edges    []string `json:"edges"`
}
type pathRequest struct {
	side                Ledger
	kind, start, target string
	depth, limit        int
}

// Bounded BFS implements hop-shortest paths within the declared depth. Amounts
// are neither weights nor additive path totals. Collection choice is an enum.
func pathQuery(r pathRequest) (string, map[string]any, error) {
	collection, err := edgeCollection(r.side)
	if err != nil {
		return "", nil, err
	}
	if r.depth < 1 || r.depth > 8 || r.limit < 1 || r.limit > 25 {
		return "", nil, fmt.Errorf("path bounds exceed contract")
	}
	direction, order, unique, filter := "OUTBOUND", "dfs", "path", ""
	bind := map[string]any{"start": r.start, "depth": r.depth, "limit": r.limit}
	switch r.kind {
	case "neighborhood":
		direction = "ANY"
		unique = "none" // Self-loop observations remain visible.
	case "paths":
		filter = "FILTER v._id == @target"
		bind["target"] = r.target
	case "shortest":
		order = "bfs"
		filter = "FILTER v._id == @target"
		bind["target"] = r.target
		bind["limit"] = 1
	case "cycles":
		unique = "none"
		filter = "FILTER v._id == @start"
	default:
		return "", nil, fmt.Errorf("unsupported bounded query")
	}
	if r.start == r.target && (r.kind == "paths" || r.kind == "shortest") {
		unique = "none" // A non-empty return-to-start path is a cycle.
	}
	q := fmt.Sprintf("WITH entities FOR v, e, p IN 1..@depth %s @start %s OPTIONS {order: '%s', uniqueVertices: '%s', uniqueEdges: 'path'} %s LIMIT @limit RETURN {vertices:p.vertices[*]._id, edges:p.edges[*]._id}", direction, collection, order, unique, filter)
	return q, bind, nil
}

func validatePath(p pathRow, r pathRequest, index map[string]observation) error {
	if len(p.Edges) == 0 || len(p.Edges) > r.depth || len(p.Vertices) != len(p.Edges)+1 || p.Vertices[0] != r.start {
		return fmt.Errorf("invalid bounded path shape")
	}
	collection, err := edgeCollection(r.side)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for i, id := range p.Edges {
		if !strings.HasPrefix(id, collection+"/") || seen[id] {
			return fmt.Errorf("opposite ledger, component, or repeated edge entered path")
		}
		seen[id] = true
		e, ok := index[strings.TrimPrefix(id, collection+"/")]
		if !ok || e.Ledger != r.side || e.TerminalEligible || e.EconomicFlowStatus != "not_established" {
			return fmt.Errorf("path contains unsupported edge")
		}
		from, to := p.Vertices[i], p.Vertices[i+1]
		if !(e.From == from && e.To == to) && !(r.kind == "neighborhood" && e.To == from && e.From == to) {
			return fmt.Errorf("path endpoint discontinuity")
		}
	}
	if (r.kind == "paths" || r.kind == "shortest") && p.Vertices[len(p.Vertices)-1] != r.target {
		return fmt.Errorf("path missed target")
	}
	if r.kind == "cycles" && p.Vertices[len(p.Vertices)-1] != r.start {
		return fmt.Errorf("cycle did not close")
	}
	return nil
}

func queryGate(ctx context.Context, c *client, m *model) ([]QueryResult, error) {
	results := []QueryResult{}
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		edges := m.edges(side)
		if len(edges) == 0 {
			results = append(results, QueryResult{Ledger: side, Kind: "paths", Status: "empty_selected_cohort"})
			continue
		}
		index := map[string]observation{}
		adj := map[string][]string{}
		for _, e := range edges {
			index[e.Key] = e
			adj[e.From] = append(adj[e.From], e.To)
		}
		start, target := samplePath(edges, adj)
		requests := []pathRequest{{side, "neighborhood", start, "", 3, 25}, {side, "paths", start, target, 4, 25}, {side, "shortest", start, target, 4, 1}}
		cycle, depth := sampleCycle(adj)
		if cycle != "" {
			requests = append(requests, pathRequest{side, "cycles", cycle, "", depth, 25})
		} else {
			results = append(results, QueryResult{Ledger: side, Kind: "cycles", Status: "no_cycle_sample_within_bound"})
		}
		for _, r := range requests {
			q, bind, err := pathQuery(r)
			if err != nil {
				return nil, err
			}
			n := 0
			started := time.Now()
			err = c.query(ctx, q, bind, 5, func(raw json.RawMessage) error {
				var p pathRow
				if err := json.Unmarshal(raw, &p); err != nil {
					return err
				}
				if err := validatePath(p, r, index); err != nil {
					return err
				}
				if r.kind == "shortest" && len(p.Edges) != shortestHops(adj, start, target, r.depth) {
					return fmt.Errorf("query did not return hop-shortest path")
				}
				n++
				if n > r.limit {
					return fmt.Errorf("query exceeded result bound")
				}
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", side, r.kind, err)
			}
			if n == 0 {
				return nil, fmt.Errorf("%s %s missed known path", side, r.kind)
			}
			results = append(results, QueryResult{side, r.kind, n, float64(time.Since(started).Microseconds()) / 1000, "passed"})
		}
	}
	// One-sided evidence lookup uses component documents, never a graph hop.
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		var selected *component
		for i := range m.components {
			v := &m.components[i]
			if (side == ScheduleA && len(v.A) > 0 && len(v.B) == 0) || (side == ScheduleB && len(v.B) > 0 && len(v.A) == 0) {
				selected = v
				break
			}
		}
		if selected == nil {
			results = append(results, QueryResult{Ledger: side, Kind: "one_sided_lookup", Status: "no_one_sided_component"})
			continue
		}
		started := time.Now()
		n := 0
		err := c.query(ctx, "FOR d IN reconciliation_components FILTER d.sender_committee_id == @sender AND d.reported_recipient_committee_id == @recipient AND d._key == @key RETURN UNSET(d, '_id', '_rev')", map[string]any{"sender": selected.Sender, "recipient": selected.Recipient, "key": selected.Key}, 5, func(raw json.RawMessage) error {
			n++
			if !equalDocument(raw, selected) {
				return fmt.Errorf("one-sided component differs")
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("one-sided lookup omitted evidence")
		}
		collection, _ := edgeCollection(side)
		wanted := map[uint64]bool{}
		ordinals := selected.A
		if side == ScheduleB {
			ordinals = selected.B
		}
		for _, o := range ordinals {
			wanted[o] = true
		}
		err = c.query(ctx, "FOR d IN @@collection FILTER d.component_id == @id RETURN d.source_row_ordinal", map[string]any{"@collection": collection, "id": selected.ID}, 5, func(raw json.RawMessage) error {
			var o uint64
			if err := json.Unmarshal(raw, &o); err != nil {
				return err
			}
			if !wanted[o] {
				return fmt.Errorf("unexpected component observation")
			}
			delete(wanted, o)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(wanted) != 0 {
			return nil, fmt.Errorf("component lookup lost observations")
		}
		results = append(results, QueryResult{side, "one_sided_lookup", n, float64(time.Since(started).Microseconds()) / 1000, "passed"})
	}
	return results, nil
}

func samplePath(edges []observation, adj map[string][]string) (string, string) {
	// Prefer a genuine two-hop sample with no direct shortcut.
	for _, e := range edges {
		direct := map[string]bool{}
		for _, to := range adj[e.From] {
			direct[to] = true
		}
		for _, to := range adj[e.To] {
			if to != e.From && !direct[to] {
				return e.From, to
			}
		}
	}
	for _, e := range edges {
		if e.From != e.To {
			return e.From, e.To
		}
	}
	return edges[0].From, edges[0].To
}
func shortestHops(adj map[string][]string, start, target string, depth int) int {
	seen := map[string]bool{start: true}
	level := []string{start}
	for d := 1; d <= depth; d++ {
		next := []string{}
		for _, v := range level {
			for _, to := range adj[v] {
				if to == target {
					return d
				}
				if !seen[to] {
					seen[to] = true
					next = append(next, to)
				}
			}
		}
		level = next
	}
	return -1
}
func sampleCycle(adj map[string][]string) (string, int) {
	keys := make([]string, 0, len(adj))
	for k := range adj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Linear DFS discovers cycles without enumerating all paths. A missing sample
	// within eight hops is not a claim that the graph is acyclic.
	state := map[string]int{}
	depths := map[string]int{}
	var found string
	length := 0
	var visit func(string, int)
	visit = func(v string, d int) {
		state[v] = 1
		depths[v] = d
		for _, to := range adj[v] {
			if found != "" {
				break
			}
			if state[to] == 0 {
				visit(to, d+1)
			} else if state[to] == 1 && d-depths[to]+1 <= 8 {
				found = to
				length = d - depths[to] + 1
			}
		}
		state[v] = 2
	}
	for _, v := range keys {
		if state[v] == 0 {
			visit(v, 0)
		}
		if found != "" {
			break
		}
	}
	return found, length
}
