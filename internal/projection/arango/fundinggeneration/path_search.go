package fundinggeneration

import (
	"context"
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

type pathTopology map[string][]graphread.Link

func (t pathTopology) add(link graphread.Link) error {
	if !validDigest(link.Key) || graphread.Kind(link.From) != "committee" || graphread.Kind(link.To) == "" {
		return fmt.Errorf("invalid typed topology link")
	}
	t[link.From] = append(t[link.From], link)
	return nil
}
func (t pathTopology) order() error {
	for id, links := range t {
		sort.Slice(links, func(i, j int) bool { return links[i].ID() < links[j].ID() })
		for i := 1; i < len(links); i++ {
			if links[i].ID() == links[i-1].ID() {
				return fmt.Errorf("duplicate topology link at %s", id)
			}
		}
	}
	return nil
}

type PathSearch struct {
	State         string `json:"state"`
	Examined      uint64 `json:"links_examined"`
	CycleClosures uint64 `json:"cycle_closing_edges_skipped"`
	HopFrontiers  uint64 `json:"stops_at_committee_hop_bound"`
	NoOutgoing    uint64 `json:"stops_without_selected_outgoing_links"`
	MorePaths     string `json:"more_paths_within_hop_bound"`
}

// Search enumerates directed simple committee paths. Counters describe this
// bounded search, not a graph-wide cycle/terminal census. It never uses amounts.
func searchPaths(ctx context.Context, chain, endings pathTopology, start, target string, maxHops, limit int, budget uint64) ([][]graphread.Link, PathSearch, error) {
	out := [][]graphread.Link{}
	stats := PathSearch{State: "complete_within_hop_bound", MorePaths: "no"}
	seen := map[string]bool{start: true}
	path := []graphread.Link{}
	stopped := false
	examine := func() bool {
		if stats.Examined == budget {
			stats.State = "truncated_expansion_budget"
			stats.MorePaths = "unknown"
			stopped = true
			return false
		}
		stats.Examined++
		return true
	}
	emit := func(last *graphread.Link) {
		if len(out) == limit {
			stats.State = "truncated_path_limit"
			stats.MorePaths = "yes"
			stopped = true
			return
		}
		p := append([]graphread.Link{}, path...)
		if last != nil {
			p = append(p, *last)
		}
		out = append(out, p)
	}
	var visit func(string, int) error
	visit = func(id string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if id == target {
			emit(nil)
			return nil
		}
		for _, link := range endings[id] {
			if !examine() {
				return nil
			}
			if link.To == target {
				emit(&link)
				if stopped {
					return nil
				}
			}
		}
		next := chain[id]
		if len(next) == 0 {
			stats.NoOutgoing++
			return nil
		}
		for _, link := range next {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !examine() {
				return nil
			}
			if seen[link.To] {
				stats.CycleClosures++
				continue
			}
			if depth == maxHops {
				stats.HopFrontiers++
				break
			}
			seen[link.To] = true
			path = append(path, link)
			if err := visit(link.To, depth+1); err != nil {
				return err
			}
			path = path[:len(path)-1]
			delete(seen, link.To)
			if stopped {
				return nil
			}
		}
		return nil
	}
	if maxHops < 0 || maxHops > 8 || limit < 1 || limit > 10 || budget < 1 || budget > 100000 {
		return nil, stats, fmt.Errorf("invalid path search bounds")
	}
	if err := visit(start, 0); err != nil {
		return nil, stats, err
	}
	return out, stats, nil
}
