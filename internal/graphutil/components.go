package graphutil

import (
	"context"
	"sort"
)

// StrongComponents uses iterative Kosaraju without recursive stack growth or
// path enumeration. Callers supply the complete node set and mutually reversed
// adjacency maps. Traversal is O(V+E), followed by canonical component sorting.
// Connectivity carries no financial interpretation.
func StrongComponents(ctx context.Context, nodes []string, adj, reverse map[string][]string) ([][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type frame struct {
		id   string
		next int
	}
	seen := map[string]bool{}
	finish := make([]string, 0, len(nodes))
	for _, start := range nodes {
		if seen[start] {
			continue
		}
		seen[start] = true
		stack := []frame{{id: start}}
		for len(stack) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			top := &stack[len(stack)-1]
			if top.next == len(adj[top.id]) {
				finish = append(finish, top.id)
				stack = stack[:len(stack)-1]
				continue
			}
			next := adj[top.id][top.next]
			top.next++
			if !seen[next] {
				seen[next] = true
				stack = append(stack, frame{id: next})
			}
		}
	}
	seen = map[string]bool{}
	result := [][]string{}
	for i := len(finish) - 1; i >= 0; i-- {
		start := finish[i]
		if seen[start] {
			continue
		}
		seen[start] = true
		stack, component := []string{start}, []string{}
		for len(stack) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			id := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			component = append(component, id)
			for _, next := range reverse[id] {
				if !seen[next] {
					seen[next] = true
					stack = append(stack, next)
				}
			}
		}
		sort.Strings(component)
		result = append(result, component)
	}
	sort.Slice(result, func(i, j int) bool { return result[i][0] < result[j][0] })
	return result, nil
}
