package flowreconciliation

import (
	"context"
	"fmt"
	"sort"
)

// Candidate buckets connect only opposite ledgers. Components retain all
// competing exact, amount-only, date-only, and exact role-conflict candidates.
// Unioning buckets is linear in membership, not a Cartesian pair expansion.
func Reconcile(ctx context.Context, a, b []Observation, calculationID string) ([]Assertion, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := len(a) + len(b)
	parent := make([]int, n)
	size := make([]int, n)
	for i := range n {
		parent[i] = i
		size[i] = 1
	}
	var find func(int) int
	find = func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	join := func(x, y int) {
		x, y = find(x), find(y)
		if x == y {
			return
		}
		if size[x] < size[y] {
			x, y = y, x
		}
		parent[y] = x
		size[x] += size[y]
	}
	type key struct {
		sender, recipient, role, kind string
		amount                        int64
		date                          int32
	}
	type bucket struct{ a, b []int }
	buckets := map[key]*bucket{}
	add := func(k key, index int, isA bool) {
		v := buckets[k]
		if v == nil {
			v = &bucket{}
			buckets[k] = v
		}
		if isA {
			v.a = append(v.a, index)
		} else {
			v.b = append(v.b, index)
		}
	}
	for side, rows := range [][]Observation{a, b} {
		var prior uint64
		for i, o := range rows {
			if i%8192 == 0 && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if o.Ordinal == 0 || o.Ordinal <= prior || o.SubID == "" || !validCommittee(o.Sender) || !validCommittee(o.Recipient) || !validRole(o.Role) {
				return nil, fmt.Errorf("invalid or unordered reconciliation observation")
			}
			prior = o.Ordinal
			index := i
			if side == 1 {
				index += len(a)
			}
			add(key{o.Sender, o.Recipient, o.Role, "amount", o.Amount, 0}, index, side == 0)
			if o.Date != nil {
				add(key{o.Sender, o.Recipient, o.Role, "date", 0, *o.Date}, index, side == 0)
				add(key{o.Sender, o.Recipient, "", "exact_any_role", o.Amount, *o.Date}, index, side == 0)
			}
		}
	}
	for _, g := range buckets {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(g.a) == 0 || len(g.b) == 0 {
			continue
		}
		anchor := g.a[0]
		for _, i := range g.b {
			join(anchor, i)
		}
		for _, i := range g.a {
			join(i, g.b[0])
		}
	}
	type component struct{ ai, bi []int }
	groups := map[int]*component{}
	for i := range n {
		root := find(i)
		g := groups[root]
		if g == nil {
			g = &component{}
			groups[root] = g
		}
		if i < len(a) {
			g.ai = append(g.ai, i)
		} else {
			g.bi = append(g.bi, i-len(a))
		}
	}
	ordered := make([]*component, 0, len(groups))
	for _, g := range groups {
		ordered = append(ordered, g)
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		li, ri := n, n
		if len(left.ai) > 0 {
			li = left.ai[0]
		} else {
			li = len(a) + left.bi[0]
		}
		if len(right.ai) > 0 {
			ri = right.ai[0]
		} else {
			ri = len(a) + right.bi[0]
		}
		return li < ri
	})
	result := make([]Assertion, 0, len(ordered))
	for _, g := range ordered {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		r := Assertion{A: []uint64{}, B: []uint64{}, State: "ambiguous_candidates"}
		for _, i := range g.ai {
			r.A = append(r.A, a[i].Ordinal)
			if err := addMoney(&r.AAmount, a[i].Amount); err != nil {
				return nil, err
			}
		}
		for _, i := range g.bi {
			r.B = append(r.B, b[i].Ordinal)
			if err := addMoney(&r.BAmount, b[i].Amount); err != nil {
				return nil, err
			}
		}
		var left, right Observation
		if len(g.ai) > 0 {
			left = a[g.ai[0]]
		}
		if len(g.bi) > 0 {
			right = b[g.bi[0]]
		}
		r.State = componentState(len(g.ai), len(g.bi), left, right)
		// Both ledgers' amounts remain independent, including corroborated pairs.
		r.ID = hashJSON(struct {
			Calculation string
			A, B        []uint64
		}{calculationID, r.A, r.B})
		result = append(result, r)
	}
	return result, nil
}

func componentState(na, nb int, left, right Observation) string {
	if nb == 0 {
		return "unmatched_schedule_a"
	}
	if na == 0 {
		return "unmatched_schedule_b"
	}
	if na != 1 || nb != 1 {
		return "ambiguous_candidates"
	}
	switch {
	case left.Role != right.Role:
		return "conflicting_role"
	case left.Date == nil || right.Date == nil:
		return "candidate_missing_date"
	case left.Amount != right.Amount:
		return "conflicting_amount"
	case *left.Date != *right.Date:
		return "candidate_date_disagreement"
	default:
		return "corroborated_exact_signature"
	}
}

func validCommittee(s string) bool {
	if len(s) != 9 || s[0] != 'C' {
		return false
	}
	for i := 1; i < 9; i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
func validRole(s string) bool {
	switch s {
	case "contribution", "in_kind", "affiliated_transfer", "refund_or_repayment", "loan":
		return true
	}
	return false
}
