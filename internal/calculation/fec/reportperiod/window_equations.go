package reportperiod

import (
	"fmt"
	"math/big"
	"slices"
)

// An observation index references one captured report; nil means a window field.
// No solved value replaces an amount. Coefficients define the signed residual.
type Operand struct {
	ObservationIndex *int   `json:"observation_index"`
	Field            string `json:"field"`
	Coefficient      int    `json:"coefficient"`
}

type Equation struct {
	Kind               string    `json:"kind"`
	Operands           []Operand `json:"operands"`
	State              string    `json:"state"` // balanced, mismatch, unavailable
	ResidualMinorUnits *string   `json:"residual_minor_units"`
	Blockers           []string  `json:"blockers"`
}

func (r *WindowReview) equations() {
	for _, i := range r.Membership.ChainCandidateIndexes {
		if r.Membership.Observations[i].WindowRelation != "inside" {
			continue
		}
		r.equation("report_individual_subtotal", []Operand{{&i, windowFields[2], 1}, {&i, windowFields[0], -1}, {&i, windowFields[1], -1}}, nil)
		r.equation("report_cash", []Operand{{&i, windowFields[6], 1}, {&i, windowFields[5], -1}, {&i, windowFields[3], -1}, {&i, windowFields[4], 1}}, nil)
	}
	// Order ALL intersecting cohorts, including unresolved or unbound ones.
	// Never bridge across one by sorting only the usable documents.
	cohorts := []Cohort{}
	for _, c := range r.Membership.Cohorts {
		if intersects(c.Period, r.Membership.Window) {
			cohorts = append(cohorts, c)
		}
	}
	for i := 1; i < len(cohorts); i++ {
		a, b := cohorts[i-1], cohorts[i]
		_, end := bounds(a.Period)
		start, _ := bounds(b.Period)
		if end != start || a.ChainCandidateIndex == nil || b.ChainCandidateIndex == nil {
			continue
		}
		blockers := []string{}
		for j, c := range cohorts {
			if j != i-1 && j != i && (intersects(c.Period, a.Period) || intersects(c.Period, b.Period)) {
				blockers = append(blockers, "ambiguous_handoff_scope")
			}
		}
		for _, j := range r.Membership.UngroupedIndexes {
			p := r.Membership.Observations[j].Period
			if p == nil || intersects(*p, a.Period) || intersects(*p, b.Period) {
				blockers = append(blockers, "ambiguous_handoff_scope")
			}
		}
		r.equation("adjacent_cash_carry_forward", []Operand{{b.ChainCandidateIndex, windowFields[5], 1}, {a.ChainCandidateIndex, windowFields[6], -1}}, blockers)
	}
	r.equation("window_individual_subtotal", []Operand{{nil, windowFields[2], 1}, {nil, windowFields[0], -1}, {nil, windowFields[1], -1}}, nil)
	r.equation("window_cash", []Operand{{nil, windowFields[6], 1}, {nil, windowFields[5], -1}, {nil, windowFields[3], -1}, {nil, windowFields[4], 1}}, nil)
}

func (r *WindowReview) equation(kind string, operands []Operand, blockers []string) {
	e := Equation{Kind: kind, Operands: operands, State: "unavailable", Blockers: append([]string{}, blockers...)}
	sum := new(big.Int)
	for _, o := range operands {
		var value *string
		if o.ObservationIndex != nil {
			for _, b := range r.Bindings {
				if b.ObservationIndex == nil || *b.ObservationIndex != *o.ObservationIndex {
					continue
				}
				f := boundField(b, o.Field)
				if f != nil && f.ReportedValueBound {
					value = f.Metadata.MinorUnits
				}
			}
		} else {
			for _, f := range r.Fields {
				if f.Name == o.Field && f.ReportedWindowReady {
					value = f.WindowValueMinorUnits
				}
			}
		}
		if value == nil {
			e.Blockers = append(e.Blockers, fmt.Sprintf("unavailable_operand:%s", o.Field))
			continue
		}
		n, _ := new(big.Int).SetString(*value, 10)
		sum.Add(sum, n.Mul(n, big.NewInt(int64(o.Coefficient))))
	}
	slices.Sort(e.Blockers)
	e.Blockers = slices.Compact(e.Blockers)
	if len(e.Blockers) == 0 {
		value := sum.String()
		e.ResidualMinorUnits = &value
		e.State = "mismatch"
		if value == "0" {
			e.State = "balanced"
		}
	}
	r.Equations = append(r.Equations, e)
}
