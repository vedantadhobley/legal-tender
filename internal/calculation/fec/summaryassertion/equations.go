package summaryassertion

import (
	"fmt"
	"math/big"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

type term struct {
	field       string
	coefficient int
}

// Both cash variants are diagnostics for every assertion, not a form inference
// or a fallback that selects whichever equation happens to balance.
var equations = map[string][]term{
	"cash":                 {{"COH_BOP", 1}, {"TTL_RECEIPTS", 1}, {"TTL_DISB", -1}, {"COH_COP", -1}},
	"cash_federal_columns": {{"COH_BOP", 1}, {"TTL_FED_RECEIPTS", 1}, {"TTL_FED_DISB", -1}, {"COH_COP", -1}},
	"individual":           {{"INDV_ITEM_CONTB", 1}, {"INDV_UNITEM_CONTB", 1}, {"INDV_CONTB", -1}},
}

func diagnostics(r committeesummary.Record) (map[string]Equation, error) {
	out := map[string]Equation{}
	for name, terms := range equations {
		e := Equation{State: "equal", Operands: []Operand{}}
		var delta big.Int
		missing, invalid := false, false
		for _, t := range terms {
			v, ok := r.Money[t.field]
			if !ok {
				return nil, fmt.Errorf("missing typed operand %s", t.field)
			}
			e.Operands = append(e.Operands, Operand{t.field, t.coefficient, r.SourceFields[t.field], v})
			switch v.State {
			case committeesummary.Blank:
				missing = true
			case committeesummary.Invalid:
				invalid = true
			case committeesummary.Valid:
				if v.MinorUnits == nil {
					return nil, fmt.Errorf("nil valid operand %s", t.field)
				}
				n, ok := new(big.Int).SetString(*v.MinorUnits, 10)
				if !ok {
					return nil, fmt.Errorf("invalid typed cents for %s", t.field)
				}
				if t.coefficient < 0 {
					delta.Sub(&delta, n)
				} else {
					delta.Add(&delta, n)
				}
			default:
				return nil, fmt.Errorf("unknown typed operand state")
			}
		}
		switch {
		case invalid:
			e.State = "invalid"
		case missing:
			e.State = "missing"
		default:
			d := delta.String()
			e.Delta = &d
			if delta.Sign() != 0 {
				e.State = "different"
			}
		}
		out[name] = e
	}
	return out, nil
}
