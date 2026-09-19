package flowreconciliation

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// SourceLocator names a selected physical occurrence, never a payment.
type SourceLocator struct {
	Side      string `json:"side"`
	FactSetID string `json:"fact_set_id"`
	Ordinal   uint64 `json:"source_row_ordinal"`
}

// LookupSources validates a published calculation and its complete source
// backing, then seeks to a bounded set of full Parquet source rows.
func LookupSources(ctx context.Context, root, publication string, refs []SourceLocator) ([]SourceExample, uint64, error) {
	if len(refs) == 0 || len(refs) > 64 {
		return nil, 0, fmt.Errorf("source lookup requires 1..64 locators")
	}
	r, _, err := readPublication(ctx, root, publication)
	if err != nil {
		return nil, 0, err
	}
	example := ReviewExample{}
	wanted := map[SourceLocator]bool{}
	for _, ref := range refs {
		fact := ""
		switch ref.Side {
		case "schedule_a":
			fact = r.Input.A.FactSetID
		case "schedule_b":
			fact = r.Input.B.FactSetID
		default:
			return nil, 0, fmt.Errorf("explicit source ledger required")
		}
		if ref.FactSetID != fact || ref.Ordinal == 0 || wanted[ref] {
			return nil, 0, fmt.Errorf("invalid or repeated source locator")
		}
		wanted[ref] = true
	}
	for _, side := range []string{"schedule_a", "schedule_b"} {
		d, fact := r.A.Observations, r.Input.A.FactSetID
		if side == "schedule_b" {
			d, fact = r.B.Observations, r.Input.B.FactSetID
		}
		reader, err := artifact.Open[Observation](ctx, filepath.Join(root, PublicationBase), d)
		if err != nil {
			return nil, 0, err
		}
		for {
			o, ok, err := reader.Next()
			if err != nil {
				reader.Abort()
				return nil, 0, err
			}
			if !ok {
				break
			}
			ref := SourceLocator{side, fact, o.Ordinal}
			if !wanted[ref] {
				continue
			}
			delete(wanted, ref)
			if side == "schedule_a" {
				example.A = append(example.A, o)
			} else {
				example.B = append(example.B, o)
			}
		}
		if err := reader.Close(); err != nil {
			return nil, 0, err
		}
	}
	if len(wanted) != 0 {
		return nil, 0, fmt.Errorf("locator is outside selected observation cohort")
	}
	return reviewSourceExamples(ctx, root, r, []ReviewExample{example})
}
