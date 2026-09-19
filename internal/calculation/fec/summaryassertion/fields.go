package summaryassertion

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/summarypublication"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type FieldValue struct {
	Field string                      `json:"field"`
	Raw   string                      `json:"raw"`
	Value committeesummary.MoneyValue `json:"value"`
}

type AssertionFields struct {
	AssertionID          string       `json:"assertion_id"`
	RepresentativeFactID string       `json:"representative_fact_id"`
	Fields               []FieldValue `json:"fields"`
}

// ReadAssertionFields verifies the same immutable summary publication and reads
// extra scalars without changing assertion identities or diagnostic equations.
// Each representative is used once; its retained members are not another ledger.
func ReadAssertionFields(ctx context.Context, root, path string, expected WindowComparison, names []string) ([]AssertionFields, error) {
	for i, name := range names {
		if !slices.Contains(committeesummary.MoneyFields(), name) || slices.Contains(names[:i], name) {
			return nil, fmt.Errorf("invalid or repeated summary money field %s", name)
		}
	}
	m, err := summarypublication.Load(ctx, root, path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(summarypublication.ManifestPath(root, m.FactSetID))
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	input := Input{m.FactSetID, digest(raw), m.SourceReleaseID, m.SourceReleaseManifestSHA256, m.SourceArtifact.SHA256}
	if len(raw) > 4<<20 || input != expected.SummaryInput || m.Cycle != expected.Cycle {
		return nil, fmt.Errorf("summary field input differs from verified comparison")
	}
	out := []AssertionFields{}
	wanted := map[string]int{}
	if expected.Summary != nil {
		for i, a := range expected.Summary.Assertions {
			if _, exists := wanted[a.RepresentativeFactID]; exists {
				return nil, fmt.Errorf("repeated summary representative")
			}
			wanted[a.RepresentativeFactID] = i
			out = append(out, AssertionFields{a.ID, a.RepresentativeFactID, []FieldValue{}})
		}
	}
	r, err := artifact.Open[summarypublication.Fact](ctx, root, m.Facts)
	if err != nil {
		return nil, err
	}
	defer r.Abort()
	var count uint64
	for {
		f, ok, err := r.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count++
		i, wantedFact := wanted[f.FactID]
		if !wantedFact {
			continue
		}
		a := expected.Summary.Assertions[i]
		if len(out[i].Fields) != 0 || len(a.Members) == 0 || !reflect.DeepEqual(member(f), a.Members[0]) || f.Record.SourceFields["CMTE_ID"] != expected.CommitteeID {
			return nil, fmt.Errorf("summary representative identity mismatch")
		}
		for _, name := range names {
			raw, rawOK := f.Record.SourceFields[name]
			value, typedOK := f.Record.Money[name]
			if !rawOK || !typedOK {
				return nil, fmt.Errorf("missing verified summary money field %s", name)
			}
			out[i].Fields = append(out[i].Fields, FieldValue{name, raw, value})
		}
		delete(wanted, f.FactID)
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	if len(wanted) != 0 || count != m.Facts.RecordCount {
		return nil, fmt.Errorf("summary field conservation failed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
