package flowreconciliation

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
)

// SourceReader pins verified manifests and selected membership for its lifetime.
// Lookup hashes only the requested shards, then seeks full physical rows. It
// never follows current pointers or mutates published evidence. Safe for readers
// in parallel; callers must bound concurrency and mount published storage read-only.
type SourceReader struct {
	root             string
	result           Result
	manifestSHA256   string
	a, b             []Observation
	ashards, bshards []reviewShard
}

func OpenSourceReader(ctx context.Context, root, publication string) (*SourceReader, error) {
	r, content, err := readResultHeader(root, publication)
	if err != nil {
		return nil, err
	}
	r, a, b, _, digest, err := readReviewEvidence(ctx, ReviewOptions{
		ResultPath:   filepath.Join(root, PublicationBase, "manifests", r.CalculationSetID+".json"),
		EvidenceRoot: filepath.Join(root, PublicationBase),
	})
	if err != nil {
		return nil, err
	}
	if digest != hashBytes(content) {
		return nil, fmt.Errorf("source publication changed while opening")
	}
	inputs, am, bm, err := loadInputs(ctx, Options{StorageRoot: root, Cycle: r.Cycle,
		ScheduleA: filepath.Join(root, "facts/fec/schedule-a/columnar/manifests", r.Input.A.FactSetID+".json"),
		ScheduleB: filepath.Join(root, "facts/fec/schedule-b/columnar/manifests", r.Input.B.FactSetID+".json"),
		Release:   filepath.Join(root, "releases/fec/manifests", r.Input.ReleaseID+".json")})
	if err != nil {
		return nil, err
	}
	if inputs != r.Input {
		return nil, fmt.Errorf("source reader ancestry differs from calculation")
	}
	s := &SourceReader{root: root, result: r, manifestSHA256: digest, a: a, b: b}
	for _, v := range am.Shards {
		s.ashards = append(s.ashards, reviewShard{shard{v.StorageKey, v.FirstSourceRowOrdinal, v.LastSourceRowOrdinal, v.Facts, v.Bytes}, v.SHA256})
	}
	for _, v := range bm.Shards {
		s.bshards = append(s.bshards, reviewShard{shard{v.StorageKey, v.FirstSourceRowOrdinal, v.LastSourceRowOrdinal, v.Facts, v.Bytes}, v.SHA256})
	}
	for _, rows := range [][]Observation{s.a, s.b} {
		sort.Slice(rows, func(i, j int) bool { return rows[i].Ordinal < rows[j].Ordinal })
	}
	return s, nil
}

func (s *SourceReader) CalculationID() string { return s.result.CalculationSetID }

// SourceExpectation binds an observation to its exact published ancestry.
// Checking a row ordinal alone would allow a changed amount or source release
// in a consumer's saved report to pass unnoticed.
type SourceExpectation struct {
	Calculation CalculationReference
	Inputs      Inputs
	Cycle       string
	Locator     SourceLocator
	Observation Observation
}

func (s *SourceReader) LookupExpected(ctx context.Context, e SourceExpectation) (SourceExample, error) {
	if err := ctx.Err(); err != nil {
		return SourceExample{}, err
	}
	if e.Calculation != (CalculationReference{s.result.CalculationSetID, s.manifestSHA256}) || e.Inputs != s.result.Input || e.Cycle != s.result.Cycle || e.Locator.Ordinal != e.Observation.Ordinal {
		return SourceExample{}, fmt.Errorf("connection ancestry differs from published source")
	}
	rows := s.a
	if e.Locator.Side == "schedule_b" {
		rows = s.b
	} else if e.Locator.Side != "schedule_a" {
		return SourceExample{}, fmt.Errorf("explicit source ledger required")
	}
	i := sort.Search(len(rows), func(i int) bool { return rows[i].Ordinal >= e.Locator.Ordinal })
	if i == len(rows) || !reflect.DeepEqual(rows[i], e.Observation) {
		return SourceExample{}, fmt.Errorf("connection differs from selected source observation")
	}
	full, err := s.Lookup(ctx, []SourceLocator{e.Locator})
	if err != nil {
		return SourceExample{}, err
	}
	if len(full) != 1 {
		return SourceExample{}, fmt.Errorf("connection lookup must return exactly one source row")
	}
	return full[0], nil
}

func (s *SourceReader) Lookup(ctx context.Context, refs []SourceLocator) ([]SourceExample, error) {
	if len(refs) < 1 || len(refs) > 64 {
		return nil, fmt.Errorf("source lookup requires 1..64 locators")
	}
	a, b := map[uint64]Observation{}, map[uint64]Observation{}
	for _, ref := range refs {
		rows, fact, wanted := s.a, s.result.Input.A.FactSetID, a
		switch ref.Side {
		case "schedule_a":
		case "schedule_b":
			rows, fact, wanted = s.b, s.result.Input.B.FactSetID, b
		default:
			return nil, fmt.Errorf("explicit source ledger required")
		}
		i := sort.Search(len(rows), func(i int) bool { return rows[i].Ordinal >= ref.Ordinal })
		_, duplicate := wanted[ref.Ordinal]
		if ref.FactSetID != fact || duplicate || i == len(rows) || rows[i].Ordinal != ref.Ordinal {
			return nil, fmt.Errorf("foreign, unselected, or repeated source locator")
		}
		wanted[ref.Ordinal] = rows[i]
	}
	as, err := scheduleaparquet.NewSchema()
	if err != nil {
		return nil, err
	}
	bs, err := schedulebparquet.NewSchema()
	if err != nil {
		return nil, err
	}
	period, _ := strconv.ParseInt(s.result.Cycle, 10, 64)
	left, err := reviewRows(ctx, s.root, "schedule_a", s.result.Input.A.FactSetID, s.ashards, as.Parquet(), a, selectA, func(v aRow) int64 { return v.Period }, period)
	if err != nil {
		return nil, err
	}
	right, err := reviewRows(ctx, s.root, "schedule_b", s.result.Input.B.FactSetID, s.bshards, bs.Parquet(), b, selectB, func(v bRow) int64 { return v.Period }, period)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}
