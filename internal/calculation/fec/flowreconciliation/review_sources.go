package flowreconciliation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type reviewShard struct {
	shard
	sha string
}

func reviewSourceExamples(ctx context.Context, root string, r Result, examples []ReviewExample) ([]SourceExample, uint64, error) {
	// Existing loaders validate immutable manifests, release ancestry and complete
	// shard hashes. Source row decoding below seeks only to the requested rows.
	for _, id := range []string{r.Input.A.FactSetID, r.Input.B.FactSetID, strings.TrimPrefix(r.Input.ReleaseID, "fec-")} {
		if len(id) != 64 || strings.IndexFunc(id, func(c rune) bool { return !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') }) >= 0 {
			return nil, 0, fmt.Errorf("invalid source identity")
		}
	}
	inputs, a, b, err := loadInputs(ctx, Options{StorageRoot: root, Cycle: r.Cycle,
		ScheduleA: filepath.Join(root, "facts/fec/schedule-a/columnar/manifests", r.Input.A.FactSetID+".json"),
		ScheduleB: filepath.Join(root, "facts/fec/schedule-b/columnar/manifests", r.Input.B.FactSetID+".json"),
		Release:   filepath.Join(root, "releases/fec/manifests", r.Input.ReleaseID+".json")})
	if err != nil {
		return nil, 0, err
	}
	if inputs != r.Input {
		return nil, 0, fmt.Errorf("review source ancestry differs from calculation")
	}
	as, err := scheduleaparquet.NewSchema()
	if err != nil {
		return nil, 0, err
	}
	bs, err := schedulebparquet.NewSchema()
	if err != nil {
		return nil, 0, err
	}
	ashards, bshards := []reviewShard{}, []reviewShard{}
	for _, s := range a.Shards {
		ashards = append(ashards, reviewShard{shard{s.StorageKey, s.FirstSourceRowOrdinal, s.LastSourceRowOrdinal, s.Facts, s.Bytes}, s.SHA256})
	}
	for _, s := range b.Shards {
		bshards = append(bshards, reviewShard{shard{s.StorageKey, s.FirstSourceRowOrdinal, s.LastSourceRowOrdinal, s.Facts, s.Bytes}, s.SHA256})
	}
	wantedA, wantedB := map[uint64]Observation{}, map[uint64]Observation{}
	for _, e := range examples {
		for _, o := range e.A {
			wantedA[o.Ordinal] = o
		}
		for _, o := range e.B {
			wantedB[o.Ordinal] = o
		}
	}
	period, _ := strconv.ParseInt(r.Cycle, 10, 64)
	left, err := reviewRows(ctx, root, "schedule_a", r.Input.A.FactSetID, ashards, as.Parquet(), wantedA, selectA, func(v aRow) int64 { return v.Period }, period)
	if err != nil {
		return nil, 0, err
	}
	right, err := reviewRows(ctx, root, "schedule_b", r.Input.B.FactSetID, bshards, bs.Parquet(), wantedB, selectB, func(v bRow) int64 { return v.Period }, period)
	return append(left, right...), uint64(len(ashards) + len(bshards)), err
}

func reviewRows[T any](ctx context.Context, root, side, factID string, shards []reviewShard, schema *parquet.Schema, wanted map[uint64]Observation, selectRow func(T) (DecisionKey, *Observation, error), cycle func(T) int64, period int64) ([]SourceExample, error) {
	out := []SourceExample{}
	projected := parquet.SchemaOf(new(T))
	conversion, err := parquet.Convert(projected, schema)
	if err != nil {
		return nil, err
	}
	found := 0
	for _, s := range shards {
		ordinals := []uint64{}
		for ordinal := range wanted {
			if ordinal >= s.first && ordinal <= s.last {
				ordinals = append(ordinals, ordinal)
			}
		}
		if len(ordinals) == 0 {
			continue
		}
		sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
		rows, err := readReviewShard(ctx, root, side, factID, s, schema, projected, conversion, ordinals, wanted, selectRow, cycle, period)
		if err != nil {
			return nil, err
		}
		found += len(rows)
		out = append(out, rows...)
	}
	if found != len(wanted) {
		return nil, fmt.Errorf("source review omitted or repeated an ordinal")
	}
	return out, nil
}

func readReviewShard[T any](ctx context.Context, root, side, factID string, s reviewShard, schema, projected *parquet.Schema, conversion parquet.Conversion, ordinals []uint64, wanted map[uint64]Observation, selectRow func(T) (DecisionKey, *Observation, error), cycle func(T) int64, period int64) ([]SourceExample, error) {
	path, err := artifact.Resolve(root, s.key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Hash and read the same open file. Long-lived source readers cannot rely
	// solely on a successful startup hash after a shard is replaced or damaged.
	h := sha256.New()
	buf := make([]byte, 256<<10)
	var size uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := f.Read(buf)
		size += uint64(n)
		_, _ = h.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if size != s.bytes || hex.EncodeToString(h.Sum(nil)) != s.sha {
		return nil, fmt.Errorf("source shard digest or size mismatch")
	}
	physical, err := parquet.OpenFile(f, int64(s.bytes))
	if err != nil {
		return nil, err
	}
	if physical.Schema().String() != schema.String() || physical.NumRows() != int64(s.rows) {
		return nil, fmt.Errorf("source review schema or row count mismatch")
	}
	reader := parquet.NewReader(physical)
	defer reader.Close()
	out := []SourceExample{}
	columns := schema.Columns()
	for _, ordinal := range ordinals {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := reader.SeekToRow(int64(ordinal - s.first)); err != nil {
			return nil, err
		}
		rows := make([]parquet.Row, 1)
		n, err := reader.ReadRows(rows)
		if n != 1 || err != nil && err != io.EOF {
			return nil, fmt.Errorf("source review row read: %d, %v", n, err)
		}
		fields := make(map[string]any, len(columns))
		for _, value := range rows[0] {
			column := value.Column()
			if column < 0 || column >= len(columns) || len(columns[column]) != 1 {
				return nil, fmt.Errorf("unsupported source column")
			}
			name := columns[column][0]
			if _, exists := fields[name]; exists {
				return nil, fmt.Errorf("repeated source column")
			}
			switch {
			case value.IsNull():
				fields[name] = nil
			case value.Kind() == parquet.ByteArray:
				fields[name] = string(value.ByteArray())
			case value.Kind() == parquet.Boolean:
				fields[name] = value.Boolean()
			case value.Kind() == parquet.Int64:
				fields[name] = strconv.FormatInt(value.Int64(), 10)
			case value.Kind() == parquet.Int32:
				fields[name] = value.Int32()
			default:
				return nil, fmt.Errorf("unsupported source value kind")
			}
		}
		if len(fields) != len(columns) {
			return nil, fmt.Errorf("incomplete source row")
		}
		if _, err := conversion.Convert(rows); err != nil {
			return nil, err
		}
		var decoded T
		if err := projected.Reconstruct(&decoded, rows[0]); err != nil {
			return nil, err
		}
		_, observation, err := selectRow(decoded)
		if err != nil {
			return nil, err
		}
		if observation == nil || !reflect.DeepEqual(*observation, wanted[ordinal]) || cycle(decoded) != period {
			return nil, fmt.Errorf("saved observation differs from immutable source at %s ordinal %d", side, ordinal)
		}
		out = append(out, SourceExample{side, factID, ordinal, s.sha, fields})
	}
	return out, nil
}
