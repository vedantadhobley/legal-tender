package receiptreferences

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Retain the previous reader as a differential oracle, not a runtime fallback.
func genericReferenceReader(p *parquet.File) (referenceRowReader, error) {
	return parquet.NewGenericReader[Row](p), nil
}

func referenceParquetFixture(t *testing.T, rows []Row) ([]byte, *scheduleaparquet.Schema) {
	t.Helper()
	s, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	selected := parquet.SchemaOf(new(Row))
	var b bytes.Buffer
	w := parquet.NewWriter(&b, s.Parquet(), parquet.MaxRowsPerRowGroup(37), parquet.PageBufferSize(512))
	for _, input := range rows {
		r := make(parquet.Row, s.ColumnCount())
		for i, path := range s.Parquet().Columns() {
			c, _ := s.Parquet().Lookup(path...)
			v := parquet.NullValue()
			if c.MaxDefinitionLevel == 0 {
				switch c.Node.Type().Kind() {
				case parquet.ByteArray:
					v = parquet.ByteArrayValue([]byte(""))
				case parquet.Int64:
					v = parquet.Int64Value(-135)
				case parquet.Int32:
					v = parquet.Int32Value(-2)
				case parquet.Boolean:
					v = parquet.BooleanValue(false)
				default:
					t.Fatal("unexpected fixture type")
				}
			}
			r[i] = v.Level(0, 0, i)
		}
		for i, v := range selected.Deconstruct(nil, input) {
			at, ok := s.ColumnIndex(selected.Columns()[i][0])
			if !ok {
				t.Fatal("fixture binding")
			}
			r[at] = v.Level(v.RepetitionLevel(), v.DefinitionLevel(), at)
		}
		if _, err := w.WriteRows([]parquet.Row{r}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), s
}

func TestNarrowReferenceReaderExactValuesAcrossPagesAndGroups(t *testing.T) {
	rows := make([]Row, 513)
	variants := []*string{nil, ptr(""), ptr(" "), ptr("duplicate"), ptr("É中\\\t\n"), ptr(strings.Repeat("x", 4096))}
	for i := range rows {
		rows[i] = Row{Ordinal: int64(i + 1), Cycle: 2024, Normalization: "valid", Recipient: variants[i%6], File: variants[(i+1)%6], Transaction: variants[(i+2)%6], BackReference: variants[(i+3)%6], BackSchedule: variants[(i+4)%6], Schedule: variants[(i+5)%6], Line: variants[i%6]}
	}
	b, _ := referenceParquetFixture(t, rows)
	p, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Schema().Columns()) != 99 || len(p.RowGroups()) < 2 {
		t.Fatal("fixture must exercise wide schema and multiple groups")
	}
	for _, size := range []int{1, 7, 128, 8192} {
		r, err := newNarrowReferenceReader(p)
		if err != nil {
			t.Fatal(err)
		}
		old := parquet.NewGenericReader[Row](p)
		got, baseline := make([]Row, size), make([]Row, size)
		var seen int
		for {
			n, err := r.Read(got)
			count, oldErr := old.Read(baseline[:n])
			if n != count || (oldErr != nil && oldErr != io.EOF) || !reflect.DeepEqual(got[:n], baseline[:n]) || !reflect.DeepEqual(got[:n], rows[seen:seen+n]) {
				t.Fatalf("batch %d at %d changed a selected value", size, seen)
			}
			seen += n
			if err == io.EOF {
				break
			}
			if err != nil || n == 0 {
				t.Fatal("narrow reader progress", err)
			}
		}
		if seen != len(rows) {
			t.Fatal("fixture conservation")
		}
		r.Close()
		old.Close()
	}
}

func TestNarrowReferenceReaderRejectsIncompatibleColumns(t *testing.T) {
	base := parquet.SchemaOf(new(Row))
	for _, change := range []string{"missing", "nullable", "type", "nested", "repeated"} {
		t.Run(change, func(t *testing.T) {
			g := parquet.Group{}
			for _, f := range base.Fields() {
				g[f.Name()] = f
			}
			switch change {
			case "missing":
				delete(g, "tran_id")
			case "nullable":
				g["tran_id"] = parquet.String()
			case "type":
				g["tran_id"] = parquet.Optional(parquet.Leaf(parquet.Int64Type))
			case "nested":
				g["extra"] = parquet.Group{"nested": parquet.String()}
			case "repeated":
				g["tran_id"] = parquet.Repeated(parquet.String())
			}
			var b bytes.Buffer
			w := parquet.NewWriter(&b, parquet.NewSchema("bad", g))
			if err := w.Write(map[string]any{}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			p, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := newNarrowReferenceReader(p); err == nil {
				t.Fatal("unsupported projection accepted")
			}
		})
	}
}

func TestNarrowScanPreservesFailureAndSourceGuards(t *testing.T) {
	root := t.TempDir()
	rows := fixture()
	b, schema := referenceParquetFixture(t, rows)
	path := filepath.Join(root, "source.parquet")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(b)
	s := occ.ScheduleAColumnarShard{StorageKey: "source.parquet", Facts: uint64(len(rows)), Bytes: uint64(len(b)), SHA256: hex.EncodeToString(digest[:]), FirstSourceRowOrdinal: 1}
	for _, fail := range []string{"size", "digest", "count", "ordinal", "cycle", "cancel", "consumer"} {
		t.Run(fail, func(t *testing.T) {
			shard, cycle := s, int64(2024)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("consumer error")
			visited := false
			switch fail {
			case "size":
				shard.Bytes++
			case "digest":
				shard.SHA256 = strings.Repeat("0", 64)
			case "count":
				shard.Facts++
			case "ordinal":
				shard.FirstSourceRowOrdinal++
			case "cycle":
				cycle++
			case "cancel":
				cancel()
			}
			err := scanShard(ctx, root, shard, cycle, schema, func([]Row) error { visited = true; return failure })
			if err == nil || (fail != "consumer" && visited) || (fail == "consumer" && !errors.Is(err, failure)) || (fail == "cancel" && !errors.Is(err, context.Canceled)) {
				t.Fatal("source guard lost", err)
			}
		})
	}
}

// All ten fields, every row of the same eight whole source shards used in the
// profile. The old generic reader remains separate from the new reconstruction.
func TestRetainedNarrowReaderComparison(t *testing.T) {
	root := os.Getenv("LT_REFERENCE_PROFILE_STORAGE")
	if root == "" {
		t.Skip("retained narrow-reader comparison not configured")
	}
	b, err := os.ReadFile(os.Getenv("LT_REFERENCE_PROFILE_MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(b)
	if hex.EncodeToString(digest[:]) != os.Getenv("LT_REFERENCE_PROFILE_SHA256") {
		t.Fatal("pinned manifest changed")
	}
	var m occ.ScheduleAColumnarManifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	shards, err := profileShards(m.Shards, 8)
	if err != nil {
		t.Fatal(err)
	}
	cycle, err := strconv.ParseInt(m.Cycle, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	schema, _ := scheduleaparquet.NewSchema()
	for _, s := range shards {
		t.Run(fmt.Sprint(s.Index), func(t *testing.T) {
			path, err := artifact.Resolve(root, s.StorageKey)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			p, err := parquet.OpenFile(f, int64(s.Bytes))
			if err != nil {
				t.Fatal(err)
			}
			old := parquet.NewGenericReader[Row](p)
			defer old.Close()
			buffer := make([]Row, 8192)
			var seen uint64
			err = scanShard(t.Context(), root, s, cycle, schema, func(rows []Row) error {
				n, err := old.Read(buffer[:len(rows)])
				if (err != nil && err != io.EOF) || n != len(rows) {
					return fmt.Errorf("baseline reader count/error: %d %v", n, err)
				}
				for i := range rows {
					if !reflect.DeepEqual(rows[i], buffer[i]) {
						return fmt.Errorf("selected field changed at ordinal %d", rows[i].Ordinal)
					}
				}
				seen += uint64(n)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if n, err := old.Read(buffer[:1]); n != 0 || err != io.EOF || seen != s.Facts {
				t.Fatal("baseline source conservation")
			}
			t.Logf("all ten selected fields equal on %d rows", seen)
		})
	}
}
