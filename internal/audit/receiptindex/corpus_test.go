package receiptindex_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/audit/receiptindex"
)

// This gate uses the physical full-row reader, not the benchmark's source
// projection, sort comparator, or readback helper. It is an independent mapping
// check using the same pinned Parquet library, not an independent decoder.
func TestRetainedCorpus(t *testing.T) {
	dir, root := os.Getenv("LT_RECEIPT_INDEX_AUDIT"), os.Getenv("LT_RECEIPT_INDEX_STORAGE")
	if dir == "" || root == "" {
		t.Skip("retained receipt-index corpus not configured")
	}
	var middleID string
	for _, name := range []string{"first", "middle", "last", "replay"} {
		t.Run(name, func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join(dir, name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var result receiptindex.Result
			if err := json.Unmarshal(b, &result); err != nil {
				t.Fatal(err)
			}
			if result.State != "complete_bounded_benchmark" || result.Evidence.ProductionReady || result.Evidence.IdentityResolved {
				t.Fatal("scope promotion")
			}
			evidenceBytes, _ := json.Marshal(result.Evidence)
			sum := sha256.Sum256(evidenceBytes)
			if hex.EncodeToString(sum[:]) != result.EvidenceID {
				t.Fatal("evidence digest")
			}
			if name == "middle" {
				middleID = result.EvidenceID
			}
			if name == "replay" && result.EvidenceID != middleID {
				t.Fatal("replay changed evidence")
			}
			var retained uint64
			for _, files := range [][]receiptindex.File{result.Evidence.SourceOrder, result.Evidence.ReportOrder} {
				var count uint64
				for _, file := range files {
					f, err := os.Open(filepath.Join(dir, name, file.Name))
					if err != nil {
						t.Fatal(err)
					}
					h := sha256.New()
					n, err := io.Copy(h, f)
					f.Close()
					if err != nil || uint64(n) != file.Bytes || hex.EncodeToString(h.Sum(nil)) != file.SHA256 {
						t.Fatal("stored file integrity", file.Name, err)
					}
					retained += file.Bytes
					count += file.Rows
				}
				if count != result.Evidence.SampleRows {
					t.Fatal("layout row conservation")
				}
			}
			if retained != result.OutputBytes || retained > result.Evidence.MaxOutputBytes {
				t.Fatal("byte conservation")
			}
			if name == "replay" {
				return
			} // exact file digests also matched by evidence ID
			checkSourceMapping(t, root, filepath.Join(dir, name), result.Evidence)
		})
	}
}

func checkSourceMapping(t *testing.T, root, dir string, e receiptindex.Evidence) {
	t.Helper()
	f, err := os.Open(filepath.Join(root, e.SourceShard.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	p, err := parquet.OpenFile(f, int64(e.SourceShard.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewReader(p)
	defer r.Close()
	columns := map[string]int{}
	for i, path := range p.Schema().Columns() {
		if len(path) == 1 {
			columns[path[0]] = i
		}
	}
	// Explicit source-to-Go mapping: do not derive expectations from Row tags.
	mapping := map[string]string{
		"Ordinal": "lt_source_row_ordinal", "Cycle": "lt_two_year_transaction_period", "Normalization": "lt_normalization_state",
		"Recipient": "cmte_id", "File": "file_num", "Transaction": "tran_id", "BackReference": "back_ref_tran_id",
		"BackSchedule": "back_ref_sched_nm", "Schedule": "schedule_type", "Line": "line_num", "Entity": "entity_tp",
		"Contributor": "contbr_id", "CleanContributor": "clean_contbr_id", "Conduit": "conduit_cmte_id",
		"ReceiptType": "receipt_tp", "Individual": "is_individual", "Memo": "lt_memoed_subtotal",
		"Amount": "lt_receipt_amount_minor_units", "AmountState": "lt_receipt_amount_state",
	}
	typ := reflect.TypeOf(receiptindex.Row{})
	if len(mapping) != typ.NumField() {
		t.Fatal("unchecked access column")
	}
	indexes := make([]int, typ.NumField())
	for i := range indexes {
		column, ok := columns[mapping[typ.Field(i).Name]]
		if !ok {
			t.Fatal("missing physical source column")
		}
		indexes[i] = column
	}
	raw := make([]parquet.Row, 256)
	values := make([]parquet.Value, len(columns))
	position, available := 0, 0
	var seen uint64
	for _, file := range e.SourceOrder {
		out, err := os.Open(filepath.Join(dir, file.Name))
		if err != nil {
			t.Fatal(err)
		}
		reader := parquet.NewGenericReader[receiptindex.Row](out)
		rows := make([]receiptindex.Row, 256)
		for {
			n, err := reader.Read(rows)
			for _, projected := range rows[:n] {
				if position == available {
					var sourceErr error
					available, sourceErr = r.ReadRows(raw)
					position = 0
					if available == 0 || (sourceErr != nil && sourceErr != io.EOF) {
						t.Fatal("physical source read", sourceErr)
					}
				}
				clear(values)
				for _, v := range raw[position] {
					values[v.Column()] = v
				}
				position++
				got := reflect.ValueOf(projected)
				for i, index := range indexes {
					v, field := values[index], got.Field(i)
					if v.IsNull() {
						if field.Kind() != reflect.Pointer || !field.IsNil() {
							t.Fatalf("null mismatch row %d field %s", seen, typ.Field(i).Name)
						}
						continue
					}
					if field.Kind() == reflect.Pointer {
						if field.IsNil() {
							t.Fatal("lost value")
						}
						field = field.Elem()
					}
					var same bool
					switch field.Kind() {
					case reflect.String:
						same = field.String() == string(v.ByteArray())
					case reflect.Bool:
						same = field.Bool() == v.Boolean()
					case reflect.Int64:
						if v.Kind() == parquet.Int32 {
							same = field.Int() == int64(v.Int32())
						} else {
							same = field.Int() == v.Int64()
						}
					default:
						t.Fatal("unhandled field", typ.Field(i).Name)
					}
					if !same {
						t.Fatalf("physical source mismatch row %d field %s", seen, typ.Field(i).Name)
					}
				}
				if uint64(projected.Ordinal) != e.First+seen {
					t.Fatal("occurrence identity mismatch")
				}
				seen++
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatal(io.ErrNoProgress)
			}
		}
		reader.Close()
		out.Close()
	}
	if seen != e.SampleRows || e.First+seen-1 != e.Last {
		t.Fatal("physical source sample range")
	}
	t.Logf("verified %d rows x %d access fields against full physical source rows", seen, len(mapping))
}
