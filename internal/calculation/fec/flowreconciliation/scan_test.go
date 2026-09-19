package flowreconciliation

import (
	"context"
	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestFullSchemaScanAndReadback(t *testing.T) {
	root := t.TempDir()
	shards := []shard{writeBShard(t, root, 1), writeBShard(t, root, 7)}
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	coords := func(r bRow) (int64, int64, *int64) { return r.Ordinal, r.Period, r.Amount }
	run := func(workers int) scanned {
		got, err := scan(context.Background(), root, shards, schema.Parquet(), 2024, workers, selectB, coords, func(string) {})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	a, b := run(1), run(4)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("worker-dependent result")
	}
	if a.total != (Measures{12, 12, 800}) || len(a.observations) != 6 {
		t.Fatal(a.total, len(a.observations))
	}
	if a.observations[0].Amount != 100 || a.observations[1].Amount != -100 || a.observations[2].Amount != 0 || a.observations[3].Ordinal != 7 {
		t.Fatal("grain changed", a.observations)
	}
	first, err := writeSide(context.Background(), root, "fixture", "schedule-b", a)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := writeSide(context.Background(), root, "fixture", "schedule-b", b)
	if err != nil || !reflect.DeepEqual(first, replay) {
		t.Fatal("artifact replay differs", err)
	}
	if first.Selected != (Measures{6, 6, 0}) {
		t.Fatal(first.Selected)
	}
	// A null-date cohort and empty artifacts must also survive exact readback.
	if _, err := writeVerified(context.Background(), root, "fixture", "empty", []Observation{}); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*shard){func(s *shard) { s.first++ }, func(s *shard) { s.rows++ }, func(s *shard) { s.last-- }} {
		bad := shards[0]
		edit(&bad)
		if got := scanShard(context.Background(), root, bad, schema.Parquet(), 2024, selectB, coords); got.err == nil {
			t.Fatal("invalid physical boundary")
		}
	}
	if got := scanShard(context.Background(), root, shards[0], schema.Parquet(), 2022, selectB, coords); got.err == nil {
		t.Fatal("cycle mismatch")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scan(ctx, root, shards, schema.Parquet(), 2024, 2, selectB, coords, func(string) {}); err == nil {
		t.Fatal("ignored cancellation")
	}
	f, err := os.Create(filepath.Join(root, "reduced.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[bRow](f)
	w.Close()
	f.Close()
	info, _ := os.Stat(f.Name())
	reduced := shard{key: "reduced.parquet", bytes: uint64(info.Size())}
	if got := scanShard(context.Background(), root, reduced, schema.Parquet(), 2024, selectB, coords); got.err == nil {
		t.Fatal("reduced physical schema accepted")
	}
	path := filepath.Join(root, first.Observations.StorageKey)
	if err := os.WriteFile(path, []byte("corrupt artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := writeSide(context.Background(), root, "fixture", "schedule-b", a); err == nil {
		t.Fatal("corrupt reuse accepted")
	}
}

func TestReceiverSelectionAndDetachedEvidence(t *testing.T) {
	r := aRow{Ordinal: 1, Period: 2024, Normalization: "valid", SubID: "1", Recipient: ptr("C00000002"), Raw: ptr("C00000001"), Clean: ptr("C00000001"), Type: ptr("18K"), Amount: ptr(int64(100)), AmountState: "reported_value", Date: ptr(int32(20000))}
	for _, tc := range []struct{ code, role string }{{"18K", "contribution"}, {"15Z", "in_kind"}, {"18G", "affiliated_transfer"}, {"22Z", "refund_or_repayment"}} {
		r.Type = &tc.code
		_, o, err := selectA(r)
		if err != nil || o == nil || o.Role != tc.role {
			t.Fatal(o, err)
		}
	}
	_, o, _ := selectA(r)
	*r.Date = 3
	if *o.Date != 20000 {
		t.Fatal("retained mutable date")
	}
	r.Memo = true
	if _, o, err := selectA(r); err != nil || o != nil {
		t.Fatal("memo included", err)
	}
	r.Normalization = "invalid"
	if _, _, err := selectA(r); err == nil {
		t.Fatal("invalid normalization")
	}
}

func writeBShard(t *testing.T, root string, first uint64) shard {
	t.Helper()
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	key := strconv.FormatUint(first, 10) + ".parquet"
	f, err := os.Create(filepath.Join(root, key))
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewWriter(f, schema.Parquet())
	amounts := []int64{100, -100, 0, 100, 100, 200}
	for i, amount := range amounts {
		values := make([]string, scheduleb.FieldCount)
		for j := range values {
			values[j] = "\\N"
		}
		code, memo := "24K", "\\N"
		if i == 3 {
			memo = "X"
		}
		if i == 4 {
			code = "24T"
		}
		if i == 5 {
			code = "\\N"
		}
		for name, value := range map[string]string{"sub_id": strconv.FormatUint(first+uint64(i), 10), "filing_form": "F3X", "two_year_transaction_period": "2024", "line_num": "23", "schedule_type": "SB", "cmte_id": "C00000001", "recipient_cmte_id": "C00000002", "clean_recipient_cmte_id": "C00000002", "disb_tp": code, "memo_cd": memo, "tran_id": "REPEATED-TRANSACTION", "action_cd": "N", "disb_amt": strconv.FormatInt(amount/100, 10) + ".00"} {
			index, ok := scheduleb.ColumnIndex(name)
			if !ok {
				t.Fatal(name)
			}
			values[index] = value
		}
		// Include preceding physical rows so the decoder owns the true locator.
		ordinal := first + uint64(i)
		decoder := scheduleb.NewDecoder(strings.NewReader(strings.Repeat(strings.Join(values, "\t")+"\n", int(ordinal))))
		for range ordinal {
			if !decoder.Scan() {
				t.Fatal(decoder.Err())
			}
		}
		row, err := schema.Encode(nil, decoder.Row(), schedulebparquet.Metadata{SourceRowOrdinal: first + uint64(i), SourceRawByteLength: 1}, schedulebparquet.Derived{DisbursementAmountMinorUnits: ptr(amount), DisbursementAmountSourceScale: ptr(int32(2)), DisbursementAmountState: "reported_value", BundledRefundState: "source_null", TwoYearTransactionPeriod: 2024, MemoedSubtotal: i == 3})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(f.Name())
	return shard{key, first, first + 5, 6, uint64(info.Size())}
}
