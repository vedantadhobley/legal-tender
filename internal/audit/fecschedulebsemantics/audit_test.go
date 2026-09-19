package fecschedulebsemantics

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
)

func ptr[T any](v T) *T { return &v }

func TestReportingRoleRequiresFormAndSchedule(t *testing.T) {
	for _, test := range []struct{ form, line, schedule, want string }{
		{"F3X", "23", "SB", "federal_contribution"},
		{"F3P", "23", "SB", "operating_expenditure"},
		{"F3", "18", "SB", "authorized_committee_transfer"},
		{"F3X", "22", "SB", "affiliated_or_party_transfer"},
		{"F3X", "23", "SE", "unresolved_schedule"},
		{"F3X", "28a", "SB", "unresolved_form_line"},
		{"F3X", "99", "SB", "unresolved_form_line"},
		{"F3X", "30B", "SB", "federal_election_activity"},
		{"F3X", "31B", "SB", "unresolved_form_line"},
	} {
		if got := reportingRole(test.form, cell(&test.line), cell(&test.schedule)); got != test.want {
			t.Errorf("%+v: %s", test, got)
		}
	}
}

func TestProfilePreservesActionsMemosAmountsAndLineage(t *testing.T) {
	base := probeRow{Ordinal: 1, Period: 2024, SubID: "100", Form: "F3X", Schedule: ptr("SB"), Line: ptr("23"), Amount: ptr(int64(1099)), AmountState: "reported_value", Sender: ptr("C00000001"), RawRecipient: ptr("C00000002"), CleanRecipient: ptr("C00000002"), Action: ptr("N"), Transaction: ptr("REUSED"), OriginalSubmission: ptr("99"), BackReferenceTransaction: ptr("PARENT")}
	rows := []probeRow{base, base, base, base}
	rows[1].Ordinal = 2
	rows[1].SubID = "101"
	rows[1].Action = ptr("C")
	rows[1].Amount = ptr(int64(-99))
	rows[2].Ordinal = 3
	rows[2].SubID = "102"
	rows[2].Memo = ptr("X")
	rows[2].Memoed = true
	rows[3].Ordinal = 4
	rows[3].SubID = "103"
	rows[3].Memo = ptr("")
	rows[3].Amount = nil
	rows[3].AmountState = "source_null"
	groups := make(map[Shape]*Group)
	for i := range rows {
		if err := observe(groups, &rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	var total Measures
	for _, group := range groups {
		if err := mergeMeasures(&total, group.Measures); err != nil {
			t.Fatal(err)
		}
	}
	if len(groups) != 4 || total.Rows != 4 || total.AmountMinorUnits != 2099 || total.NonMemoRows != 3 || total.NonMemoAmountMinorUnits != 1000 || total.MissingAmountRows != 1 || total.TransactionRows != 4 || total.BackReferenceTransactionRows != 4 {
		t.Fatalf("lost evidence: %+v, groups=%d", total, len(groups))
	}
	rows[0].Memoed = true
	if err := observe(groups, &rows[0]); err == nil {
		t.Fatal("accepted inconsistent derived memo flag")
	}
	rows[0].Memoed = false
	rows[0].AmountState = "source_null"
	if err := observe(groups, &rows[0]); err == nil {
		t.Fatal("accepted inconsistent derived amount")
	}
}

func TestRecipientIdentityAndOverflow(t *testing.T) {
	for _, test := range []struct {
		raw, clean *string
		want       string
	}{
		{ptr("C00000001"), ptr("C00000001"), "exact_matching_committee_id"},
		{ptr("C00000001"), ptr("C00000002"), "conflicting_committee_ids"},
		{ptr("C00000001"), nil, "raw_committee_id_only"},
		{ptr("INVALID"), ptr("C00000002"), "clean_committee_id_only"},
		{ptr(" C00000001"), nil, "no_valid_committee_id"},
	} {
		if got := recipientIdentity(test.raw, test.clean); got != test.want {
			t.Errorf("%+v: %s", test, got)
		}
	}
	for _, test := range []struct{ start, amount int64 }{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		value := test.start
		if err := addMoney(&value, test.amount); err == nil || value != test.start {
			t.Fatal("overflow was not rejected atomically")
		}
	}
}

func TestParquetScanConservesRowsAndRejectsWrongCycleOrSchema(t *testing.T) {
	root := t.TempDir()
	shard := writeTestShard(t, root)
	result := scanShard(context.Background(), root, "2024", shard)
	if result.err != nil || result.rows != 2 || len(result.groups) != 1 {
		t.Fatalf("scan: %+v", result)
	}
	for _, group := range result.groups {
		if group.Example.Ordinal != 1 || group.Measures.AmountMinorUnits != 200 {
			t.Fatalf("group: %+v", group)
		}
	}
	manifest := fecoccurrence.ScheduleBColumnarManifest{Shards: []fecoccurrence.ScheduleBColumnarShard{shard}}
	a, rows, err := scan(context.Background(), Options{StorageRoot: root, Cycle: "2024", Workers: 1, Progress: func(string) {}}, manifest)
	if err != nil || rows != 2 {
		t.Fatal(err)
	}
	b, _, err := scan(context.Background(), Options{StorageRoot: root, Cycle: "2024", Workers: 4, Progress: func(string) {}}, manifest)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("worker-order result changed: %v", err)
	}
	if got := scanShard(context.Background(), root, "2022", shard); got.err == nil {
		t.Fatal("accepted wrong cycle")
	}
	shard.FirstSourceRowOrdinal = 2
	if got := scanShard(context.Background(), root, "2024", shard); got.err == nil {
		t.Fatal("accepted bad locator")
	}
	file, err := os.Create(filepath.Join(root, "wrong.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewGenericWriter[probeRow](file)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	file.Close()
	info, _ := os.Stat(file.Name())
	shard.StorageKey = "wrong.parquet"
	shard.Bytes = uint64(info.Size())
	if got := scanShard(context.Background(), root, "2024", shard); got.err == nil {
		t.Fatal("accepted incomplete physical schema")
	}
}

func writeTestShard(t *testing.T, root string) fecoccurrence.ScheduleBColumnarShard {
	t.Helper()
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	var source strings.Builder
	for i := 1; i <= 2; i++ {
		values := make([]string, scheduleb.FieldCount)
		for j := range values {
			values[j] = "\\N"
		}
		for name, value := range map[string]string{"sub_id": strconv.Itoa(i), "filing_form": "F3X", "two_year_transaction_period": "2024", "line_num": "23", "schedule_type": "SB", "disb_amt": "1.00"} {
			index, _ := scheduleb.ColumnIndex(name)
			values[index] = value
		}
		source.WriteString(strings.Join(values, "\t") + "\n")
	}
	decoder := scheduleb.NewDecoder(strings.NewReader(source.String()))
	file, err := os.Create(filepath.Join(root, "fixture.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewWriter(file, schema.Parquet())
	for i := 1; i <= 2; i++ {
		if !decoder.Scan() {
			t.Fatalf("source decode: %v", decoder.Err())
		}
		row := decoder.Row()
		encoded, err := schema.Encode(nil, row, schedulebparquet.Metadata{SourceRowOrdinal: uint64(i), SourceRawByteLength: 1}, schedulebparquet.Derived{DisbursementAmountMinorUnits: ptr(int64(100)), DisbursementAmountSourceScale: ptr(int32(2)), DisbursementAmountState: "reported_value", BundledRefundState: "source_null", TwoYearTransactionPeriod: 2024})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.WriteRows([]parquet.Row{encoded}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(file.Name())
	return fecoccurrence.ScheduleBColumnarShard{StorageKey: "fixture.parquet", FirstSourceRowOrdinal: 1, LastSourceRowOrdinal: 2, Facts: 2, Bytes: uint64(info.Size())}
}
