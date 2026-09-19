package receiptindex

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func pointer[T any](v T) *T { return &v }

func fixtureRows() []Row {
	return []Row{
		{Ordinal: 1, Cycle: 2024, Normalization: "valid", Recipient: pointer("C00000002"), File: pointer("42"),
			Transaction: pointer("same"), BackReference: pointer("\tref|λ"), BackSchedule: pointer("SA"),
			Schedule: pointer("SA"), Line: pointer("11AI"), Entity: pointer("IND"), Contributor: pointer(""),
			CleanContributor: pointer("C00000001"), Conduit: pointer("C00000003"), ReceiptType: pointer("15E"),
			Individual: pointer(false), Memo: true, Amount: pointer(int64(-123)), AmountState: "reported_value"},
		{Ordinal: 2, Cycle: 2024, Normalization: "valid", Recipient: pointer("C00000002"), File: pointer("42"),
			Transaction: pointer("same"), Individual: pointer(true), Amount: pointer(int64(math.MaxInt64)), AmountState: "reported_value"},
		{Ordinal: 3, Cycle: 2024, Normalization: "valid", Recipient: pointer(""), File: nil, Transaction: pointer(""), AmountState: "source_null"},
		{Ordinal: 4, Cycle: 2024, Normalization: "valid", Recipient: nil, File: pointer(""), Transaction: nil, Amount: pointer(int64(0)), AmountState: "reported_value"},
	}
}

func TestLayoutsPreserveEveryProjectedValueAndDuplicateOccurrence(t *testing.T) {
	ctx := context.Background()
	rows := fixtureRows()
	dir := t.TempDir()
	cap := &budget{limit: 1 << 20}
	first, err := measure(ctx, dir, "source.parquet", rows, cap)
	if err != nil {
		t.Fatal(err)
	}
	again, err := measure(ctx, dir, "replay.parquet", rows, cap)
	if err != nil || first.SHA256 != again.SHA256 || first.ValuesSHA256 != again.ValuesSHA256 {
		t.Fatal("unstable replay", err)
	}
	slices.SortFunc(rows, compare)
	var ordinals []int64
	for _, row := range rows {
		ordinals = append(ordinals, row.Ordinal)
	}
	if !reflect.DeepEqual(ordinals, []int64{4, 3, 1, 2}) {
		t.Fatal("lost null/empty distinction or duplicate", ordinals)
	}
	ordered, err := measure(ctx, dir, "report.parquet", rows, cap)
	if err != nil {
		t.Fatal(err)
	}
	if ordered.Rows != 4 || cap.used != first.Bytes+again.Bytes+ordered.Bytes {
		t.Fatal("conservation")
	}
	rows[0].Amount = pointer(int64(99))
	if _, err := verify(ctx, filepath.Join(dir, ordered.Name), rows); err == nil {
		t.Fatal("changed value accepted")
	}
	if _, err := measure(ctx, dir, first.Name, rows, cap); err == nil {
		t.Fatal("overwrote existing file")
	}
}

func TestBudgetCancellationAndCorruptFileFailClosed(t *testing.T) {
	dir := t.TempDir()
	cap := &budget{limit: 10}
	if _, err := measure(context.Background(), dir, "limited.parquet", fixtureRows(), cap); err == nil {
		t.Fatal("budget ignored")
	}
	if cap.used > cap.limit {
		t.Fatal("budget overshot")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := measure(ctx, dir, "cancelled.parquet", fixtureRows(), &budget{limit: 1 << 20}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := verify(context.Background(), filepath.Join(dir, "limited.parquet"), fixtureRows()); err == nil {
		t.Fatal("partial output accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "limited.parquet")); err != nil {
		t.Fatal("failed-attempt evidence removed", err)
	}
}

func TestCloneOwnsSourceMemoryAndValidationRejectsBadMetadata(t *testing.T) {
	r := fixtureRows()[0]
	c := cloneRow(r)
	*r.Recipient, *r.Amount, *r.Individual = "changed", 9, true
	if *c.Recipient != "C00000002" || *c.Amount != -123 || *c.Individual {
		t.Fatal("source buffer alias")
	}
	if err := validateRow(c, 1, 2024); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Row{
		{Ordinal: 0, Cycle: 2024, Normalization: "valid", AmountState: "source_null"},
		{Ordinal: 1, Cycle: 2022, Normalization: "valid", AmountState: "source_null"},
		{Ordinal: 1, Cycle: 2024, Normalization: "invalid", AmountState: "source_null"},
		{Ordinal: 1, Cycle: 2024, Normalization: "valid", AmountState: "reported_value"},
		{Ordinal: 1, Cycle: 2024, Normalization: "valid", AmountState: "source_null", Amount: pointer(int64(0))},
	} {
		if err := validateRow(bad, 1, 2024); err == nil {
			t.Fatal("invalid source accepted", bad)
		}
	}
}

func TestOptionBounds(t *testing.T) {
	base := Options{StorageRoot: "/unused", Manifest: "/unused/facts", Cycle: "2024", OutputDirectory: "/unused/new",
		BuildSHA256: strings.Repeat("a", 64), MaxRows: 100000, RunRows: 10000, MaxOutputBytes: 1 << 20}
	if err := validateOptions(base); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Options){
		func(o *Options) { o.MaxRows = 1000001 }, func(o *Options) { o.RunRows = 100001 },
		func(o *Options) { o.RunRows = 1 }, func(o *Options) { o.ShardIndex = -1 },
		func(o *Options) { o.MaxOutputBytes = 0 }, func(o *Options) { o.MaxOutputBytes = 1<<30 + 1 },
		func(o *Options) { o.BuildSHA256 = strings.Repeat("A", 64) },
	} {
		o := base
		mutate(&o)
		if _, err := Run(context.Background(), o); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
}
