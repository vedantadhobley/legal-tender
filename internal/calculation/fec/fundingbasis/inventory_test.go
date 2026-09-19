package fundingbasis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

func ptr[T any](v T) *T { return &v }

func baseRow() receiptRow {
	return receiptRow{Cycle: 2024, Normalization: "valid", Recipient: ptr("C00000001"), Individual: ptr(true), ReceiptType: ptr("15"), Amount: ptr(int64(123)), AmountState: "reported_value"}
}

func TestClassification(t *testing.T) {
	for _, tc := range []struct {
		name, component string
		edit            func(*receiptRow)
	}{
		{"individual", "itemized_individual_only", func(r *receiptRow) {}},
		{"committee", "committee_flow_only", func(r *receiptRow) {
			r.Individual = ptr(false)
			r.Contributor = ptr("C00000002")
			r.CleanContributor = r.Contributor
			r.ReceiptType = ptr("15K")
		}},
		{"overlap", "overlapping_individual_and_committee", func(r *receiptRow) {
			r.Contributor = ptr("C00000002")
			r.CleanContributor = r.Contributor
			r.ReceiptType = ptr("15K")
		}},
		{"memo", "memo_subtotal", func(r *receiptRow) { r.Memo = true }},
		{"null class", "unresolved_individual_class", func(r *receiptRow) { r.Individual = nil }},
		{"null amount", "unknown_amount", func(r *receiptRow) { r.Amount = nil; r.AmountState = "source_null" }},
		{"other", "other_reported_receipt", func(r *receiptRow) { r.Individual = ptr(false) }},
		{"bad recipient", "unresolved_recipient", func(r *receiptRow) { r.Recipient = ptr("BAD") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := baseRow()
			tc.edit(&r)
			k := classify(r)
			if k.Component != tc.component {
				t.Fatalf("%+v", k)
			}
		})
	}
	// Null classification does not become excluded-individual, even on memo rows.
	r := baseRow()
	r.Memo = true
	r.Individual = nil
	k := classify(r)
	if k.IndividualDecision != "unresolved_individual_class" || k.Component != "memo_subtotal" {
		t.Fatal(k)
	}
	r = baseRow()
	r.ReceiptType = ptr("15E")
	r.ConduitID = ptr("C00000003")
	if k := classify(r); k.Component != "itemized_individual_only" || k.ReceiptRole != committeeflows.RoleEarmarked {
		t.Fatal(k)
	}
	// A conduit field must not manufacture a committee source ID or second row.
	if classify(r).CommitteeDecision != committeeflows.DecisionExcludedNoSource {
		t.Fatal(classify(r))
	}
}

func TestMeasuresExactSignedAndUnknown(t *testing.T) {
	var m Measures
	for _, amount := range []*int64{ptr(int64(201)), ptr(int64(-102)), ptr(int64(0)), nil} {
		r := baseRow()
		r.Amount = amount
		if err := m.observe(r); err != nil {
			t.Fatal(err)
		}
	}
	if !m.valid() || m.Rows != 4 || m.Known != 3 || m.Unknown != 1 || m.Signed != 99 || m.Positive != 201 || m.Negative != -102 {
		t.Fatal(m)
	}
	for _, pair := range [][2]int64{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		if _, err := add(pair[0], pair[1]); err == nil {
			t.Fatal("overflow accepted")
		}
	}
	if n, err := add(math.MinInt64, math.MaxInt64); err != nil || n != -1 {
		t.Fatal(n, err)
	}
}

func fixture(t *testing.T, root string, rows []receiptRow, shardSize int) occ.ScheduleAColumnarManifest {
	t.Helper()
	content, err := os.ReadFile("../../../../contracts/sources/fec/schedule-a/v1/fixtures/dump-2026-08-23/targeted-individual.copy")
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Split(strings.TrimSuffix(string(content), "\n"), "\t")
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	m := occ.ScheduleAColumnarManifest{Cycle: "2024", FactSetID: strings.Repeat("a", 64), Counts: occ.ScheduleAFactCounts{Facts: uint64(len(rows)), ValidFacts: uint64(len(rows)), SourceOccurrences: uint64(len(rows))}}
	var input bytes.Buffer
	decoder := schedulea.NewDecoder(&input)
	for first := 0; first < len(rows); first += shardSize {
		end := min(first+shardSize, len(rows))
		name := fmt.Sprintf("%d.parquet", len(m.Shards))
		path := filepath.Join(root, name)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		w := parquet.NewWriter(f, schema.Parquet())
		for i := first; i < end; i++ {
			r := rows[i]
			values := append([]string(nil), base...)
			set := func(name, value string) {
				index, ok := schedulea.ColumnIndex(name)
				if !ok {
					t.Fatal(name)
				}
				values[index] = value
			}
			source := func(p *string) string {
				if p == nil {
					return `\N`
				}
				return *p
			}
			set("cmte_id", source(r.Recipient))
			set("contbr_id", source(r.Contributor))
			set("clean_contbr_id", source(r.CleanContributor))
			set("receipt_tp", source(r.ReceiptType))
			set("conduit_cmte_id", source(r.ConduitID))
			set("conduit_cmte_nm", "SEPARATE CONDUIT")
			set("contbr_nm", "SOURCE PERSON")
			set("contbr_employer", "REPORTED EMPLOYER")
			set("contbr_occupation", "")
			set("sub_id", strconv.Itoa(1000+i))
			set("two_year_transaction_period", strconv.FormatInt(r.Cycle, 10))
			if r.Individual == nil {
				set("is_individual", `\N`)
			} else if *r.Individual {
				set("is_individual", "t")
			} else {
				set("is_individual", "f")
			}
			set("memo_cd", `\N`)
			if r.Memo {
				set("memo_cd", "X")
			}
			set("contb_receipt_amt", `\N`)
			if r.Amount != nil {
				value := *r.Amount
				sign := ""
				if value < 0 {
					sign = "-"
					value = -value
				}
				set("contb_receipt_amt", fmt.Sprintf("%s%d.%02d", sign, value/100, value%100))
			}
			input.WriteString(strings.Join(values, "\t") + "\n")
			if !decoder.Scan() {
				t.Fatal(decoder.Err())
			}
			scale := int32(2)
			encoded, err := schema.Encode(nil, decoder.Row(), scheduleaparquet.Metadata{SourceRowOrdinal: uint64(i + 1), SourceRawByteLength: uint64(len(decoder.Row().Raw()))}, scheduleaparquet.Derived{
				NormalizationState: r.Normalization, ReceiptAmountMinorUnits: r.Amount, ReceiptAmountState: r.AmountState, ReceiptAmountSourceScale: &scale,
				AggregateYTDState: "source_null", TwoYearTransactionPeriod: r.Cycle, MemoedSubtotal: r.Memo,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.WriteRows([]parquet.Row{encoded}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		m.Shards = append(m.Shards, occ.ScheduleAColumnarShard{Index: uint64(len(m.Shards)), FirstSourceRowOrdinal: uint64(first + 1), LastSourceRowOrdinal: uint64(end),
			Facts: uint64(end - first), ValidFacts: uint64(end - first), SourceRows: uint64(end - first), Bytes: uint64(len(b)), StorageKey: name, SHA256: hex.EncodeToString(sum[:])})
	}
	return m
}

func inventoryFixture(t *testing.T) (*Reader, []receiptRow) {
	t.Helper()
	root := t.TempDir()
	var rows []receiptRow
	for i := 0; i < 20; i++ {
		r := baseRow()
		r.Amount = ptr(int64(i * 101))
		if i%2 == 0 {
			r.ConduitID = ptr("C00000003")
			r.ReceiptType = ptr("15E")
		}
		if i%3 == 0 {
			r.Amount = ptr(-int64(i * 101))
		}
		if i%5 == 0 {
			r.Memo = true
		}
		rows = append(rows, r)
	}
	for _, recipient := range []*string{nil, ptr(""), ptr("BAD")} {
		r := baseRow()
		r.Recipient = recipient
		rows = append(rows, r)
	}
	r := baseRow()
	r.Amount = nil
	r.AmountState = "source_null"
	rows = append(rows, r)
	m := fixture(t, root, rows, 7)
	if err := dense(m); err != nil {
		t.Fatal(err)
	}
	var previous Result
	for _, workers := range []int{1, 4} {
		b, err := scan(context.Background(), root, m, workers, nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := assemble(m, strings.Repeat("b", 64), b)
		if err != nil {
			t.Fatal(err)
		}
		if workers == 4 && !reflect.DeepEqual(result, previous) {
			t.Fatal("worker count changed result")
		}
		previous = result
	}
	return &Reader{root, previous, m}, rows
}

func TestInventoryAndSourcePagination(t *testing.T) {
	r, rows := inventoryFixture(t)
	if r.result.Total.Rows != uint64(len(rows)) || r.result.Total.Unknown != 1 {
		t.Fatal(r.result.Total)
	}
	var want Measures
	for _, row := range rows {
		if err := want.observe(row); err != nil {
			t.Fatal(err)
		}
	}
	if want != r.result.Total {
		t.Fatal(want, r.result.Total)
	}
	for _, component := range []string{"", "itemized_individual_only", "memo_subtotal", "unknown_amount"} {
		var got []uint64
		after := uint64(0)
		for {
			page, err := r.Query(context.Background(), Query{Committee: "C00000001", Component: component, After: after, Limit: 3})
			if err != nil {
				t.Fatal(err)
			}
			for _, receipt := range page.Receipts {
				got = append(got, receipt.Ordinal)
				if len(receipt.Fields) != 99 || receipt.Fields["contbr_nm"] != "SOURCE PERSON" || receipt.Fields["contbr_employer"] != "REPORTED EMPLOYER" ||
					receipt.Fields["conduit_cmte_nm"] != "SEPARATE CONDUIT" || receipt.Fields["contbr_occupation"] != "" || receipt.Fields["lt_source_row_ordinal"] != strconv.FormatUint(receipt.Ordinal, 10) {
					t.Fatal(receipt)
				}
			}
			if !page.HasMore {
				if page.NextAfter != nil {
					t.Fatal("terminal cursor")
				}
				break
			}
			if page.NextAfter == nil || *page.NextAfter <= after {
				t.Fatal("nonadvancing cursor")
			}
			after = *page.NextAfter
		}
		var expected []uint64
		for i, row := range rows {
			if row.Recipient != nil && *row.Recipient == "C00000001" && (component == "" || classify(row).Component == component) {
				expected = append(expected, uint64(i+1))
			}
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s got %v want %v", component, got, expected)
		}
	}
	page, err := r.Query(context.Background(), Query{Committee: "C99999999", Limit: 10})
	if err != nil || page.HasMore || len(page.Receipts) != 0 {
		t.Fatal(page, err)
	}
	for _, q := range []Query{{Committee: "BAD", Limit: 1}, {Committee: "C00000001", Limit: 101}, {Committee: "C00000001", Limit: 1, Component: "unknown"}, {Committee: "C00000001", Limit: 1, After: 1000}} {
		if _, err := r.Query(context.Background(), q); err == nil {
			t.Fatal("invalid query accepted")
		}
	}
}

func TestRejectInvalidBackingAndCancellation(t *testing.T) {
	r, _ := inventoryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scan(ctx, r.root, r.manifest, 4, nil); err == nil {
		t.Fatal("cancelled scan succeeded")
	}
	if _, err := r.Query(ctx, Query{Committee: "C00000001", Limit: 1}); err == nil {
		t.Fatal("cancelled query succeeded")
	}
	f, err := os.OpenFile(filepath.Join(r.root, r.manifest.Shards[0].StorageKey), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("changed"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := r.Query(context.Background(), Query{Committee: "C00000001", Limit: 1}); err == nil {
		t.Fatal("changed shard accepted")
	}
	for _, edit := range []func(*receiptRow){func(r *receiptRow) { r.Cycle = 2022 }, func(r *receiptRow) { r.Normalization = "invalid" }, func(r *receiptRow) { r.AmountState = "source_null" }, func(r *receiptRow) { r.Ordinal = 2 }} {
		row := baseRow()
		row.Ordinal = 1
		edit(&row)
		if validRow(row, 1, 2024) == nil {
			t.Fatal("invalid row accepted")
		}
	}
}

func TestAssessmentSourceBoundaryAndNoNetworkTotal(t *testing.T) {
	r, _ := inventoryFixture(t)
	trace := candidateupstream.Result{Cycle: "2024", CalculationID: "trace", Candidate: "candidate", Inputs: candidateupstream.Inputs{Sources: flow.Inputs{A: flow.FactReference{FactSetID: r.result.Input.FactSetID, ManifestSHA256: r.result.Input.ManifestSHA256, Facts: r.result.Input.Facts}}},
		Nodes: []candidateupstream.Node{{CommitteeID: "C00000001", Authorized: true}, {CommitteeID: "C99999999"}}}
	a, err := r.Assess(trace)
	if err != nil {
		t.Fatal(err)
	}
	if a.TerminalEligible || len(a.Committees) != 2 || a.Committees[0].Coverage != "reported_schedule_a_rows_not_complete_funding" || a.Committees[1].Coverage != "no_schedule_a_rows_in_exact_snapshot" {
		t.Fatal(a)
	}
	trace.Inputs.Sources.A.ManifestSHA256 = "changed"
	if _, err := r.Assess(trace); err == nil {
		t.Fatal("mixed source accepted")
	}
	r.result.Total.Rows++
	if validate(r.result) == nil {
		t.Fatal("nonconserving inventory accepted")
	}
}

func TestRejectTamperedInventoryBeforeSourceLoading(t *testing.T) {
	r, _ := inventoryFixture(t)
	for _, edit := range []func(*Result){
		func(v *Result) { v.TerminalEligible = true },
		func(v *Result) { v.CalculationID = strings.Repeat("0", 64) },
		func(v *Result) { v.Input.ManifestSHA256 = strings.Repeat("0", 64) },
		func(v *Result) { v.Buckets[0].Shards = nil },
	} {
		encoded, err := json.Marshal(r.result)
		if err != nil {
			t.Fatal(err)
		}
		var value Result
		if err := json.Unmarshal(encoded, &value); err != nil {
			t.Fatal(err)
		}
		edit(&value)
		encoded, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "inventory.json")
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(context.Background(), r.root, path); err == nil {
			t.Fatal("tampered inventory accepted")
		}
	}
	changed := r.manifest
	changed.Shards = append([]occ.ScheduleAColumnarShard(nil), changed.Shards...)
	changed.Shards[0].Facts++
	if dense(changed) == nil {
		t.Fatal("nonconserving shard accepted")
	}
	if _, err := scan(context.Background(), r.root, changed, 1, nil); err == nil {
		t.Fatal("wrong physical row count accepted")
	}
}
