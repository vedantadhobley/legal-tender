package disbursements

import (
	"context"
	"encoding/json"
	"github.com/parquet-go/parquet-go"
	occurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }
func baseInput() Input {
	return Input{Sender: ptr("C00000001"), Form: "F3X", Line: ptr("23"), Schedule: ptr("SB"), Amount: ptr(int64(100)), AmountState: "reported_value"}
}

func TestMachinePolicyFixtures(t *testing.T) {
	content, err := os.ReadFile("../../../../contracts/calculations/fec/processed-disbursement-reporting/v1/fixtures/policy-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name, Form           string
		Line, Schedule, Memo *string
		Decision, Role       string
	}
	if err := json.Unmarshal(content, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			in := baseInput()
			in.Form = c.Form
			in.Line = c.Line
			in.Schedule = c.Schedule
			in.MemoCode = c.Memo
			in.Memoed = c.Memo != nil && *c.Memo == "X"
			got, err := Evaluate(in)
			if err != nil || got.Decision != c.Decision || got.Role != c.Role || !validKey(got) {
				t.Fatalf("%+v: %v", got, err)
			}
		})
	}
	seen := map[[2]string]bool{}
	for _, rule := range LineRules() {
		k := [2]string{rule.Form, rule.Line}
		if seen[k] {
			t.Fatal("duplicate rule")
		}
		seen[k] = true
		in := baseInput()
		in.Form = rule.Form
		in.Line = &rule.Line
		got, err := Evaluate(in)
		if err != nil || got.Role != rule.Role || got.Scope != rule.Scope {
			t.Fatal(rule, got, err)
		}
	}
	rules := LineRules()
	rules[0].Role = "changed"
	if LineRules()[0].Role == "changed" {
		t.Fatal("mutable policy")
	}
}

func TestEvidenceDoesNotCreateOwnershipOrFlow(t *testing.T) {
	for _, amount := range []int64{-100, 0, 100} {
		for _, code := range []*string{nil, ptr(""), ptr("24K"), ptr("24Z"), ptr("15E")} {
			in := baseInput()
			in.Amount = &amount
			in.DisbursementType = code
			in.RawRecipient = ptr("C00000001")
			in.BeneficiaryName = ptr("A VENDOR OR BENEFICIARY")
			in.ConduitName = ptr("A REPORTED NAME")
			got, err := Evaluate(in)
			if err != nil || got.Decision != Included || got.Role != "federal_contribution" || !got.SelfRecipient || got.RecipientIdentity != "raw_committee_id_only" {
				t.Fatal(got, err)
			}
		}
	}
	in := baseInput()
	in.CleanRecipient = ptr("C00000001")
	got, _ := Evaluate(in)
	if !got.SelfRecipient || got.RecipientIdentity != "clean_committee_id_only" {
		t.Fatal(got)
	}
	in.RawRecipient = ptr("C00000002")
	got, _ = Evaluate(in)
	if got.RecipientIdentity != "conflicting_committee_ids" {
		t.Fatal(got)
	}
	in.Amount = nil
	in.AmountState = "source_null"
	got, _ = Evaluate(in)
	if got.Decision != Unresolved || got.Reason != "missing_amount" {
		t.Fatal(got)
	}
	in.Amount = ptr(int64(1))
	if _, err := Evaluate(in); err == nil {
		t.Fatal("accepted inconsistent money")
	}
	in = baseInput()
	in.Memoed = true
	if _, err := Evaluate(in); err == nil {
		t.Fatal("accepted inconsistent memo")
	}
	in = baseInput()
	in.Sender = ptr(" C00000001")
	got, _ = Evaluate(in)
	if got.Reason != "invalid_filer_id" {
		t.Fatal(got)
	}
	if cell(nil) == cell(ptr("")) {
		t.Fatal("collapsed null and empty")
	}
}

func TestScanAndResultConservation(t *testing.T) {
	root := t.TempDir()
	shard := writeShard(t, root)
	manifest := occurrence.ScheduleBColumnarManifest{Cycle: "2024", FactSetID: strings.Repeat("a", 64), SourceReleaseID: "fec-" + strings.Repeat("b", 64), SourceArtifactSHA256: strings.Repeat("c", 64), PhysicalSchemaVersion: schedulebparquet.PhysicalSchemaVersion, Counts: occurrence.ScheduleBColumnarCounts{Facts: 6}, Shards: []occurrence.ScheduleBColumnarShard{shard}}
	run := func(workers int) Result {
		scan, err := scanFacts(context.Background(), Options{StorageRoot: root, Workers: workers, Progress: func(string) {}}, manifest)
		if err != nil {
			t.Fatal(err)
		}
		result, err := assemble(manifest, strings.Repeat("d", 64), scan)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	a, b := run(1), run(4)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("worker-dependent output")
	}
	if a.Total.Rows != 6 || a.Total.Amount != 400 || a.Decisions[Included].Rows != 3 || a.Decisions[Included].Amount != 0 || a.Decisions[Memo].Amount != 100 || a.Decisions[Separate].Amount != 100 || a.Decisions[Unresolved].Amount != 200 || a.State != "complete_with_unresolved" {
		t.Fatalf("bad conservation: %+v", a)
	}
	for _, mutate := range []func(*Result){func(r *Result) { r.GraphEligible = true }, func(r *Result) { r.Total.Amount++ }, func(r *Result) { r.Groups[0].Measures.Rows++ }, func(r *Result) { r.State = "complete" }, func(r *Result) { r.PolicyVersion = "v2" }, func(r *Result) { r.Input.ManifestSHA256 = "bad" }, func(r *Result) { r.Groups[0].Key.Role = "guessed"; r.GroupsSHA256 = hashJSON(r.Groups) }, func(r *Result) { r.Groups[0].Key.Decision = Separate; r.GroupsSHA256 = hashJSON(r.Groups) }} {
		content, _ := json.Marshal(a)
		var c Result
		json.Unmarshal(content, &c)
		mutate(&c)
		if Validate(c) == nil {
			t.Fatal("accepted mutated result")
		}
	}
	for _, cycle := range []string{"2022", "bad"} {
		if r := scanShard(context.Background(), root, cycle, shard); r.err == nil {
			t.Fatal("accepted wrong cycle")
		}
	}
	bad := shard
	bad.FirstSourceRowOrdinal++
	if r := scanShard(context.Background(), root, "2024", bad); r.err == nil {
		t.Fatal("accepted wrong locator")
	}
	bad = shard
	bad.Facts++
	if r := scanShard(context.Background(), root, "2024", bad); r.err == nil {
		t.Fatal("accepted wrong count")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scanFacts(ctx, Options{StorageRoot: root, Workers: 2}, manifest); err == nil {
		t.Fatal("ignored cancellation")
	}
	file, err := os.Create(filepath.Join(root, "wrong.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[factRow](file)
	w.Close()
	file.Close()
	info, _ := os.Stat(file.Name())
	bad = shard
	bad.StorageKey = "wrong.parquet"
	bad.Bytes = uint64(info.Size())
	if r := scanShard(context.Background(), root, "2024", bad); r.err == nil {
		t.Fatal("accepted reduced schema")
	}
}

func TestOverflowAndInvalidOptions(t *testing.T) {
	for _, c := range []struct{ a, b int64 }{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		m := Measures{Amount: c.a}
		if merge(&m, Measures{Amount: c.b}) == nil {
			t.Fatal("money overflow")
		}
	}
	m := Measures{Rows: math.MaxUint64}
	if merge(&m, Measures{Rows: 1}) == nil {
		t.Fatal("count overflow")
	}
	if validMeasures(Measures{Rows: 1, AmountRows: 2}) || validMeasures(Measures{Amount: 1}) {
		t.Fatal("invalid counts")
	}
	for _, w := range []int{-1, 17} {
		if _, err := Calculate(context.Background(), Options{Workers: w}); err == nil {
			t.Fatal("invalid workers")
		}
	}
}

func writeShard(t *testing.T, root string) occurrence.ScheduleBColumnarShard {
	t.Helper()
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(root, "facts.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewWriter(file, schema.Parquet())
	amounts := []int64{100, -100, 0, 100, 100, 200}
	var source strings.Builder
	for i := range amounts {
		values := make([]string, scheduleb.FieldCount)
		for j := range values {
			values[j] = "\\N"
		}
		form, line, memo := "F3X", "23", "\\N"
		if i == 3 {
			memo = "X"
		}
		if i == 4 {
			form, line = "F4", "21A"
		}
		if i == 5 {
			line = "21"
		}
		for name, value := range map[string]string{"sub_id": strconv.Itoa(i + 1), "filing_form": form, "two_year_transaction_period": "2024", "line_num": line, "schedule_type": "SB", "cmte_id": "C00000001", "memo_cd": memo, "tran_id": "REPEATED-TRANSACTION", "action_cd": "N", "disb_amt": strconv.FormatInt(amounts[i]/100, 10) + ".00"} {
			index, _ := scheduleb.ColumnIndex(name)
			values[index] = value
		}
		source.WriteString(strings.Join(values, "\t") + "\n")
	}
	decoder := scheduleb.NewDecoder(strings.NewReader(source.String()))
	for i, amount := range amounts {
		if !decoder.Scan() {
			t.Fatal(decoder.Err())
		}
		row, err := schema.Encode(nil, decoder.Row(), schedulebparquet.Metadata{SourceRowOrdinal: uint64(i + 1), SourceRawByteLength: 1}, schedulebparquet.Derived{DisbursementAmountMinorUnits: ptr(amount), DisbursementAmountSourceScale: ptr(int32(2)), DisbursementAmountState: "reported_value", BundledRefundState: "source_null", TwoYearTransactionPeriod: 2024, MemoedSubtotal: i == 3})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
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
	return occurrence.ScheduleBColumnarShard{StorageKey: "facts.parquet", FirstSourceRowOrdinal: 1, LastSourceRowOrdinal: 6, Facts: 6, Bytes: uint64(info.Size())}
}
