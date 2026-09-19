package flowreconciliation

import (
	"context"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestReviewPreservesAmbiguityAndSignedDates(t *testing.T) {
	a := []Observation{obs(1, 100, ptr(int32(20)), "contribution"), obs(2, 100, ptr(int32(21)), "contribution"), obs(3, -100, ptr(int32(30)), "contribution")}
	b := []Observation{obs(1, 100, ptr(int32(20)), "contribution"), obs(2, -100, ptr(int32(35)), "contribution")}
	for i := range a {
		a[i].Type = "18K"
	}
	for i := range b {
		b[i].Type = "24K"
	}
	assertions, err := Reconcile(context.Background(), a, b, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	got, err := profileComponents(context.Background(), a, b, assertions)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Shapes) != 2 || got.Shapes[0].Key.State != "ambiguous_candidates" || got.Shapes[0].SharedExactSignatures != 1 || got.Shapes[0].UniqueExactSignatures != 1 || !got.Shapes[0].Key.HasExactSignature {
		t.Fatal(got.Shapes)
	}
	if len(got.Dates) != 1 || got.Dates[0] != (ReviewDate{-5, 1}) {
		t.Fatal(got.Dates)
	}
	if got.GraphEligible {
		t.Fatal("review authorized graph")
	}
	for range 10 {
		replay, err := profileComponents(context.Background(), a, b, assertions)
		if err != nil || !reflect.DeepEqual(replay, got) {
			t.Fatal("nondeterministic review", err)
		}
	}
	repeated := append([]Observation(nil), a...)
	repeated[1].Date = ptr(int32(20))
	shared, unique := exactSignatures(repeated, b)
	if shared != 1 || unique != 0 {
		t.Fatal(shared, unique)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := profileComponents(ctx, a, b, assertions); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestSourceReviewSeeksAndVerifiesFullRows(t *testing.T) {
	root := t.TempDir()
	s := writeBShard(t, root, 7)
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	wanted := map[uint64]Observation{}
	for i, amount := range []int64{100, -100, 0} {
		ordinal := int64(7 + i)
		_, o, err := selectB(bRow{Ordinal: ordinal, Period: 2024, SubID: strconv.FormatInt(ordinal, 10), Sender: ptr("C00000001"), Raw: ptr("C00000002"), Clean: ptr("C00000002"), Form: "F3X", Line: ptr("23"), Schedule: ptr("SB"), Type: ptr("24K"), Amount: ptr(amount), AmountState: "reported_value"})
		if err != nil || o == nil {
			t.Fatal(err)
		}
		wanted[uint64(ordinal)] = *o
	}
	run := func(w map[uint64]Observation, period int64) ([]SourceExample, error) {
		content, err := os.ReadFile(filepath.Join(root, s.key))
		if err != nil {
			return nil, err
		}
		return reviewRows(context.Background(), root, "schedule_b", "fixture", []reviewShard{{s, hashBytes(content)}}, schema.Parquet(), w, selectB, func(r bRow) int64 { return r.Period }, period)
	}
	rows, err := run(wanted, 2024)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || len(rows[0].Fields) != 98 || rows[1].Fields["lt_disbursement_amount_minor_units"] != "-100" || rows[0].Fields["recipient_nm"] != nil {
		t.Fatal("source grain changed", rows)
	}
	wrong := wanted[7]
	wrong.Amount++
	wanted[7] = wrong
	if _, err := run(wanted, 2024); err == nil {
		t.Fatal("modified observation accepted")
	}
	wrong.Amount--
	wanted[7] = wrong
	if _, err := run(wanted, 2022); err == nil {
		t.Fatal("wrong period accepted")
	}
	wanted[200] = wrong
	if _, err := run(wanted, 2024); err == nil {
		t.Fatal("missing ordinal accepted")
	}
}
