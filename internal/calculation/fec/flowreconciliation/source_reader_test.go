package flowreconciliation

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestPinnedSourceLookupRejectsCorruptionAndForeignMembership(t *testing.T) {
	root := t.TempDir()
	s := writeBShard(t, root, 7)
	content, err := os.ReadFile(filepath.Join(root, s.key))
	if err != nil {
		t.Fatal(err)
	}
	reader := &SourceReader{root: root, result: Result{Cycle: "2024", Input: Inputs{B: FactReference{FactSetID: "fixture"}}}, bshards: []reviewShard{{s, hashBytes(content)}}}
	for i, amount := range []int64{100, -100, 0} {
		ordinal := int64(7 + i)
		_, o, err := selectB(bRow{Ordinal: ordinal, Period: 2024, SubID: strconv.FormatInt(ordinal, 10), Sender: ptr("C00000001"), Raw: ptr("C00000002"), Clean: ptr("C00000002"), Form: "F3X", Line: ptr("23"), Schedule: ptr("SB"), Type: ptr("24K"), Amount: ptr(amount), AmountState: "reported_value"})
		if err != nil || o == nil {
			t.Fatal(err)
		}
		reader.b = append(reader.b, *o)
	}
	refs := []SourceLocator{{Side: "schedule_b", FactSetID: "fixture", Ordinal: 8}}
	expected := SourceExpectation{Calculation: CalculationReference{}, Inputs: reader.result.Input, Cycle: "2024", Locator: refs[0], Observation: reader.b[1]}
	if row, err := reader.LookupExpected(context.Background(), expected); err != nil || row.Ordinal != 8 {
		t.Fatal(row, err)
	}
	for _, edit := range []func(*SourceExpectation){
		func(e *SourceExpectation) { e.Calculation.ManifestSHA256 = "other" },
		func(e *SourceExpectation) { e.Inputs.B.FactSetID = "other" },
		func(e *SourceExpectation) { e.Cycle = "2022" },
		func(e *SourceExpectation) { e.Locator.Side = "schedule_a" },
		func(e *SourceExpectation) { e.Observation.Amount++ },
		func(e *SourceExpectation) { e.Observation.SubID = "other" },
		func(e *SourceExpectation) { e.Observation.Date = ptr(int32(10)) },
		func(e *SourceExpectation) { e.Observation.Sender = "C00000099" },
	} {
		bad := expected
		edit(&bad)
		if _, err := reader.LookupExpected(context.Background(), bad); err == nil {
			t.Fatal("source expectation mismatch accepted")
		}
	}
	rows, err := reader.Lookup(context.Background(), refs)
	if err != nil || len(rows) != 1 || len(rows[0].Fields) != 98 || rows[0].Fields["lt_disbursement_amount_minor_units"] != "-100" {
		t.Fatal(rows, err)
	}
	for _, bad := range [][]SourceLocator{nil, {{Side: "both", FactSetID: "fixture", Ordinal: 8}}, {{Side: "schedule_b", FactSetID: "foreign", Ordinal: 8}}, {{Side: "schedule_b", FactSetID: "fixture", Ordinal: 10}}, append(refs, refs...)} {
		if _, err := reader.Lookup(context.Background(), bad); err == nil {
			t.Fatal("accepted foreign/unselected source", bad)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.Lookup(ctx, refs); err == nil {
		t.Fatal("ignored cancellation")
	}
	// No manifest/current lookup is needed after opening. A same-size change
	// anywhere in the requested shard must still fail before returning a row.
	content[len(content)/2] ^= 1
	if err := os.WriteFile(filepath.Join(root, s.key), content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Lookup(context.Background(), refs); err == nil {
		t.Fatal("served damaged source shard")
	}
}

func TestSourceReaderRequiresPublishedAncestry(t *testing.T) {
	o, inputs, a, b := publicationFixture(t)
	if _, err := publishLoaded(context.Background(), o, inputs, a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSourceReader(context.Background(), o.StorageRoot, filepath.Join(o.OutputRoot, "current", o.Cycle+".json")); err == nil {
		t.Fatal("accepted missing source ancestry")
	}
}
