package candidateupstream

import (
	"context"
	"reflect"
	"testing"
)

func TestConnectionWitnessesPreserveSourceAndTrace(t *testing.T) {
	rows := fixture()
	r, err := analyze(context.Background(), "2024", candidate, Inputs{}, links(), nil, rows)
	if err != nil {
		t.Fatal(err)
	}
	w, err := connectionWitnesses(context.Background(), r, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != len(r.Nodes)-2 {
		t.Fatal("missing or duplicate witness", len(w))
	}
	for i, o := range w {
		if !reflect.DeepEqual(o, rows[o.Ordinal-1]) || i > 0 && w[i-1].Ordinal >= o.Ordinal {
			t.Fatal("source changed or ordering unstable")
		}
	}
	before := r.CalculationID
	verifyWitnesses(t, r, rows)
	if r.CalculationID != before {
		t.Fatal("trace changed")
	}
	if _, err := connectionWitnesses(context.Background(), r, rows[:1]); err == nil {
		t.Fatal("missing witnesses accepted")
	}
	broken := append(rows[:0:0], rows...)
	broken[2].Recipient = cm(999)
	if _, err := connectionWitnesses(context.Background(), r, broken); err == nil {
		t.Fatal("foreign endpoint accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connectionWitnesses(ctx, r, rows); err == nil {
		t.Fatal("cancellation ignored")
	}
}
