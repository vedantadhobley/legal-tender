package flowreconciliation

import (
	"context"
	"encoding/json"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Optional corpus replay reuses already selected evidence, not another scan of
// hundreds of millions of facts. Full-source and worker-equivalence checks are
// separate gates. This recomputes every candidate component from its evidence.
func TestCompleteGateReplay(t *testing.T) {
	root := os.Getenv("LT_FLOW_RECONCILIATION_GATE")
	if root == "" {
		t.Skip("set LT_FLOW_RECONCILIATION_GATE to an isolated complete gate directory")
	}
	data, err := os.ReadFile(filepath.Join(root, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r Result
	if err := strictJSON(data, &r); err != nil {
		t.Fatal(err)
	}
	id := hashJSON(struct {
		Version, Cycle string
		Inputs         Inputs
		Policy         Policy
	}{Version, r.Cycle, r.Input, r.Policy})
	if id != r.CalculationSetID || r.GraphEligible {
		t.Fatal("invalid result identity or graph boundary")
	}
	read := func(d artifact.Descriptor) []Observation {
		reader, err := artifact.Open[Observation](context.Background(), filepath.Join(root, "gate"), d)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Abort()
		rows := []Observation{}
		for {
			row, ok, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			rows = append(rows, row)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	a, b := read(r.A.Observations), read(r.B.Observations)
	for range 3 {
		rows, err := Reconcile(context.Background(), a, b, id)
		if err != nil {
			t.Fatal(err)
		}
		summary, err := verifyAssertions(a, b, rows, id)
		if err != nil || !reflect.DeepEqual(summary, r.Summary) {
			t.Fatal("summary replay differs", err)
		}
		descriptor, err := writeVerified(context.Background(), filepath.Join(root, "gate"), id, "assertions", rows)
		if err != nil || !reflect.DeepEqual(descriptor, r.Assertions) {
			t.Fatal("complete component replay differs", err)
		}
	}
	// Ensure the saved JSON itself was not changed by replay.
	after, err := os.ReadFile(filepath.Join(root, "result.json"))
	if err != nil || !json.Valid(after) || string(after) != string(data) {
		t.Fatal("result mutated", err)
	}
}
