package flowreconciliation

import (
	"context"
	"encoding/json"
	receiver "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
	"os"
	"path/filepath"
	"testing"
)

func TestReviewRejectsChangedEvidence(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	a := []Observation{obs(1, 100, ptr(int32(20)), "contribution")}
	b := []Observation{obs(1, 100, ptr(int32(21)), "contribution")}
	r := Result{SchemaVersion: Version, Cycle: "2024", State: "complete_candidate_reconciliation", Policy: Policy{SenderPolicy, receiver.PolicyVersion, disbursements.PolicyVersion, MatchPolicy, SenderRules()}}
	r.Input.A.Facts = 1
	r.Input.B.Facts = 1
	r.CalculationSetID = hashJSON(struct {
		Version, Cycle string
		Inputs         Inputs
		Policy         Policy
	}{Version, r.Cycle, r.Input, r.Policy})
	assertions, _ := Reconcile(ctx, a, b, r.CalculationSetID)
	r.Summary, _ = verifyAssertions(a, b, assertions, r.CalculationSetID)
	r.A.Total = Measures{1, 1, 100}
	r.B.Total = r.A.Total
	r.A.Selected = r.A.Total
	r.B.Selected = r.A.Total
	r.A.Decisions = []Bucket{{DecisionKey{State: "fixture"}, r.A.Total}}
	r.B.Decisions = append([]Bucket(nil), r.A.Decisions...)
	var err error
	r.A.Observations, err = writeVerified(ctx, root, r.CalculationSetID, "schedule-a", a)
	if err != nil {
		t.Fatal(err)
	}
	r.B.Observations, err = writeVerified(ctx, root, r.CalculationSetID, "schedule-b", b)
	if err != nil {
		t.Fatal(err)
	}
	r.Assertions, err = writeVerified(ctx, root, r.CalculationSetID, "assertions", assertions)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "result.json")
	write := func(v Result) {
		data, _ := json.Marshal(v)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(r)
	options := ReviewOptions{ResultPath: path, EvidenceRoot: root}
	if _, _, _, _, _, err := readReviewEvidence(ctx, options); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Result){func(v *Result) { v.GraphEligible = true }, func(v *Result) { v.SchemaVersion = "unknown" }, func(v *Result) { v.A.Total.Rows++ }, func(v *Result) { v.A.Selected.Amount++ }, func(v *Result) { v.Assertions.CompressedSHA256 = "wrong" }} {
		v := r
		mutate(&v)
		write(v)
		if _, _, _, _, _, err := readReviewEvidence(ctx, options); err == nil {
			t.Fatal("accepted mutated evidence")
		}
	}
	write(r)
	if err := os.WriteFile(filepath.Join(root, r.A.Observations.StorageKey), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, err := readReviewEvidence(ctx, options); err == nil {
		t.Fatal("accepted corrupt evidence")
	}
}
