package flowreconciliation

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
)

func TestMachineComponentFixtures(t *testing.T) {
	content, err := os.ReadFile("../../../../contracts/calculations/fec/committee-flow-reconciliation/v1/fixtures/components.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name   string
		A, B   []Observation
		States []string
	}
	if err := json.Unmarshal(content, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			rows, err := Reconcile(context.Background(), c.A, c.B, "machine-fixture")
			if err != nil {
				t.Fatal(err)
			}
			states := []string{}
			for _, r := range rows {
				states = append(states, r.State)
			}
			if !reflect.DeepEqual(states, c.States) {
				t.Fatal(states, c.States)
			}
			if _, err := verifyAssertions(c.A, c.B, rows, "machine-fixture"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func obs(ordinal uint64, amount int64, date *int32, role string) Observation {
	return Observation{Ordinal: ordinal, SubID: "reported-id", Sender: "C00000001", Recipient: "C00000002", Role: role, Date: date, Amount: amount}
}
func TestCandidateComponents(t *testing.T) {
	day := ptr(int32(20000))
	next := ptr(int32(20001))
	one := obs(1, 100, day, "contribution")
	for _, tc := range []struct {
		name   string
		a, b   []Observation
		states []string
	}{
		{"exact", []Observation{one}, []Observation{one}, []string{"corroborated_exact_signature"}},
		{"date mismatch", []Observation{one}, []Observation{obs(1, 100, next, "contribution")}, []string{"candidate_date_disagreement"}},
		{"missing date", []Observation{one}, []Observation{obs(1, 100, nil, "contribution")}, []string{"candidate_missing_date"}},
		{"both missing date", []Observation{obs(1, 100, nil, "contribution")}, []Observation{obs(1, 100, nil, "contribution")}, []string{"candidate_missing_date"}},
		{"amount mismatch", []Observation{one}, []Observation{obs(1, 101, day, "contribution")}, []string{"conflicting_amount"}},
		{"sign not normalized", []Observation{one}, []Observation{obs(1, -100, day, "contribution")}, []string{"conflicting_amount"}},
		{"in kind not cash", []Observation{one}, []Observation{obs(1, 100, day, "in_kind")}, []string{"conflicting_role"}},
		{"roles need exact other fields", []Observation{one}, []Observation{obs(1, 101, day, "in_kind")}, []string{"unmatched_schedule_a", "unmatched_schedule_b"}},
		{"neither date nor amount", []Observation{one}, []Observation{obs(1, 101, next, "contribution")}, []string{"unmatched_schedule_a", "unmatched_schedule_b"}},
		{"no greedy exact preference", []Observation{one, obs(2, 100, next, "contribution")}, []Observation{one}, []string{"ambiguous_candidates"}},
		{"repeated transaction id not deduplicated", []Observation{one, obs(2, 100, day, "contribution")}, []Observation{one, obs(2, 100, day, "contribution")}, []string{"ambiguous_candidates"}},
		{"same side not connected alone", []Observation{one, obs(2, 100, day, "contribution")}, nil, []string{"unmatched_schedule_a", "unmatched_schedule_a"}},
		{"empty", nil, nil, []string{}},
		{"transitive alternatives", []Observation{one, obs(2, 200, next, "contribution")}, []Observation{obs(1, 100, next, "contribution"), obs(2, 200, day, "contribution")}, []string{"ambiguous_candidates"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Reconcile(context.Background(), tc.a, tc.b, "fixture")
			if err != nil {
				t.Fatal(err)
			}
			states := []string{}
			for _, r := range got {
				states = append(states, r.State)
			}
			if !reflect.DeepEqual(states, tc.states) {
				t.Fatal(states, tc.states)
			}
			if _, err := verifyAssertions(tc.a, tc.b, got, "fixture"); err != nil {
				t.Fatal(err)
			}
			for range 8 {
				replay, err := Reconcile(context.Background(), tc.a, tc.b, "fixture")
				if err != nil || !reflect.DeepEqual(got, replay) {
					t.Fatal("nondeterministic components", err)
				}
			}
		})
	}
	other := one
	other.Recipient = "C00000003"
	got, err := Reconcile(context.Background(), []Observation{one}, []Observation{other}, "fixture")
	if err != nil || len(got) != 2 {
		t.Fatal("cross-endpoint match", got, err)
	}
}

func TestRejectInvalidEvidence(t *testing.T) {
	one := obs(1, 100, ptr(int32(20000)), "contribution")
	for _, edit := range []func(*Observation){func(o *Observation) { o.Ordinal = 0 }, func(o *Observation) { o.SubID = "" }, func(o *Observation) { o.Role = "guessed" }, func(o *Observation) { o.Sender = "invalid" }} {
		bad := one
		edit(&bad)
		if _, err := Reconcile(context.Background(), []Observation{bad}, nil, "fixture"); err == nil {
			t.Fatal("invalid observation")
		}
	}
	if _, err := Reconcile(context.Background(), []Observation{one, one}, nil, "fixture"); err == nil {
		t.Fatal("reused ordinal")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Reconcile(ctx, nil, nil, "fixture"); err == nil {
		t.Fatal("cancellation")
	}
	for _, edit := range []func(*Assertion){func(r *Assertion) { r.State = "merged" }, func(r *Assertion) { r.AAmount++ }, func(r *Assertion) { r.ID = "invalid" }, func(r *Assertion) { r.BAmount = -r.BAmount }} {
		got, _ := Reconcile(context.Background(), []Observation{one}, []Observation{one}, "fixture")
		edit(&got[0])
		if _, err := verifyAssertions([]Observation{one}, []Observation{one}, got, "fixture"); err == nil {
			t.Fatal("mutated assertion accepted")
		}
	}
	got, _ := Reconcile(context.Background(), []Observation{one}, nil, "fixture")
	if _, err := verifyAssertions([]Observation{one}, nil, append(got, got...), "fixture"); err == nil {
		t.Fatal("reused fact")
	}
	if _, err := verifyAssertions([]Observation{one}, nil, nil, "fixture"); err == nil {
		t.Fatal("missing fact")
	}
	for _, pair := range [][2]int64{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		v := pair[0]
		if addMoney(&v, pair[1]) == nil {
			t.Fatal("money overflow")
		}
	}
	m := Measures{Rows: math.MaxUint64}
	if merge(&m, Measures{Rows: 1}) == nil {
		t.Fatal("row overflow")
	}
}
