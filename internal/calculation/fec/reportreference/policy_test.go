package reportreference

import "testing"

func ptr(s string) *string { return &s }
func TestReferencePrecedence(t *testing.T) {
	base := Input{Ordinal: 1, ScopeValid: true, Transaction: ptr("a"), BackReference: ptr("b"), BackSchedule: ptr("SA"), SourceCount: 1, TargetCount: 1, TargetOrdinal: 2, TargetSchedule: ptr("SA"), TargetLine: ptr("11AI")}
	for _, tc := range []struct {
		want   string
		change func(*Input)
	}{
		{"exact_same_report_reference", func(*Input) {}},
		{"no_report_reference", func(v *Input) { v.BackReference = nil; v.BackSchedule = nil; v.ScopeValid = false }},
		{"invalid_report_scope", func(v *Input) { v.ScopeValid = false }},
		{"incomplete_report_reference", func(v *Input) { v.Transaction = nil }},
		{"duplicate_source_transaction_id", func(v *Input) { v.SourceCount = 2; v.TargetCount = 2; v.BackSchedule = nil }},
		{"ambiguous_target_transaction_id", func(v *Input) { v.TargetCount = 2; v.BackSchedule = nil }},
		{"missing_reference_schedule", func(v *Input) { v.BackSchedule = nil; v.TargetCount = 0 }},
		{"target_absent_from_cycle_report", func(v *Input) { v.TargetCount = 0 }},
		{"self_reference", func(v *Input) { v.TargetOrdinal = 1 }},
		{"reference_schedule_mismatch", func(v *Input) { v.BackSchedule = ptr("SA11B") }},
		{"exact_same_report_reference", func(v *Input) { v.BackSchedule = ptr("SA11AI") }},
	} {
		v := base
		tc.change(&v)
		state, target := Decide(v)
		if state != tc.want || (target != 0) != (state == "exact_same_report_reference") {
			t.Fatal(tc.want, state, target)
		}
	}
}
