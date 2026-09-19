package flowreconciliation

import (
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
	"testing"
)

func ptr[T any](v T) *T { return &v }
func baseInput() disbursements.Input {
	return disbursements.Input{Sender: ptr("C00000001"), RawRecipient: ptr("C00000002"), CleanRecipient: ptr("C00000002"), Form: "F3X", Line: ptr("23"), Schedule: ptr("SB"), DisbursementType: ptr("24K"), AmountState: "reported_value", Amount: ptr(int64(100))}
}

func TestSenderRules(t *testing.T) {
	seen := map[[2]string]bool{}
	for _, rule := range SenderRules() {
		k := [2]string{rule.ReportingRole, rule.Code}
		if seen[k] {
			t.Fatal("duplicate rule")
		}
		seen[k] = true
		covered := false
		for _, line := range disbursements.LineRules() {
			if line.Scope != "regular_committee" || line.Role != rule.ReportingRole {
				continue
			}
			covered = true
			for _, amount := range []int64{-100, 0, 100} {
				in := baseInput()
				in.Form = line.Form
				in.Line = &line.Line
				in.DisbursementType = &rule.Code
				in.Amount = &amount
				got, role, err := EvaluateSender(in)
				if err != nil || got.State != Included || role != rule.FlowRole {
					t.Fatal(rule, line, got, role, err)
				}
			}
		}
		if !covered {
			t.Fatal("unreachable sender rule", rule)
		}
	}
	rules := SenderRules()
	rules[0].Code = "changed"
	if SenderRules()[0].Code == "changed" {
		t.Fatal("mutable policy")
	}
}

func TestSenderEvidenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		edit        func(*disbursements.Input)
	}{
		{"null type", "unresolved_flow_type", func(i *disbursements.Input) { i.DisbursementType = nil }},
		{"empty type", "unresolved_flow_type", func(i *disbursements.Input) { i.DisbursementType = ptr("") }},
		{"earmark", "held_earmarked_forwarding", func(i *disbursements.Input) { i.DisbursementType = ptr("24T") }},
		{"intermediary", "held_earmarked_forwarding", func(i *disbursements.Input) { i.DisbursementType = ptr("24I") }},
		{"wrong line type", "unresolved_role_type_combination", func(i *disbursements.Input) { i.DisbursementType = ptr("24G") }},
		{"vendor line", "excluded_non_flow_reporting_role", func(i *disbursements.Input) { i.Line = ptr("21B") }},
		{"raw only", "unresolved_recipient_identity", func(i *disbursements.Input) { i.CleanRecipient = nil }},
		{"clean only", "unresolved_recipient_identity", func(i *disbursements.Input) { i.RawRecipient = nil }},
		{"conflicting ids", "unresolved_recipient_identity", func(i *disbursements.Input) { i.CleanRecipient = ptr("C00000003") }},
		{"no ids", "excluded_no_committee_recipient", func(i *disbursements.Input) { i.RawRecipient = nil; i.CleanRecipient = nil }},
		{"self", "unresolved_self_recipient", func(i *disbursements.Input) { i.RawRecipient = i.Sender; i.CleanRecipient = i.Sender }},
		{"conduit name", "unresolved_intermediary_evidence", func(i *disbursements.Input) { i.ConduitName = ptr("REPORTED NAME") }},
		{"memo X", disbursements.Memo, func(i *disbursements.Input) { i.MemoCode = ptr("X"); i.Memoed = true }},
		{"other scope", disbursements.Separate, func(i *disbursements.Input) { i.Form = "F4"; i.Line = ptr("22") }},
		{"unknown form", disbursements.Unresolved, func(i *disbursements.Input) { i.Form = "FUTURE" }},
		{"missing amount", disbursements.Unresolved, func(i *disbursements.Input) { i.Amount = nil; i.AmountState = "source_null" }},
		{"beneficiary is not ownership", Included, func(i *disbursements.Input) { i.BeneficiaryName = ptr("UNRESOLVED NAME") }},
		{"memo Y is not X", Included, func(i *disbursements.Input) { i.MemoCode = ptr("Y") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := baseInput()
			tc.edit(&in)
			d, role, err := EvaluateSender(in)
			if err != nil || d.State != tc.state || (d.State == Included) != (role != "") {
				t.Fatal(d, role, err)
			}
		})
	}
	for _, edit := range []func(*disbursements.Input){func(i *disbursements.Input) { i.Memoed = true }, func(i *disbursements.Input) { i.AmountState = "source_null" }} {
		in := baseInput()
		edit(&in)
		if _, _, err := EvaluateSender(in); err == nil {
			t.Fatal("invalid normalization accepted")
		}
	}
}
