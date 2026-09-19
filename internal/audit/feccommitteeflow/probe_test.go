package feccommitteeflow

import (
	"testing"

	committeeflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
)

func TestAddRowKeepsDiagnosticStatesDistinct(t *testing.T) {
	committee := "C00000001"
	recipient := "C00000002"
	cleanOnly := "C00000003"
	individual := false
	individualTrue := true
	amount := int64(1250)
	memo := "X"
	inbound := "15K"
	outbound := "24K"
	rows := []probeRow{
		{SourceRowOrdinal: 1, Normalization: "valid", RecipientID: &recipient, ContributorID: &committee, CleanContributorID: &committee, Individual: &individual, ReceiptTypeCode: &inbound, AmountMinorUnits: &amount, AmountState: "reported_value"},
		{SourceRowOrdinal: 2, Normalization: "valid", RecipientID: &recipient, ContributorID: &committee, CleanContributorID: &committee, Individual: &individualTrue, ReceiptTypeCode: &inbound, AmountMinorUnits: &amount, AmountState: "reported_value"},
		{SourceRowOrdinal: 3, Normalization: "valid", RecipientID: &recipient, CleanContributorID: &cleanOnly, Individual: &individual, ReceiptTypeCode: &inbound, AmountMinorUnits: &amount, AmountState: "reported_value"},
		{SourceRowOrdinal: 4, Normalization: "valid", RecipientID: &recipient, Individual: &individual, ReceiptTypeCode: &inbound, AmountMinorUnits: &amount, AmountState: "reported_value"},
		{SourceRowOrdinal: 5, Normalization: "valid", RecipientID: &recipient, ContributorID: &committee, CleanContributorID: &committee, Individual: &individual, MemoCode: &memo, MemoedSubtotal: true, ReceiptTypeCode: &inbound, AmountMinorUnits: &amount, AmountState: "reported_value"},
		{SourceRowOrdinal: 6, Normalization: "valid", RecipientID: &recipient, ContributorID: &committee, CleanContributorID: &committee, Individual: &individual, ReceiptTypeCode: &outbound, AmountMinorUnits: &amount, AmountState: "reported_value"},
	}
	result := newShardResult()
	for _, row := range rows {
		if err := addRow(&result, row); err != nil {
			t.Fatal(err)
		}
	}
	for state, want := range map[string]uint64{
		"included_receiver_reported_committee_flow": 2,
		"unresolved_one_sided_source_committee_id":  1,
		"excluded_no_source_committee_id":           1,
		"excluded_memo_subtotal":                    1,
		"excluded_outbound_receipt_role":            1,
	} {
		if got := result.decisions[state].rows; got != want {
			t.Fatalf("decision %s rows = %d, want %d", state, got, want)
		}
	}
	if len(result.edges) != 1 || result.edges[edgeKey{source: committee, recipient: recipient, role: "registered_filer_contribution"}].amount != amount*2 {
		t.Fatalf("unexpected included edge groups: %+v", result.edges)
	}
}

func TestSourceIdentityRequiresExactMatchingCommitteeIDs(t *testing.T) {
	one := "C00000001"
	two := "C00000002"
	invalid := "P00000001"
	for name, test := range map[string]struct {
		raw, clean *string
		state      string
		id         string
	}{
		"exact":      {&one, &one, "exact_matching_committee_id", one},
		"conflict":   {&one, &two, "conflicting_committee_ids", ""},
		"raw only":   {&one, &invalid, "raw_committee_id_only", one},
		"clean only": {nil, &two, "clean_committee_id_only", two},
		"none":       {nil, &invalid, "no_valid_committee_id", ""},
	} {
		t.Run(name, func(t *testing.T) {
			state, id := committeeflows.ClassifySourceIdentity(test.raw, test.clean)
			if state != test.state || id != test.id {
				t.Fatalf("got (%s, %s), want (%s, %s)", state, id, test.state, test.id)
			}
		})
	}
}

func TestReceiptRoleSeparatesInboundFromRelatedNonFlowRows(t *testing.T) {
	for code, want := range map[string]string{
		"15K": "included_receiver_reported_committee_flow",
		"18G": "included_receiver_reported_committee_flow",
		"22Z": "included_receiver_reported_committee_flow",
		"24K": "excluded_outbound_receipt_role",
		"18J": "excluded_semantic_memo_receipt_role",
		"15E": "excluded_earmarked_receipt_role",
		"10":  "excluded_noncommittee_receipt_role",
		"15":  "unresolved_receipt_role",
	} {
		t.Run(code, func(t *testing.T) {
			_, decision := committeeflows.ClassifyReceiptRole(&code)
			if decision != want {
				t.Fatalf("receipt %s decision = %s, want %s", code, decision, want)
			}
		})
	}
}
