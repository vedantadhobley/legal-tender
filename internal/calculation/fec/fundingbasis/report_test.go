package fundingbasis

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

func reportPair(t *testing.T, reverse bool) []Receipt {
	t.Helper()
	reader, _ := inventoryFixture(t)
	page, err := reader.Query(context.Background(), Query{Committee: "C00000001", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	rows := page.Receipts
	for i := range rows {
		f := rows[i].Fields
		f["file_num"] = "123"
		f["tran_id"] = []string{"ORIGINAL", "MEMO"}[i]
		f["schedule_type"] = "SA"
		f["line_num"] = "11AI"
		f["back_ref_tran_id"] = nil
		f["back_ref_sched_nm"] = nil
		f["conduit_cmte_id"] = nil
		f["contbr_id"] = nil
		f["clean_contbr_id"] = nil
		f["entity_tp"] = "IND"
		f["receipt_tp"] = "15E"
		f["lt_memoed_subtotal"] = false
		f["lt_receipt_amount_minor_units"] = "200000"
		f["lt_receipt_amount_state"] = "reported_value"
	}
	rows[1].Fields["lt_memoed_subtotal"] = true
	rows[1].Fields["entity_tp"] = "PAC"
	rows[1].Fields["receipt_tp"] = nil
	rows[1].Fields["contbr_id"] = "C00000003"
	rows[1].Fields["clean_contbr_id"] = "C00000003"
	// Unequal money is evidence, not a failed identity match or a new cash leg.
	rows[1].Fields["lt_receipt_amount_minor_units"] = "20000"
	if reverse {
		rows[1].Fields["back_ref_tran_id"] = "ORIGINAL"
		rows[1].Fields["back_ref_sched_nm"] = "SA11AI"
	} else {
		rows[0].Fields["back_ref_tran_id"] = "MEMO"
		rows[0].Fields["back_ref_sched_nm"] = "SA11AI"
	}
	for i := range rows {
		rekey(t, &rows[i])
	}
	return rows
}

func rekey(t *testing.T, r *Receipt) {
	t.Helper()
	f := r.Fields
	s := func(k string) *string {
		if f[k] == nil {
			return nil
		}
		return ptr(f[k].(string))
	}
	// Derive through the production decoder after supplying the changed key.
	base := baseRow()
	base.Recipient = s("cmte_id")
	base.ReceiptType = s("receipt_tp")
	base.Contributor = s("contbr_id")
	base.CleanContributor = s("clean_contbr_id")
	base.Memo = f["lt_memoed_subtotal"].(bool)
	base.Individual = nil
	if f["is_individual"] != nil {
		base.Individual = ptr(f["is_individual"].(bool))
	}
	base.Amount = nil
	if f["lt_receipt_amount_minor_units"] != nil {
		amount, err := strconv.ParseInt(f["lt_receipt_amount_minor_units"].(string), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		base.Amount = &amount
	}
	base.AmountState = f["lt_receipt_amount_state"].(string)
	base.ConduitID = s("conduit_cmte_id")
	r.Key = classify(base)
	if _, err := decodeEvidence(*r); err != nil {
		t.Fatal(err)
	}
}

func TestReportAssociationBothDirectionsAndNoAdditionalMoney(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		rows := reportPair(t, reverse)
		refs, links, err := resolveReport(rows, "C00000001", "123")
		if err != nil || len(refs) != 2 || len(links) != 1 {
			t.Fatal(refs, links, err)
		}
		d := links[0]
		if d.State != "reported_earmark_memo_association" || d.ConduitID == nil || *d.ConduitID != "C00000003" || d.AdditionalAmount != "0" || d.TerminalEligible || d.AmountComparison != "different_reported_amount" {
			t.Fatal(d)
		}
		againRefs, againLinks, err := resolveReport(rows, "C00000001", "123")
		if err != nil || !reflect.DeepEqual(refs, againRefs) || !reflect.DeepEqual(links, againLinks) {
			t.Fatal("unstable replay", err)
		}
	}
}

func TestReportAssociationRefusesAmbiguityAndRoleGuesses(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		edit        func([]Receipt) []Receipt
	}{
		{"missing target", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { r[0].Fields["back_ref_tran_id"] = "ABSENT"; return r }},
		{"missing schedule", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { r[0].Fields["back_ref_sched_nm"] = nil; return r }},
		{"wrong line", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { r[0].Fields["back_ref_sched_nm"] = "SA12"; return r }},
		{"other schedule", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { r[0].Fields["back_ref_sched_nm"] = "SB"; return r }},
		{"no memo", "related_role_unresolved", func(r []Receipt) []Receipt { r[1].Fields["lt_memoed_subtotal"] = false; return r }},
		{"person memo", "related_role_unresolved", func(r []Receipt) []Receipt { r[1].Fields["entity_tp"] = "IND"; return r }},
		{"transfer memo", "related_role_unresolved", func(r []Receipt) []Receipt { r[1].Fields["receipt_tp"] = "18J"; return r }},
		{"one sided id", "related_committee_id_unresolved", func(r []Receipt) []Receipt { r[1].Fields["clean_contbr_id"] = nil; return r }},
		{"conflicting id", "related_committee_id_unresolved", func(r []Receipt) []Receipt { r[1].Fields["clean_contbr_id"] = "C00000004"; return r }},
		{"different original id", "conflicting_committee_evidence", func(r []Receipt) []Receipt { r[0].Fields["contbr_id"] = "C00000004"; return r }},
		{"different conduit id", "conflicting_committee_evidence", func(r []Receipt) []Receipt { r[0].Fields["conduit_cmte_id"] = "C00000004"; return r }},
		{"unknown original role", "original_contributor_role_unresolved", func(r []Receipt) []Receipt { r[0].Fields["entity_tp"] = nil; return r }},
		{"duplicate target", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { return append(r, copyReportRow(r[1], 3)) }},
		{"duplicate source", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { return append(r, copyReportRow(r[0], 3)) }},
		{"shared memo", "shared_related_record_unresolved", func(r []Receipt) []Receipt {
			other := copyReportRow(r[0], 3)
			other.Fields["tran_id"] = "ANOTHER"
			return append(r, other)
		}},
		{"self reference", "ambiguous_or_incomplete_reference_evidence", func(r []Receipt) []Receipt { r[0].Fields["back_ref_tran_id"] = "ORIGINAL"; return r }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.edit(reportPair(t, false))
			for i := range rows {
				rekey(t, &rows[i])
			}
			_, links, err := resolveReport(rows, "C00000001", "123")
			if err != nil || len(links) == 0 || links[0].State != tc.state || links[0].ConduitID != nil {
				t.Fatal(links, err)
			}
		})
	}
	rows := reportPair(t, false)
	rows[0].Fields["file_num"] = "124"
	if _, _, err := resolveReport(rows, "C00000001", "123"); err == nil {
		t.Fatal("cross-filing join accepted")
	}
	rows = reportPair(t, false)
	delete(rows[0].Fields, "file_num")
	if _, _, err := resolveReport(rows, "C00000001", "123"); err == nil {
		t.Fatal("missing physical field accepted")
	}
	rows = reportPair(t, false)
	delete(rows[0].Fields, "schedule_type")
	if _, _, err := resolveReport(rows, "C00000001", "123"); err == nil {
		t.Fatal("missing schedule field accepted")
	}
}

func copyReportRow(r Receipt, ordinal uint64) Receipt {
	fields := make(map[string]any, len(r.Fields))
	for k, v := range r.Fields {
		fields[k] = v
	}
	r.Fields = fields
	r.Ordinal = ordinal
	r.Fields["lt_source_row_ordinal"] = strconv.FormatUint(ordinal, 10)
	return r
}

func TestReportReviewScopeReplayAndCancellation(t *testing.T) {
	r, sourceRows := inventoryFixture(t)
	page, err := r.Query(context.Background(), Query{Committee: "C00000001", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	file := page.Receipts[0].Fields["file_num"].(string)
	got, err := r.ReviewReport(context.Background(), "C00000001", file, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, row := range sourceRows {
		if row.Recipient != nil && *row.Recipient == "C00000001" {
			want++
		}
	}
	if len(got.Receipts) != want || len(got.References) != want || got.TerminalEligible {
		t.Fatalf("report rows=%d references=%d want=%d", len(got.Receipts), len(got.References), want)
	}
	again, err := r.ReviewReport(context.Background(), "C00000001", file, nil)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("unstable report", err)
	}
	if got, err := r.reviewReport(context.Background(), "C00000001", file, 1, nil); err == nil || len(got.Receipts) != 0 {
		t.Fatal("resource limit returned partial success")
	}
	absent, err := r.ReviewReport(context.Background(), "C00000001", "99999999", nil)
	if err != nil || len(absent.Receipts) != 0 {
		t.Fatal(absent, err)
	}
	for _, file := range []string{"", "0", " 123", "123.0", "../123"} {
		if _, err := r.ReviewReport(context.Background(), "C00000001", file, nil); err == nil {
			t.Fatal(file)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ReviewReport(ctx, "C00000001", file, nil); err == nil {
		t.Fatal("cancellation passed")
	}
}
