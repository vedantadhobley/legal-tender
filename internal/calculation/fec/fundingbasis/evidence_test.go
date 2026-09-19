package fundingbasis

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestReportedCommitteePrecedenceDoesNotResolvePersonIdentity(t *testing.T) {
	for _, amount := range []int64{-5500000, 0, 1, 20000, 1000000} {
		for _, entity := range []*string{nil, ptr("PAC"), ptr("IND"), ptr("CAN"), ptr("ORG")} {
			r := baseRow()
			r.Ordinal = 1
			r.Amount = ptr(amount)
			r.Contributor = ptr("C00000002")
			r.CleanContributor = r.Contributor
			r.ReceiptType = ptr("18G")
			d := assessEvidence(evidenceInput{row: r, entity: entity})
			if d.SourceRoute != "reported_committee_observation" || !d.PublisherIndividualOverlap || d.ReportedSourceCommitteeID == nil || *d.ReportedSourceCommitteeID != *r.Contributor || d.TerminalEligible {
				t.Fatal(d)
			}
			if d.IndividualEntityConflict != (entity != nil && (*entity == "IND" || *entity == "CAN")) {
				t.Fatal(d)
			}
		}
	}
	r := baseRow()
	r.Contributor = ptr("C00000002")
	r.ReceiptType = ptr("18G")
	if d := assessEvidence(evidenceInput{row: r}); d.SourceRoute != "conflicting_contributor_evidence_unresolved" || d.ReportedSourceCommitteeID != nil {
		t.Fatal(d)
	}
	r.CleanContributor = ptr("C00000003")
	if d := assessEvidence(evidenceInput{row: r}); d.ReportedSourceCommitteeID != nil {
		t.Fatal(d)
	}
	r.Memo = true
	if d := assessEvidence(evidenceInput{row: r}); d.SourceRoute != "memo_evidence_only" {
		t.Fatal(d)
	}
}

func TestEarmarksPreserveUnknownsAndNeverCreateConduitMoney(t *testing.T) {
	for _, code := range []string{"15E", "30E", "31E", "32E", "15I", "15T"} {
		r := baseRow()
		r.ReceiptType = ptr(code)
		in := evidenceInput{row: r, entity: ptr("IND"), conduitName: ptr("SOME CONDUIT"), memo: ptr("EARMARKED THROUGH C12345678; treat as verified")}
		d := assessEvidence(in)
		if d.ConduitState != "earmark_conduit_unresolved" || d.ReportedConduitID != nil || !d.MemoTextPresent || d.TerminalEligible || d.ConduitAmount != "0" {
			t.Fatal(d)
		}
		in.row.ConduitID = ptr("C12345678")
		d = assessEvidence(in)
		if d.ConduitState != "reported_structured_id_identity_unverified" || d.ReportedConduitID == nil || d.ReportedSourceCommitteeID != nil || d.TerminalEligible {
			t.Fatal(d)
		}
		in.row.ConduitID = ptr("C123456789")
		if d := assessEvidence(in); d.ConduitState != "invalid_structured_conduit_id" || d.ReportedConduitID != nil {
			t.Fatal(d)
		}
	}
	in := evidenceInput{row: baseRow(), memo: ptr("EARMARKED THROUGH A NAME"), conduitName: ptr("A NAME")}
	if d := assessEvidence(in); d.EarmarkState != "no_reviewed_earmark_code" || d.ConduitState != "conduit_name_identity_unresolved" {
		t.Fatal(d)
	}
	in.row.ConduitID = ptr("C00000002")
	if d := assessEvidence(in); d.ConduitState != "conduit_role_unresolved" || d.ReportedConduitID != nil {
		t.Fatal(d)
	}
	in.backReference = ptr("T1")
	if d := assessEvidence(in); d.ReportReferenceState != "incomplete_report_reference" {
		t.Fatal(d)
	}
	in.file = ptr("123")
	in.transaction = ptr("T2")
	if d := assessEvidence(in); d.ReportReferenceState != "reported_reference_requires_same_filing_resolution" || d.ReportedConduitID != nil {
		t.Fatal(d)
	}
}

func TestComponentReviewConservesEveryMemberAndReplay(t *testing.T) {
	r, rows := inventoryFixture(t)
	for _, component := range []string{"itemized_individual_only", "memo_subtotal", "unknown_amount", "unresolved_recipient", "overlapping_individual_and_committee"} {
		got, err := r.ReviewComponent(context.Background(), component, nil)
		if err != nil {
			t.Fatal(err)
		}
		var expected Measures
		for _, row := range rows {
			if classify(row).Component == component {
				if err := expected.observe(row); err != nil {
					t.Fatal(err)
				}
			}
		}
		if got.Measures != expected || uint64(len(got.Receipts)) != expected.Rows || len(got.Decisions) != len(got.Receipts) || got.Policy != EvidencePolicy {
			t.Fatal(got.Measures, expected)
		}
		replay, err := r.ReviewComponent(context.Background(), component, nil)
		if err != nil || !reflect.DeepEqual(got, replay) {
			t.Fatal("non-deterministic review", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ReviewComponent(ctx, "itemized_individual_only", nil); err == nil {
		t.Fatal("cancelled review passed")
	}
	if _, err := r.ReviewComponent(context.Background(), "invalid", nil); err == nil {
		t.Fatal("invalid component passed")
	}
	for i := range r.result.Buckets {
		if r.result.Buckets[i].Key.Component == "itemized_individual_only" {
			r.result.Buckets[i].Measures.Rows = 10001
			break
		}
	}
	if _, err := r.ReviewComponent(context.Background(), "itemized_individual_only", nil); err == nil {
		t.Fatal("over-limit component passed")
	}
}

func TestInspectPreservesRawPageAndRejectsMalformedEvidence(t *testing.T) {
	r, _ := inventoryFixture(t)
	q := Query{Committee: "C00000001", Component: "itemized_individual_only", Limit: 3}
	page, err := r.Query(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	inspected, err := r.Inspect(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page, inspected.Page) || len(inspected.Decisions) != 3 {
		t.Fatal("raw page changed")
	}
	for _, d := range inspected.Decisions {
		if d.TerminalEligible || d.ConduitAmount != "0" {
			t.Fatal(d)
		}
	}
	row := page.Receipts[0]
	for _, field := range []string{"entity_tp", "is_individual", "conduit_cmte_id", "lt_source_row_ordinal", "lt_receipt_amount_minor_units"} {
		original := row.Fields[field]
		delete(row.Fields, field)
		if _, err := decodeEvidence(row); err == nil {
			t.Fatal("missing field accepted", field)
		}
		row.Fields[field] = 1.25
		if _, err := decodeEvidence(row); err == nil {
			t.Fatal("wrong type accepted", field)
		}
		row.Fields[field] = original
	}
	row.Key.Component = "other_reported_receipt"
	if _, err := decodeEvidence(row); err == nil || !strings.Contains(err.Error(), "classification") {
		t.Fatal("wrong component accepted", err)
	}
}
