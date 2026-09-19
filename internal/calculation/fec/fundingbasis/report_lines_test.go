package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func lineReport(t *testing.T) ReportEvidence {
	t.Helper()
	rows := reportPair(t, false)
	for i := range rows {
		rows[i].Fields["filing_form"] = "F3"
		rows[i].Fields["memo_cd"] = nil
	}
	rows[1].Fields["memo_cd"] = "X"
	return ReportEvidence{Cycle: "2024", Committee: "C00000001", File: "123", Scope: "published_schedule_a_cycle_report", Receipts: rows}
}

func TestReportLinesSeparateMembershipAxesAndRetainOrdinals(t *testing.T) {
	report := lineReport(t)
	report.Receipts[0].Fields["is_individual"] = false
	rekey(t, &report.Receipts[0])
	report.Receipts[1].Fields["is_individual"] = false
	rekey(t, &report.Receipts[1])
	// The broad predicate excludes both rows; line membership includes only the
	// non-memo row. A false individual flag cannot hide the memo distinction.
	got, err := assembleReportLines(context.Background(), report, ReceiptSource{})
	if err != nil || got.Total.Rows != 2 || got.Total.Signed != 220000 || got.LinePopulation.Rows != 1 || got.LinePopulation.Signed != 200000 {
		t.Fatal(got, err)
	}
	if len(got.Groups) != 2 || len(got.TransactionIssues) != 0 || got.ComparisonReady || got.TerminalEligible {
		t.Fatal(got)
	}
	for _, g := range got.Groups {
		if g.Key.Individual != "false" || len(g.Ordinals) != 1 {
			t.Fatal(g)
		}
	}
	again, err := assembleReportLines(context.Background(), report, ReceiptSource{})
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("unstable replay", err)
	}
	id := got.ReviewID
	got.ReviewID = ""
	body, _ := json.Marshal(got)
	d := sha256.Sum256(body)
	if id != hex.EncodeToString(d[:]) {
		t.Fatal("wrong review identity")
	}
	changed, err := assembleReportLines(context.Background(), report, ReceiptSource{ReleaseID: "other"})
	if err != nil || changed.ReviewID == id {
		t.Fatal("source ancestry absent from identity", err)
	}
}

func TestReportLinesReviewedScopeAndUnknowns(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		value       any
		want        string
	}{
		{"f3x", "filing_form", "F3X", "reviewed_nonmemo_line"},
		{"unreviewed form", "filing_form", "F3P", "outside_reviewed_form_line"},
		{"missing form", "filing_form", nil, "outside_reviewed_form_line"},
		{"other line", "line_num", "12", "outside_reviewed_form_line"},
		{"blank line", "line_num", "", "outside_reviewed_form_line"},
		{"other schedule", "schedule_type", "SB", "outside_reviewed_form_line"},
		{"memo", "memo_cd", "X", "excluded_memo_subtotal"},
		{"blank memo", "memo_cd", "", "reviewed_nonmemo_line"},
		{"unknown memo", "memo_cd", "Y", "unresolved_memo_code"},
		{"lowercase memo", "memo_cd", "x", "unresolved_memo_code"},
		{"null individual", "is_individual", nil, "reviewed_nonmemo_line"},
		{"negative", "lt_receipt_amount_minor_units", "-25", "reviewed_nonmemo_line"},
		{"zero", "lt_receipt_amount_minor_units", "0", "reviewed_nonmemo_line"},
		{"unknown amount", "lt_receipt_amount_minor_units", nil, "unresolved_line_amount"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := lineReport(t)
			r.Receipts = r.Receipts[:1]
			f := r.Receipts[0].Fields
			f[tc.field] = tc.value
			f["lt_memoed_subtotal"] = f["memo_cd"] == "X"
			if f["lt_receipt_amount_minor_units"] == nil {
				f["lt_receipt_amount_state"] = "source_null"
			}
			rekey(t, &r.Receipts[0])
			got, err := assembleReportLines(context.Background(), r, ReceiptSource{})
			if err != nil || len(got.Groups) != 1 || got.Groups[0].Key.Disposition != tc.want || got.Total.Rows != 1 || !got.Total.valid() {
				t.Fatal(got, err)
			}
			if (got.LinePopulation.Rows == 1) != (tc.want == "reviewed_nonmemo_line") || got.ComparisonReady || got.TerminalEligible {
				t.Fatal(got)
			}
			if tc.name == "null individual" && got.Groups[0].Key.Individual != "source_null" {
				t.Fatal(got)
			}
		})
	}
}

func TestReportLinesFailClosedAndPreserveIdentityIssues(t *testing.T) {
	for _, value := range []any{nil, "", "ORIGINAL"} {
		r := lineReport(t)
		r.Receipts[1].Fields["tran_id"] = value
		got, err := assembleReportLines(context.Background(), r, ReceiptSource{})
		if err != nil || got.Total.Rows != 2 || len(got.TransactionIssues) != 1 || !slices.Contains(got.Blockers, "missing_or_repeated_transaction_identity") {
			t.Fatal(got, err)
		}
	}
	for _, edit := range []func(*ReportEvidence){
		func(r *ReportEvidence) { r.Scope = "partial_page" },
		func(r *ReportEvidence) { r.Cycle = "2022" },
		func(r *ReportEvidence) { r.Receipts[0].Fields["file_num"] = "124" },
		func(r *ReportEvidence) { r.Receipts[0].Fields["memo_cd"] = "X" },
		func(r *ReportEvidence) { delete(r.Receipts[0].Fields, "filing_form") },
		func(r *ReportEvidence) { r.Receipts[0].Fields["line_num"] = 11 },
		func(r *ReportEvidence) { r.Receipts[1] = r.Receipts[0] },
		func(r *ReportEvidence) {
			for i := range r.Receipts {
				r.Receipts[i].Fields["lt_receipt_amount_minor_units"] = "9223372036854775807"
			}
		},
	} {
		r := lineReport(t)
		edit(&r)
		if got, err := assembleReportLines(context.Background(), r, ReceiptSource{}); err == nil || got.ReviewID != "" {
			t.Fatal("invalid evidence produced success", got, err)
		}
	}
	r := lineReport(t)
	r.Receipts = nil
	got, err := assembleReportLines(context.Background(), r, ReceiptSource{})
	if err != nil || !slices.Contains(got.Blockers, "no_rows_in_snapshot") {
		t.Fatal(got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := assembleReportLines(ctx, r, ReceiptSource{}); err == nil {
		t.Fatal("cancelled empty report succeeded")
	}
}

// Bounded audit output for independently comparing retained original filings.
// The IDs below select test witnesses, never runtime policy exceptions.
func TestReportLinesPublishedCorpus(t *testing.T) {
	output := os.Getenv("LT_REPORT_LINES_OUTPUT")
	if output == "" {
		t.Skip("requires published inventory and a writable audit output directory")
	}
	ctx := context.Background()
	r, err := Open(ctx, os.Getenv("LT_REPORT_LINES_STORAGE"), os.Getenv("LT_REPORT_LINES_BASIS"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ committee, file string }{
		{"C00843367", "1714573"}, {"C00843367", "1766866"}, {"C00843367", "1743911"},
		{"C00843367", "1780310"}, {"C00843367", "1780346"},
		{"C00849901", "1730369"}, {"C00849901", "1753173"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			report, err := r.ReviewReport(ctx, tc.committee, tc.file, func(s string) { t.Log(s) })
			if err != nil {
				t.Fatal(err)
			}
			lines, err := assembleReportLines(ctx, report, ReceiptSource{r.manifest.SourceReleaseID, r.manifest.SourceReleaseManifestSHA256})
			if err != nil {
				t.Fatal(err)
			}
			again, err := r.ReviewReportLines(ctx, tc.committee, tc.file, nil)
			if err != nil || !reflect.DeepEqual(lines, again) {
				t.Fatal("report replay differs", err)
			}
			for suffix, value := range map[string]any{"report": report, "lines": lines} {
				body, err := json.MarshalIndent(value, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(output, tc.file+"-"+suffix+".json"), body, 0o640); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
