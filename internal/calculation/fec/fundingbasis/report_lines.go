package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

const ReportLinePolicy = "fec/reported-itemized-line-membership@1.0.0"

type ReportLineKey struct {
	Form        Cell   `json:"filing_form"`
	Schedule    Cell   `json:"schedule_type"`
	Line        Cell   `json:"line_num"`
	Memo        Cell   `json:"memo_code"`
	Individual  string `json:"publisher_individual_state"`
	Disposition string `json:"disposition"`
}

type ReportLineGroup struct {
	Key      ReportLineKey `json:"key"`
	Measures Measures      `json:"measures"`
	Ordinals []uint64      `json:"source_row_ordinals"`
}

type ReportTransactionIssue struct {
	Transaction Cell     `json:"transaction_id"`
	Ordinals    []uint64 `json:"source_row_ordinals"`
}

type ReportLines struct {
	SchemaVersion     string                   `json:"schema_version"`
	ReviewID          string                   `json:"review_id"`
	Policy            string                   `json:"policy"`
	Cycle             string                   `json:"cycle"`
	Committee         string                   `json:"committee_id"`
	File              string                   `json:"file_num"`
	InventoryID       string                   `json:"inventory_calculation_id"`
	Input             Input                    `json:"receipt_input"`
	Source            ReceiptSource            `json:"receipt_source"`
	Total             Measures                 `json:"total"`
	LinePopulation    Measures                 `json:"reviewed_nonmemo_line_population"`
	Groups            []ReportLineGroup        `json:"groups"`
	TransactionIssues []ReportTransactionIssue `json:"transaction_identity_issues"`
	Blockers          []string                 `json:"comparison_blockers"`
	ComparisonReady   bool                     `json:"comparison_ready"`
	TerminalEligible  bool                     `json:"terminal_attribution_eligible"`
}

// ReviewReportLines exhausts a bounded report in the verified fact snapshot.
// It is not a whole-cycle query plan or an original-filing parser.
func (r *Reader) ReviewReportLines(ctx context.Context, committee, file string, progress func(string)) (ReportLines, error) {
	report, err := r.ReviewReport(ctx, committee, file, progress)
	if err != nil {
		return ReportLines{}, err
	}
	return assembleReportLines(ctx, report, ReceiptSource{r.manifest.SourceReleaseID, r.manifest.SourceReleaseManifestSHA256})
}

// The initial comparison scope is F3/F3X Schedule A line 11AI. Individual
// classification is a diagnostic axis, not a prerequisite for line membership.
// Other forms/lines are preserved but need a separately reviewed mapping.
func reportLineDisposition(k ReportLineKey, amountKnown bool) string {
	if !k.Form.Present || (k.Form.Value != "F3" && k.Form.Value != "F3X") || k.Schedule != (Cell{true, "SA"}) || k.Line != (Cell{true, "11AI"}) {
		return "outside_reviewed_form_line"
	}
	if k.Memo.Present && k.Memo.Value == "X" {
		return "excluded_memo_subtotal"
	}
	if k.Memo.Present && k.Memo.Value != "" {
		return "unresolved_memo_code"
	}
	if !amountKnown {
		return "unresolved_line_amount"
	}
	return "reviewed_nonmemo_line"
}

func assembleReportLines(ctx context.Context, report ReportEvidence, source ReceiptSource) (ReportLines, error) {
	out := ReportLines{SchemaVersion: "legal-tender.fec.receipt-report-line-review.v1", Policy: ReportLinePolicy, Cycle: report.Cycle,
		Committee: report.Committee, File: report.File, InventoryID: report.InventoryID, Input: report.Input, Source: source,
		Groups: []ReportLineGroup{}, TransactionIssues: []ReportTransactionIssue{},
		Blockers: []string{"original_filing_membership_unverified", "report_period_and_account_coverage_unverified", "effective_report_selection_unverified", "cycle_summary_compatibility_unverified"}}
	cycle, err := strconv.ParseInt(report.Cycle, 10, 64)
	if err != nil || report.Scope != "published_schedule_a_cycle_report" || len(report.Receipts) > maxReportReviewRows {
		return ReportLines{}, fmt.Errorf("invalid bounded report scope")
	}
	groups := map[ReportLineKey]*ReportLineGroup{}
	transactions := map[Cell][]uint64{}
	unresolved := false
	var prior uint64
	for _, receipt := range report.Receipts {
		if err := ctx.Err(); err != nil {
			return ReportLines{}, err
		}
		in, err := decodeEvidence(receipt)
		if err != nil {
			return ReportLines{}, err
		}
		if receipt.Ordinal <= prior || in.row.Cycle != cycle || in.row.Recipient == nil || *in.row.Recipient != report.Committee || in.file == nil || *in.file != report.File {
			return ReportLines{}, fmt.Errorf("report line identity mismatch")
		}
		prior = receipt.Ordinal
		cell := func(name string) (Cell, error) {
			v, ok := receipt.Fields[name]
			if !ok {
				return Cell{}, fmt.Errorf("missing source field %s", name)
			}
			if v == nil {
				return Cell{}, nil
			}
			s, ok := v.(string)
			if !ok {
				return Cell{}, fmt.Errorf("invalid source field %s", name)
			}
			return Cell{true, s}, nil
		}
		var k ReportLineKey
		for name, dst := range map[string]*Cell{"filing_form": &k.Form, "schedule_type": &k.Schedule, "line_num": &k.Line, "memo_cd": &k.Memo} {
			*dst, err = cell(name)
			if err != nil {
				return ReportLines{}, err
			}
		}
		if in.row.Memo != (k.Memo.Present && k.Memo.Value == "X") {
			return ReportLines{}, fmt.Errorf("memo normalization mismatch")
		}
		k.Individual = "source_null"
		if in.row.Individual != nil {
			k.Individual = strconv.FormatBool(*in.row.Individual)
		}
		k.Disposition = reportLineDisposition(k, in.row.Amount != nil)
		unresolved = unresolved || k.Disposition == "unresolved_memo_code" || k.Disposition == "unresolved_line_amount"
		g := groups[k]
		if g == nil {
			g = &ReportLineGroup{Key: k, Ordinals: []uint64{}}
			groups[k] = g
		}
		g.Ordinals = append(g.Ordinals, receipt.Ordinal)
		if err := g.Measures.observe(in.row); err != nil {
			return ReportLines{}, err
		}
		if err := out.Total.observe(in.row); err != nil {
			return ReportLines{}, err
		}
		if k.Disposition == "reviewed_nonmemo_line" {
			if err := out.LinePopulation.observe(in.row); err != nil {
				return ReportLines{}, err
			}
		}
		id := Cell{}
		if in.transaction != nil {
			id = Cell{true, *in.transaction}
		}
		transactions[id] = append(transactions[id], receipt.Ordinal)
	}
	var total Measures
	for _, g := range groups {
		if !g.Measures.valid() || uint64(len(g.Ordinals)) != g.Measures.Rows {
			return ReportLines{}, fmt.Errorf("line group conservation failed")
		}
		if err := total.merge(g.Measures); err != nil {
			return ReportLines{}, err
		}
		out.Groups = append(out.Groups, *g)
	}
	if total != out.Total || out.Total.Rows != uint64(len(report.Receipts)) {
		return ReportLines{}, fmt.Errorf("report line conservation failed")
	}
	for id, ordinals := range transactions {
		if !id.Present || id.Value == "" || len(ordinals) > 1 {
			out.TransactionIssues = append(out.TransactionIssues, ReportTransactionIssue{id, ordinals})
		}
	}
	if len(out.TransactionIssues) > 0 {
		out.Blockers = append(out.Blockers, "missing_or_repeated_transaction_identity")
	}
	if unresolved {
		out.Blockers = append(out.Blockers, "unresolved_reviewed_line_membership")
	}
	if out.Total.Rows == 0 {
		out.Blockers = append(out.Blockers, "no_rows_in_snapshot")
	}
	sort.Slice(out.Groups, func(i, j int) bool {
		a, _ := json.Marshal(out.Groups[i].Key)
		b, _ := json.Marshal(out.Groups[j].Key)
		return string(a) < string(b)
	})
	sort.Slice(out.TransactionIssues, func(i, j int) bool {
		return out.TransactionIssues[i].Ordinals[0] < out.TransactionIssues[j].Ordinals[0]
	})
	if err := ctx.Err(); err != nil {
		return ReportLines{}, err
	}
	body, err := json.Marshal(out)
	if err != nil {
		return ReportLines{}, err
	}
	d := sha256.Sum256(body)
	out.ReviewID = hex.EncodeToString(d[:])
	return out, nil
}
