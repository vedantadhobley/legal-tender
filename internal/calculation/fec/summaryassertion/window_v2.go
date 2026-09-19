package summaryassertion

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

const WindowComparisonV2 = "legal-tender.fec.summary-report-window.v2"

// SummaryCycleSpan describes dates, not money or a proven committee lifetime.
// A prefix/suffix is outside this assertion's reported span, not necessarily
// absent from all source evidence. Internal coverage lives in the window review.
type SummaryCycleSpan struct {
	Cycle                        reportperiod.Period `json:"cycle"`
	State                        string              `json:"state"`
	Reported                     *SpanInterval       `json:"reported"`
	Prefix                       *SpanInterval       `json:"outside_reported_prefix"`
	Suffix                       *SpanInterval       `json:"outside_reported_suffix"`
	Blockers                     []string            `json:"blockers"`
	FinancialCoverageEstablished bool                `json:"financial_coverage_established"`
	OutsideActivityKnown         bool                `json:"outside_activity_known"`
}

type SpanInterval struct {
	reportperiod.Period
	Days int64 `json:"days"`
}

// CompareWindowV2 compares the declared reported span while keeping unrepresented
// calendar boundaries explicit. It never manufactures an earlier zero report.
func CompareWindowV2(ctx context.Context, request WindowComparisonRequest) (WindowComparison, error) {
	return compareWindowRequest(ctx, request, WindowComparisonV2)
}

func summaryCycleSpan(a Assertion, conflicts []string, year int) *SummaryCycleSpan {
	r := &SummaryCycleSpan{Cycle: reportperiod.Period{Start: fmt.Sprintf("%04d-01-01", year-1), End: fmt.Sprintf("%04d-12-31", year)}, State: "unqualified", Blockers: []string{}}
	if a.CoverageStart.State != committeesummary.Valid || a.CoverageEnd.State != committeesummary.Valid || a.CoverageStart.Value == nil || a.CoverageEnd.Value == nil {
		r.Blockers = append(r.Blockers, "summary_coverage_unavailable")
		return r
	}
	start, e1 := time.Parse("2006-01-02", *a.CoverageStart.Value)
	end, e2 := time.Parse("2006-01-02", *a.CoverageEnd.Value)
	lo, e3 := time.Parse("2006-01-02", r.Cycle.Start)
	hi, e4 := time.Parse("2006-01-02", r.Cycle.End)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || end.Before(start) || start.Before(lo) || end.After(hi) {
		r.Blockers = append(r.Blockers, "summary_coverage_outside_ordered_cycle")
		return r
	}
	r.State = "reported_span_only"
	r.Reported = spanInterval(start, end)
	if start.After(lo) {
		r.Prefix = spanInterval(lo, start.AddDate(0, 0, -1))
	}
	if end.Before(hi) {
		r.Suffix = spanInterval(end.AddDate(0, 0, 1), hi)
	}
	if slices.Contains(conflicts, "CVG_START_DT") || slices.Contains(conflicts, "CVG_END_DT") {
		r.State = "conflicting_reported_span"
		r.Blockers = append(r.Blockers, "conflicting_summary_dates")
	}
	return r
}

func spanInterval(start, end time.Time) *SpanInterval {
	return &SpanInterval{reportperiod.Period{Start: start.Format("2006-01-02"), End: end.Format("2006-01-02")}, (end.Unix()-start.Unix())/86400 + 1}
}
