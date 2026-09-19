package summaryassertion

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
)

func comparedV2(t *testing.T, s Result, w reportperiod.WindowReview) WindowComparison {
	t.Helper()
	r, err := compareWindowVersion(s, w, WindowComparisonV2)
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != WindowComparisonV2 || r.FinancialUseEligible || r.TerminalAttributionEligible || r.SameReportMembershipProven || !reflect.DeepEqual(r.Window, w) {
		t.Fatal("changed original evidence/guards")
	}
	for _, v := range r.Comparisons {
		span := v.CycleSpan
		if span == nil || span.FinancialCoverageEstablished || span.OutsideActivityKnown {
			t.Fatal("missing span or invented coverage")
		}
		if span.Reported != nil {
			lo, _ := time.Parse("2006-01-02", span.Cycle.Start)
			hi, _ := time.Parse("2006-01-02", span.Cycle.End)
			days := span.Reported.Days
			if span.Prefix != nil {
				days += span.Prefix.Days
			}
			if span.Suffix != nil {
				days += span.Suffix.Days
			}
			if days != (hi.Unix()-lo.Unix())/86400+1 {
				t.Fatal("nonconserving calendar envelope")
			}
		}
	}
	return r
}

func TestV2LateReportedSpanQualifiesWithoutInventingCyclePrefix(t *testing.T) {
	row := source()
	row["CVG_START_DT"], row["CVG_END_DT"] = "20230401", "20240430"
	s, w := comparisonFixture(t, row)
	w.Membership.Window = reportperiod.Period{Start: "2023-04-01", End: "2024-04-30"}
	r := comparedV2(t, s, w)
	for i, f := range r.Comparisons[0].Fields {
		if f.ReportedComparisonReady != (i != 5) || (f.DeltaMinorUnits != nil) != (i != 5) {
			t.Fatal(i, f)
		}
		if i < 5 && f.ScopeBasis != "reported_summary_coverage" {
			t.Fatal(f)
		}
	}
	span := r.Comparisons[0].CycleSpan
	if span.State != "reported_span_only" || span.Prefix.Start != "2023-01-01" || span.Prefix.End != "2023-03-31" || span.Prefix.Days != 90 || span.Suffix.Start != "2024-05-01" || span.Suffix.End != "2024-12-31" || span.Suffix.Days != 245 || span.Reported.Days != 396 {
		t.Fatal(span)
	}
	prior := compared(t, s, w)
	if prior.Comparisons[0].CycleSpan != nil || prior.Comparisons[0].Fields[0].ReportedComparisonReady {
		t.Fatal("changed v1")
	}
	if !reflect.DeepEqual(r, comparedV2(t, s, w)) {
		t.Fatal("unstable v2 replay")
	}
	if path := os.Getenv("LT_SUMMARY_SPAN_FIXTURE"); path != "" {
		raw, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(raw, '\n'), 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func TestV2MatchesBothFlowBoundariesAndKeepsStockScope(t *testing.T) {
	for _, tc := range []struct {
		start, end string
		ready      []int
	}{
		{"2023-01-01", "2024-12-31", []int{0, 1, 2, 3, 4, 5, 6}},
		{"2023-04-01", "2024-12-31", []int{6}},
		{"2023-01-01", "2024-04-30", []int{5}},
		{"2023-04-01", "2024-04-30", nil},
	} {
		s, w := comparisonFixture(t)
		w.Membership.Window = reportperiod.Period{Start: tc.start, End: tc.end}
		for i, f := range comparedV2(t, s, w).Comparisons[0].Fields {
			if f.ReportedComparisonReady != slices.Contains(tc.ready, i) {
				t.Fatal(tc, f)
			}
		}
	}
}

func TestV2EnvelopeDoesNotBridgeInternalGapsOrReplaceMissingAmounts(t *testing.T) {
	s, w := comparisonFixture(t)
	w.Fields[0].ReportedWindowReady = false
	w.Fields[0].WindowValueMinorUnits = nil
	partial := "200"
	w.Fields[0].ObservedSumMinorUnits = &partial
	r := comparedV2(t, s, w)
	f := r.Comparisons[0].Fields[0]
	if f.DeltaMinorUnits != nil || f.ReportedComparisonReady || !slices.Contains(f.Blockers, "reported_window_field_unready") {
		t.Fatal(f)
	}
	if !r.Comparisons[0].Fields[1].ReportedComparisonReady {
		t.Fatal("spread field blocker")
	}
	span := r.Comparisons[0].CycleSpan
	if span.Prefix != nil || span.Suffix != nil || span.State != "reported_span_only" {
		t.Fatal(span)
	}
}

func TestV2PreservesVariantConflictsAndInvalidDates(t *testing.T) {
	for _, field := range []string{"CMTE_NM", "TTL_RECEIPTS", "COH_BOP", "CMTE_DSGN", "CVG_START_DT", "CVG_END_DT"} {
		a, b := source(), source()
		b[field] = "changed"
		s, w := comparisonFixture(t, a, b, a)
		r := comparedV2(t, s, w)
		prior := compared(t, s, w)
		if !reflect.DeepEqual(r.Summary, prior.Summary) {
			t.Fatal("changed variants")
		}
		for i, v := range r.Comparisons {
			for j, f := range v.Fields {
				old := prior.Comparisons[i].Fields[j]
				if f.ReportedComparisonReady != old.ReportedComparisonReady || !reflect.DeepEqual(f.DeltaMinorUnits, old.DeltaMinorUnits) {
					t.Fatal(field, f)
				}
			}
		}
	}
	for _, dates := range [][2]string{{"", "20241231"}, {"20240230", "20241231"}, {"20250101", "20241231"}, {"20220101", "20241231"}} {
		row := source()
		row["CVG_START_DT"], row["CVG_END_DT"] = dates[0], dates[1]
		s, w := comparisonFixture(t, row)
		span := comparedV2(t, s, w).Comparisons[0].CycleSpan
		if span.State != "unqualified" || span.Reported != nil || span.Prefix != nil || span.Suffix != nil {
			t.Fatal(span)
		}
	}
	a, b := source(), source()
	b["CVG_START_DT"] = "20230401"
	s, w := comparisonFixture(t, a, b)
	for _, v := range comparedV2(t, s, w).Comparisons {
		if v.CycleSpan.State != "conflicting_reported_span" || v.CycleSpan.Reported == nil {
			t.Fatal("lost dated conflicting variant")
		}
	}
}

func TestV2DifferencesDoNotNeedArithmeticAgreement(t *testing.T) {
	row := source()
	row["TTL_RECEIPTS"] = "-92233720368547758.08"
	row["CVG_START_DT"] = "20230401"
	s, w := comparisonFixture(t, row)
	w.Membership.Window.Start = "2023-04-01"
	r := comparedV2(t, s, w)
	f := r.Comparisons[0].Fields[3]
	if !f.ReportedComparisonReady || f.State != "different" || *f.DeltaMinorUnits != "-9223372036854777808" || r.Summary.Assertions[0].Equations["cash"].State != "different" {
		t.Fatal(f)
	}
	if _, err := compareWindowVersion(s, w, "unreviewed"); err == nil {
		t.Fatal("unknown policy")
	}
}

func TestV2SpanLeapDayAndSourceCycleNotFixedTo2024(t *testing.T) {
	for _, year := range []int{2020, 2022, 2024, 2026} {
		start, end := time.Date(year, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(year, 3, 0, 0, 0, 0, 0, time.UTC)
		row := source()
		row["CVG_START_DT"], row["CVG_END_DT"] = start.Format("20060102"), end.Format("20060102")
		s, w := comparisonFixture(t, row)
		s.Cycle = start.Format("2006")
		w.Membership.Evidence.Query.Cycle = year
		w.Membership.Window = reportperiod.Period{Start: start.Format("2006-01-02"), End: end.Format("2006-01-02")}
		r := comparedV2(t, s, w)
		if r.Comparisons[0].CycleSpan.Reported.Days != int64(end.Day()) || !r.Comparisons[0].Fields[0].ReportedComparisonReady {
			t.Fatal(year)
		}
	}
}

func TestV2PolicyFieldsAndGuards(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/calculations/fec/summary-report-window/v2/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Version    string          `json:"version"`
		Fields     []string        `json:"summary_fields"`
		Guards     map[string]bool `json:"guards"`
		SpanGuards map[string]bool `json:"span_guards"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Version != WindowComparisonV2 || len(p.Fields) != len(windowRules) || len(p.Guards) != 3 || len(p.SpanGuards) != 2 {
		t.Fatal(p)
	}
	for i, rule := range windowRules {
		if p.Fields[i] != rule.summary {
			t.Fatal("field drift")
		}
	}
	for _, guards := range []map[string]bool{p.Guards, p.SpanGuards} {
		for _, value := range guards {
			if value {
				t.Fatal("guard drift")
			}
		}
	}
}
