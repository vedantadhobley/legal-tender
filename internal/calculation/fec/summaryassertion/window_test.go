package summaryassertion

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

func comparisonFixture(t *testing.T, rows ...map[string]string) (Result, reportperiod.WindowReview) {
	t.Helper()
	if len(rows) == 0 {
		rows = []map[string]string{source()}
	}
	for _, row := range rows {
		if _, ok := row["CMTE_DSGN"]; !ok {
			row["CMTE_DSGN"] = "P"
		}
	}
	s := grouped(t, records(t, rows...))
	i := 0
	w := reportperiod.WindowReview{Version: reportperiod.WindowVersion,
		Membership: reportperiod.Review{Window: reportperiod.Period{Start: "2023-01-01", End: "2024-12-31"},
			Evidence: reportmetadata.Review{Query: reportmetadata.Query{CommitteeID: "C00000001", Cycle: 2024},
				Pages: []reportmetadata.PageReview{{Records: []reportmetadata.Record{{Raw: json.RawMessage(`{"committee_type":"H"}`)}}}}},
			Observations: []reportperiod.Observation{{ReportForm: "Form 3"}}},
		Bindings: []reportperiod.WindowBinding{{ObservationIndex: &i}}, Fields: []reportperiod.WindowField{}}
	for j, value := range []string{"200", "300", "500", "2000", "500", "1000", "2500"} {
		w.Fields = append(w.Fields, reportperiod.WindowField{Name: windowRules[j].window, ReportedWindowReady: true, WindowValueMinorUnits: &value, MemberBindingIndexes: []int{0}})
	}
	return s, w
}

func compared(t *testing.T, s Result, w reportperiod.WindowReview) WindowComparison {
	t.Helper()
	r, err := compareWindow(s, w)
	if err != nil {
		t.Fatal(err)
	}
	if r.SameReportMembershipProven || r.FinancialUseEligible || r.TerminalAttributionEligible || r.SourceAlignment != "independent_snapshots" {
		t.Fatal("promoted evidence", r)
	}
	if !reflect.DeepEqual(r.Window, w) || !reflect.DeepEqual(r.SummaryInput, s.Input) {
		t.Fatal("lost original evidence")
	}
	return r
}

func TestSummaryWindowExactSignedComparisonAndEvidence(t *testing.T) {
	s, w := comparisonFixture(t)
	r := compared(t, s, w)
	for _, f := range r.Comparisons[0].Fields {
		if !f.ReportedComparisonReady || f.State != "equal" || *f.DeltaMinorUnits != "0" {
			t.Fatal(f)
		}
	}
	if !reflect.DeepEqual(r, compared(t, s, w)) {
		t.Fatal("unstable replay")
	}
	row := source()
	row["TTL_RECEIPTS"] = "-92233720368547758.08"
	s, w = comparisonFixture(t, row)
	r = compared(t, s, w)
	f := r.Comparisons[0].Fields[3]
	if f.State != "different" || *f.DeltaMinorUnits != "-9223372036854777808" || r.Summary.Assertions[0].Equations["cash"].State != "different" {
		t.Fatal(f)
	}
	if path := os.Getenv("LT_SUMMARY_WINDOW_FIXTURE"); path != "" {
		raw, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(raw, '\n'), 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSummaryWindowDatesAreFieldSpecific(t *testing.T) {
	for _, tc := range []struct {
		name, summaryStart, summaryEnd, windowStart, windowEnd string
		ready                                                  []int
	}{
		{"late_first_report", "20230401", "20240430", "2023-04-01", "2024-04-30", []int{6}},
		{"shorter_window", "20230101", "20241231", "2023-01-01", "2024-04-30", []int{5}},
		{"partial_cycle_to_date", "20230101", "20240430", "2023-01-01", "2024-04-30", []int{0, 1, 2, 3, 4, 5, 6}},
		{"late_window_same_end", "20230101", "20241231", "2024-01-01", "2024-12-31", []int{6}},
		{"missing_start", "", "20241231", "2023-01-01", "2024-12-31", nil},
		{"invalid_end", "20230101", "20240230", "2023-01-01", "2024-12-31", nil},
		{"reversed", "20240401", "20230101", "2023-01-01", "2024-12-31", nil},
		{"outside_cycle", "20220101", "20241231", "2023-01-01", "2024-12-31", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := source()
			row["CVG_START_DT"], row["CVG_END_DT"] = tc.summaryStart, tc.summaryEnd
			s, w := comparisonFixture(t, row)
			w.Membership.Window = reportperiod.Period{Start: tc.windowStart, End: tc.windowEnd}
			for i, f := range compared(t, s, w).Comparisons[0].Fields {
				if f.ReportedComparisonReady != slices.Contains(tc.ready, i) || (f.DeltaMinorUnits != nil) != f.ReportedComparisonReady {
					t.Fatal(i, f)
				}
			}
		})
	}
}

func TestSummaryWindowConflictIsolationAndFanout(t *testing.T) {
	for _, field := range []string{"CAND_ID", "CMTE_NM", "TTL_RECEIPTS", "COH_BOP", "CMTE_TP", "CMTE_DSGN", "CVG_START_DT", "CVG_END_DT"} {
		t.Run(field, func(t *testing.T) {
			a, b := source(), source()
			b[field] = "changed"
			s, w := comparisonFixture(t, a, b, a)
			r := compared(t, s, w)
			members := 0
			for _, a := range r.Summary.Assertions {
				members += len(a.Members)
			}
			if members != 3 {
				t.Fatal("discarded fanout/duplicate")
			}
			for _, v := range r.Comparisons {
				for _, f := range v.Fields {
					want := field == "CAND_ID" || field == "CMTE_NM" || ((field == "TTL_RECEIPTS" || field == "COH_BOP") && f.SummaryField != field)
					if f.ReportedComparisonReady != want {
						t.Fatal(field, f)
					}
				}
			}
		})
	}
}

func TestSummaryWindowMissingAndInvalidDoNotBecomeZero(t *testing.T) {
	for _, value := range []string{"", "invalid", "0"} {
		row := source()
		row["INDV_UNITEM_CONTB"] = value
		s, w := comparisonFixture(t, row)
		r := compared(t, s, w)
		for i, f := range r.Comparisons[0].Fields {
			if f.ReportedComparisonReady != (i != 1 || value == "0") {
				t.Fatal(f)
			}
		}
	}
	s, w := comparisonFixture(t)
	w.Fields[3].ReportedWindowReady = false
	w.Fields[3].WindowValueMinorUnits = nil
	partial := "2000"
	w.Fields[3].ObservedSumMinorUnits = &partial
	r := compared(t, s, w)
	if r.Comparisons[0].Fields[3].DeltaMinorUnits != nil || !r.Comparisons[0].Fields[6].ReportedComparisonReady {
		t.Fatal("partial sum substituted")
	}
	s.Committees = nil
	r = compared(t, s, w)
	if r.Summary != nil || len(r.Comparisons) != 0 || !slices.Equal(r.Blockers, []string{"summary_committee_absent"}) {
		t.Fatal(r)
	}
}

func TestSummaryWindowRequiresSameTypeFormAndCycle(t *testing.T) {
	for _, kind := range []string{"H", "S", "N", "Q", "O", "U", "V", "W", "X", "Y", "P", "Z", "", "unknown"} {
		row := source()
		row["CMTE_TP"] = kind
		s, w := comparisonFixture(t, row)
		w.Membership.Evidence.Pages[0].Records[0].Raw, _ = json.Marshal(map[string]string{"committee_type": kind})
		want := slices.Contains([]string{"H", "S", "N", "Q", "O", "U", "V", "W", "X", "Y"}, kind)
		if kind != "H" && kind != "S" {
			w.Membership.Observations[0].ReportForm = "Form 3X"
		}
		if compared(t, s, w).Comparisons[0].Fields[0].ReportedComparisonReady != want {
			t.Fatal(kind)
		}
	}
	for _, raw := range []string{`{}`, `{"committee_type":null}`, `{"committee_type":5}`, `{"committee_type":"S"}`} {
		s, w := comparisonFixture(t)
		w.Membership.Evidence.Pages[0].Records[0].Raw = json.RawMessage(raw)
		if compared(t, s, w).Comparisons[0].Fields[0].ReportedComparisonReady {
			t.Fatal(raw)
		}
	}
	s, w := comparisonFixture(t)
	w.Membership.Observations[0].ReportForm = "Form 3X"
	if compared(t, s, w).Comparisons[0].Fields[0].ReportedComparisonReady {
		t.Fatal("form mismatch")
	}
	s.Cycle = "2022"
	if _, err := compareWindow(s, w); err == nil {
		t.Fatal("cycle mismatch")
	}
	if _, err := CompareWindow(context.Background(), WindowComparisonRequest{}); err == nil {
		t.Fatal("unverified inputs")
	}
}

func TestSummaryWindowPolicyMatchesImplementedFieldsAndGuards(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/calculations/fec/summary-report-window/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Version string          `json:"version"`
		Fields  []string        `json:"summary_fields"`
		Guards  map[string]bool `json:"guards"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Version != WindowComparisonVersion || len(p.Fields) != len(windowRules) || len(p.Guards) != 3 {
		t.Fatal(p)
	}
	for i, rule := range windowRules {
		if p.Fields[i] != rule.summary {
			t.Fatal("policy field drift")
		}
	}
	s, w := comparisonFixture(t)
	encoded, err := json.Marshal(compared(t, s, w))
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	for key, value := range p.Guards {
		if value || string(result[key]) != "false" {
			t.Fatal("guard drift", key)
		}
	}
}
