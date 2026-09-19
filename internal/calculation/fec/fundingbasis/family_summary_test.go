package fundingbasis

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func familySummaryFixture(t *testing.T, form string) (FamilyWindowComparison, []summaryassertion.AssertionFields) {
	t.Helper()
	w, err := compareReceiptFamilyWindow(context.Background(), familyWindowFixture(t, form))
	if err != nil {
		t.Fatal(err)
	}
	m := &w.Reviewed.Compared.FamilyReports.Membership
	m.Evidence.Query.Cycle = 2024
	cmteType := map[string]string{"F3": "H", "F3X": "Q"}[form]
	for range m.Observations {
		m.Evidence.Pages = append(m.Evidence.Pages, reportmetadata.PageReview{Records: []reportmetadata.Record{{Raw: json.RawMessage(`{"committee_type":"` + cmteType + `"}`)}}})
	}
	s := &w.Reviewed.Compared.Reported
	s.Cycle = "2024"
	s.Window.Membership = *m
	s.Window.Bindings = w.Reviewed.Compared.FamilyReports.Bindings
	s.Summary = &summaryassertion.Committee{Assertions: []summaryassertion.Assertion{{ID: "a", RepresentativeFactID: "f", CommitteeType: cmteType, Designation: "P", CoverageStart: committeesummary.Value{State: "valid", Value: &w.Window.Start}, CoverageEnd: committeesummary.Value{State: "valid", Value: &w.Window.End}, Members: []summaryassertion.Member{{FactID: "f"}, {FactID: "fanout"}}}}}
	fields := []summaryassertion.AssertionFields{{AssertionID: "a", RepresentativeFactID: "f", Fields: []summaryassertion.FieldValue{}}}
	for _, f := range w.Families {
		fields[0].Fields = append(fields[0].Fields, summaryassertion.FieldValue{Field: familySummaryField(f.Field), Raw: "preserved", Value: committeesummary.MoneyValue{State: "valid", MinorUnits: f.ReportedWindowMinorUnits}})
	}
	return w, fields
}

func TestFamilySummaryExactFormMappingsAndEvidence(t *testing.T) {
	for _, form := range []string{"F3", "F3X"} {
		w, fields := familySummaryFixture(t, form)
		before, _ := json.Marshal(w)
		r, err := compareReceiptFamilySummary(context.Background(), w, fields)
		if err != nil {
			t.Fatal(err)
		}
		for i, f := range r.Assertions[0].Fields {
			if f.FamilyIndex != i || f.Summary != fields[0].Fields[i] || f.Reported.State != "equal" || f.Comparison.State != "equal" || *f.Reported.DeltaMinorUnits != "0" || *f.Comparison.DeltaMinorUnits != "0" {
				t.Fatal(f)
			}
		}
		after, _ := json.Marshal(w)
		nested, _ := json.Marshal(r.Window)
		if string(before) != string(after) || string(before) != string(nested) || r.FinancialUseEligible || r.TerminalEligible || r.SameReportMembershipProven || r.SourceAlignment != "independent_snapshots" {
			t.Fatal("mutated evidence or eligibility")
		}
		if len(r.Window.Reviewed.Compared.Reported.Summary.Assertions[0].Members) != 2 {
			t.Fatal("lost fanout")
		}
		replay, err := compareReceiptFamilySummary(context.Background(), w, fields)
		if err != nil || !reflect.DeepEqual(r, replay) {
			t.Fatal("unstable replay", err)
		}
	}
}

func TestFamilySummaryScopedBlockersAndIndependentSides(t *testing.T) {
	for _, tc := range []struct {
		name, blocker    string
		mutate           func(*FamilyWindowComparison, []summaryassertion.AssertionFields)
		reported, detail bool
	}{
		{"blank", "summary_value_source_blank", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			f[0].Fields[0].Value = committeesummary.MoneyValue{State: committeesummary.Blank}
		}, false, false},
		{"invalid", "summary_value_invalid", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			f[0].Fields[0].Value = committeesummary.MoneyValue{State: "invalid"}
		}, false, false},
		{"field_conflict", "conflicting_reported_field", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Reviewed.Compared.Reported.Summary.ConflictFields = []string{"PTY_CMTE_CONTB"}
		}, false, false},
		{"contact_conflict", "", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Reviewed.Compared.Reported.Summary.ConflictFields = []string{"CMTE_NM"}
		}, true, true},
		{"scope_conflict", "conflicting_summary_scope", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Reviewed.Compared.Reported.Summary.ConflictFields = []string{"CMTE_DSGN"}
		}, false, false},
		{"date_mismatch", "summary_end_mismatch", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			v := "2024-12-31"
			w.Reviewed.Compared.Reported.Summary.Assertions[0].CoverageEnd.Value = &v
		}, false, false},
		{"unsupported_type", "unqualified_summary_committee_type", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Reviewed.Compared.Reported.Summary.Assertions[0].CommitteeType = "P"
		}, false, false},
		{"form_mismatch", "report_summary_type_or_form_mismatch", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Reviewed.Compared.FamilyReports.Membership.Observations[0].ReportForm = "Form 3X"
		}, false, false},
		{"missing_detail", "reported_window_field_unready", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Families[0].ComparisonWindowReady = false
			w.Families[0].ComparisonWindowMinorUnits = nil
		}, true, false},
		{"missing_report", "reported_window_field_unready", func(w *FamilyWindowComparison, f []summaryassertion.AssertionFields) {
			w.Families[0].ReportedWindowReady = false
			w.Families[0].ReportedWindowMinorUnits = nil
			w.Families[0].ComparisonWindowReady = false
			w.Families[0].ComparisonWindowMinorUnits = nil
		}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, fields := familySummaryFixture(t, "F3")
			tc.mutate(&w, fields)
			r, err := compareReceiptFamilySummary(context.Background(), w, fields)
			if err != nil {
				t.Fatal(err)
			}
			f := r.Assertions[0].Fields[0]
			if f.Reported.ReportedComparisonReady != tc.reported || f.Comparison.ReportedComparisonReady != tc.detail || (f.Reported.DeltaMinorUnits != nil) != tc.reported || (f.Comparison.DeltaMinorUnits != nil) != tc.detail {
				t.Fatal(f)
			}
			if tc.blocker != "" && !slices.Contains(f.Comparison.Blockers, tc.blocker) {
				t.Fatal(f)
			}
			if tc.name == "field_conflict" && r.Assertions[0].Fields[1].Reported.State != "equal" {
				t.Fatal("unrelated field blocked")
			}
		})
	}
}

func TestFamilySummarySignedDifferencesVariantsAndRejection(t *testing.T) {
	w, fields := familySummaryFixture(t, "F3")
	v := "-9223372036854775809"
	fields[0].Fields[0].Value.MinorUnits = &v
	a := w.Reviewed.Compared.Reported.Summary.Assertions[0]
	a.ID = "b"
	a.RepresentativeFactID = "g"
	w.Reviewed.Compared.Reported.Summary.Assertions = append(w.Reviewed.Compared.Reported.Summary.Assertions, a)
	f := fields[0]
	f.AssertionID = "b"
	f.RepresentativeFactID = "g"
	fields = append(fields, f)
	r, err := compareReceiptFamilySummary(context.Background(), w, fields)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Assertions) != 2 {
		t.Fatal("selected variant")
	}
	for _, a := range r.Assertions {
		if *a.Fields[0].Reported.DeltaMinorUnits != "-9223372036854776009" || a.Fields[0].Reported.State != "different" {
			t.Fatal(a)
		}
	}
	if _, err = compareReceiptFamilySummary(context.Background(), w, fields[:1]); err == nil {
		t.Fatal("accepted missing assertion")
	}
	fields[0].AssertionID = "changed"
	if _, err = compareReceiptFamilySummary(context.Background(), w, fields); err == nil {
		t.Fatal("accepted different assertion")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w, fields = familySummaryFixture(t, "F3X")
	if _, err = compareReceiptFamilySummary(ctx, w, fields); err == nil {
		t.Fatal("ignored cancellation")
	}
	w.Reviewed.Compared.Reported.Summary = nil
	r, err = compareReceiptFamilySummary(context.Background(), w, nil)
	if err != nil || len(r.Assertions) != 0 || !slices.Contains(r.Blockers, "summary_committee_absent") {
		t.Fatal(r, err)
	}
}

func TestFamilySummaryPolicyAndSourceMap(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-families/v1/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var source struct {
		Forms map[string]struct {
			Leaves []struct {
				ID, Line string
				Sequence int
				Summary  string `json:"summary_field"`
			}
		}
	}
	if err = json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile("../../../../contracts/calculations/fec/receipt-family-summary/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Version  string
		Mappings map[string]map[string]string
		Guards   map[string]bool
	}
	if err = json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Version != FamilySummaryVersion {
		t.Fatal(policy)
	}
	for _, guard := range policy.Guards {
		if guard {
			t.Fatal("promoted guard")
		}
	}
	for _, form := range []string{"F3", "F3X"} {
		if len(policy.Mappings[form]) != len(reportscope.ReceiptFamilyFields(form)) {
			t.Fatal("map count")
		}
		for _, spec := range reportscope.ReceiptFamilyFields(form) {
			name := familySummaryField(spec)
			if name == "" || policy.Mappings[form][spec.ID] != name {
				t.Fatal(spec, name)
			}
			found := false
			for _, f := range source.Forms[form].Leaves {
				if f.ID == spec.ID && f.Line == spec.Line && f.Sequence == spec.Sequence && f.Summary == name {
					found = true
				}
			}
			if !found {
				t.Fatal("source map mismatch", spec, name)
			}
			spec.Form = "F3P"
			if familySummaryField(spec) != "" {
				t.Fatal("accepted unsupported form")
			}
		}
	}
}
