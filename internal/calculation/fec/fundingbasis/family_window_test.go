package fundingbasis

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func familyWindowFixture(t *testing.T, form string) FamilyAbsenceReview {
	t.Helper()
	a := FamilyAbsenceReview{Version: FamilyAbsenceVersion}
	m := &a.Compared.FamilyReports.Membership
	m.Evidence.Endpoint = map[string]string{"F3": "/v1/reports/house-senate/", "F3X": "/v1/reports/pac-party/"}[form]
	m.Window = reportperiod.Period{Start: "2024-01-01", End: "2024-02-29"}
	m.ObservedPartitionReady = true
	for i, p := range []reportperiod.Period{{Start: "2024-01-01", End: "2024-01-31"}, {Start: "2024-02-01", End: "2024-02-29"}} {
		o, b, g := receiptReportFixture()
		o.Period = &p
		o.ReportForm = map[string]string{"F3": "Form 3", "F3X": "Form 3X"}[form]
		o.FileNumber = strconv.Itoa(i + 1)
		b.ObservationIndex = &i
		b.Fields = nil
		g[0].Key.Form.Value = form
		g[0].Key.Line.Value = "11B"
		for j, spec := range reportscope.ReceiptFamilyFields(form) {
			value := "0"
			if j == 0 {
				value = "100"
			}
			b.Fields = append(b.Fields, reportperiod.FieldBinding{Name: spec.MetadataField, ReportedValueBound: true, Metadata: reportperiod.MetadataAmount{State: "valid", MinorUnits: &value}})
		}
		r, err := compareReceiptFamilyReport(o, b, g)
		if err != nil {
			t.Fatal(err)
		}
		r.BindingIndex = i
		c := reportscope.LineCensus{Form: form, Complete: true, Lines: []reportscope.CensusLine{{Tag: "SA11B", RecordOrdinals: []int{3}}}}
		z := qualifyFamilyAbsence(r, c)
		z.ReportIndex = i
		a.Compared.FamilyReports.Bindings = append(a.Compared.FamilyReports.Bindings, b)
		a.Compared.Reports = append(a.Compared.Reports, r)
		a.Reports = append(a.Reports, z)
		m.Observations = append(m.Observations, o)
		m.ChainCandidateIndexes = append(m.ChainCandidateIndexes, i)
	}
	return a
}

func TestFamilyWindowExactAndPreserving(t *testing.T) {
	for _, form := range []string{"F3", "F3X"} {
		a := familyWindowFixture(t, form)
		before, _ := json.Marshal(a)
		r, err := compareReceiptFamilyWindow(context.Background(), a)
		if err != nil {
			t.Fatal(err)
		}
		for i, f := range r.Families {
			want := "0"
			basis := "qualified_reported_zero_without_occurrences"
			if i == 0 {
				want = "200"
				basis = "nonmemo_occurrence_subtotal"
			}
			if f.State != "equal" || !f.ReportedWindowReady || !f.ComparisonWindowReady || *f.ReportedWindowMinorUnits != want || *f.ComparisonWindowMinorUnits != want || *f.DeltaMinorUnits != "0" || f.ComparisonCoverage.CoveredDays != 60 || len(f.Members) != 2 {
				t.Fatal(f)
			}
			for _, member := range f.Members {
				if member.ValueBasis != basis {
					t.Fatal(member)
				}
			}
		}
		after, _ := json.Marshal(a)
		nested, _ := json.Marshal(r.Reviewed)
		if string(before) != string(after) || string(before) != string(nested) || r.FinancialUseEligible || r.TerminalEligible {
			t.Fatal("changed evidence or financial eligibility")
		}
	}
}

func TestFamilyWindowCoverageAndMissingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		mutate        func(*FamilyAbsenceReview)
	}{
		{"missing_document", "unqualified_candidate_family", func(a *FamilyAbsenceReview) {
			a.Compared.Reports = a.Compared.Reports[:1]
			a.Reports = a.Reports[:1]
			a.Compared.FamilyReports.Bindings = a.Compared.FamilyReports.Bindings[:1]
		}},
		{"gap", "family_coverage_gaps", func(a *FamilyAbsenceReview) {
			a.Compared.FamilyReports.Membership.Observations[1].Period.Start = "2024-02-02"
		}},
		{"overlap", "family_coverage_overlaps", func(a *FamilyAbsenceReview) {
			a.Compared.FamilyReports.Membership.Observations[1].Period.Start = "2024-01-31"
		}},
		{"cross_boundary", "unqualified_candidate_family", func(a *FamilyAbsenceReview) {
			a.Compared.FamilyReports.Membership.Observations[0].WindowRelation = "crosses_boundary"
			a.Compared.FamilyReports.Membership.Observations[0].Period.Start = "2023-12-31"
		}},
		{"unresolved_cohort", "unresolved_report_cohort", func(a *FamilyAbsenceReview) {
			a.Compared.FamilyReports.Membership.PartitionBlockers = []string{"unresolved_report_cohort"}
		}},
		{"partial_metadata", "partial_metadata_traversal", func(a *FamilyAbsenceReview) {
			a.Compared.FamilyReports.Membership.PartitionBlockers = []string{"partial_metadata_traversal"}
		}},
		{"unready_partition", "reported_partition_unready", func(a *FamilyAbsenceReview) { a.Compared.FamilyReports.Membership.ObservedPartitionReady = false }},
		{"no_documents", "no_qualified_family_members", func(a *FamilyAbsenceReview) {
			a.Compared.Reports = nil
			a.Reports = nil
			a.Compared.FamilyReports.Bindings = nil
		}},
		{"no_candidates", "no_qualified_family_members", func(a *FamilyAbsenceReview) { a.Compared.FamilyReports.Membership.ChainCandidateIndexes = nil }},
		{"wrong_form", "unqualified_candidate_family", func(a *FamilyAbsenceReview) { a.Compared.Reports[1].Families[0].Field.Form = "F3X" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := familyWindowFixture(t, "F3")
			tc.mutate(&a)
			r, err := compareReceiptFamilyWindow(context.Background(), a)
			if err != nil {
				t.Fatal(err)
			}
			f := r.Families[0]
			if f.State != "blocked" || f.ReportedWindowMinorUnits != nil || f.ComparisonWindowMinorUnits != nil || f.DeltaMinorUnits != nil || !slices.Contains(f.ReportedBlockers, tc.blocker) {
				t.Fatal(f)
			}
			if tc.name == "missing_document" && (f.Members[1].ReportIndex != nil || f.ReportedCoverage.GapDays != 29 || *f.ObservedReportedMinorUnits != "100") {
				t.Fatal(f)
			}
			if tc.name == "cross_boundary" && (f.Members[0].ComparisonMinorUnits != nil || *f.ObservedComparisonMinorUnits != "100") {
				t.Fatal("prorated boundary", f)
			}
		})
	}
}

func TestFamilyWindowDetailFailureIsFieldLocal(t *testing.T) {
	for _, reason := range []string{"no_nonmemo_family_detail", "unresolved_memo_code", "unresolved_line_amount", "profile_report_scope_mismatch"} {
		a := familyWindowFixture(t, "F3")
		f := &a.Compared.Reports[1].Families[0]
		f.State = "blocked"
		f.DetailMinorUnits = nil
		f.Blockers = []string{reason}
		r, err := compareReceiptFamilyWindow(context.Background(), a)
		if err != nil {
			t.Fatal(err)
		}
		v := r.Families[0]
		if !v.ReportedWindowReady || v.ComparisonWindowReady || v.DeltaMinorUnits != nil || *v.ReportedWindowMinorUnits != "200" || *v.ObservedComparisonMinorUnits != "100" || v.ComparisonCoverage.GapDays != 29 || !slices.Contains(v.Members[1].ComparisonBlockers, reason) {
			t.Fatal(v)
		}
		if r.Families[1].State != "equal" {
			t.Fatal("contaminated another field")
		}
	}
	a := familyWindowFixture(t, "F3")
	a.Compared.Reports[1].Families[1].Binding.ReportedValueBound = false
	r, err := compareReceiptFamilyWindow(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if r.Families[1].ReportedWindowReady || r.Families[1].ComparisonWindowReady || r.Families[0].State != "equal" {
		t.Fatal("blank field treated as zero or contaminated sibling")
	}
}

func TestFamilyWindowDifferencesSignedSumsAndNoDedup(t *testing.T) {
	a := familyWindowFixture(t, "F3")
	for i, amount := range []string{"-100", "50"} {
		a.Compared.Reports[i].Families[0].State = "different"
		a.Compared.Reports[i].Families[0].DetailMinorUnits = &amount
	}
	r, err := compareReceiptFamilyWindow(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	f := r.Families[0]
	if f.State != "different" || *f.ComparisonWindowMinorUnits != "-50" || *f.DeltaMinorUnits != "250" {
		t.Fatal(f)
	}
	// Keep per-report discrepancies even when their window differences cancel.
	v := "300"
	a.Compared.Reports[1].Families[0].DetailMinorUnits = &v
	r, err = compareReceiptFamilyWindow(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if r.Families[0].State != "equal" || r.Families[0].Members[0].State != "different" || r.Families[0].Members[1].State != "different" {
		t.Fatal("hid cancelling discrepancies")
	}
	// Window arithmetic does not overflow int64; each operand remains exact.
	for i := range a.Compared.Reports {
		v := "9223372036854775807"
		a.Compared.Reports[i].Families[0].Binding.Metadata.MinorUnits = &v
		a.Compared.Reports[i].Families[0].DetailMinorUnits = &v
	}
	r, err = compareReceiptFamilyWindow(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if *r.Families[0].ReportedWindowMinorUnits != "18446744073709551614" || *r.Families[0].ComparisonWindowMinorUnits != "18446744073709551614" {
		t.Fatal("overflow or value deduplication")
	}
}

func TestFamilyWindowExcludesOutsideAndSuperseded(t *testing.T) {
	for _, outside := range []bool{true, false} {
		a := familyWindowFixture(t, "F3")
		m := &a.Compared.FamilyReports.Membership
		if outside {
			m.Observations[1].WindowRelation = "outside"
			m.Observations[1].Period = &reportperiod.Period{Start: "2024-03-01", End: "2024-03-31"}
		} else {
			m.ChainCandidateIndexes = []int{0}
		}
		m.Window.End = "2024-01-31"
		r, err := compareReceiptFamilyWindow(context.Background(), a)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Families[0].Members) != 1 || *r.Families[0].ReportedWindowMinorUnits != "100" {
			t.Fatal("added outside or superseded source")
		}
	}
}

func TestFamilyWindowErrorsAndContract(t *testing.T) {
	a := familyWindowFixture(t, "F3")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := compareReceiptFamilyWindow(ctx, a); err == nil {
		t.Fatal("ignored cancellation")
	}
	v := "invalid"
	a.Compared.Reports[0].Families[0].Binding.Metadata.MinorUnits = &v
	if _, err := compareReceiptFamilyWindow(context.Background(), a); err == nil {
		t.Fatal("accepted invalid operand")
	}
	body, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-family-window/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Version, UpstreamVersion string
		Families                 map[string][]string
		Guards                   map[string]bool
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Version != FamilyWindowVersion {
		t.Fatal("changed contract")
	}
	for _, form := range []string{"F3", "F3X"} {
		ids := []string{}
		for _, f := range reportscope.ReceiptFamilyFields(form) {
			ids = append(ids, f.ID)
		}
		if !slices.Equal(ids, p.Families[form]) {
			t.Fatal("changed subset")
		}
	}
	for _, guard := range p.Guards {
		if guard {
			t.Fatal("promoted financial eligibility")
		}
	}
}
