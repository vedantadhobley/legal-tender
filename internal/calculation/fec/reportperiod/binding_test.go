package reportperiod

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func bindingFixture(t *testing.T, mutate func([]string, []string, map[string]any), prefix bool) BindingRequest {
	t.Helper()
	a := row(101, "2024-01-01", "2024-01-31", "101")
	a["is_amended"], a["amendment_indicator"] = true, "N"
	b := row(102, "2024-01-01", "2024-01-31", "101", "102")
	b["amendment_indicator"] = "A"
	f := make([]string, 123)
	f[0], f[1], f[9], f[13], f[14] = "F3XA", "C12345678", "Q1", "20240101", "20240131"
	for _, seq := range []int{30, 31, 32, 24, 45, 26, 66, 23, 27} {
		f[seq-1] = "0.00"
	}
	for _, name := range []string{"individual_itemized_contributions_period", "individual_unitemized_contributions_period", "total_individual_contributions_period", "total_receipts_period", "total_disbursements_period", "cash_on_hand_beginning_period", "cash_on_hand_end_period"} {
		b[name] = 0
	}
	h := []string{"HDR", "FEC", "8.4", "TEST", "1", "FEC-101", "1"}
	if mutate != nil {
		mutate(h, f, b)
	}
	body := []byte(strings.Join(h, "\x1c") + "\n" + strings.Join(f, "\x1c") + "\n")
	status, extra := "200", ""
	if prefix {
		status = "206"
		extra = fmt.Sprintf("Content-Range: bytes 0-%d/%d\r\n", len(body)-1, len(body)+100)
	}
	headers := []byte(fmt.Sprintf("HTTP/2 %s \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n%s\r\n", status, len(body), extra))
	dir := t.TempDir()
	d := reportscope.Request{SourceURL: "https://docquery.fec.gov/dcdev/posted/102.fec", BodyPath: filepath.Join(dir, "body"), HeadersPath: filepath.Join(dir, "headers"), BodySHA256: hash(body), HeadersSHA256: hash(headers)}
	write(t, d.BodyPath, body)
	write(t, d.HeadersPath, headers)
	return BindingRequest{Request{capture(t, false, a, b), "2024-01-01", "2024-01-31"}, d}
}

func TestBoundFieldsAreNotCashOrCycleTotals(t *testing.T) {
	request := bindingFixture(t, nil, false)
	r, err := BindFields(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ScopeBound || len(r.Fields) != 7 || r.CycleTotalReady || r.CashBasisReady || r.TerminalAttributionEligible || r.Membership.FinancialMembershipReady {
		t.Fatal("wrong readiness", r.ScopeBlockers)
	}
	for _, f := range r.Fields {
		if !f.ReportedValueBound || f.DeltaMinorUnits == nil || *f.DeltaMinorUnits != "0" {
			t.Fatal("explicit zero did not bind", f)
		}
	}
	again, err := BindFields(context.Background(), request)
	if err != nil || !bytes.Equal(marshal(r), marshal(again)) {
		t.Fatal("unstable replay", err)
	}
}

func TestBindingScopeCounterexamples(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		mutate        func([]string, []string, map[string]any)
	}{
		{"root", "header_chain_identity_mismatch", func(h, f []string, b map[string]any) { h[5] = "FEC-999" }},
		{"sequence", "header_chain_identity_mismatch", func(h, f []string, b map[string]any) { h[6] = "NaN" }},
		{"zero_sequence", "header_chain_identity_mismatch", func(h, f []string, b map[string]any) { h[6] = "0" }},
		{"filer", "cover_metadata_scope_mismatch", func(h, f []string, b map[string]any) { f[1] = "C87654321" }},
		{"period", "cover_metadata_scope_mismatch", func(h, f []string, b map[string]any) { f[13] = "20240102" }},
		{"type", "cover_metadata_scope_mismatch", func(h, f []string, b map[string]any) { f[9] = "YE" }},
		{"indicator", "amendment_indicator_mismatch", func(h, f []string, b map[string]any) { b["amendment_indicator"] = "N" }},
		{"superseded", "not_observed_chain_candidate", func(h, f []string, b map[string]any) { b["is_amended"] = true }},
		{"missing_chain", "not_observed_chain_candidate", func(h, f []string, b map[string]any) { b["amendment_chain"] = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := BindFields(context.Background(), bindingFixture(t, tc.mutate, false))
			if err != nil || r.ScopeBound || !slices.Contains(r.ScopeBlockers, tc.blocker) {
				t.Fatal("unsafe scope", r.ScopeBlockers, err)
			}
			for _, f := range r.Fields {
				if f.ReportedValueBound {
					t.Fatal("unsafe field")
				}
			}
		})
	}
	r, err := BindFields(context.Background(), bindingFixture(t, nil, true))
	if err != nil || r.ScopeBound || !slices.Contains(r.ScopeBlockers, "partial_document_capture") || len(r.Fields) != 7 {
		t.Fatal("prefix promoted or erased", err)
	}
}

func TestFieldFailuresStayLocal(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		mutate        func([]string, []string, map[string]any)
	}{
		{"blank", "cover_field:blank", func(h, f []string, b map[string]any) { f[30] = "" }},
		{"invalid", "cover_field:invalid", func(h, f []string, b map[string]any) { f[30] = "0.001" }},
		{"null", "metadata_field:source_null", func(h, f []string, b map[string]any) { b["individual_unitemized_contributions_period"] = nil }},
		{"mismatch", "reported_value_mismatch", func(h, f []string, b map[string]any) { f[30] = "-0.01" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := BindFields(context.Background(), bindingFixture(t, tc.mutate, false))
			if err != nil || !r.ScopeBound || r.Fields[1].ReportedValueBound || !slices.Contains(r.Fields[1].Blockers, tc.blocker) || !r.Fields[0].ReportedValueBound || !r.Fields[3].ReportedValueBound {
				t.Fatal("field contamination", r.Fields, err)
			}
		})
	}
	r, err := BindFields(context.Background(), bindingFixture(t, func(h, f []string, b map[string]any) { f[44] = "1.00" }, false))
	if err != nil || r.Fields[3].ReportedValueBound || !slices.Contains(r.Fields[3].Blockers, "conflicting_cover_positions") || !r.Fields[1].ReportedValueBound {
		t.Fatal("duplicate position fallback", err)
	}
}

func TestBindingErrorsAndWindowBoundary(t *testing.T) {
	r := bindingFixture(t, nil, false)
	r.Membership.Start = "2024-01-02"
	out, err := BindFields(context.Background(), r)
	if err != nil || out.ScopeBound || !slices.Contains(out.ScopeBlockers, "report_not_inside_requested_window") {
		t.Fatal("apportioned report", err)
	}
	r.Document.SourceURL = "https://docquery.fec.gov/dcdev/posted/999.fec"
	out, err = BindFields(context.Background(), r)
	if err != nil || out.ObservationIndex != nil || out.ScopeBound {
		t.Fatal("invented metadata witness", err)
	}
	r.Document.BodySHA256 = strings.Repeat("0", 64)
	if _, err = BindFields(context.Background(), r); err == nil {
		t.Fatal("accepted changed bytes")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = BindFields(ctx, r); err == nil {
		t.Fatal("ignored cancellation")
	}
}
