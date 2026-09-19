package reportfield

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestSameFilePeriodComparisonAndUnrelatedConflicts(t *testing.T) {
	r := fixture(t, "699.07", func(c []string) {
		c[28] = "699033.00"
		c[30] = "699.00"
		c[74] = "69910.00"
		c[93] = "699.10"
		c[20] = "invalid"
	})
	r.MetadataCaptures = []string{
		metadata(t, "/v1/filings/", json.Number("699.07"), map[string]any{"is_amended": true, "previous_file_number": -11}),
		metadata(t, "/v1/reports/pac-party/", json.Number("700.07"), map[string]any{"is_amended": false, "previous_file_number": 101}),
	}
	a, err := CompareTotalReceipts(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if a.Period == nil || a.Period.Start != "2024-04-01" || a.Period.End != "2024-06-30" || len(a.Comparisons) != 2 || len(a.CoverFields) != 2 || len(a.CoverBlockers) != 0 {
		t.Fatal("lost report field scope", a.CoverBlockers)
	}
	if a.FinancialComponentEligible || a.CycleComparisonReady || a.TerminalAttributionEligible || a.Evidence.FinancialSelectionReady {
		t.Fatal("promoted financial readiness")
	}
	for i, want := range []string{"0", "100"} {
		c := a.Comparisons[i]
		if !c.Comparable || c.DeltaMinorUnits == nil || *c.DeltaMinorUnits != want {
			t.Fatal("unrelated conflict blocked reported pair", c)
		}
	}
	if !slices.Contains(a.Evidence.Issues, "invalid_cover_amount") || len(a.Evidence.Differences) == 0 || a.Evidence.Cover.Fields[74] != "69910.00" {
		t.Fatal("lost unrelated evidence")
	}
	for i := 0; i < 8; i++ {
		again, err := CompareTotalReceipts(context.Background(), r)
		if err != nil || !bytes.Equal(jsonBytes(a), jsonBytes(again)) {
			t.Fatal("non-deterministic replay", err)
		}
	}
}

func TestRelevantCoverConflictsAndBlankVersusZero(t *testing.T) {
	for _, tc := range []struct {
		name, amount, delta string
		change              func([]string)
		block               string
	}{
		{"zero", "0.00", "0", nil, ""},
		{"negative", "-5.25", "525", nil, ""},
		{"blank", "", "", nil, "cover_total_receipts:blank"},
		{"bad_amount", "bad", "", nil, "cover_total_receipts:invalid"},
		{"sub_cent", "0.001", "", nil, "cover_total_receipts:invalid"},
		{"line_disagreement", "1.00", "", func(c []string) { c[43] = "2.00" }, "cover_total_receipts_conflict"},
		{"missing_line_19", "1.00", "", func(c []string) { c[43] = "" }, "cover_total_receipts:blank"},
		{"bad_date", "0", "", func(c []string) { c[12] = "20240431" }, "invalid_cover_identity_or_period"},
		{"reverse_date", "0", "", func(c []string) { c[12] = "20240701" }, "invalid_cover_identity_or_period"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := fixture(t, tc.amount, tc.change)
			r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("0.00"), nil)}
			a, err := CompareTotalReceipts(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			c := a.Comparisons[0]
			if tc.block != "" {
				if c.Comparable || c.DeltaMinorUnits != nil || !slices.Contains(c.Blockers, tc.block) {
					t.Fatal("invalid pair accepted", c)
				}
			} else if !c.Comparable || c.DeltaMinorUnits == nil || *c.DeltaMinorUnits != tc.delta {
				t.Fatal("valid point lost", c)
			}
		})
	}
}

func TestRequiredMetadataScopeAndAmounts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		changes map[string]any
		want    string
	}{
		{"committee", map[string]any{"committee_id": "C87654321"}, "metadata_scope:committee_id"},
		{"form", map[string]any{"form_type": "F3"}, "metadata_scope:form_type"},
		{"missing_start", map[string]any{"coverage_start_date": nil}, "metadata_scope:coverage_start_date"},
		{"different_period", map[string]any{"coverage_end_date": "2024-09-30"}, "metadata_scope:coverage_end_date"},
		{"nonmidnight", map[string]any{"coverage_end_date": "2024-06-30T12:00:00"}, "metadata_scope:coverage_end_date"},
		{"missing_report_type", map[string]any{"report_type": nil}, "metadata_scope:report_type"},
		{"different_method", map[string]any{"means_filed": "electronic"}, "metadata_scope:means_filed"},
		{"different_amendment", map[string]any{"amendment_indicator": "A"}, "metadata_scope:amendment_indicator"},
		{"wrong_url", map[string]any{"fec_url": "https://docquery.fec.gov/paper/posted/102.fec"}, "metadata_scope:fec_url"},
		{"null", map[string]any{"total_receipts": nil}, "metadata_amount:source_null"},
		{"sub_cent", map[string]any{"total_receipts": json.Number("0.001")}, "metadata_amount:invalid"},
		{"exponent", map[string]any{"total_receipts": json.Number("1e2")}, "metadata_amount:invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := fixture(t, "0", nil)
			r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("0"), tc.changes)}
			a, err := CompareTotalReceipts(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			c := a.Comparisons[0]
			if c.Comparable || c.DeltaMinorUnits != nil || !slices.Contains(c.Blockers, tc.want) {
				t.Fatal("bad metadata scope accepted", c)
			}
		})
	}
}

func TestNoSelectionFallbackAndExactLargeDifference(t *testing.T) {
	r := fixture(t, "-92233720368547758.08", nil)
	r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("92233720368547758.07"), nil)}
	a, err := CompareTotalReceipts(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Comparisons[0].Comparable || *a.Comparisons[0].DeltaMinorUnits != "18446744073709551615" {
		t.Fatal("difference overflow")
	}
	r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("0"), map[string]any{"file_number": 102})}
	a, err = CompareTotalReceipts(context.Background(), r)
	if err != nil || len(a.Comparisons) != 0 {
		t.Fatal("joined another file by period", err)
	}
	r.MetadataCaptures = []string{metadata(t, "/v1/reports/house-senate/", json.Number("0"), nil)}
	a, err = CompareTotalReceipts(context.Background(), r)
	if err != nil || !slices.Contains(a.Comparisons[0].Blockers, "unsupported_metadata_endpoint") {
		t.Fatal("used wrong report endpoint", err)
	}
	r.MetadataCaptures = nil
	a, err = CompareTotalReceipts(context.Background(), r)
	if err != nil || len(a.Comparisons) != 0 || a.FinancialComponentEligible {
		t.Fatal("invented a comparison", err)
	}
}

func TestSourcesAreReverified(t *testing.T) {
	r := fixture(t, "1.00", nil)
	p := metadata(t, "/v1/filings/", json.Number("1.00"), nil)
	r.MetadataCaptures = []string{p}
	write(t, filepath.Join(filepath.Dir(p), "body.json"), []byte("{}"))
	if _, err := CompareTotalReceipts(context.Background(), r); err == nil {
		t.Fatal("trusted tampered metadata")
	}
	r.MetadataCaptures = nil
	write(t, r.BodyPath, []byte("tampered"))
	if _, err := CompareTotalReceipts(context.Background(), r); err == nil {
		t.Fatal("trusted tampered body")
	}
}

func TestMultipleCoversDespiteUnrelatedInvalidAmount(t *testing.T) {
	r := fixture(t, "0", func(c []string) { c[20] = "bad" })
	b, err := os.ReadFile(r.BodyPath)
	if err != nil {
		t.Fatal(err)
	}
	_, cover, _ := bytes.Cut(b, []byte{'\n'})
	b = append(b, '\n')
	b = append(b, cover...)
	h, _ := os.ReadFile(r.HeadersPath)
	h = bytes.Replace(h, []byte(fmt.Sprintf("Content-Length: %d", len(b)-1-len(cover))), []byte(fmt.Sprintf("Content-Length: %d", len(b))), 1)
	write(t, r.BodyPath, b)
	r.BodySHA256 = digest(b)
	write(t, r.HeadersPath, h)
	r.HeadersSHA256 = digest(h)
	r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("0"), nil)}
	a, err := CompareTotalReceipts(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if a.Comparisons[0].Comparable || !slices.Contains(a.CoverBlockers, "multiple_financial_covers") {
		t.Fatal("invalid unrelated field concealed multiple covers")
	}
}

func TestPartialResponseCannotQualifyReportField(t *testing.T) {
	r := fixture(t, "0", nil)
	b, _ := os.ReadFile(r.BodyPath)
	b = append(b, '\n')
	write(t, r.BodyPath, b)
	r.BodySHA256 = digest(b)
	h := []byte(fmt.Sprintf("HTTP/2 206 \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\nContent-Range: bytes 0-%d/%d\r\n\r\n", len(b), len(b)-1, len(b)+1000))
	write(t, r.HeadersPath, h)
	r.HeadersSHA256 = digest(h)
	r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("0"), nil)}
	a, err := CompareTotalReceipts(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if a.Comparisons[0].Comparable || !slices.Contains(a.CoverBlockers, "incomplete_document") {
		t.Fatal("prefix qualified whole report")
	}
}
