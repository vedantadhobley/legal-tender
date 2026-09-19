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

func unitemizedFixture(t *testing.T, itemized, unitemized, total string, changes map[string]any) UnitemizedReview {
	t.Helper()
	r := fixture(t, "1", func(c []string) { c[28] = itemized; c[29] = unitemized; c[30] = total; c[79] = "999999.99" })
	r.MetadataCaptures = []string{metadata(t, "/v1/reports/pac-party/", nil, changes)}
	out, err := ReviewUnitemized(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestUnitemizedExplicitAmountAndSubtotalAreIndependent(t *testing.T) {
	a := unitemizedFixture(t, "100", "25.00", "125", map[string]any{
		"individual_itemized_contributions_period": "100", "individual_unitemized_contributions_period": json.Number("25.00"), "total_individual_contributions_period": "125.00",
	})
	if a.Cover.Unitemized.MinorUnits == nil || *a.Cover.Unitemized.MinorUnits != "2500" || a.Cover.SubtotalState != "balanced" || a.Metadata[0].Summary.SubtotalState != "balanced" || !a.Metadata[0].Comparison.Comparable || *a.Metadata[0].Comparison.DeltaMinorUnits != "0" {
		t.Fatalf("lost reported positive amount: %+v", a)
	}
	if string(a.Metadata[0].Summary.Unitemized.Raw) != "25.00" || string(a.Cover.Unitemized.Raw) != `"25.00"` {
		t.Fatal("collapsed numeric and string source types")
	}
	if a.FinancialComponentEligible || a.CycleComparisonReady || a.TerminalAttributionEligible || a.DonorComposition != "not_identified_by_summary" || a.AmountMethod != "explicit_reported_field_only" {
		t.Fatal("promoted money or identity")
	}
	b := unitemizedFixture(t, "999", "25.00", "1", map[string]any{
		"individual_itemized_contributions_period": "-100", "individual_unitemized_contributions_period": "25.00", "total_individual_contributions_period": "0",
	})
	if *b.Cover.Unitemized.MinorUnits != "2500" || b.Cover.SubtotalState != "mismatch" || b.Metadata[0].Summary.SubtotalState != "mismatch" || !b.Metadata[0].Comparison.Comparable || *b.Metadata[0].Comparison.DeltaMinorUnits != "0" {
		t.Fatal("subtotal replaced or erased explicit unitemized amount")
	}
	if *b.Cover.SubtotalDeltaMinorUnits != "-102300" || *b.Metadata[0].Summary.SubtotalDeltaMinorUnits != "7500" {
		t.Fatal("wrong subtotal equation")
	}
}

func TestUnitemizedStatesNeverBecomeAResidual(t *testing.T) {
	for _, tc := range []struct {
		name, paper string
		api         any
		want        string
		ready       bool
	}{
		{"zero", "0.00", "0.00", "valid", true},
		{"negative", "-5.00", json.Number("-5.00"), "valid", true},
		{"blank", "", "", "blank", false},
		{"null", "0.00", nil, "source_null", false},
		{"invalid", "bad", "bad", "invalid", false},
		{"fraction", "0.001", json.Number("0.001"), "invalid", false},
		{"whitespace", "0.00", " 0.00", "invalid", false},
		{"exponent", "0.00", json.Number("1e2"), "invalid", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := unitemizedFixture(t, "100", tc.paper, "1000", map[string]any{"individual_itemized_contributions_period": "100", "individual_unitemized_contributions_period": tc.api, "total_individual_contributions_period": "1000"})
			m := a.Metadata[0]
			if m.Summary.Unitemized.State != tc.want || m.Comparison.Comparable != tc.ready {
				t.Fatal("bad observed state", m)
			}
			if !tc.ready && (m.Comparison.DeltaMinorUnits != nil || m.Summary.Unitemized.MinorUnits != nil) {
				t.Fatal("invented amount/delta")
			}
			if a.Cover.Unitemized.State != "valid" && (a.Cover.Unitemized.MinorUnits != nil || a.Cover.SubtotalDeltaMinorUnits != nil) {
				t.Fatal("filled cover gap")
			}
		})
	}
}

func TestUnitemizedEndpointAbsenceIsNotNull(t *testing.T) {
	r := fixture(t, "0", func(c []string) { c[29] = "0.00" })
	r.MetadataCaptures = []string{metadata(t, "/v1/filings/", json.Number("10000"), nil), metadata(t, "/v1/reports/house-senate/", json.Number("10000"), map[string]any{"individual_unitemized_contributions_period": "10000"})}
	a, err := ReviewUnitemized(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"not_supplied_by_endpoint", "unsupported_endpoint"} {
		m := a.Metadata[i]
		if m.Availability != want || m.Summary != nil || m.Comparison.Comparable || m.Comparison.DeltaMinorUnits != nil {
			t.Fatal("invented endpoint mapping", m)
		}
	}
	if *a.Cover.Unitemized.MinorUnits != "0" || a.Cover.SubtotalState != "unavailable" {
		t.Fatal("zero needs no donor/detail inference")
	}
	r.MetadataCaptures = nil
	a, err = ReviewUnitemized(context.Background(), r)
	if err != nil || len(a.Metadata) != 0 || *a.Cover.Unitemized.MinorUnits != "0" {
		t.Fatal("missing metadata erased cover")
	}
}

func TestUnitemizedScopeMismatchKeepsEachAssertion(t *testing.T) {
	for _, tc := range []struct {
		field string
		value any
		block string
	}{
		{"committee_id", "C87654321", "metadata_scope:committee_id"},
		{"coverage_end_date", "2024-09-30", "metadata_scope:coverage_end_date"},
		{"coverage_start_date", nil, "metadata_scope:coverage_start_date"},
		{"coverage_end_date", "2024-06-30T12:00:00", "metadata_scope:coverage_end_date"},
		{"coverage_end_date", "2024-03-31", "metadata_scope:reversed_period"},
		{"report_form", "Form 3", "metadata_scope:report_form"},
		{"report_type", nil, "metadata_scope:report_type"},
		{"means_filed", "electronic", "metadata_scope:means_filed"},
		{"amendment_indicator", "A", "metadata_scope:amendment_indicator"},
		{"fec_url", "https://docquery.fec.gov/paper/posted/102.fec", "metadata_scope:fec_url"},
	} {
		t.Run(tc.field+tc.block, func(t *testing.T) {
			changes := map[string]any{"individual_unitemized_contributions_period": "25", tc.field: tc.value}
			a := unitemizedFixture(t, "100", "25", "125", changes)
			m := a.Metadata[0]
			if m.Comparison.Comparable || m.Comparison.DeltaMinorUnits != nil || !slices.Contains(m.Comparison.Blockers, tc.block) || *m.Summary.Unitemized.MinorUnits != "2500" {
				t.Fatal("lost scope or independent value", m)
			}
		})
	}
}

func TestUnitemizedCoverScopeDespiteUnrelatedInvalidAmount(t *testing.T) {
	for _, kind := range []string{"complete", "multiple_covers", "partial"} {
		t.Run(kind, func(t *testing.T) {
			r := fixture(t, "bad", func(c []string) { c[29] = "0.00" })
			b, err := os.ReadFile(r.BodyPath)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "multiple_covers" {
				_, cover, _ := bytes.Cut(b, []byte{'\n'})
				b = append(append(b, '\n'), cover...)
			}
			b = append(b, '\n')
			h := fmt.Sprintf("HTTP/2 200 \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(b))
			block := "multiple_financial_covers"
			if kind == "partial" {
				h = fmt.Sprintf("HTTP/2 206 \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\nContent-Range: bytes 0-%d/%d\r\n\r\n", len(b), len(b)-1, len(b)+1000)
				block = "incomplete_document"
			}
			write(t, r.BodyPath, b)
			r.BodySHA256 = digest(b)
			write(t, r.HeadersPath, []byte(h))
			r.HeadersSHA256 = digest([]byte(h))
			r.MetadataCaptures = []string{metadata(t, "/v1/reports/pac-party/", nil, map[string]any{"individual_unitemized_contributions_period": "0.00"})}
			a, err := ReviewUnitemized(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			if *a.Cover.Unitemized.MinorUnits != "0" || !slices.Contains(a.Evidence.Issues, "invalid_cover_amount") {
				t.Fatal("lost independent fields")
			}
			if kind == "complete" {
				if !a.Metadata[0].Comparison.Comparable || *a.Metadata[0].Comparison.DeltaMinorUnits != "0" {
					t.Fatal("unrelated invalid amount blocked comparison")
				}
			} else if a.Metadata[0].Comparison.Comparable || !slices.Contains(a.CoverScopeBlockers, block) {
				t.Fatal("invalid document scope qualified a pair")
			}
		})
	}
}

func TestUnitemizedRechecksSourcesAndReplays(t *testing.T) {
	r := fixture(t, "0", func(c []string) { c[28] = "100"; c[29] = "25"; c[30] = "125" })
	r.MetadataCaptures = []string{
		metadata(t, "/v1/reports/pac-party/", nil, map[string]any{"individual_unitemized_contributions_period": "25", "is_amended": true}),
		metadata(t, "/v1/reports/pac-party/", nil, map[string]any{"individual_unitemized_contributions_period": "30", "is_amended": false}),
	}
	a, err := ReviewUnitemized(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"0", "500"} {
		if !a.Metadata[i].Comparison.Comparable || *a.Metadata[i].Comparison.DeltaMinorUnits != want {
			t.Fatal("chose an endpoint winner")
		}
	}
	for i := 0; i < 8; i++ {
		again, err := ReviewUnitemized(context.Background(), r)
		if err != nil || !bytes.Equal(jsonBytes(a), jsonBytes(again)) {
			t.Fatal("unstable replay", err)
		}
	}
	write(t, filepath.Join(filepath.Dir(r.MetadataCaptures[0]), "body.json"), []byte("{}"))
	if _, err := ReviewUnitemized(context.Background(), r); err == nil {
		t.Fatal("trusted modified metadata")
	}
	r.MetadataCaptures = nil
	write(t, r.BodyPath, []byte("changed"))
	if _, err := ReviewUnitemized(context.Background(), r); err == nil {
		t.Fatal("trusted modified body")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReviewUnitemized(ctx, r); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestUnitemizedUnsupportedCoverDoesNotEraseMetadata(t *testing.T) {
	r := fixture(t, "0", nil)
	b, err := os.ReadFile(r.BodyPath)
	if err != nil {
		t.Fatal(err)
	}
	b = bytes.Replace(b, []byte("P3.4"), []byte("P0.0"), 1)
	write(t, r.BodyPath, b)
	r.BodySHA256 = digest(b)
	r.MetadataCaptures = []string{metadata(t, "/v1/reports/pac-party/", nil, map[string]any{"individual_unitemized_contributions_period": "2256923.61"})}
	a, err := ReviewUnitemized(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	m := a.Metadata[0]
	if a.Cover != nil || a.CoverPeriod != nil || m.Period == nil || m.Period.Start != "2024-04-01" || *m.Summary.Unitemized.MinorUnits != "225692361" || m.Comparison.Comparable {
		t.Fatal("borrowed an unsupported cover or erased metadata")
	}
}
