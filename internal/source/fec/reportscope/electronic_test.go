package reportscope

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
)

func electronicFixture(f3 bool) []string {
	width, form, report, start, end := 123, "F3XN", 10, 14, 15
	if f3 {
		width, form, report, start, end = 93, "F3N", 12, 16, 17
	}
	f := make([]string, width)
	f[0], f[1], f[report-1], f[start-1], f[end-1] = form, "C12345678", "Q1", "20240101", "20240331"
	return []string{"HDR\x1cFEC\x1c8.4\x1cTEST\x1c1\x1c\x1c", strings.Join(f, "\x1c")}
}

func electronicRequest(t *testing.T, rows []string, prefix bool) Request {
	r := requestFor(t, []byte(strings.Join(rows, "\n")+"\n"), prefix)
	r.SourceURL = "https://docquery.fec.gov/dcdev/posted/101.fec"
	return r
}

func TestElectronicLayoutsAndLosslessFraming(t *testing.T) {
	for _, f3 := range []bool{false, true} {
		rows := electronicFixture(f3)
		seq := 31
		if f3 {
			seq = 34
		}
		setField(rows, 1, seq, "-0.01")
		if !f3 {
			setField(rows, 1, 75, "2024")
		}
		r := electronicRequest(t, rows, false)
		a, err := AssessElectronic(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		count := 100
		if f3 {
			count = 70
		}
		if a.Disposition != "electronic_cover_parsed" || len(a.Cover.Amounts) != count || len(a.Header) != 7 || a.SchemaSHA256 != ElectronicSchemaSHA256 || len(a.PeriodFields) != 7 {
			t.Fatalf("unexpected layout %+v", a)
		}
		if a.PeriodFields[1].Amounts[0].MinorUnits != "-1" {
			t.Fatal("lost signed cents")
		}
		var rebuilt []byte
		for _, rec := range a.Records {
			if rec.Offset != len(rebuilt) || rec.Bytes != len(rec.Raw) || rec.SHA256 != digest(rec.Raw) {
				t.Fatal("lost locator")
			}
			rebuilt = append(rebuilt, rec.Raw...)
		}
		if digest(rebuilt) != r.BodySHA256 || a.FinancialSelectionReady || a.HistoryComplete {
			t.Fatal("lost bytes or promoted readiness")
		}
		b, err := AssessElectronic(context.Background(), r)
		if err != nil || !bytes.Equal(jsonBytes(a), jsonBytes(b)) {
			t.Fatal("unstable replay", err)
		}
	}
}

func TestElectronicCounterexamples(t *testing.T) {
	for _, tc := range []struct {
		name, issue string
		mutate      func([]string) []string
	}{
		{"version", "unsupported_electronic_header", func(r []string) []string { setField(r, 0, 3, "8.5"); return r }},
		{"header_short", "unsupported_electronic_header", func(r []string) []string { r[0] = strings.TrimSuffix(r[0], "\x1c"); return r }},
		{"width", "electronic_cover_width_mismatch", func(r []string) []string { r[1] += "\x1c"; return r }},
		{"short", "electronic_cover_width_mismatch", func(r []string) []string { r[1] = strings.TrimSuffix(r[1], "\x1c"); return r }},
		{"text", "unsupported_or_incomplete_electronic_cover", func(r []string) []string { setField(r, 1, 3, "\xff"); return r }},
		{"form", "unsupported_or_incomplete_electronic_cover", func(r []string) []string { setField(r, 1, 1, "F3PN"); return r }},
		{"date", "invalid_cover_identity_or_period", func(r []string) []string { setField(r, 1, 14, "20240132"); return r }},
		{"duplicate", "multiple_financial_covers", func(r []string) []string { setField(r, 1, 31, "?"); return append(r, "F3A\x1c\xff") }},
		{"other_form", "additional_unqualified_form_or_header", func(r []string) []string { return append(r, "F3PN\x1c\xff") }},
		{"other_header", "additional_unqualified_form_or_header", func(r []string) []string { return append(r, r[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := AssessElectronic(context.Background(), electronicRequest(t, tc.mutate(electronicFixture(false)), false))
			if err != nil || !slices.Contains(a.Issues, tc.issue) {
				t.Fatal("unsafe interpretation", a.Issues, err)
			}
		})
	}
}

func TestElectronicBlankZeroAndPartialAreDistinct(t *testing.T) {
	rows := electronicFixture(false)
	rows[0] += "\x1coptional comment"
	setField(rows, 1, 30, "0")
	setField(rows, 1, 32, "0.001")
	a, err := AssessElectronic(context.Background(), electronicRequest(t, rows, true))
	if err != nil {
		t.Fatal(err)
	}
	if a.Cover == nil || len(a.Header) != 8 || a.CaptureExtent != "prefix" || a.Disposition != "unresolved" {
		t.Fatal("lost prefix cover")
	}
	if a.PeriodFields[0].Amounts[0].State != "valid" || a.PeriodFields[0].Amounts[0].MinorUnits != "0" || a.PeriodFields[1].Amounts[0].State != "blank" || a.PeriodFields[2].Amounts[0].State != "invalid" {
		t.Fatal("collapsed field states")
	}
	r := electronicRequest(t, rows, false)
	r.MetadataCaptures = []string{"not-accepted"}
	if _, err := AssessElectronic(context.Background(), r); err == nil {
		t.Fatal("accepted metadata")
	}
	r.MetadataCaptures = nil
	r.BodySHA256 = strings.Repeat("0", 64)
	if _, err := AssessElectronic(context.Background(), r); err == nil {
		t.Fatal("accepted changed bytes")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := AssessElectronic(ctx, r); err == nil {
		t.Fatal("ignored cancellation")
	}
}
