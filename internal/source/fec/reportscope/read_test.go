package reportscope

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func paperFixture() []string {
	cover := make([]string, 125)
	cover[0], cover[1], cover[9], cover[12], cover[13] = "F3XA", "C12345678", "M10", "20240901", "20240930"
	sc1 := make([]string, 47)
	sc1[0], sc1[1], sc1[2], sc1[8] = "SC1", cover[1], "EXAMPLE BANK", "15.00"
	return []string{"HDR\x1cP3.4\x1cTEST\x1c1234\x1c\x1c\x1c", strings.Join(cover, "\x1c"), strings.Join(sc1, "\x1c")}
}

func setField(rows []string, row, seq int, v string) {
	f := strings.Split(rows[row], "\x1c")
	f[seq-1] = v
	rows[row] = strings.Join(f, "\x1c")
}

func write(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}
func jsonBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func requestFor(t *testing.T, body []byte, prefix bool) Request {
	t.Helper()
	dir := t.TempDir()
	status, extra := "200", ""
	if prefix {
		status = "206"
		extra = fmt.Sprintf("Content-Range: bytes 0-%d/%d\r\n", len(body)-1, len(body)+1000)
	}
	h := []byte(fmt.Sprintf("HTTP/2 %s \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n%s\r\n", status, len(body), extra))
	r := Request{SourceURL: "https://docquery.fec.gov/paper/posted/101.fec", BodyPath: filepath.Join(dir, "body.fec"), HeadersPath: filepath.Join(dir, "body.headers"), BodySHA256: digest(body), HeadersSHA256: digest(h)}
	write(t, r.BodyPath, body)
	write(t, r.HeadersPath, h)
	return r
}

func TestPaperScopeAndRawConservation(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n"} {
		for _, trailing := range []bool{false, true} {
			body := []byte(strings.Join(paperFixture(), ending))
			if trailing {
				body = append(body, ending...)
			}
			r := requestFor(t, body, false)
			a, err := Assess(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			if a.Disposition != "supplemental_attachment_shape" || a.Cover.AmountPresence != "all_blank" || len(a.Cover.Amounts) != 100 || a.SchemaSHA256 != PaperSchemaSHA256 {
				t.Fatalf("wrong scope: %+v", a)
			}
			var rebuilt []byte
			for i, row := range a.Records {
				if row.Offset != len(rebuilt) || row.Ordinal != i+1 || row.Bytes != len(row.Raw) || row.SHA256 != digest(row.Raw) || !row.Complete {
					t.Fatal("lost record locator")
				}
				rebuilt = append(rebuilt, row.Raw...)
			}
			if !bytes.Equal(rebuilt, body) || a.HistoryComplete || a.FinancialSelectionReady || a.OriginalImageVerified {
				t.Fatal("lost raw evidence or promoted readiness")
			}
			for _, f := range a.Cover.Amounts {
				if f.Raw != "" || f.State != "blank" || f.MinorUnits != "" {
					t.Fatal("invented a zero")
				}
			}
			again, err := Assess(context.Background(), r)
			if err != nil || !bytes.Equal(jsonBytes(a), jsonBytes(again)) {
				t.Fatal("non-deterministic replay", err)
			}
		}
	}
}

func TestScopeCounterexamples(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]string) []string
		want   string
	}{
		{"explicit_zero", func(r []string) []string { setField(r, 1, 21, "0.00"); return r }, "financial_cover_present"},
		{"negative", func(r []string) []string { setField(r, 1, 22, "-15.25"); return r }, "financial_cover_present"},
		{"nonmoney_fields", func(r []string) []string { setField(r, 1, 28, "X"); setField(r, 1, 73, "2024"); return r }, "supplemental_attachment_shape"},
		{"bad_money", func(r []string) []string { setField(r, 1, 21, "?"); return r }, "unresolved"},
		{"sub_cent", func(r []string) []string { setField(r, 1, 122, "0.001"); return r }, "unresolved"},
		{"short_cover", func(r []string) []string { r[1] = strings.TrimSuffix(r[1], "\x1c"); return r }, "unresolved"},
		{"extra_cover", func(r []string) []string { r[1] += "\x1c"; return r }, "unresolved"},
		{"short_sc1", func(r []string) []string { r[2] = strings.TrimSuffix(r[2], "\x1c"); return r }, "unresolved"},
		{"empty_loan", func(r []string) []string { setField(r, 2, 9, ""); return r }, "unresolved"},
		{"bad_loan", func(r []string) []string { setField(r, 2, 9, "1e3"); return r }, "unresolved"},
		{"wrong_filer", func(r []string) []string { setField(r, 2, 2, "C87654321"); return r }, "unresolved"},
		{"invalid_period", func(r []string) []string { setField(r, 1, 13, "20240931"); return r }, "unresolved"},
		{"reverse_period", func(r []string) []string { setField(r, 1, 13, "20241001"); return r }, "unresolved"},
		{"mixed_schedules", func(r []string) []string { return append(r, "SA11AI\x1cUNKNOWN") }, "unresolved"},
		{"duplicate_cover", func(r []string) []string { setField(r, 1, 21, "0"); return append(r, r[1]) }, "unresolved"},
		{"no_supplement", func(r []string) []string { return r[:2] }, "unresolved"},
		{"unknown_header", func(r []string) []string { r[0] = "HDR\x1cFEC\x1c8.4"; return r }, "unresolved"},
		{"nonempty_header_extra", func(r []string) []string { r[0] += "NOT_UNUSED"; return r }, "unresolved"},
		{"unsupported_byte", func(r []string) []string { setField(r, 1, 3, "\xff"); return r }, "unresolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := requestFor(t, []byte(strings.Join(tc.mutate(paperFixture()), "\n")), false)
			a, err := Assess(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			if a.Disposition != tc.want || a.HistoryComplete || a.FinancialSelectionReady || a.OriginalImageVerified {
				t.Fatalf("unexpected disposition %s", a.Disposition)
			}
			if tc.name == "explicit_zero" && (a.Cover.Amounts[0].MinorUnits != "0" || a.Cover.Amounts[0].State != "valid") {
				t.Fatal("zero collapsed")
			}
		})
	}
}

func TestPrefixCannotEstablishAttachmentAbsence(t *testing.T) {
	for _, trailing := range []string{"", "\n"} {
		r := requestFor(t, []byte(strings.Join(paperFixture(), "\n")+trailing), true)
		a, err := Assess(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		if a.Disposition != "unresolved" || a.CaptureExtent != "prefix" || !slices.Contains(a.Issues, "partial_document_capture") || a.Records[2].Complete != (trailing != "") {
			t.Fatal("prefix treated as complete", a.Disposition)
		}
	}
}

func TestPinnedTransportFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Request)
	}{
		{"body_hash", func(r *Request) { r.BodySHA256 = strings.Repeat("0", 64) }},
		{"header_hash", func(r *Request) { r.HeadersSHA256 = strings.Repeat("0", 64) }},
		{"credentials_url", func(r *Request) { r.SourceURL += "?api_key=DO_NOT_PRINT" }},
		{"unsupported_host", func(r *Request) { r.SourceURL = "https://example.com/paper/posted/101.fec" }},
		{"metadata_budget", func(r *Request) { r.MetadataCaptures = make([]string, 5) }},
		{"wrong_length", func(r *Request) {
			b, _ := os.ReadFile(r.HeadersPath)
			b = bytes.Replace(b, []byte("Content-Length: "), []byte("Content-Length: 9"), 1)
			write(t, r.HeadersPath, b)
			r.HeadersSHA256 = digest(b)
		}},
		{"bad_headers", func(r *Request) {
			b, _ := os.ReadFile(r.HeadersPath)
			b = append(b, []byte("HTTP/2 200\r\n\r\n")...)
			write(t, r.HeadersPath, b)
			r.HeadersSHA256 = digest(b)
		}},
		{"body_budget", func(r *Request) {
			b := bytes.Repeat([]byte{'X'}, MaxBodyBytes+1)
			write(t, r.BodyPath, b)
			r.BodySHA256 = digest(b)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := requestFor(t, []byte(strings.Join(paperFixture(), "\n")), false)
			tc.mutate(&r)
			_, err := Assess(context.Background(), r)
			if err == nil || strings.Contains(err.Error(), "DO_NOT_PRINT") {
				t.Fatal("unsafe acceptance/error", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Assess(ctx, Request{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestResponseAndRecordBudgets(t *testing.T) {
	for name, change := range map[string]func(string) string{
		"http_error":       func(h string) string { return strings.Replace(h, "HTTP/2 200", "HTTP/2 404", 1) },
		"duplicate_length": func(h string) string { return strings.Replace(h, "\r\n\r\n", "\r\nContent-Length: 1\r\n\r\n", 1) },
		"encoding":         func(h string) string { return strings.Replace(h, "\r\n\r\n", "\r\nContent-Encoding: gzip\r\n\r\n", 1) },
		"bad_date":         func(h string) string { return strings.Replace(h, "Thu, 10 Sep 2026 07:00:00 GMT", "invalid", 1) },
		"range_on_200": func(h string) string {
			return strings.Replace(h, "\r\n\r\n", "\r\nContent-Range: bytes 0-1/3\r\n\r\n", 1)
		},
		"missing_range": func(h string) string { return strings.Replace(h, "HTTP/2 200", "HTTP/2 206", 1) },
	} {
		t.Run(name, func(t *testing.T) {
			r := requestFor(t, []byte(strings.Join(paperFixture(), "\n")), false)
			h, _ := os.ReadFile(r.HeadersPath)
			h = []byte(change(string(h)))
			write(t, r.HeadersPath, h)
			r.HeadersSHA256 = digest(h)
			if _, err := Assess(context.Background(), r); err == nil {
				t.Fatal("invalid transport accepted")
			}
		})
	}
	for _, b := range [][]byte{bytes.Repeat([]byte{'X'}, (64<<10)+1), bytes.Repeat([]byte{'\n'}, MaxRecords+1)} {
		if _, err := Assess(context.Background(), requestFor(t, b, false)); err == nil {
			t.Fatal("record budget not enforced")
		}
	}
}
