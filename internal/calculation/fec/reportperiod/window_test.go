package reportperiod

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

func windowFixture(t *testing.T, change func(int, []string, map[string]any)) WindowRequest {
	t.Helper()
	dir := t.TempDir()
	set := DocumentSet{Version: DocumentSetVersion, Documents: []DocumentReference{}}
	rows := []map[string]any{}
	for i, dates := range [][2]string{{"2024-01-01", "2024-01-31"}, {"2024-02-01", "2024-02-29"}} {
		id := 101 + i
		raw := row(id, dates[0], dates[1], fmt.Sprint(id))
		raw["amendment_indicator"] = "N"
		f := make([]string, 123)
		f[0], f[1], f[9], f[13], f[14] = "F3XN", "C12345678", "Q1", strings.ReplaceAll(dates[0], "-", ""), strings.ReplaceAll(dates[1], "-", "")
		values := []string{"100", "20", "120", "150", "50", "10", "110"}
		if i == 1 {
			values = []string{"5", "0", "5", "20", "40", "110", "90"}
		}
		for j, seqs := range [][]int{{30}, {31}, {32}, {24, 45}, {26, 66}, {23}, {27}} {
			for _, seq := range seqs {
				f[seq-1] = values[j]
			}
			raw[windowFields[j]] = json.Number(values[j])
		}
		if change != nil {
			change(i, f, raw)
		}
		body := []byte("HDR\x1cFEC\x1c8.4\x1cTEST\x1c1\x1c\x1c\n" + strings.Join(f, "\x1c") + "\n")
		headers := []byte(fmt.Sprintf("HTTP/2 200 \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(body)))
		b, h := fmt.Sprintf("%d.fec", id), fmt.Sprintf("%d.headers", id)
		write(t, filepath.Join(dir, b), body)
		write(t, filepath.Join(dir, h), headers)
		set.Documents = append(set.Documents, DocumentReference{fmt.Sprintf("https://docquery.fec.gov/dcdev/posted/%d.fec", id), reportmetadata.Artifact{Path: b, SHA256: hash(body), Bytes: int64(len(body))}, reportmetadata.Artifact{Path: h, SHA256: hash(headers), Bytes: int64(len(headers))}})
		rows = append(rows, raw)
	}
	path := filepath.Join(dir, "documents.json")
	write(t, path, marshal(set))
	return WindowRequest{Request{capture(t, false, rows...), "2024-01-01", "2024-02-29"}, path}
}

func alterDocuments(t *testing.T, path string, change func(*DocumentSet)) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var set DocumentSet
	if err := json.Unmarshal(b, &set); err != nil {
		t.Fatal(err)
	}
	change(&set)
	write(t, path, marshal(set))
}

func reviewWindow(t *testing.T, request WindowRequest) WindowReview {
	t.Helper()
	r, err := ReviewWindow(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if r.FinancialCycleTotalReady || r.CashBasisReady || r.TerminalAttributionEligible {
		t.Fatal("promoted funding use")
	}
	for _, f := range r.Fields {
		if f.Coverage.CoveredDays+f.Coverage.GapDays != f.Coverage.WindowDays {
			t.Fatal("nonconserving coverage")
		}
	}
	return r
}

func TestWindowSumsFlowsAndSelectsCashBoundaries(t *testing.T) {
	request := windowFixture(t, nil)
	r := reviewWindow(t, request)
	for i, want := range []string{"10500", "2000", "12500", "17000", "9000", "1000", "9000"} {
		f := r.Fields[i]
		if !f.ReportedWindowReady || f.WindowValueMinorUnits == nil || *f.WindowValueMinorUnits != want || f.Coverage.CoveredDays != 60 || len(f.MemberBindingIndexes) != 2 {
			t.Fatal("wrong aggregate", f)
		}
		if i >= 5 && f.ObservedSumMinorUnits != nil {
			t.Fatal("summed cash stocks")
		}
	}
	for _, e := range r.Equations {
		if e.State != "balanced" || *e.ResidualMinorUnits != "0" {
			t.Fatal("wrong equation", e)
		}
	}
	if len(r.Equations) != 7 {
		t.Fatal("missing equation")
	}
	again := reviewWindow(t, request)
	if !bytes.Equal(marshal(r), marshal(again)) {
		t.Fatal("unstable replay")
	}
	alterDocuments(t, request.DocumentsPath, func(s *DocumentSet) { slices.Reverse(s.Documents) })
	other := reviewWindow(t, request)
	for i, f := range other.Fields {
		if *f.WindowValueMinorUnits != *r.Fields[i].WindowValueMinorUnits {
			t.Fatal("document order changed money")
		}
	}
}

func TestWindowArithmeticDoesNotEraseReportedFields(t *testing.T) {
	for _, cash := range []bool{false, true} {
		r := reviewWindow(t, windowFixture(t, func(i int, f []string, raw map[string]any) {
			if i != 1 {
				return
			}
			if cash {
				f[22] = "100"
				raw[windowFields[5]] = json.Number("100")
			} else {
				f[31] = "6"
				raw[windowFields[2]] = json.Number("6")
			}
		}))
		for _, f := range r.Fields {
			if !f.ReportedWindowReady {
				t.Fatal("equation mismatch contaminated field", f)
			}
		}
		kinds := map[string]bool{}
		for _, e := range r.Equations {
			if e.State == "mismatch" {
				kinds[e.Kind] = true
			}
		}
		if cash && (!kinds["adjacent_cash_carry_forward"] || !kinds["report_cash"]) {
			t.Fatal("hidden cash mismatch")
		}
		if !cash && (!kinds["report_individual_subtotal"] || !kinds["window_individual_subtotal"]) {
			t.Fatal("hidden subtotal mismatch")
		}
	}
}

func TestWindowFieldFailureIsLocal(t *testing.T) {
	r := reviewWindow(t, windowFixture(t, func(i int, f []string, raw map[string]any) {
		if i == 1 {
			f[30] = ""
		}
	}))
	if r.Fields[1].ReportedWindowReady || r.Fields[1].WindowValueMinorUnits != nil || *r.Fields[1].ObservedSumMinorUnits != "2000" || r.Fields[1].Coverage.GapDays != 29 {
		t.Fatal("blank treated as zero", r.Fields[1])
	}
	if len(r.Fields[1].Missing) != 1 || r.Fields[1].Missing[0].ObservationIndex != 1 {
		t.Fatal("lost missing witness")
	}
	for i, f := range r.Fields {
		if i != 1 && !f.ReportedWindowReady {
			t.Fatal("unrelated field blocked", f)
		}
	}
	for _, e := range r.Equations {
		if e.Kind == "window_individual_subtotal" && e.State != "unavailable" {
			t.Fatal("solved missing unitemized")
		}
	}
}

func TestMissingDocumentsAndCoverageNeverInventZero(t *testing.T) {
	for _, empty := range []bool{false, true} {
		request := windowFixture(t, nil)
		alterDocuments(t, request.DocumentsPath, func(s *DocumentSet) {
			if empty {
				s.Documents = []DocumentReference{}
			} else {
				s.Documents = s.Documents[:1]
			}
		})
		r := reviewWindow(t, request)
		for _, f := range r.Fields {
			if f.ReportedWindowReady || f.WindowValueMinorUnits != nil || len(f.Missing) == 0 {
				t.Fatal("incomplete field promoted", f)
			}
			if empty && f.ObservedSumMinorUnits != nil {
				t.Fatal("absence became zero")
			}
		}
		for _, e := range r.Equations {
			if e.Kind == "adjacent_cash_carry_forward" && e.State != "unavailable" {
				t.Fatal("bridged missing cover")
			}
		}
	}
	for _, window := range [][2]string{{"2024-01-01", "2024-03-31"}, {"2024-01-02", "2024-02-29"}} {
		request := windowFixture(t, nil)
		request.Membership.Start, request.Membership.End = window[0], window[1]
		r := reviewWindow(t, request)
		for _, f := range r.Fields {
			if f.ReportedWindowReady || f.WindowValueMinorUnits != nil {
				t.Fatal("gap or boundary ignored", f)
			}
		}
	}
}

func TestOverlapAndSignedAmounts(t *testing.T) {
	request := windowFixture(t, func(i int, f []string, raw map[string]any) {
		if i == 1 {
			f[13] = "20240131"
			raw["coverage_start_date"] = "2024-01-31"
		}
	})
	r := reviewWindow(t, request)
	if r.Fields[3].Coverage.OverlapDays != 1 || r.Fields[3].ReportedWindowReady || *r.Fields[3].ObservedSumMinorUnits != "17000" {
		t.Fatal("overlap hidden", r.Fields[3])
	}
	r = reviewWindow(t, windowFixture(t, func(i int, f []string, raw map[string]any) {
		if i == 1 {
			f[23], f[44] = "-200", "-200"
			raw[windowFields[3]] = json.Number("-200")
		}
	}))
	if !r.Fields[3].ReportedWindowReady || *r.Fields[3].WindowValueMinorUnits != "-5000" {
		t.Fatal("lost signed amount")
	}
}

func TestWindowRejectsAmbiguousOrChangedInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*DocumentSet)
	}{
		{"duplicate", func(s *DocumentSet) { s.Documents = append(s.Documents, s.Documents[0]) }},
		{"count", func(s *DocumentSet) {
			for len(s.Documents) <= MaxWindowDocuments {
				s.Documents = append(s.Documents, s.Documents[0])
			}
		}},
		{"null", func(s *DocumentSet) { s.Documents = nil }},
		{"version", func(s *DocumentSet) { s.Version = "future" }},
		{"size", func(s *DocumentSet) { s.Documents[0].Body.Bytes++ }},
		{"hash", func(s *DocumentSet) { s.Documents[0].Body.SHA256 = strings.Repeat("0", 64) }},
		{"budget", func(s *DocumentSet) { s.Documents[0].Body.Bytes = MaxWindowDocumentBytes + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := windowFixture(t, nil)
			alterDocuments(t, r.DocumentsPath, tc.change)
			if _, err := ReviewWindow(context.Background(), r); err == nil {
				t.Fatal("accepted unsafe input")
			}
		})
	}
	for _, raw := range []string{`{"version":"legal-tender.fec.report-document-set.v1","documents":[],"documents":[]}`, `{"version":"legal-tender.fec.report-document-set.v1","documents":[],"trusted":true}`, `{"version":"\ud800","documents":[]}`} {
		r := windowFixture(t, nil)
		write(t, r.DocumentsPath, []byte(raw))
		if _, err := ReviewWindow(context.Background(), r); err == nil {
			t.Fatal("accepted unsafe JSON")
		}
	}
	r := windowFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReviewWindow(ctx, r); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestWindowAbsenceIsNotExplicitZero(t *testing.T) {
	request := windowFixture(t, func(i int, f []string, raw map[string]any) {
		for _, seq := range []int{30, 31, 32, 24, 45, 26, 66, 23, 27} {
			f[seq-1] = "0"
		}
		for _, name := range windowFields {
			raw[name] = json.Number("0")
		}
	})
	r := reviewWindow(t, request)
	for _, f := range r.Fields {
		if !f.ReportedWindowReady || f.WindowValueMinorUnits == nil || *f.WindowValueMinorUnits != "0" {
			t.Fatal("explicit zero lost", f)
		}
	}
	request.Membership.CapturePath = capture(t, false)
	alterDocuments(t, request.DocumentsPath, func(s *DocumentSet) { s.Documents = []DocumentReference{} })
	r = reviewWindow(t, request)
	for _, f := range r.Fields {
		if f.ReportedWindowReady || f.WindowValueMinorUnits != nil || f.ObservedSumMinorUnits != nil {
			t.Fatal("empty population promoted", f)
		}
	}
}

func TestNarrowWindowIgnoresOutsideScopeFailure(t *testing.T) {
	request := windowFixture(t, func(i int, f []string, raw map[string]any) {
		if i == 1 {
			raw["means_filed"] = "paper"
		}
	})
	request.Membership.End = "2024-01-31"
	r := reviewWindow(t, request)
	for _, f := range r.Fields {
		if !f.ReportedWindowReady || len(f.MemberBindingIndexes) != 1 {
			t.Fatal("outside issue contaminated window", f)
		}
	}
}

func TestWindowCumulativeBudgetPrecedesBodyParsing(t *testing.T) {
	request := windowFixture(t, nil)
	path := filepath.Join(t.TempDir(), "sparse-body")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(4 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	alterDocuments(t, request.DocumentsPath, func(s *DocumentSet) {
		d := s.Documents[0]
		d.Body.Path = path
		d.Body.Bytes = 4 << 20
		s.Documents = []DocumentReference{d, d, d, d}
	})
	if _, err := ReviewWindow(context.Background(), request); err == nil || err.Error() != "document-set artifact budget exceeded" {
		t.Fatal("cumulative budget not enforced", err)
	}
}
