package reportperiod

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

func row(file int, start, end string, ids ...any) map[string]any {
	return map[string]any{"file_number": file, "committee_id": "C12345678", "cycle": 2024,
		"report_form": "Form 3X", "report_type": "Q1", "report_year": 2024, "means_filed": "e-file",
		"coverage_start_date": start, "coverage_end_date": end, "is_amended": false,
		"most_recent": false, "amendment_chain": ids}
}

func marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func write(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func capture(t *testing.T, partial bool, rows ...map[string]any) string {
	t.Helper()
	b, err := os.ReadFile("../../../../contracts/sources/fec/report-metadata/v1/record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]struct {
			Required []string `json:"required"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Defs["PacParty"].Required) == 0 {
		t.Fatal("missing source test schema")
	}
	for _, r := range rows {
		for _, key := range schema.Defs["PacParty"].Required {
			if _, ok := r[key]; !ok {
				r[key] = nil
			}
		}
	}
	count := len(rows)
	if partial {
		count++
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	body := marshal(map[string]any{"api_version": "1.0", "pagination": reportmetadata.Pagination{Count: int64(count), IsCountExact: !partial, Page: 1, Pages: 1, PerPage: 100}, "results": rows})
	headers := []byte(fmt.Sprintf("HTTP/2 200 \r\nContent-Type: application/json\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(body)))
	dir := t.TempDir()
	write(t, filepath.Join(dir, "body.json"), body)
	write(t, filepath.Join(dir, "headers"), headers)
	c := reportmetadata.Capture{Contract: reportmetadata.Contract, SchemaSHA256: reportmetadata.SwaggerSHA256, Endpoint: "/v1/reports/pac-party/",
		Query: reportmetadata.Query{CommitteeID: "C12345678", Cycle: 2024, PerPage: 100},
		Pages: []reportmetadata.PageCapture{{Page: 1, ObservedAt: "2026-09-10T07:00:00Z", TimeBasis: "http_date",
			Body: reportmetadata.Artifact{Path: "body.json", SHA256: hash(body), Bytes: int64(len(body))}, Headers: reportmetadata.Artifact{Path: "headers", SHA256: hash(headers), Bytes: int64(len(headers))}}}}
	path := filepath.Join(dir, "capture.json")
	write(t, path, marshal(c))
	return path
}

func inspect(t *testing.T, path, start, end string) Review {
	t.Helper()
	r, err := Inspect(context.Background(), Request{path, start, end})
	if err != nil {
		t.Fatal(err)
	}
	if r.FinancialMembershipReady || r.CycleTotalReady || r.Evidence.HistoryComplete {
		t.Fatal("promoted financial coverage")
	}
	if r.Evidence.Rows != len(r.Observations) {
		t.Fatal("lost source membership")
	}
	seen := append([]int{}, r.UngroupedIndexes...)
	for _, c := range r.Cohorts {
		seen = append(seen, c.ObservationIndexes...)
	}
	slices.Sort(seen)
	if len(seen) != len(r.Observations) {
		t.Fatal("lost cohort membership")
	}
	for i, index := range seen {
		if i != index {
			t.Fatal("duplicate/missing cohort member")
		}
	}
	for _, c := range []Coverage{r.PublisherCoverage, r.ChainCoverage} {
		if c.CoveredDays+c.GapDays != c.WindowDays || c.OverlapDays > c.CoveredDays {
			t.Fatal("nonconserving timeline")
		}
	}
	return r
}

func TestExplicitChainAndAdjacentLeapYearCoverage(t *testing.T) {
	original := row(901, "2024-01-01", "2024-01-31", "901")
	original["is_amended"] = true
	// A lower numeric file ID is intentionally the asserted successor. Neither
	// sorting file IDs nor most_recent (false here) is the membership policy.
	amendment := row(102, "2024-01-01T00:00:00", "2024-01-31T00:00:00", json.Number("901.0"), "102")
	last := row(103, "2024-02-01", "2024-02-29", "103")
	last["report_type"] = "TER" // No inferred zero periods after termination.
	path := capture(t, false, original, amendment, last)
	r := inspect(t, path, "2024-01-01", "2024-02-29")
	if !r.ObservedPartitionReady || r.ChainCoverage.CoveredDays != 60 || len(r.Cohorts) != 2 || !slices.Equal(r.ChainCandidateIndexes, []int{1, 2}) {
		t.Fatal("lost positive structural partition", r)
	}
	for i := 0; i < 5; i++ {
		if !bytes.Equal(marshal(r), marshal(inspect(t, path, "2024-01-01", "2024-02-29"))) {
			t.Fatal("unstable replay")
		}
	}
	r = inspect(t, path, "2024-01-01", "2024-03-31")
	if r.ObservedPartitionReady || r.ChainCoverage.GapDays != 31 {
		t.Fatal("termination inferred future coverage")
	}
}

func TestUnsafeCohortsDoNotSelectFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		change        func(map[string]any, map[string]any)
	}{
		{"paper", "non_electronic_replacement_unqualified", func(a, b map[string]any) { b["means_filed"] = "paper"; b["amendment_chain"] = nil }},
		{"unknown", "unknown_amended_status", func(a, b map[string]any) { a["is_amended"] = nil }},
		{"two_members", "require_one_publisher_member", func(a, b map[string]any) { a["is_amended"] = false }},
		{"no_member", "require_one_publisher_member", func(a, b map[string]any) { b["is_amended"] = true }},
		{"missing_predecessor", "chain_reference_outside_cohort", func(a, b map[string]any) { b["amendment_chain"] = []any{"999", "102"} }},
		{"omitted_version", "chain_membership_not_closed", func(a, b map[string]any) { b["amendment_chain"] = []any{"102"} }},
		{"contradictory_prefix", "inconsistent_chain_prefix", func(a, b map[string]any) { a["amendment_chain"] = []any{"999", "101"} }},
		{"repeated_id", "invalid_or_missing_chain", func(a, b map[string]any) { b["amendment_chain"] = []any{"101", "101", "102"} }},
		{"wrong_tail", "invalid_or_missing_chain", func(a, b map[string]any) { b["amendment_chain"] = []any{"102", "101"} }},
		{"negative_ref", "invalid_or_missing_chain", func(a, b map[string]any) { b["amendment_chain"] = []any{-101, 102} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := row(101, "2024-01-01", "2024-01-31", "101"), row(102, "2024-01-01", "2024-01-31", "101", "102")
			a["is_amended"] = true
			tc.change(a, b)
			r := inspect(t, capture(t, false, a, b), "2024-01-01", "2024-01-31")
			if r.ObservedPartitionReady || len(r.ChainCandidateIndexes) != 0 || !slices.Contains(r.Cohorts[0].Blockers, tc.blocker) {
				t.Fatal("unsafe chain selected", r.Cohorts)
			}
		})
	}
}

func TestCoverageOverlapsGapsAndBoundaryAmounts(t *testing.T) {
	for _, tc := range []struct {
		name, aStart, aEnd, bStart, bEnd string
		gap, overlap                     int64
		crossing                         bool
	}{
		{"shared_day", "2024-01-01", "2024-01-15", "2024-01-15", "2024-01-31", 0, 1, false},
		{"nested", "2024-01-01", "2024-01-31", "2024-01-10", "2024-01-20", 0, 11, false},
		{"gap", "2024-01-01", "2024-01-15", "2024-01-17", "2024-01-31", 1, 0, false},
		{"boundary", "2023-12-01", "2024-01-15", "2024-01-16", "2024-02-01", 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := row(101, tc.aStart, tc.aEnd, "101"), row(102, tc.bStart, tc.bEnd, "102")
			r := inspect(t, capture(t, false, a, b), "2024-01-01", "2024-01-31")
			if r.ObservedPartitionReady || r.ChainCoverage.GapDays != tc.gap || r.ChainCoverage.OverlapDays != tc.overlap || (len(r.ChainCoverage.CrossBoundaryIndexes) > 0) != tc.crossing {
				t.Fatal("wrong coverage", r.ChainCoverage)
			}
		})
	}
}

func TestScopeAndPartialCaptureStayExplicit(t *testing.T) {
	for _, tc := range []struct {
		field string
		value any
	}{
		{"coverage_start_date", nil}, {"coverage_start_date", "2024-02-01"}, {"coverage_end_date", "2024-01-31T01:00:00"},
		{"report_form", "Form 3P"}, {"report_year", nil}, {"report_type", nil},
	} {
		t.Run(tc.field+fmt.Sprint(tc.value), func(t *testing.T) {
			a := row(101, "2024-01-01", "2024-01-31", "101")
			a[tc.field] = tc.value
			r := inspect(t, capture(t, false, a), "2024-01-01", "2024-01-31")
			if r.ObservedPartitionReady || len(r.UngroupedIndexes) != 1 {
				t.Fatal("invalid scope qualified")
			}
		})
	}
	a := row(101, "2024-01-01", "2024-01-31", "101")
	r := inspect(t, capture(t, true, a), "2024-01-01", "2024-01-31")
	if r.ObservedPartitionReady || r.ChainCoverage.GapDays != 0 || !slices.Contains(r.PartitionBlockers, "partial_metadata_traversal") {
		t.Fatal("interval coverage closed source traversal")
	}
	r = inspect(t, capture(t, false), "2024-01-01", "2024-01-31")
	if r.ObservedPartitionReady || r.ChainCoverage.GapDays != 31 {
		t.Fatal("empty query became zero funding")
	}
}

func TestChangedPeriodsAndOutsideCohorts(t *testing.T) {
	a, b := row(101, "2024-01-01", "2024-01-15", "101"), row(102, "2024-01-01", "2024-01-31", "101", "102")
	a["is_amended"] = true
	r := inspect(t, capture(t, false, a, b), "2024-01-01", "2024-01-31")
	if r.ObservedPartitionReady || len(r.Cohorts) != 2 || len(r.ChainCandidateIndexes) != 0 {
		t.Fatal("changed dates invented same-scope membership")
	}
	a = row(101, "2023-01-01", "2023-01-31", "101")
	a["means_filed"] = "paper"
	b = row(102, "2024-01-01", "2024-01-31", "102")
	r = inspect(t, capture(t, false, a, b), "2024-01-01", "2024-01-31")
	if !r.ObservedPartitionReady || len(r.Cohorts) != 2 {
		t.Fatal("outside-window conflict blocked unrelated dates")
	}
}

func TestInputFailureAndNoTrustedSelections(t *testing.T) {
	path := capture(t, false, row(101, "2024-01-01", "2024-01-31", "101"))
	for _, dates := range [][2]string{{"bad", "2024-01-31"}, {"2024-01-01T00:00:00", "2024-01-31"}, {"2024-02-01", "2024-01-31"}, {"0001-01-01", "9999-12-31"}} {
		if _, err := Inspect(context.Background(), Request{path, dates[0], dates[1]}); err == nil {
			t.Fatal("invalid window accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Inspect(ctx, Request{path, "2024-01-01", "2024-01-31"}); err != context.Canceled {
		t.Fatal(err)
	}
	write(t, filepath.Join(filepath.Dir(path), "body.json"), []byte("{}"))
	if _, err := Inspect(context.Background(), Request{path, "2024-01-01", "2024-01-31"}); err == nil {
		t.Fatal("tampered body accepted")
	}
	a := row(101, "2024-01-01", "2024-01-31", "101")
	if _, err := Inspect(context.Background(), Request{capture(t, false, a, a), "2024-01-01", "2024-01-31"}); err == nil {
		t.Fatal("duplicate file counted")
	}
}
