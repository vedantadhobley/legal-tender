package reportscope

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

func metadataFixture(t *testing.T, amended any, previous any, file int, overrides ...map[string]any) string {
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
	row := map[string]any{}
	for _, name := range schema.Defs["Filings"].Required {
		row[name] = nil
	}
	row["file_number"], row["committee_id"], row["cycle"] = file, "C12345678", 2024
	row["is_amended"], row["previous_file_number"] = amended, previous
	row["form_type"], row["report_type"], row["coverage_start_date"] = "F3X", "M10", "2024-09-01"
	row["coverage_end_date"] = "2024-09-30T00:00:00"
	for _, fields := range overrides {
		for key, value := range fields {
			row[key] = value
		}
	}
	body := jsonBytes(map[string]any{"api_version": "1.0", "pagination": reportmetadata.Pagination{Count: 1, IsCountExact: true, Page: 1, Pages: 1, PerPage: 100}, "results": []any{row}})
	headers := []byte(fmt.Sprintf("HTTP/2 200 \r\nContent-Type: application/json\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(body)))
	dir := t.TempDir()
	c := reportmetadata.Capture{Contract: reportmetadata.Contract, SchemaSHA256: reportmetadata.SwaggerSHA256, Endpoint: "/v1/filings/", Query: reportmetadata.Query{FileNumbers: []int64{int64(file)}, PerPage: 100}, Pages: []reportmetadata.PageCapture{{Page: 1, ObservedAt: "2026-09-10T07:00:00Z", TimeBasis: "http_date", Body: reportmetadata.Artifact{Path: "body.json", SHA256: digest(body), Bytes: int64(len(body))}, Headers: reportmetadata.Artifact{Path: "headers", SHA256: digest(headers), Bytes: int64(len(headers))}}}}
	write(t, filepath.Join(dir, "body.json"), body)
	write(t, filepath.Join(dir, "headers"), headers)
	path := filepath.Join(dir, "capture.json")
	write(t, path, jsonBytes(c))
	return path
}

func TestMetadataAssertionsAreNotSelection(t *testing.T) {
	r := requestFor(t, []byte(strings.Join(paperFixture(), "\n")), false)
	r.MetadataCaptures = []string{metadataFixture(t, true, -11, 101), metadataFixture(t, nil, 101, 101), metadataFixture(t, false, nil, 102)}
	a, err := Assess(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if a.Disposition != "supplemental_attachment_shape" || len(a.Metadata) != 2 || len(a.MetadataInputs) != 3 || a.MetadataInputs[2].MatchingRows != 0 || len(a.Differences) != 2 || len(a.Issues) != 0 || a.HistoryComplete || a.FinancialSelectionReady {
		t.Fatalf("lost scoped metadata: %+v", a)
	}
	for _, d := range a.Differences {
		if !slices.Equal(d.AssertionIndexes, []int{0, 1}) {
			t.Fatal("lost assertion index")
		}
		if d.Field == "previous_file_number" && (string(d.Values[0]) != "-11" || string(d.Values[1]) != "101") {
			t.Fatal("rewrote links")
		}
	}
	for i := 0; i < 10; i++ {
		again, err := Assess(context.Background(), r)
		if err != nil || !bytes.Equal(jsonBytes(a), jsonBytes(again)) {
			t.Fatal("non-deterministic metadata replay", err)
		}
	}
	r.MetadataCaptures = append(r.MetadataCaptures, r.MetadataCaptures[0])
	if _, err := Assess(context.Background(), r); err == nil {
		t.Fatal("duplicate capture accepted")
	}
}

func TestMetadataIntegrityIsRechecked(t *testing.T) {
	r := requestFor(t, []byte(strings.Join(paperFixture(), "\n")), false)
	p := metadataFixture(t, false, nil, 101)
	r.MetadataCaptures = []string{p}
	write(t, filepath.Join(filepath.Dir(p), "body.json"), []byte("{}"))
	if _, err := Assess(context.Background(), r); err == nil {
		t.Fatal("modified metadata trusted")
	}
}

func TestMetadataScopeDifferencesDoNotRewriteTheCover(t *testing.T) {
	r := requestFor(t, []byte(strings.Join(paperFixture(), "\n")), false)
	r.MetadataCaptures = []string{metadataFixture(t, false, nil, 101, map[string]any{
		"committee_id": "C87654321", "coverage_end_date": "2024-09-30T12:00:00", "report_type": "Q3",
	})}
	a, err := Assess(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"metadata_cover_difference:committee_id", "metadata_cover_difference:coverage_end_date", "metadata_cover_difference:report_type"}
	if !slices.Equal(a.Issues, want) || a.Cover.CommitteeID != "C12345678" || a.Disposition != "supplemental_attachment_shape" || a.FinancialSelectionReady {
		t.Fatal("lost scoped disagreement")
	}
	for i := 0; i < 10; i++ {
		again, err := Assess(context.Background(), r)
		if err != nil || !bytes.Equal(jsonBytes(a), jsonBytes(again)) {
			t.Fatal("nondeterministic conflict ordering", err)
		}
	}
}
