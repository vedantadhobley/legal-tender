package reportfield

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func jsonBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func write(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T, amount string, change func([]string)) reportscope.Request {
	t.Helper()
	dir := t.TempDir()
	cover := make([]string, 125)
	cover[0], cover[1], cover[9], cover[12], cover[13] = "F3XN", "C12345678", "Q2", "20240401", "20240630"
	cover[21], cover[43] = amount, amount
	if change != nil {
		change(cover)
	}
	body := []byte("HDR\x1cP3.4\x1cTEST\x1c1234\x1c\x1c\x1c\n" + strings.Join(cover, "\x1c"))
	headers := []byte(fmt.Sprintf("HTTP/2 200 \r\nContent-Type: binary/octet-stream\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(body)))
	r := reportscope.Request{SourceURL: "https://docquery.fec.gov/paper/posted/101.fec", BodyPath: filepath.Join(dir, "body.fec"), BodySHA256: digest(body), HeadersPath: filepath.Join(dir, "headers"), HeadersSHA256: digest(headers)}
	write(t, r.BodyPath, body)
	write(t, r.HeadersPath, headers)
	return r
}

func metadata(t *testing.T, endpoint string, amount any, changes map[string]any) string {
	t.Helper()
	dir := t.TempDir()
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
	def, formField, formValue, field := "Filings", "form_type", "F3X", "total_receipts"
	if endpoint != "/v1/filings/" {
		def, formField, formValue, field = "PacParty", "report_form", "Form 3X", "total_receipts_period"
	}
	if endpoint == "/v1/reports/house-senate/" {
		def = "HouseSenate"
	}
	row := map[string]any{}
	if len(schema.Defs[def].Required) == 0 {
		t.Fatal("missing endpoint test schema", def)
	}
	for _, name := range schema.Defs[def].Required {
		row[name] = nil
	}
	row["file_number"], row["committee_id"], row["cycle"] = 101, "C12345678", 2024
	row[formField], row["report_type"], row["means_filed"], row["amendment_indicator"] = formValue, "Q2", "paper", "N"
	row["coverage_start_date"], row["coverage_end_date"] = "2024-04-01", "2024-06-30T00:00:00"
	row[field] = amount
	for k, v := range changes {
		row[k] = v
	}
	body := jsonBytes(map[string]any{"api_version": "1.0", "pagination": reportmetadata.Pagination{Count: 1, IsCountExact: true, Page: 1, Pages: 1, PerPage: 100}, "results": []any{row}})
	headers := []byte(fmt.Sprintf("HTTP/2 200 \r\nContent-Type: application/json\r\nContent-Length: %d\r\nDate: Thu, 10 Sep 2026 07:00:00 GMT\r\n\r\n", len(body)))
	file := 102
	if v, ok := row["file_number"].(int); ok {
		file = v
	}
	c := reportmetadata.Capture{Contract: reportmetadata.Contract, SchemaSHA256: reportmetadata.SwaggerSHA256, Endpoint: endpoint,
		Query: reportmetadata.Query{CommitteeID: "C12345678", Cycle: 2024, PerPage: 100}, Pages: []reportmetadata.PageCapture{{Page: 1, ObservedAt: "2026-09-10T07:00:00Z", TimeBasis: "http_date", Body: reportmetadata.Artifact{Path: "body.json", SHA256: digest(body), Bytes: int64(len(body))}, Headers: reportmetadata.Artifact{Path: "headers", SHA256: digest(headers), Bytes: int64(len(headers))}}}}
	if endpoint == "/v1/filings/" {
		c.Query = reportmetadata.Query{FileNumbers: []int64{int64(file)}, PerPage: 100}
	} else if committee, ok := row["committee_id"].(string); ok {
		c.Query.CommitteeID = committee
	}
	write(t, filepath.Join(dir, "body.json"), body)
	write(t, filepath.Join(dir, "headers"), headers)
	path := filepath.Join(dir, "capture.json")
	write(t, path, jsonBytes(c))
	return path
}
