package reportmetadata

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// All 51 observations stay separate across the three retained endpoints.
func TestRetainedMetadataCorpus(t *testing.T) {
	root := os.Getenv("LT_REPORT_METADATA_GO_AUDIT")
	if root == "" {
		t.Skip("requires pinned report-metadata captures; no network")
	}
	pinsRaw, err := os.ReadFile("../../../../docs/audit/fixtures/receipt-report-metadata-2026-09-10.sha256")
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(pinsRaw)), "\n") {
		parts := strings.Fields(line)
		pins[parts[1]] = parts[0]
	}
	results := map[string]Review{}
	for _, tc := range []struct {
		stem, endpoint, committee string
		rows                      int
	}{
		{"filing-witnesses", "/v1/filings/", "", 7},
		{"sid-reports", "/v1/reports/house-senate/", "C00843367", 10},
		{"nrcc-reports", "/v1/reports/pac-party/", "C00075820", 34},
	} {
		t.Run(tc.stem, func(t *testing.T) {
			dir := t.TempDir()
			c := Capture{Contract: Contract, SchemaSHA256: SwaggerSHA256, Endpoint: tc.endpoint, Query: Query{CommitteeID: tc.committee, Cycle: 2024, PerPage: 100}}
			if tc.committee == "" {
				c.Query = Query{FileNumbers: []int64{1766839, 1780310, 1780346, 1833804, 1876290, 1882886, 1813890}, PerPage: 100}
			}
			page := PageCapture{Page: 1, TimeBasis: "http_date"}
			for _, ext := range []string{".json", ".headers"} {
				name := tc.stem + ext
				b, err := os.ReadFile(filepath.Join(root, name))
				if err != nil {
					t.Fatal(err)
				}
				if digest(b) != pins[name] {
					t.Fatal("unpinned corpus")
				}
				if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
					t.Fatal(err)
				}
				a := Artifact{Path: name, SHA256: digest(b), Bytes: int64(len(b))}
				if ext == ".json" {
					page.Body = a
				} else {
					page.Headers = a
				}
			}
			// These are retained server Date observations, not invented client clocks.
			switch tc.stem {
			case "filing-witnesses":
				page.ObservedAt = "2026-09-10T07:45:47Z"
			case "sid-reports":
				page.ObservedAt = "2026-09-10T07:45:48Z"
			case "nrcc-reports":
				page.ObservedAt = "2026-09-10T07:49:06Z"
			}
			c.Pages = []PageCapture{page}
			out, err := readTest(t, c, filepath.Join(dir, "capture.json"))
			if err != nil {
				t.Fatal(err)
			}
			if out.State != "validated_observations" || out.Rows != tc.rows {
				t.Fatalf("unexpected corpus review: %+v", out)
			}
			again, err := ReadCapture(context.Background(), filepath.Join(dir, "capture.json"))
			if err != nil || string(jsonBytes(again)) != string(jsonBytes(out)) {
				t.Fatal("replay mismatch", err)
			}
			results[tc.stem] = out
		})
	}
	if len(results) != 3 {
		return
	}
	find := func(stem, file string) map[string]json.RawMessage {
		t.Helper()
		for _, r := range results[stem].Pages[0].Records {
			if r.FileNumber == file {
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(r.Raw, &fields); err != nil {
					t.Fatal(err)
				}
				return fields
			}
		}
		t.Fatal("witness missing")
		return nil
	}
	f := find("filing-witnesses", "1833804")
	r := find("nrcc-reports", "1833804")
	if string(f["is_amended"]) != "false" || string(r["is_amended"]) != "true" || string(f["amendment_chain"]) == string(r["amendment_chain"]) {
		t.Fatal("endpoint assertions collapsed")
	}
	if string(find("filing-witnesses", "1882886")["previous_file_number"]) != "-1147523" || string(find("sid-reports", "1780346")["individual_itemized_contributions_period"]) != `"0.00"` {
		t.Fatal("source representation lost")
	}
}
