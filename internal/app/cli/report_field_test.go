package cli

import (
	"bytes"
	"testing"
)

func TestReportTotalReceiptsCLI(t *testing.T) {
	for _, command := range []string{"compare-report-total-receipts", "review-report-unitemized"} {
		for _, tc := range []struct {
			args []string
			code int
		}{
			{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--trusted", "true"}, 2},
			{[]string{"--source-url", "https://example.com", "--body", "missing", "--body-sha256", "bad", "--headers", "missing", "--headers-sha256", "bad"}, 1},
		} {
			var out, stderr bytes.Buffer
			if code := Run(append([]string{"pipeline", "fec", command}, tc.args...), &out, &stderr); code != tc.code || out.Len() != 0 {
				t.Fatalf("bad CLI result: %d %s", code, stderr.String())
			}
		}
	}
}
