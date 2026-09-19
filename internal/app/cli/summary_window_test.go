package cli

import (
	"bytes"
	"testing"
)

func TestSummaryWindowCLIInputs(t *testing.T) {
	for _, command := range []string{"compare-summary-report-window", "compare-summary-report-window-v2"} {
		for _, tc := range []struct {
			args []string
			code int
		}{
			{[]string{"--help"}, 0}, {nil, 2}, {[]string{"extra"}, 2}, {[]string{"--trusted", "true"}, 2},
			{[]string{"--committee-id", "C00000001"}, 2},
			{[]string{"--storage-root", "missing", "--summary-facts", "missing", "--capture", "missing", "--documents", "missing", "--start", "2023-01-01", "--end", "2024-12-31"}, 1},
		} {
			var out, stderr bytes.Buffer
			if code := Run(append([]string{"pipeline", "fec", command}, tc.args...), &out, &stderr); code != tc.code || out.Len() != 0 {
				t.Fatal(code, out.String())
			}
		}
	}
}
