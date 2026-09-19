package cli

import (
	"bytes"
	"testing"
)

func TestReceiptWindowCLIInputs(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"extra"}, 2}, {[]string{"--trusted", "true"}, 2},
		{[]string{"--storage-root", "missing", "--summary-facts", "missing", "--capture", "missing", "--documents", "missing", "--start", "2023-01-01", "--end", "2024-12-31", "--profile", "missing", "--profile-sha256", "invalid"}, 1},
	} {
		for _, command := range []string{"compare-receipt-report-window", "compare-receipt-families", "review-receipt-family-absence", "compare-receipt-family-window", "compare-receipt-family-summary"} {
			var out, stderr bytes.Buffer
			if code := Run(append([]string{"pipeline", "fec", command}, tc.args...), &out, &stderr); code != tc.code || out.Len() != 0 {
				t.Fatal(code, out.String())
			}
		}
	}
}

func TestReceiptFamiliesVersionFlagIsScopedAndStrict(t *testing.T) {
	for _, tc := range []struct {
		command, version string
		code             int
	}{
		{"compare-receipt-families", "v2", 1},
		{"compare-receipt-families", "v1", 1},
		{"compare-receipt-families", "v3", 2},
		{"compare-receipt-family-window", "v2", 2},
	} {
		var out, stderr bytes.Buffer
		args := []string{"pipeline", "fec", tc.command, "--comparison-version", tc.version, "--storage-root", "missing", "--summary-facts", "missing", "--capture", "missing", "--documents", "missing", "--start", "2023-01-01", "--end", "2024-12-31", "--profile", "missing", "--profile-sha256", "invalid"}
		if code := Run(args, &out, &stderr); code != tc.code || out.Len() != 0 {
			t.Fatal(tc, code, stderr.String())
		}
	}
}
