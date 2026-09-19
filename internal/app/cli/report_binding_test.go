package cli

import (
	"bytes"
	"testing"
)

func TestReportBindingCLIRequiresExactInputs(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{nil, 2},
		{[]string{"extra"}, 2},
		{[]string{"--metadata-capture", "unaccepted"}, 2},
		{[]string{"--capture", "missing", "--start", "2024-01-01", "--end", "2024-12-31", "--source-url", "https://docquery.fec.gov/dcdev/posted/101.fec", "--body", "missing", "--body-sha256", "invalid", "--headers", "missing", "--headers-sha256", "invalid"}, 1},
	} {
		var out, stderr bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "review-report-field-binding"}, tc.args...), &out, &stderr)
		if code != tc.code || out.Len() != 0 {
			t.Fatal("wrong CLI disposition", code, out.String())
		}
	}
}
