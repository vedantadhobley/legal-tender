package cli

import (
	"bytes"
	"testing"
)

func TestReportPeriodsCLI(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--capture", "missing"}, 2},
		{[]string{"--capture", "missing", "--start", "2024-01-01", "--end", "2024-01-31"}, 1},
		{[]string{"--trusted", "true"}, 2}, {[]string{"--selected-file", "101"}, 2},
	} {
		var out, stderr bytes.Buffer
		if got := Run(append([]string{"pipeline", "fec", "review-report-period-membership"}, tc.args...), &out, &stderr); got != tc.code || out.Len() != 0 {
			t.Fatalf("unexpected CLI %d: %s", got, stderr.String())
		}
	}
}
