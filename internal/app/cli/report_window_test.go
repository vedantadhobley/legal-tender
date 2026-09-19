package cli

import (
	"bytes"
	"testing"
)

func TestReportWindowCLIInputs(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"extra"}, 2}, {[]string{"--trusted", "true"}, 2},
		{[]string{"--capture", "missing", "--documents", "missing", "--start", "2024-01-01", "--end", "2024-12-31"}, 1},
	} {
		var out, stderr bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "review-report-window"}, tc.args...), &out, &stderr); code != tc.code || out.Len() != 0 {
			t.Fatal("wrong CLI disposition", code, out.String())
		}
	}
}
