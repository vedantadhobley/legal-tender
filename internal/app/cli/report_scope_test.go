package cli

import (
	"bytes"
	"testing"
)

func TestReportScopeCLIArguments(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--body", "missing"}, 2},
		{[]string{"--trusted", "true"}, 2}, {[]string{"--source-url", "https://example.com", "--body", "missing", "--body-sha256", "bad", "--headers", "missing", "--headers-sha256", "bad"}, 1},
	} {
		var out, stderr bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "review-report-scope"}, tc.args...), &out, &stderr)
		if code != tc.code || out.Len() != 0 {
			t.Fatalf("unexpected CLI result: %d %s", code, stderr.String())
		}
	}
}
