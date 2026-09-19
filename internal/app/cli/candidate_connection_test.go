package cli

import (
	"bytes"
	"testing"
)

func TestCandidateConnectionCommandBoundary(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--terminal-policy", "leaf"}, 2},
		{[]string{"--storage-root", "/missing", "--candidate-report", "/missing/report.json", "--expected-report-id", "invalid", "--source-row-ordinal", "1"}, 1},
	} {
		var out, diagnostic bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "inspect-candidate-connection"}, tc.args...), &out, &diagnostic); code != tc.code {
			t.Fatal(code, diagnostic.String())
		}
		if out.Len() != 0 {
			t.Fatal("success output on failure")
		}
	}
}
