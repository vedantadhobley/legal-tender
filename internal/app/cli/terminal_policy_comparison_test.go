package cli

import (
	"bytes"
	"testing"
)

func TestTerminalPolicyComparisonCommandBoundary(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
	}{
		{args: []string{"--help"}, code: 0},
		{args: nil, code: 2},
		{args: []string{"--candidate-dossier", "/missing", "--expected-dossier-id", "bad"}, code: 1},
		{args: []string{"extra"}, code: 2},
	} {
		var stdout, stderr bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "compare-terminal-policies"}, test.args...), &stdout, &stderr)
		if code != test.code {
			t.Fatalf("code = %d, want %d: %s", code, test.code, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatal("success output on failure")
		}
	}
}
