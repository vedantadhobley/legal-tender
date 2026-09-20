package cli

import (
	"bytes"
	"testing"
)

func TestDirectSourceAttributionCommandBoundary(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
	}{
		{args: []string{"--help"}, code: 0},
		{args: nil, code: 1},
		{args: []string{"extra"}, code: 2},
		{args: []string{"--storage-root", "/missing", "--schedule-a-facts", "/missing", "--participant-manifest", "/missing", "--expected-participant-id", "bad", "--receipt-bundle", "/missing"}, code: 1},
	} {
		var stdout, stderr bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "calculate-direct-source-attribution"}, test.args...), &stdout, &stderr)
		if code != test.code {
			t.Fatalf("code = %d, want %d: %s", code, test.code, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatal("success output on failure")
		}
	}
}
