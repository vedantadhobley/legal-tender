package cli

import (
	"bytes"
	"testing"
)

func TestCandidateUpstreamCommandBoundary(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{nil, 2},
		{[]string{"--ledger", "schedule_b"}, 2},
		{[]string{"--storage-root", "/missing", "--observation-bundle", "/missing/bundle", "--linkage-facts", "/missing/linkage", "--cycle", "2024", "--candidate", "H0AA00001"}, 1},
	} {
		var out, diagnostic bytes.Buffer
		args := append([]string{"pipeline", "fec", "trace-candidate-committee-receipts"}, tc.args...)
		if code := Run(args, &out, &diagnostic); code != tc.code {
			t.Fatal(code, diagnostic.String())
		}
		if out.Len() != 0 {
			t.Fatal("success JSON on help or failure")
		}
	}
}
