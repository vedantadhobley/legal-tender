package cli

import (
	"bytes"
	"testing"
)

func TestCandidateDossierCommandBoundary(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{nil, 2},
		{[]string{"--storage-root", "/missing", "--candidate-report", "/missing/report", "--expected-report-id", "bad", "--candidate-interpretations", "/missing/interpretations"}, 1},
		{[]string{"extra"}, 2},
	} {
		var out, diagnostic bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "build-candidate-dossier"}, tc.args...), &out, &diagnostic); code != tc.code {
			t.Fatalf("code = %d, want %d: %s", code, tc.code, diagnostic.String())
		}
		if out.Len() != 0 {
			t.Fatal("success output on failure")
		}
	}
}
