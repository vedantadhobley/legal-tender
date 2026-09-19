package cli

import (
	"bytes"
	"testing"
)

func TestReceiptReferenceTopologyCLI(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{nil, 2},
		{[]string{"--reference-manifest", "/missing/manifest.json", "--output-dir", "/missing/output"}, 2},
		{[]string{"--unknown"}, 2},
		{[]string{"--reference-manifest", "/missing/manifest.json", "--output-dir", "/missing/output", "--expected-reference-id", "bad"}, 1},
	} {
		var out, stderr bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "build-receipt-reference-topology"}, tc.args...), &out, &stderr)
		if code != tc.code {
			t.Fatal(code, tc.code, stderr.String())
		}
		if code != 0 && out.Len() != 0 {
			t.Fatal("failure emitted result", out.String())
		}
	}
}
