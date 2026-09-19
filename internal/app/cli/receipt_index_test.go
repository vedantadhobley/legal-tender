package cli

import (
	"bytes"
	"testing"
)

func TestReceiptIndexBenchmarkCommandBoundary(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--resolve-donors"}, 2},
		{[]string{"--storage-root", "/missing", "--schedule-a-facts", "/missing/facts", "--cycle", "2024", "--shard-index", "0", "--output-dir", "/missing/new", "--max-output-bytes", "0"}, 1},
	} {
		var out, diagnostic bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "benchmark-receipt-reference-index"}, tc.args...), &out, &diagnostic); code != tc.code {
			t.Fatal(code, diagnostic.String())
		}
		if out.Len() != 0 {
			t.Fatal("success output on failure/help")
		}
	}
}
