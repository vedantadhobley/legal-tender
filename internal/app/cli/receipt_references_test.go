package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestReceiptReferencesCLI(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--max-rows", "10"}, 2}, {[]string{"--storage-root", "/missing", "--schedule-a-facts", "/missing", "--cycle", "2024", "--output-dir", "/missing", "--merge-fan-in", "100"}, 1}} {
		var out, err bytes.Buffer
		if got := Run(append([]string{"pipeline", "fec", "join-receipt-references"}, tc.args...), &out, &err); got != tc.code {
			t.Fatal(got, err.String())
		}
		if out.Len() != 0 {
			t.Fatal("success output on failure")
		}
	}
}

func TestReceiptReferenceReaderLimit(t *testing.T) {
	for _, readers := range []string{"0", "8", "9"} {
		var out, stderr bytes.Buffer
		code := Run([]string{"pipeline", "fec", "join-receipt-references", "--storage-root", "/missing", "--schedule-a-facts", "/missing", "--cycle", "2024", "--output-dir", "/missing/output", "--scan-workers", readers}, &out, &stderr)
		if code != 1 || out.Len() != 0 || strings.Contains(stderr.String(), "1..8 source scanners") != (readers != "8") {
			t.Fatal("reader limit validation", readers, code, stderr.String())
		}
	}
}
