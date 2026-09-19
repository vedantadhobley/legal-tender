package cli

import (
	"bytes"
	"testing"
)

func TestSummaryReceiptReviewOptions(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--unknown"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--basis-result", "absent", "--summary-facts", "absent", "--cycle", "2024", "--committee", "C00000001"}, 1},
		{[]string{"--storage-root", t.TempDir(), "--basis-result", "absent", "--summary-facts", "absent", "--cycle", "2023", "--committee", "C00000001"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--basis-result", "absent", "--summary-facts", "absent", "--cycle", "2024", "--committee", "BAD"}, 2},
	} {
		var out, errout bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "review-summary-receipt-compatibility"}, tc.args...), &out, &errout); code != tc.code || out.Len() != 0 {
			t.Fatal(code, tc.code, errout.String())
		}
	}
}
