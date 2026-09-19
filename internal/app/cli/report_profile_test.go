package cli

import (
	"bytes"
	"testing"
)

func TestReceiptReportProfileOptions(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--unknown"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--summary-facts", "absent", "--cycle", "2024", "--profile-version", "2"}, 1},
		{[]string{"--storage-root", t.TempDir(), "--summary-facts", "absent", "--cycle", "2024", "--profile-version", "3"}, 2},
		{[]string{"--storage-root", t.TempDir(), "--summary-facts", "absent", "--cycle", "2024"}, 1},
		{[]string{"--storage-root", t.TempDir(), "--summary-facts", "absent", "--cycle", "2023"}, 2},
	} {
		var out, errout bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "profile-receipt-report-scope"}, tc.args...), &out, &errout); code != tc.code || out.Len() != 0 {
			t.Fatal(code, tc.code, errout.String())
		}
	}
}
