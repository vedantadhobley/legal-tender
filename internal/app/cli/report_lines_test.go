package cli

import (
	"bytes"
	"testing"
)

func TestReportLinesCLIOptions(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{nil, 2},
		{[]string{"--storage-root", "/absent", "--basis-result", "absent", "--committee", "C00000001", "--file-number", "001"}, 2},
		{[]string{"--storage-root", "/absent", "--basis-result", "absent", "--committee", "C00000001", "--file-number", "0"}, 2},
		{[]string{"--storage-root", "/absent", "--basis-result", "absent", "--committee", "C00000001", "--file-number", "1", "extra"}, 2},
		{[]string{"--storage-root", "/absent", "--basis-result", "absent", "--committee", "C00000001", "--file-number", "1"}, 1},
	} {
		var out, errout bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "review-funding-report-lines"}, tc.args...), &out, &errout); code != tc.code || out.Len() != 0 {
			t.Fatal(tc, code, out.String(), errout.String())
		}
	}
}
