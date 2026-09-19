package cli

import (
	"bytes"
	"testing"
)

func TestDisbursementReportingFlags(t *testing.T) {
	for _, args := range [][]string{nil, {"--cycle", "2023"}, {"--workers", "17"}, {"--unexpected"}, {"positional"}} {
		var out, err bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "calculate-disbursement-reporting"}, args...), &out, &err)
		if code != 2 || out.Len() != 0 {
			t.Fatalf("%v: %d %s", args, code, &out)
		}
	}
	var out, err bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "calculate-disbursement-reporting", "--help"}, &out, &err); code != 0 {
		t.Fatal(code)
	}
	out.Reset()
	err.Reset()
	if code := Run([]string{"pipeline", "fec", "calculate-disbursement-reporting", "--storage-root", t.TempDir(), "--facts", "/missing", "--cycle", "2024"}, &out, &err); code != 1 || out.Len() != 0 {
		t.Fatal("failed command emitted a result", code)
	}
}
