package cli

import (
	"bytes"
	"testing"
)

func TestFlowReconciliationFlags(t *testing.T) {
	for _, args := range [][]string{nil, {"--cycle", "2023"}, {"--workers", "17"}, {"--unexpected"}, {"positional"}} {
		var out, stderr bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "reconcile-committee-flows"}, args...), &out, &stderr); code != 2 || out.Len() != 0 {
			t.Fatal(code, out.String())
		}
	}
	var out, stderr bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "reconcile-committee-flows", "--help"}, &out, &stderr); code != 0 {
		t.Fatal(code)
	}
	out.Reset()
	stderr.Reset()
	args := []string{"pipeline", "fec", "reconcile-committee-flows", "--storage-root", t.TempDir(), "--output-root", t.TempDir(), "--schedule-a-facts", "/missing-a", "--schedule-b-facts", "/missing-b", "--release", "/missing-release", "--cycle", "2024"}
	if code := Run(args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("failure emitted result", code)
	}
}
