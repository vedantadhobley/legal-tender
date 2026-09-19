package cli

import (
	"bytes"
	"testing"
)

func TestReviewCommitteeFlowsCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--unknown"}, {"extra"}} {
		var out, err bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "review-committee-flows"}, args...), &out, &err); code != 2 || out.Len() != 0 {
			t.Fatal(code, out.String())
		}
	}
	var out, err bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "review-committee-flows", "--help"}, &out, &err); code != 0 {
		t.Fatal(code)
	}
	out.Reset()
	err.Reset()
	if code := Run([]string{"pipeline", "fec", "review-committee-flows", "--result", "/missing", "--storage-root", t.TempDir(), "--evidence-root", t.TempDir()}, &out, &err); code != 1 || out.Len() != 0 {
		t.Fatal("failure emitted result", code)
	}
}
