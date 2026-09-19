package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestCommitteeFlowGraphDispatchAndCredentialGuard(t *testing.T) {
	base := []string{"pipeline", "fec", "probe-arango-committee-flow-evidence"}
	var out, errout bytes.Buffer
	if code := Run(append(base, "--help"), &out, &errout); code != 0 || !strings.Contains(errout.String(), "projection-bundle") {
		t.Fatal(code, errout.String())
	}
	out.Reset()
	errout.Reset()
	if code := Run(base, &out, &errout); code != 2 {
		t.Fatal(code, errout.String())
	}
	t.Setenv("FLOW_GRAPH_TEST_PASSWORD", "")
	out.Reset()
	errout.Reset()
	args := append(base, "--storage-root", t.TempDir(), "--cycle", "2024", "--projection-bundle", "must-not-read", "--endpoint", "http://arango", "--password-env", "FLOW_GRAPH_TEST_PASSWORD")
	if code := Run(args, &out, &errout); code != 1 || out.Len() != 0 || !strings.Contains(errout.String(), "credentials required separately") {
		t.Fatal(code, errout.String())
	}
}
