package cli

import (
	"bytes"
	"testing"
)

func TestCommitteeFlowPublicationCommands(t *testing.T) {
	for _, name := range []string{"publish-committee-flow-reconciliation", "publish-committee-flow-evidence-bundle", "verify-committee-flow-evidence-bundle"} {
		for _, args := range [][]string{nil, {"--unexpected"}, {"positional"}} {
			var out, diagnostic bytes.Buffer
			if code := Run(append([]string{"pipeline", "fec", name}, args...), &out, &diagnostic); code != 2 || out.Len() != 0 {
				t.Fatal(name, code)
			}
		}
		var out, diagnostic bytes.Buffer
		if code := Run([]string{"pipeline", "fec", name, "--help"}, &out, &diagnostic); code != 0 || out.Len() != 0 {
			t.Fatal(name, code)
		}
	}
	for _, args := range [][]string{
		{"publish-committee-flow-reconciliation", "--schedule-a-facts", "/missing", "--schedule-b-facts", "/missing", "--release", "/missing", "--cycle", "2024"},
		{"publish-committee-flow-evidence-bundle", "--calculation", "/missing", "--committee-facts", "/missing", "--cycle", "2024"},
		{"verify-committee-flow-evidence-bundle", "--bundle", "/missing"},
	} {
		var out, diagnostic bytes.Buffer
		command := append([]string{"pipeline", "fec"}, args...)
		command = append(command, "--storage-root", t.TempDir())
		if code := Run(command, &out, &diagnostic); code != 1 || out.Len() != 0 {
			t.Fatal("failure emitted success", code, out.String())
		}
	}
}
