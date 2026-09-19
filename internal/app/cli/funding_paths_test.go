package cli

import (
	"bytes"
	"testing"
)

func TestFundingPathsCLIRefusesPolicyAndInputOverrides(t *testing.T) {
	for _, command := range []string{"inspect-funding-paths", "validate-funding-paths"} {
		for _, flag := range []string{"--cycle=2024", "--resume", "--terminal-policy=origin", "--password=example", "--flow-bundle=other", "--cursor=other"} {
			var out, err bytes.Buffer
			if code := Run([]string{"pipeline", "fec", command, flag}, &out, &err); code != 2 || out.Len() != 0 {
				t.Fatal(command, flag, code)
			}
		}
		var out, err bytes.Buffer
		if code := Run([]string{"pipeline", "fec", command, "--help"}, &out, &err); code != 0 {
			t.Fatal(code, err.String())
		}
	}
	for _, ledger := range []string{"both", "schedule_e"} {
		var out, err bytes.Buffer
		if code := Run([]string{"pipeline", "fec", "inspect-funding-paths", "--ledger", ledger, "--from-committee", "C00000001", "--target", "C00000002"}, &out, &err); code != 2 {
			t.Fatal(code)
		}
	}
}
