package cli

import (
	"bytes"
	"testing"
)

func TestTerminalAssessmentCLIRequiresPinnedAutomaticScope(t *testing.T) {
	for _, flag := range []string{"", "--cycle=2024", "--candidate=H0AA00001", "--terminal-policy=origin", "--ledger=both", "--password=example", "--max-hops=3", "--flow-bundle=other", "--resume", "unexpected"} {
		var out, err bytes.Buffer
		args := []string{"pipeline", "fec", "assess-terminal-sources"}
		if flag != "" {
			args = append(args, flag)
		}
		if code := Run(args, &out, &err); code != 2 || out.Len() != 0 {
			t.Fatal(flag, code)
		}
	}
	var out, err bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "assess-terminal-sources", "--help"}, &out, &err); code != 0 {
		t.Fatal(code, err.String())
	}
}

func TestReceiptRoleCLIRefusesPolicyAndScopeOverrides(t *testing.T) {
	for _, arg := range []string{"--cycle=2024", "--committee=C00000001", "--workers=0", "--workers=9", "--max-rows=10", "--terminal-policy=origin", "--password=example", "--expected-assessment-id=other", "unexpected"} {
		var out, err bytes.Buffer
		if code := Run([]string{"pipeline", "fec", "profile-terminal-receipt-roles", arg}, &out, &err); code != 2 || out.Len() != 0 {
			t.Fatal(arg, code)
		}
	}
	var out, err bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "profile-terminal-receipt-roles", "--help"}, &out, &err); code != 0 {
		t.Fatal(code)
	}
}
