package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestFundingWindowCLIRejectsCyclePolicyAndSourceOverrides(t *testing.T) {
	for _, flag := range []string{"--cycle=2024", "--password=example", "--terminal-policy=origin", "--flow-bundle=other", "--ending-family=independent_support", "--receipt-ordinal=1"} {
		var out, diagnostic bytes.Buffer
		if code := Run([]string{"pipeline", "fec", "inspect-funding-window-paths", flag}, &out, &diagnostic); code != 2 || out.Len() != 0 {
			t.Fatal(flag, code)
		}
	}
	var out, diagnostic bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "inspect-funding-window-paths", "--help"}, &out, &diagnostic); code != 0 {
		t.Fatal(code, diagnostic.String())
	}
	base := []string{"pipeline", "fec", "inspect-funding-window-paths", "--inputs=absent", "--expected-inputs-sha256=absent", "--storage-root=/unused", "--endpoint=http://unused.invalid", "--from-committee=C00000001", "--target=C00000002"}
	for _, args := range [][]string{{"--ledger=both"}, {"--ledger=schedule_a", "--start-date=2024-01-01"}, {"--ledger=schedule_a", "--start-date=2024-02-30", "--end-date=2024-03-01"}} {
		out.Reset()
		diagnostic.Reset()
		if code := Run(append(append([]string{}, base...), args...), &out, &diagnostic); code != 2 || out.Len() != 0 {
			t.Fatal(args, code)
		}
	}
}

func TestFundingWindowConnectionCLIRejectsUnsupportedOrUnqualifiedScope(t *testing.T) {
	base := []string{"pipeline", "fec", "inspect-funding-window-connections", "--inputs=absent", "--expected-inputs-sha256=absent", "--storage-root=/unused", "--endpoint=http://unused.invalid", "--ledger=schedule_a", "--target=H0CA00001"}
	for _, args := range [][]string{
		{"--receipt-ordinal=1", "--entry-family=reported_receipt", "--ending-family=candidate_authorization_context"},
		{"--from-committee=C00000001", "--ending-family=independent_support"},
		{"--from-committee=C00000001", "--ending-family=independent_opposition"},
		{"--from-committee=C00000001", "--ending-family=candidate_authorization_context", "--receipt-generation=" + strings.Repeat("a", 64)},
		{"--from-committee=C00000001", "--ending-family=candidate_authorization_context", "--start-date=2024-01-01"},
	} {
		var out, diagnostic bytes.Buffer
		if code := Run(append(append([]string{}, base...), args...), &out, &diagnostic); code != 2 || out.Len() != 0 || strings.Contains(diagnostic.String(), "window inputs:") {
			t.Fatal("invalid query reached source opening", args, code, diagnostic.String())
		}
	}
	var out, diagnostic bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "inspect-funding-window-connections", "--help"}, &out, &diagnostic); code != 0 {
		t.Fatal(code)
	}
}
