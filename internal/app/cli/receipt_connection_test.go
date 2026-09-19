package cli

import (
	"bytes"
	"testing"
)

func TestReceiptCandidateConnectionCLIIsReadOnlyAndExplicit(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2},
		{[]string{"--cycle", "2024"}, 2},
		{[]string{"--resume"}, 2},
		{[]string{"--terminal-policy", "leaf"}, 2},
		{[]string{"--candidate", "H0ZZ00001", "--source-row-ordinal", "1"}, 2},
	} {
		var out, diagnostic bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "inspect-receipt-candidate-connection"}, tc.args...), &out, &diagnostic)
		if code != tc.code || out.Len() != 0 {
			t.Fatal(code, out.String(), diagnostic.String())
		}
	}
}

func TestReceiptConnectionGateSelectsScopeWithoutManualOverrides(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0},
		{[]string{"--candidate", "H0ZZ00001"}, 2},
		{[]string{"--source-row-ordinal", "1"}, 2},
		{[]string{"--cycle", "2024"}, 2},
		{[]string{"--resume"}, 2},
		{[]string{"--expected-gate-id", "invalid"}, 2},
	} {
		var out, diagnostic bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "validate-receipt-candidate-connections"}, tc.args...), &out, &diagnostic)
		if code != tc.code || out.Len() != 0 {
			t.Fatal(code, out.String(), diagnostic.String())
		}
	}
}
