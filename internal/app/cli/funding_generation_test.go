package cli

import (
	"bytes"
	"testing"
)

func TestFundingGenerationRequiresAllFamiliesAndRejectsMutationFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--cycle", "2024"}, 2}, {[]string{"--resume"}, 2}, {[]string{"--candidate", "H0ZZ00001"}, 2}, {[]string{"--skip-outside-spending"}, 2}, {[]string{"--terminal-policy", "leaf"}, 2}, {[]string{"--expected-generation-id", "bad"}, 2},
	} {
		var out, err bytes.Buffer
		code := Run(append([]string{"pipeline", "fec", "verify-funding-generation"}, tc.args...), &out, &err)
		if code != tc.code || out.Len() != 0 {
			t.Fatal(code, out.String(), err.String())
		}
	}
}
