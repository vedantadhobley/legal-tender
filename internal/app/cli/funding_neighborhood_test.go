package cli

import (
	"bytes"
	"testing"
)

func TestFundingNeighborhoodCLIRejectsOverrides(t *testing.T) {
	for _, name := range []string{"inspect-funding-neighborhood", "validate-funding-neighborhoods"} {
		for _, flag := range []string{"--cycle=2024", "--resume", "--terminal-policy=automatic", "--password=example", "--flow-bundle=other"} {
			var out, err bytes.Buffer
			if code := Run([]string{"pipeline", "fec", name, flag}, &out, &err); code != 2 || out.Len() != 0 {
				t.Fatal(name, flag, code)
			}
		}
		var out, err bytes.Buffer
		if code := Run([]string{"pipeline", "fec", name, "--help"}, &out, &err); code != 0 {
			t.Fatal(name, code, err.String())
		}
	}
}
