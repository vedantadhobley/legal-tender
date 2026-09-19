package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestIdentityAssertionCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--workers", "0"}, {"--workers", "9"}, {"--row-limit", "5"}, {"--terminal-policy", "frontier"}, {"unexpected"}} {
		var out, err bytes.Buffer
		if code := Run(append([]string{"pipeline", "fec", "build-reported-identity-assertions"}, args...), &out, &err); code != 2 || out.Len() != 0 {
			t.Fatal("invalid invocation", args, code, out.String(), err.String())
		}
	}
	var out, err bytes.Buffer
	if code := Run([]string{"pipeline", "fec", "build-reported-identity-assertions", "--help"}, &out, &err); code != 0 || !strings.Contains(err.String(), "expected-view-id") {
		t.Fatal(code, err.String())
	}
}
