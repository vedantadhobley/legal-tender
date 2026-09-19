package cli

import (
	"bytes"
	"testing"
)

func TestParticipantCommandsRejectIncompleteArguments(t *testing.T) {
	for _, name := range []string{"publish-receipt-participants", "benchmark-receipt-participants", "inspect-receipt-participant"} {
		for _, tc := range []struct {
			args []string
			code int
		}{{nil, 2}, {[]string{"--help"}, 0}, {[]string{"--unknown"}, 2}, {[]string{"--storage-root", "/missing", "--schedule-a-facts", "/missing"}, 2}} {
			var out, stderr bytes.Buffer
			code := Run(append([]string{"pipeline", "fec", name}, tc.args...), &out, &stderr)
			if code != tc.code {
				t.Fatal(name, code, stderr.String())
			}
			if code != 0 && out.Len() != 0 {
				t.Fatal("failure emitted successful result")
			}
		}
	}
}
