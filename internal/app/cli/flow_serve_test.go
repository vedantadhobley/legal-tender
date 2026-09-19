package cli

import (
	"bytes"
	"context"
	"testing"
)

func TestServeCommitteeFlowValidatesBeforeOpening(t *testing.T) {
	for _, args := range [][]string{{"serve", "committee-flow-evidence"}, {"serve", "committee-flow-evidence", "--cycle", "2023"}, {"serve", "committee-flow-evidence", "--unknown"}} {
		var out, err bytes.Buffer
		if RunContext(context.Background(), args, &out, &err) != 2 || out.Len() != 0 {
			t.Fatal(args, out.String())
		}
	}
	var out, err bytes.Buffer
	if RunContext(context.Background(), []string{"serve", "committee-flow-evidence", "--help"}, &out, &err) != 0 {
		t.Fatal(err.String())
	}
}
