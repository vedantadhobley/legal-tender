package cli

import (
	"bytes"
	"context"
	"testing"
)

func TestReceiptGraphCLIRequiredInputs(t *testing.T) {
	var out, err bytes.Buffer
	if code := runReceiptGraph(context.Background(), nil, &out, &err); code != 2 {
		t.Fatalf("code=%d", code)
	}
	if code := runReceiptGraph(context.Background(), []string{"--help"}, &out, &err); code != 0 {
		t.Fatalf("help=%d", code)
	}
}

func TestCycleGraphCLIHasNoSampleScope(t *testing.T) {
	for _, args := range [][]string{nil, {"--max-rows", "12"}, {"--first-ordinal", "2"}, {"--layout", "expanded-v1"}, {"--compare-v1-result", "old.json"}} {
		var out, stderr bytes.Buffer
		if code := runReceiptGraphMode(context.Background(), args, &out, &stderr, true); code != 2 {
			t.Fatalf("args=%v code=%d", args, code)
		}
	}
	var out, stderr bytes.Buffer
	if code := runReceiptGraphMode(context.Background(), []string{"--help"}, &out, &stderr, true); code != 0 {
		t.Fatal(code)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("reserve-free-bytes")) {
		t.Fatal("missing storage controls")
	}
}
