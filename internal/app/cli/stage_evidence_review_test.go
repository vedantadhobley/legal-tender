package cli

import (
	"bytes"
	"context"
	"testing"
)

func TestStageEvidenceReviewFlags(t *testing.T) {
	for _, args := range [][]string{nil, {"--release", "x"}, {"--allow-replacement"}, {"--storage-root", "x", "--release", "x", "--expected-release-sha256", "x", "--candidate-stage", "x", "--expected-candidate-stage-sha256", "x", "extra"}} {
		var out, stderr bytes.Buffer
		if code := runStageEvidenceReview(context.Background(), args, &out, &stderr); code != 2 || out.Len() != 0 {
			t.Fatalf("unsafe flags accepted: %v", args)
		}
	}
	var out, stderr bytes.Buffer
	if runStageEvidenceReview(context.Background(), []string{"--help"}, &out, &stderr) != 0 {
		t.Fatal("help failed")
	}
}
