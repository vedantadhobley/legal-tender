package cli

import (
	"bytes"
	"context"
	"testing"
)

func TestConduitPublicationCLIRequiresExactInputs(t *testing.T) {
	for _, args := range [][]string{nil, {"--participant-manifest", "x"}, {"--unknown"}} {
		var out, err bytes.Buffer
		if runReceiptConduits(context.Background(), args, &out, &err) != 2 || out.Len() != 0 {
			t.Fatal("incomplete invocation accepted")
		}
	}
	var out, err bytes.Buffer
	if runReceiptConduits(context.Background(), []string{"--help"}, &out, &err) != 0 {
		t.Fatal("help failed")
	}
}

func TestSharedReferenceProfileCLIRequiresExactInputs(t *testing.T) {
	for _, args := range [][]string{nil, {"--participant-manifest", "x"}, {"--unknown"}} {
		var out, err bytes.Buffer
		if runReceiptConduitCommand(context.Background(), args, &out, &err, true, false) != 2 || out.Len() != 0 {
			t.Fatal("incomplete profile invocation accepted")
		}
	}
	var out, err bytes.Buffer
	if Run([]string{"pipeline", "fec", "profile-shared-receipt-references", "--help"}, &out, &err) != 0 {
		t.Fatal("profile help dispatch failed", err.String())
	}
}

func TestSharedSourceReviewCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--profile", "x"}, {"--unknown"}} {
		var out, err bytes.Buffer
		if runSharedReferenceReview(context.Background(), args, &out, &err) != 2 || out.Len() != 0 {
			t.Fatal("invalid review accepted")
		}
	}
	var out, err bytes.Buffer
	if Run([]string{"pipeline", "fec", "review-shared-receipt-references", "--help"}, &out, &err) != 0 {
		t.Fatal(err.String())
	}
}

func TestSharedPublicationCommandsRequireInputs(t *testing.T) {
	for _, command := range []string{"publish-shared-receipt-conduit-associations", "publish-shared-conduit-generation"} {
		for _, args := range [][]string{nil, {"--unknown"}} {
			var out, err bytes.Buffer
			if Run(append([]string{"pipeline", "fec", command}, args...), &out, &err) != 2 || out.Len() != 0 {
				t.Fatal("incomplete shared command accepted")
			}
		}
		var out, err bytes.Buffer
		if Run([]string{"pipeline", "fec", command, "--help"}, &out, &err) != 0 {
			t.Fatal("shared help dispatch failed")
		}
	}
}
