package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/recovery/funding"
)

func runStageEvidenceReview(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-release-stage-evidence", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o funding.StageReviewOptions
	f.StringVar(&o.StorageRoot, "storage-root", "", "read-only retained storage root")
	f.StringVar(&o.ReleasePath, "release", "", "exact storage-root-relative release manifest")
	f.StringVar(&o.ReleaseSHA256, "expected-release-sha256", "", "exact release byte digest")
	f.StringVar(&o.StagePath, "candidate-stage", "", "candidate control record, relative to storage root")
	f.StringVar(&o.StageSHA256, "expected-candidate-stage-sha256", "", "candidate's own byte digest; never substitutes the release pin")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.ReleasePath == "" || o.ReleaseSHA256 == "" || o.StagePath == "" || o.StageSHA256 == "" {
		fmt.Fprintln(stderr, "exact release, candidate stage, byte digests and storage root required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	r, err := funding.ReviewStage(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "stage evidence review:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "published descriptors match; exact original stage bytes=%t; recovery readiness unverified\n", r.OriginalStageBytesEqual)
	return 0
}
