package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"io"
	"time"
)

func runReviewCommitteeFlows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("review-committee-flows", flag.ContinueOnError)
	flags.SetOutput(stderr)
	result := flags.String("result", "", "saved reconciliation result JSON")
	evidence := flags.String("evidence-root", "", "root of the result's evidence artifacts")
	storage := flags.String("storage-root", "", "root of immutable source facts")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *result == "" || *evidence == "" || *storage == "" {
		fmt.Fprintln(stderr, "result, evidence root, and source storage root required")
		return 2
	}
	started := time.Now()
	r, err := flowreconciliation.Review(ctx, flowreconciliation.ReviewOptions{ResultPath: *result, EvidenceRoot: *evidence, StorageRoot: *storage})
	if err != nil {
		fmt.Fprintln(stderr, "committee-flow review:", err)
		return 1
	}
	if err := encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d shapes, %d components sampled, %d source rows reviewed, %.3f seconds\n", len(r.Shapes), len(r.Examples), len(r.SourceExamples), time.Since(started).Seconds())
	return 0
}
