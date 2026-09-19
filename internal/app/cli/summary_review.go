package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func runSummaryReceiptReview(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-summary-receipt-compatibility", flag.ContinueOnError)
	f.SetOutput(stderr)
	root := f.String("storage-root", "", "published source storage root (read-only)")
	basis := f.String("basis-result", "", "verified receipt inventory JSON")
	summary := f.String("summary-facts", "", "exact published committee-summary manifest")
	cycle := f.String("cycle", "", "expected source cycle")
	committee := f.String("committee", "", "exact committee ID; no name matching")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || *root == "" || *basis == "" || *summary == "" || !validPeriod(*cycle) || !committeeflows.ValidCommitteeID(committee) {
		fmt.Fprintln(stderr, "require --storage-root, --basis-result, --summary-facts, --cycle, and --committee")
		return 2
	}
	fmt.Fprintln(stderr, "verifying receipt inventory and exact backing artifacts")
	r, err := fundingbasis.Open(ctx, *root, *basis)
	if err != nil {
		fmt.Fprintln(stderr, "summary/receipt review:", err)
		return 1
	}
	fmt.Fprintln(stderr, "verifying summary facts and exact assertion grouping")
	result, err := r.ReviewSummary(ctx, *summary, *cycle, *committee)
	if err != nil {
		fmt.Fprintln(stderr, "summary/receipt review:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stderr, "complete: reported observations and comparison blockers; no financial difference or terminal allocation")
	return 0
}
