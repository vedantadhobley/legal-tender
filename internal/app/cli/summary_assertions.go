package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
)

func runSummaryAssertions(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("calculate-committee-summary-assertions", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("storage-root", "", "published source storage root (read-only)")
	manifest := flags.String("summary-facts", "", "exact published committee-summary manifest")
	cycle := flags.String("cycle", "", "expected source cycle")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *root == "" || *manifest == "" || !validPeriod(*cycle) {
		fmt.Fprintln(stderr, "require --storage-root, --summary-facts, and --cycle")
		return 2
	}
	result, err := summaryassertion.Run(ctx, *root, *manifest, *cycle)
	if err != nil {
		fmt.Fprintln(stderr, "summary assertions:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stderr, "complete: exact summary evidence groups and arithmetic diagnostics; no financial repair or terminal allocation")
	return 0
}
