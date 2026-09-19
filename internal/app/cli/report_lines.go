package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func runReportLines(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-funding-report-lines", flag.ContinueOnError)
	f.SetOutput(stderr)
	root := f.String("storage-root", "", "published source storage root (read-only)")
	basis := f.String("basis-result", "", "verified receipt inventory JSON")
	committee := f.String("committee", "", "exact committee ID")
	file := f.String("file-number", "", "exact positive report file number")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	n, err := strconv.ParseUint(*file, 10, 64)
	if f.NArg() != 0 || *root == "" || *basis == "" || !committeeflows.ValidCommitteeID(committee) || err != nil || n == 0 || strconv.FormatUint(n, 10) != *file {
		fmt.Fprintln(stderr, "require --storage-root, --basis-result, --committee, and positive --file-number")
		return 2
	}
	fmt.Fprintln(stderr, "verifying receipt inventory and complete bounded report membership")
	r, err := fundingbasis.Open(ctx, *root, *basis)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	result, err := r.ReviewReportLines(ctx, *committee, *file, func(s string) { fmt.Fprintln(stderr, s) })
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
