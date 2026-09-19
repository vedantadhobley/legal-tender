package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportfield"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func runReportTotalReceipts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("compare-report-total-receipts", flag.ContinueOnError)
	f.SetOutput(stderr)
	var r reportscope.Request
	registerReportEvidenceFlags(f, &r)
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || !hasReportEvidence(r) {
		fmt.Fprintln(stderr, "require --source-url, --body, --body-sha256, --headers, --headers-sha256 and no positional arguments")
		return 2
	}
	out, err := reportfield.CompareTotalReceipts(ctx, r)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0 // A valid review can contain blocked pairs; inspect each comparison.
}
