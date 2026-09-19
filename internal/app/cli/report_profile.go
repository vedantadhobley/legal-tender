package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func runReceiptReportProfile(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("profile-receipt-report-scope", flag.ContinueOnError)
	f.SetOutput(stderr)
	root := f.String("storage-root", "", "published source storage root (read-only)")
	summary := f.String("summary-facts", "", "exact published committee-summary manifest; selects same-release Schedule A")
	cycle := f.String("cycle", "", "expected source cycle")
	version := f.String("profile-version", "1", "profile wire version: 1 (included reports) or 2 (all report-line groups)")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || *root == "" || *summary == "" || !validPeriod(*cycle) || (*version != "1" && *version != "2") {
		fmt.Fprintln(stderr, "require --storage-root, --summary-facts, --cycle, and --profile-version 1 or 2")
		return 2
	}
	fmt.Fprintln(stderr, "verifying summary facts and profiling same-release Schedule A physical occurrences")
	var result any
	var err error
	progress := func(s string) { fmt.Fprintln(stderr, s) }
	if *version == "2" {
		result, err = fundingbasis.ProfileReportLines(ctx, *root, *summary, *cycle, progress)
	} else {
		result, err = fundingbasis.ProfileReports(ctx, *root, *summary, *cycle, progress)
	}
	if err != nil {
		fmt.Fprintln(stderr, "receipt report profile:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stderr, "complete occurrence profile; no effective-report selection or financial comparison")
	return 0
}
