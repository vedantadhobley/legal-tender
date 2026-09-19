package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
)

func runReportBinding(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-report-field-binding", flag.ContinueOnError)
	f.SetOutput(stderr)
	var r reportperiod.BindingRequest
	registerDocumentFlags(f, &r.Document)
	f.StringVar(&r.Membership.CapturePath, "capture", "", "verified report-endpoint capture descriptor; no HTTP")
	f.StringVar(&r.Membership.Start, "start", "", "inclusive requested window start, YYYY-MM-DD")
	f.StringVar(&r.Membership.End, "end", "", "inclusive requested window end, YYYY-MM-DD")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || !hasReportEvidence(r.Document) || r.Membership.CapturePath == "" || r.Membership.Start == "" || r.Membership.End == "" {
		fmt.Fprintln(stderr, "require pinned document flags, --capture, --start, --end and no positional arguments")
		return 2
	}
	out, err := reportperiod.BindFields(ctx, r)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0 // A blocked binding is a valid diagnostic, not financial readiness.
}
