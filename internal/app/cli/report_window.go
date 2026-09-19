package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
)

func runReportWindow(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-report-window", flag.ContinueOnError)
	f.SetOutput(stderr)
	var r reportperiod.WindowRequest
	f.StringVar(&r.Membership.CapturePath, "capture", "", "verified report-endpoint capture; no HTTP")
	f.StringVar(&r.Membership.Start, "start", "", "inclusive requested start, YYYY-MM-DD")
	f.StringVar(&r.Membership.End, "end", "", "inclusive requested end, YYYY-MM-DD")
	f.StringVar(&r.DocumentsPath, "documents", "", "versioned descriptor of pinned local documents")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || r.Membership.CapturePath == "" || r.Membership.Start == "" || r.Membership.End == "" || r.DocumentsPath == "" {
		fmt.Fprintln(stderr, "require --capture, --start, --end, --documents and no positional arguments")
		return 2
	}
	out, err := reportperiod.ReviewWindow(ctx, r)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0 // Valid diagnostics may have incomplete fields or arithmetic mismatches.
}
