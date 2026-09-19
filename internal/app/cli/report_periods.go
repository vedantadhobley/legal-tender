package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
)

func runReportPeriods(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-report-period-membership", flag.ContinueOnError)
	f.SetOutput(stderr)
	var r reportperiod.Request
	f.StringVar(&r.CapturePath, "capture", "", "verified report-endpoint capture descriptor; no HTTP")
	f.StringVar(&r.Start, "start", "", "inclusive requested coverage start, YYYY-MM-DD")
	f.StringVar(&r.End, "end", "", "inclusive requested coverage end, YYYY-MM-DD")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || r.CapturePath == "" || r.Start == "" || r.End == "" {
		fmt.Fprintln(stderr, "require --capture, --start, --end and no positional arguments")
		return 2
	}
	out, err := reportperiod.Inspect(ctx, r)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0 // A valid diagnostic may contain unresolved membership or coverage.
}
