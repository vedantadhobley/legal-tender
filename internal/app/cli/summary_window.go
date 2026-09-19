package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
)

func runSummaryWindow(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runSummaryWindowVersion(ctx, "compare-summary-report-window", args, stdout, stderr)
}

func runSummaryWindowVersion(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var r summaryassertion.WindowComparisonRequest
	f.StringVar(&r.StorageRoot, "storage-root", "", "published storage root (read-only)")
	f.StringVar(&r.SummaryManifest, "summary-facts", "", "exact published summary manifest")
	f.StringVar(&r.Window.Membership.CapturePath, "capture", "", "retained single-committee report capture")
	f.StringVar(&r.Window.Membership.Start, "start", "", "inclusive requested start, YYYY-MM-DD")
	f.StringVar(&r.Window.Membership.End, "end", "", "inclusive requested end, YYYY-MM-DD")
	f.StringVar(&r.Window.DocumentsPath, "documents", "", "pinned document-set descriptor")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || r.StorageRoot == "" || r.SummaryManifest == "" || r.Window.Membership.CapturePath == "" || r.Window.Membership.Start == "" || r.Window.Membership.End == "" || r.Window.DocumentsPath == "" {
		fmt.Fprintln(stderr, "require --storage-root, --summary-facts, --capture, --documents, --start, --end and no positional arguments")
		return 2
	}
	compare := summaryassertion.CompareWindow
	if command == "compare-summary-report-window-v2" {
		compare = summaryassertion.CompareWindowV2
	}
	out, err := compare(ctx, r)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0 // Valid blocked/different diagnostics are not execution failures.
}
