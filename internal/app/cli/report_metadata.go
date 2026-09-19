package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

func runReportMetadata(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-report-metadata", flag.ContinueOnError)
	f.SetOutput(stderr)
	path := f.String("capture", "", "local credential-free metadata capture descriptor; no HTTP")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || *path == "" {
		fmt.Fprintln(stderr, "require --capture and no positional arguments")
		return 2
	}
	result, err := reportmetadata.ReadCapture(ctx, *path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if result.State == "blocked" {
		fmt.Fprintln(stderr, "metadata observations retained; review has blocking issues")
		return 1
	}
	return 0
}
