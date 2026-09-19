package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
	"io"
	"time"
)

func calculateDisbursementReporting(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("calculate-disbursement-reporting", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("storage-root", "", "durable Legal Tender storage")
	facts := flags.String("facts", "", "published Schedule B columnar fact manifest")
	cycle := flags.String("cycle", "", "FEC two-year transaction period")
	workers := flags.Int("workers", 4, "bounded Parquet scan workers (1-16)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *root == "" || *facts == "" || !validPeriod(*cycle) || *workers < 1 || *workers > 16 {
		fmt.Fprintln(stderr, "--storage-root, --facts, an even-year --cycle, and 1-16 --workers are required; no positional arguments")
		return 2
	}
	started := time.Now()
	result, err := disbursements.Calculate(ctx, disbursements.Options{StorageRoot: *root, FactManifestPath: *facts, Cycle: *cycle, Workers: *workers, Progress: func(s string) { fmt.Fprintln(stderr, s) }})
	if err != nil {
		fmt.Fprintln(stderr, "Schedule B reporting calculation:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d facts, %d reporting groups, %.3f seconds\n", result.Total.Rows, len(result.Groups), time.Since(started).Seconds())
	return 0
}
