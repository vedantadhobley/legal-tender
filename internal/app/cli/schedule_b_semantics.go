package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/audit/fecschedulebsemantics"
)

func runAuditScheduleBSemantics(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-schedule-b-semantics", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	facts := flags.String("facts", "", "published Schedule B columnar fact manifest")
	cycle := flags.String("cycle", "", "FEC two-year transaction period")
	workers := flags.Int("workers", 4, "bounded Parquet scan workers (1-16)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *storageRoot == "" || *facts == "" || !validPeriod(*cycle) || *workers < 1 || *workers > 16 {
		fmt.Fprintln(stderr, "--storage-root, --facts, an even-year --cycle, and 1-16 --workers are required; no positional arguments")
		return 2
	}
	result, err := fecschedulebsemantics.Audit(ctx, fecschedulebsemantics.Options{
		StorageRoot: *storageRoot, FactManifestPath: *facts, Cycle: *cycle, Workers: *workers,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "Schedule B semantics audit: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
