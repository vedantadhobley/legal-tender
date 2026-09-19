package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func runFundingBasis(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var root, manifest, cycle, basis, bundle, linkages, candidate, file string
	var workers int
	var q fundingbasis.Query
	flags.StringVar(&root, "storage-root", "", "published source storage root (read-only)")
	switch command {
	case "audit-funding-coverage":
		flags.StringVar(&basis, "basis-result", "", "checked receipt inventory JSON")
		flags.StringVar(&bundle, "receipt-bundle", "", "exact receipt and candidate-summary fact bundle")
	case "calculate-committee-funding-basis":
		flags.StringVar(&manifest, "schedule-a-facts", "", "exact Schedule A Parquet manifest")
		flags.StringVar(&cycle, "cycle", "", "expected FEC source cycle")
		flags.IntVar(&workers, "workers", 4, "bounded scan workers (1..4)")
	case "review-funding-report":
		flags.StringVar(&basis, "basis-result", "", "checked receipt inventory JSON")
		flags.StringVar(&q.Committee, "committee", "", "exact recipient committee ID")
		flags.StringVar(&file, "file-number", "", "exact report file number")
	case "list-funding-receipts", "inspect-funding-receipts":
		flags.StringVar(&basis, "basis-result", "", "checked receipt inventory JSON")
		flags.StringVar(&q.Committee, "committee", "", "exact recipient committee ID")
		flags.StringVar(&q.Component, "component", "", "inventory component; empty selects all")
		flags.Uint64Var(&q.After, "after-ordinal", 0, "exclusive source-row cursor")
		flags.IntVar(&q.Limit, "limit", 20, "maximum receipt rows (1..100)")
	case "review-funding-component":
		flags.StringVar(&basis, "basis-result", "", "checked receipt inventory JSON")
		flags.StringVar(&q.Component, "component", "", "complete inventory component (maximum 10000 rows)")
	case "assess-candidate-funding-basis":
		flags.StringVar(&basis, "basis-result", "", "checked receipt inventory JSON")
		flags.StringVar(&bundle, "observation-bundle", "", "exact committee-flow evidence bundle")
		flags.StringVar(&linkages, "linkage-facts", "", "exact source-matched candidate-committee linkages")
		flags.StringVar(&cycle, "cycle", "", "expected FEC source cycle")
		flags.StringVar(&candidate, "candidate", "", "exact candidate ID")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || root == "" || command == "calculate-committee-funding-basis" && (manifest == "" || !validPeriod(cycle) || workers < 1 || workers > 4) ||
		command != "calculate-committee-funding-basis" && basis == "" || (command == "list-funding-receipts" || command == "inspect-funding-receipts") && (q.Committee == "" || q.Limit < 1 || q.Limit > 100) ||
		command == "assess-candidate-funding-basis" && (bundle == "" || linkages == "" || candidate == "" || !validPeriod(cycle)) ||
		command == "review-funding-component" && q.Component == "" || command == "review-funding-report" && (q.Committee == "" || file == "") || command == "audit-funding-coverage" && bundle == "" {
		fmt.Fprintln(stderr, "missing or invalid funding-basis options")
		return 2
	}
	progress := func(s string) { fmt.Fprintln(stderr, s) }
	var result any
	var err error
	if command == "calculate-committee-funding-basis" {
		result, err = fundingbasis.Run(ctx, fundingbasis.Options{StorageRoot: root, ScheduleA: manifest, Cycle: cycle, Workers: workers, Progress: progress})
	} else {
		var reader *fundingbasis.Reader
		reader, err = fundingbasis.Open(ctx, root, basis)
		if err == nil {
			if command == "audit-funding-coverage" {
				result, err = reader.AuditCoverage(ctx, bundle)
			} else if command == "review-funding-report" {
				result, err = reader.ReviewReport(ctx, q.Committee, file, progress)
			} else if command == "review-funding-component" {
				result, err = reader.ReviewComponent(ctx, q.Component, progress)
			} else if command == "list-funding-receipts" {
				result, err = reader.Query(ctx, q)
			} else if command == "inspect-funding-receipts" {
				result, err = reader.Inspect(ctx, q)
			} else {
				var trace candidateupstream.Result
				trace, err = candidateupstream.Run(ctx, candidateupstream.Options{StorageRoot: root, Bundle: bundle, Linkages: linkages, Cycle: cycle, Candidate: candidate, Progress: progress})
				if err == nil {
					result, err = reader.Assess(trace)
				}
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "funding basis:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stderr, "complete: source receipt evidence only; terminal allocation unresolved")
	return 0
}
