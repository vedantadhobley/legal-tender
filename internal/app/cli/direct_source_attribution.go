package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/directattribution"
)

func runDirectSourceAttribution(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("calculate-direct-source-attribution", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var options directattribution.Options
	flags.StringVar(&options.StorageRoot, "storage-root", "", "storage root containing immutable FEC publications")
	flags.StringVar(&options.ScheduleAFacts, "schedule-a-facts", "", "exact immutable Schedule A fact manifest")
	flags.StringVar(&options.ParticipantManifest, "participant-manifest", "", "exact complete-cycle participant manifest.json")
	flags.StringVar(&options.ExpectedParticipant, "expected-participant-id", "", "exact participant calculation ID")
	flags.StringVar(&options.ReceiptBundle, "receipt-bundle", "", "exact immutable candidate-receipt fact bundle")
	flags.StringVar(&options.ExpectedCalculation, "expected-calculation-id", "", "optional exact replay identity")
	flags.IntVar(&options.Workers, "workers", 8, "participant shard workers, 1..8")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	executable, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	options.BuildSHA256 = executable
	options.Progress = func(message string) { fmt.Fprintln(stderr, message) }
	result, err := directattribution.Run(ctx, options)
	if err != nil {
		fmt.Fprintln(stderr, "direct source-appearance attribution:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d candidates; calculation %s; committee-chain allocation not performed\n", len(result.Candidates), result.CalculationID)
	return 0
}
