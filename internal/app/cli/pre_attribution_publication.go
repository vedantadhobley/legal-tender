package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

func runPublishCandidateInterpretations(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-independent-expenditure-candidate-interpretations", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to interpret")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	resolution := flags.String("candidate-resolution", "", "exact candidate-resolution manifest; defaults to the cycle pointer")
	current := flags.String("current", "", "path to this cycle's active candidate-interpretation manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
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
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required\n")
		return 2
	}
	if *resolution == "" {
		*resolution = filepath.Join(*storageRoot, "calculations", "fec", "independent-expenditure-candidate-resolution", "current", *cycle+".json")
	}
	manifest, err := candidateresolution.PublishInterpretations(
		ctx,
		candidateresolution.InterpretationPublishInput{CandidateResolutionManifestPath: *resolution},
		*runID,
		candidateresolution.InterpretationPublishOptions{
			StorageRoot: *storageRoot, CurrentManifestPath: *current, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish independent-expenditure candidate interpretations: %v\n", err)
		return 1
	}
	if manifest.Cycle != *cycle {
		fmt.Fprintf(stderr, "candidate interpretations returned cycle %s, expected %s\n", manifest.Cycle, *cycle)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode candidate-interpretation manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishCommitteeFlowComparisonCandidates(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-committee-flow-comparison-candidates", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to compare")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	reconciliation := flags.String("reconciliation", "", "exact committee-flow reconciliation manifest; defaults to the cycle pointer")
	current := flags.String("current", "", "path to this cycle's active comparison-candidate manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	maxPairs := flags.Uint64("max-candidate-pairs", 10_000_000, "fail before publication if direct candidate pairs exceed this operational capacity")
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
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" || *maxPairs == 0 {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root, --run-id, and positive --max-candidate-pairs are required\n")
		return 2
	}
	if *reconciliation == "" {
		*reconciliation = filepath.Join(*storageRoot, flowreconciliation.PublicationBase, "current", *cycle+".json")
	}
	manifest, err := flowreconciliation.PublishComparisonCandidates(ctx, *runID, flowreconciliation.ComparisonPublishOptions{
		StorageRoot: *storageRoot, ReconciliationPath: *reconciliation, CurrentManifestPath: *current,
		MaxCandidatePairs: *maxPairs, Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish committee-flow comparison candidates: %v\n", err)
		return 1
	}
	if manifest.Cycle != *cycle {
		fmt.Fprintf(stderr, "committee-flow comparison candidates returned cycle %s, expected %s\n", manifest.Cycle, *cycle)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode committee-flow comparison-candidate manifest: %v\n", err)
		return 1
	}
	return 0
}
