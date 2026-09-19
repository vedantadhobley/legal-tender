package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"io"
	"time"
)

func runReconcileCommitteeFlows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runCommitteeFlowCalculation(ctx, args, stdout, stderr, false)
}

func runCommitteeFlowCalculation(ctx context.Context, args []string, stdout, stderr io.Writer, publish bool) int {
	name := "reconcile-committee-flows"
	if publish {
		name = "publish-committee-flow-reconciliation"
	}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("storage-root", "", "source storage root")
	output := new(string)
	if !publish {
		output = flags.String("output-root", "", "calculation artifact root; no active pointer")
	}
	a := flags.String("schedule-a-facts", "", "published Schedule A columnar manifest")
	b := flags.String("schedule-b-facts", "", "published Schedule B columnar manifest")
	release := flags.String("release", "", "coordinated release selecting both source bytes")
	cycle := flags.String("cycle", "", "FEC two-year transaction period")
	workers := flags.Int("workers", 4, "bounded scan workers (1-16)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *root == "" || (!publish && *output == "") || *a == "" || *b == "" || *release == "" || !validPeriod(*cycle) || *workers < 1 || *workers > 16 {
		fmt.Fprintln(stderr, "source root, both fact manifests, release, even-year cycle, and 1-16 workers required; manual calculation also requires output root")
		return 2
	}
	start := time.Now()
	calculate := flowreconciliation.Calculate
	if publish {
		calculate = flowreconciliation.Publish
	}
	r, err := calculate(ctx, flowreconciliation.Options{StorageRoot: *root, OutputRoot: *output, ScheduleA: *a, ScheduleB: *b, Release: *release, Cycle: *cycle, Workers: *workers, Progress: func(s string) { fmt.Fprintln(stderr, s) }})
	if err != nil {
		fmt.Fprintln(stderr, "committee-flow reconciliation:", err)
		return 1
	}
	if err := encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d A + %d B facts, %d candidate components, %.3f seconds\n", r.A.Total.Rows, r.B.Total.Rows, r.Assertions.RecordCount, time.Since(start).Seconds())
	return 0
}
