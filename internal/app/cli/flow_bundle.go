package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

func runCommitteeFlowBundle(ctx context.Context, args []string, stdout, stderr io.Writer, verify bool) int {
	name := "publish-committee-flow-evidence-bundle"
	if verify {
		name = "verify-committee-flow-evidence-bundle"
	}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("storage-root", "", "published storage root")
	var calculation, committee, cycle, bundle string
	if verify {
		flags.StringVar(&bundle, "bundle", "", "published bundle manifest or current pointer")
	} else {
		flags.StringVar(&calculation, "calculation", "", "published reconciliation manifest or current pointer")
		flags.StringVar(&committee, "committee-facts", "", "published same-cycle committee-master manifest")
		flags.StringVar(&cycle, "cycle", "", "expected FEC cycle")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *root == "" || (verify && bundle == "") || (!verify && (calculation == "" || committee == "" || !validPeriod(cycle))) {
		fmt.Fprintln(stderr, "storage root and exact published input paths required")
		return 2
	}
	start := time.Now()
	var b flowreconciliation.Bundle
	var err error
	if verify {
		b, _, _, err = flowreconciliation.LoadBundle(ctx, *root, bundle)
	} else {
		b, err = flowreconciliation.PublishBundle(ctx, flowreconciliation.BundleOptions{StorageRoot: *root, Calculation: calculation, Committee: committee, Cycle: cycle})
	}
	if err != nil {
		fmt.Fprintln(stderr, name+":", err)
		return 1
	}
	if err := encodeJSON(stdout, b); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: exact %s observation inputs verified in %.3f seconds; no graph mutation\n", b.Cycle, time.Since(start).Seconds())
	return 0
}
