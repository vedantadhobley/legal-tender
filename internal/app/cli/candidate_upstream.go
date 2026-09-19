package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
)

func runCandidateUpstream(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("trace-candidate-committee-receipts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	o := candidateupstream.Options{}
	flags.StringVar(&o.StorageRoot, "storage-root", "", "published source storage root (read-only)")
	flags.StringVar(&o.Bundle, "observation-bundle", "", "exact committee-flow evidence bundle")
	flags.StringVar(&o.Linkages, "linkage-facts", "", "candidate-committee linkage manifest with matching source ancestry")
	flags.StringVar(&o.Cycle, "cycle", "", "expected FEC source cycle")
	flags.StringVar(&o.Candidate, "candidate", "", "exact candidate ID; no name-based selection")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || o.StorageRoot == "" || o.Bundle == "" || o.Linkages == "" || !validPeriod(o.Cycle) || o.Candidate == "" {
		fmt.Fprintln(stderr, "storage root, observation bundle, linkage facts, cycle, and candidate required")
		return 2
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := candidateupstream.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "candidate upstream:", err)
		return 1
	}
	if err := encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d candidate observations, %d reachable committees, %d cyclic components; terminal allocation unresolved\n", len(r.CandidateObservations), len(r.Nodes), len(r.CyclicComponents))
	return 0
}
