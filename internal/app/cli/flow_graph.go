package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

func runCommitteeFlowGraph(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-arango-committee-flow-evidence", flag.ContinueOnError)
	flags.SetOutput(stderr)
	o := flowevidence.Options{}
	flags.StringVar(&o.StorageRoot, "storage-root", "", "published storage root")
	flags.StringVar(&o.Bundle, "projection-bundle", "", "exact readiness manifest or current pointer")
	flags.StringVar(&o.Cycle, "cycle", "", "expected FEC cycle")
	flags.StringVar(&o.Endpoint, "endpoint", "", "Arango URL without credentials")
	username := os.Getenv("ARANGO_USER")
	if username == "" {
		username = "root"
	}
	flags.StringVar(&o.Username, "username", username, "Arango username")
	passwordEnv := flags.String("password-env", "ARANGO_PASSWORD", "environment variable holding Arango password")
	flags.IntVar(&o.BatchSize, "batch-size", 5000, "documents per import request")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || o.StorageRoot == "" || o.Bundle == "" || !validPeriod(o.Cycle) || o.Endpoint == "" {
		fmt.Fprintln(stderr, "storage root, exact projection bundle, cycle, and endpoint required")
		return 2
	}
	o.Password = os.Getenv(*passwordEnv)
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := flowevidence.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "committee-flow evidence graph:", err)
		return 1
	}
	if err := encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: isolated %s graph, reused=%t, %.3f seconds\n", r.State, r.Reused, r.ElapsedSeconds)
	return 0
}
