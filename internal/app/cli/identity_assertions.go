package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/identityassertions"
)

func runIdentityAssertions(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("build-reported-identity-assertions", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o identityassertions.Options
	f.StringVar(&o.StorageRoot, "storage-root", "", "retained source root, read-only")
	f.StringVar(&o.ReceiptManifest, "schedule-a-facts", "", "exact immutable Schedule A manifest")
	f.StringVar(&o.CommitteeManifest, "committee-facts", "", "exact immutable committee-master manifest")
	f.StringVar(&o.Cycle, "cycle", "", "exact cycle for both independent assertion populations")
	f.StringVar(&o.ExpectedViewID, "expected-view-id", "", "optional required identity for full replay")
	f.IntVar(&o.Workers, "workers", 4, "independent source shard readers, 1..8")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.ReceiptManifest == "" || o.CommitteeManifest == "" || o.Cycle == "" || o.Workers < 1 || o.Workers > 8 {
		fmt.Fprintln(stderr, "exact source root, both fact manifests, cycle and 1..8 workers required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.BuildSHA256 = build
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := identityassertions.Scan(ctx, o, identityassertions.Consumer{})
	if err != nil {
		fmt.Fprintln(stderr, "reported identity assertions:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
