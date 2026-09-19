package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func runReceiptGraph(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runReceiptGraphMode(ctx, args, stdout, stderr, false)
}

func runReceiptGraphMode(ctx context.Context, args []string, stdout, stderr io.Writer, full bool) int {
	name := "benchmark-arango-receipt-participants"
	if full {
		name = "publish-arango-receipt-participants"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptgraph.Options
	o.FullCycle = full
	var passwordEnv string
	if full {
		o.Layout = receiptgraph.CompactLayout
		f.StringVar(&o.PublicationDirectory, "publication-dir", "", "durable cycle publication/checkpoint root")
		f.StringVar(&o.ArangoDataDirectory, "arango-data-dir", "", "actual server data mount, exposed read-only for filesystem checks")
		f.Uint64Var(&o.ReserveFreeBytes, "reserve-free-bytes", 0, "required minimum available bytes on Arango filesystem")
		f.Uint64Var(&o.MaxFilesystemGrowthBytes, "max-filesystem-growth-bytes", 0, "shared filesystem net-growth allowance; required at admission")
		f.Uint64Var(&o.MaxEncodedBytes, "max-encoded-bytes", 0, "maximum complete projected JSON payload bytes")
	} else {
		f.StringVar(&o.Layout, "layout", receiptgraph.ExpandedLayout, "physical layout: expanded-v1 or compact-v2")
		f.StringVar(&o.CompareResult, "compare-v1-result", "", "optional exact accepted v1 result; checks its live graph read-only")
		f.StringVar(&o.CompareSHA256, "expected-compare-sha256", "", "required SHA256 of the comparison result bytes")
		f.Uint64Var(&o.First, "first-ordinal", 1, "first source occurrence in sample")
		f.Uint64Var(&o.Rows, "max-rows", 100000, "exact selected rows, at most one million")
	}
	f.StringVar(&o.StorageRoot, "storage-root", "", "existing source storage")
	f.StringVar(&o.Participants, "participant-manifest", "", "exact participant manifest")
	f.StringVar(&o.ParticipantID, "expected-participant-id", "", "required participant identity")
	f.StringVar(&o.Conduits, "conduit-manifest", "", "exact qualified conduit manifest")
	f.StringVar(&o.ConduitID, "expected-conduit-id", "", "required conduit identity")
	f.StringVar(&o.Facts, "schedule-a-facts", "", "exact source fact manifest")
	f.StringVar(&o.Committees, "committee-facts", "", "same-release committee master")
	f.StringVar(&o.Candidates, "candidate-facts", "", "same-release candidate master")
	f.StringVar(&o.Linkages, "linkage-facts", "", "same-release candidate linkage")
	f.StringVar(&o.LockDirectory, "lock-dir", "", "shared writable publisher lock directory")
	f.StringVar(&o.Endpoint, "endpoint", "", "Arango endpoint; no credentials in URL")
	f.StringVar(&o.Username, "username", "root", "configured Arango user")
	f.StringVar(&passwordEnv, "password-env", "ARANGO_PASSWORD", "environment variable holding configured password")
	f.IntVar(&o.Workers, "workers", 4, "parallel batch import/readback workers, 1..8")
	f.IntVar(&o.BatchSize, "batch-rows", 1000, "maximum documents per batch, 1..5000")
	if e := f.Parse(args); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.Participants == "" || o.ParticipantID == "" || o.Conduits == "" || o.ConduitID == "" || o.Facts == "" || o.Committees == "" || o.Candidates == "" || o.Linkages == "" || o.LockDirectory == "" || o.Endpoint == "" {
		fmt.Fprintln(stderr, "exact participant/conduit/source/master/linkage inputs and isolated endpoint/lock directory required")
		return 2
	}
	o.Password = os.Getenv(passwordEnv)
	if o.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	var e error
	o.BuildSHA256, e = executableDigest(ctx)
	if e != nil {
		fmt.Fprintln(stderr, e)
		return 1
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, e := receiptgraph.Run(ctx, o)
	if e != nil {
		fmt.Fprintln(stderr, "receipt graph:", e)
		return 1
	}
	if e = encodeJSON(stdout, r); e != nil {
		fmt.Fprintln(stderr, e)
		return 1
	}
	return 0
}
