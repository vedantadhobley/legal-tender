package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/audit/receiptindex"
)

func runReceiptIndexBenchmark(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("benchmark-receipt-reference-index", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptindex.Options
	f.StringVar(&o.StorageRoot, "storage-root", "", "read-only published source root")
	f.StringVar(&o.Manifest, "schedule-a-facts", "", "exact Schedule A fact manifest")
	f.StringVar(&o.Cycle, "cycle", "", "required source cycle")
	f.StringVar(&o.OutputDirectory, "output-dir", "", "new benchmark-only directory, never overwritten")
	f.IntVar(&o.ShardIndex, "shard-index", -1, "explicit source shard index (zero-based)")
	f.IntVar(&o.MaxRows, "max-rows", 100000, "selected shard prefix, at most 1000000 rows")
	f.IntVar(&o.RunRows, "run-rows", 100000, "bounded sort run, at most 100000 rows")
	f.Uint64Var(&o.MaxOutputBytes, "max-output-bytes", 256<<20, "cumulative layout file byte budget, at most 1 GiB")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.Manifest == "" || o.Cycle == "" || o.OutputDirectory == "" || o.ShardIndex < 0 {
		fmt.Fprintln(stderr, "storage root, exact facts, cycle, explicit shard index and new output directory required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.BuildSHA256 = build
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	result, err := receiptindex.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "reference-index benchmark:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
