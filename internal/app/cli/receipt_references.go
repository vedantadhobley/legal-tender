package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	"io"
)

func runReceiptReferences(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("join-receipt-references", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptreferences.Options
	f.StringVar(&o.StorageRoot, "storage-root", "", "read-only retained source root")
	f.StringVar(&o.Manifest, "schedule-a-facts", "", "exact immutable fact manifest; no latest pointer")
	f.StringVar(&o.Cycle, "cycle", "", "source cycle")
	f.StringVar(&o.OutputDirectory, "output-dir", "", "new isolated result/workspace directory")
	f.IntVar(&o.RunRows, "run-rows", 100000, "sort run row cap, at most 100000")
	f.IntVar(&o.FanIn, "merge-fan-in", 8, "merge input cap, 2..16")
	f.IntVar(&o.ScanWorkers, "scan-workers", 8, "source reader cap, 1..8; shared process memory and CPU budget")
	f.IntVar(&o.Workers, "workers", 8, "complete-report processing workers, 1..8; shared workspace and filter budgets")
	f.IntVar(&o.FilterBytes, "filter-bytes", 64<<20, "fixed candidate filter bytes, at most 256MiB")
	f.Uint64Var(&o.MaxWorkspaceBytes, "max-workspace-bytes", 16<<30, "live temporary+retained sort bytes, at most 64GiB")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.Manifest == "" || o.Cycle == "" || o.OutputDirectory == "" {
		fmt.Fprintln(stderr, "exact source, cycle and new output directory required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.BuildSHA256 = build
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := receiptreferences.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "cycle reference join:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
