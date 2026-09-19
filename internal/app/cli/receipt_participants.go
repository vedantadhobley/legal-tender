package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func runReceiptParticipants(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptparticipants.Options
	var shards, manifest, expected string
	var ordinal uint64
	f.StringVar(&o.StorageRoot, "storage-root", "", "retained source root, read-only")
	f.StringVar(&o.Manifest, "schedule-a-facts", "", "exact immutable Schedule A manifest")
	if command == "inspect-receipt-participant" {
		f.StringVar(&manifest, "participant-manifest", "", "exact participant manifest.json")
		f.StringVar(&expected, "expected-participant-id", "", "required participant calculation ID")
		f.Uint64Var(&ordinal, "source-row-ordinal", 0, "one-based source occurrence ordinal")
	} else {
		f.StringVar(&o.Cycle, "cycle", "", "exact source cycle")
		f.StringVar(&o.OutputDirectory, "output-dir", "", "new isolated publication directory")
		f.IntVar(&o.Workers, "workers", 8, "source-to-readback shard workers, 1..8")
		f.Uint64Var(&o.MaxOutputBytes, "max-output-bytes", 8<<30, "shared output data cap, at most 32GiB; 1MiB manifest reserve is separate")
		if command == "benchmark-receipt-participants" {
			f.StringVar(&shards, "shard-indices", "", "1..8 explicit comma-separated whole shard indices; never a complete publication")
		}
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.Manifest == "" {
		fmt.Fprintln(stderr, "exact source root and fact manifest required")
		return 2
	}
	if command == "inspect-receipt-participant" {
		if manifest == "" || expected == "" || ordinal == 0 {
			fmt.Fprintln(stderr, "exact participant manifest, expected ID and positive source ordinal required")
			return 2
		}
		r, err := receiptparticipants.Inspect(ctx, o.StorageRoot, o.Manifest, manifest, expected, ordinal)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err = encodeJSON(stdout, r); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	if o.Cycle == "" || o.OutputDirectory == "" {
		fmt.Fprintln(stderr, "cycle and new output directory required")
		return 2
	}
	var indices []int
	if command == "benchmark-receipt-participants" {
		if shards == "" {
			fmt.Fprintln(stderr, "explicit benchmark shard indices required")
			return 2
		}
		for _, s := range strings.Split(shards, ",") {
			n, err := strconv.Atoi(s)
			if err != nil {
				fmt.Fprintln(stderr, "invalid shard index")
				return 2
			}
			indices = append(indices, n)
		}
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.BuildSHA256 = build
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	var r receiptparticipants.Result
	if command == "benchmark-receipt-participants" {
		r, err = receiptparticipants.Benchmark(ctx, o, indices)
	} else {
		r, err = receiptparticipants.Run(ctx, o)
	}
	if err != nil {
		fmt.Fprintln(stderr, "receipt participants:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
