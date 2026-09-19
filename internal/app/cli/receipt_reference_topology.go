package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
)

func runReceiptReferenceTopology(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("build-receipt-reference-topology", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptreferences.TopologyOptions
	f.StringVar(&o.ReferenceManifest, "reference-manifest", "", "exact completed reference manifest.json; backing is read-only")
	f.StringVar(&o.ExpectedReferenceID, "expected-reference-id", "", "required exact reference calculation ID")
	f.StringVar(&o.OutputDirectory, "output-dir", "", "new isolated result/workspace directory")
	f.IntVar(&o.RunRows, "run-rows", 100000, "sort run row cap, at most 100000")
	f.IntVar(&o.FanIn, "merge-fan-in", 8, "merge input cap, 2..16")
	f.Uint64Var(&o.MaxWorkspaceBytes, "max-workspace-bytes", 4<<30, "live temporary+retained output bytes, at most 64GiB")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.ReferenceManifest == "" || o.ExpectedReferenceID == "" || o.OutputDirectory == "" {
		fmt.Fprintln(stderr, "exact reference manifest, expected ID and new output directory required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.BuildSHA256 = build
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := receiptreferences.RunTopology(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "receipt reference topology:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
