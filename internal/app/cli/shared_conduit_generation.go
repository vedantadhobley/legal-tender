package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func runSharedConduitGeneration(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("publish-shared-conduit-generation", flag.ContinueOnError)
	f.SetOutput(stderr)
	var base fundinggeneration.ReadOptions
	var o receiptgraph.SharedOptions
	var passwordEnv string
	registerFundingReadFlags(f, &base, &passwordEnv)
	f.StringVar(&o.Calculation, "shared-conduits", "", "exact complete v2 conduit manifest")
	f.StringVar(&o.CalculationID, "expected-shared-conduit-id", "", "v2 calculation identity")
	f.StringVar(&o.PublicationDirectory, "publication-dir", "", "immutable extension publication root")
	f.StringVar(&o.LockDirectory, "lock-dir", "", "shared publisher lock directory")
	f.StringVar(&o.ArangoDataDirectory, "arango-data-dir", "", "actual Arango data directory mounted read-only")
	f.Uint64Var(&o.ReserveFreeBytes, "reserve-free-bytes", 0, "required filesystem free-space reserve")
	f.Uint64Var(&o.MaxFilesystemGrowthBytes, "max-filesystem-growth-bytes", 0, "required net filesystem growth cap")
	f.Uint64Var(&o.MaxEncodedBytes, "max-encoded-bytes", 0, "required encoded document cap")
	f.IntVar(&o.Workers, "workers", 8, "bounded import/readback workers, 1..8")
	f.IntVar(&o.BatchSize, "batch-size", 2000, "bounded batch rows, 1..5000")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected arguments")
		return 2
	}
	for _, v := range []string{base.Generation, base.GenerationSHA256, base.StorageRoot, base.GraphManifest, base.Participants, base.Conduits, base.Endpoint, o.Calculation, o.CalculationID, o.PublicationDirectory, o.LockDirectory, o.ArangoDataDirectory} {
		if v == "" {
			fmt.Fprintln(stderr, "exact base, shared inputs and bounded publication paths required")
			return 2
		}
	}
	if o.ReserveFreeBytes == 0 || o.MaxFilesystemGrowthBytes == 0 || o.MaxEncodedBytes == 0 || o.Workers < 1 || o.Workers > 8 || o.BatchSize < 1 || o.BatchSize > 5000 {
		fmt.Fprintln(stderr, "explicit storage caps and bounded workers/batches required")
		return 2
	}
	base.Password = os.Getenv(passwordEnv)
	if base.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	var err error
	base.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	o.BuildSHA256 = base.BuildSHA256
	base.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	o.Progress = base.Progress
	r, err := fundinggeneration.OpenReader(ctx, base)
	if err != nil {
		fmt.Fprintln(stderr, "base generation:", err)
		return 1
	}
	result, err := r.PublishShared(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "shared conduit generation:", err)
		return 1
	}
	fmt.Fprintf(stderr, "shared graph complete; read_only_replay=%t; manifest=%s\n", result.Publication.Reused, result.Publication.Manifest)
	if err = encodeJSON(stdout, result.Generation); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
