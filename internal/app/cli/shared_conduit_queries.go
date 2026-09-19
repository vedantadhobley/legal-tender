package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
)

func runSharedConduitQueries(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("validate-shared-conduit-queries", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o gen.ReadOptions
	var shared gen.SharedReadOptions
	var passwordEnv, expected string
	registerFundingReadFlags(f, &o, &passwordEnv)
	registerSharedReadFlags(f, &shared)
	f.StringVar(&expected, "expected-gate-id", "", "exact fresh replay identity")
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
	for _, v := range []string{o.Generation, o.GenerationSHA256, o.StorageRoot, o.GraphManifest, o.Participants, o.Conduits, o.Endpoint, shared.BaseGeneration, shared.GraphManifest, shared.Conduits} {
		if v == "" {
			fmt.Fprintln(stderr, "exact base and extended generation locators required")
			return 2
		}
	}
	o.Password = os.Getenv(passwordEnv)
	if o.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, time.Now().UTC().Format(time.RFC3339), s) }
	r, err := gen.OpenQueryReader(ctx, o, shared)
	if err != nil {
		fmt.Fprintln(stderr, "shared query generation:", err)
		return 1
	}
	out, err := r.ValidateSharedQueries(ctx, expected, o.Progress)
	if err != nil {
		fmt.Fprintln(stderr, "shared query gate:", err)
		return 1
	}
	if err = encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
