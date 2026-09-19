package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	"io"
	"os"
)

func runFundingNeighborhood(ctx context.Context, args []string, stdout, stderr io.Writer, gate bool) int {
	name := "inspect-funding-neighborhood"
	if gate {
		name = "validate-funding-neighborhoods"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o fundinggeneration.ReadOptions
	var shared fundinggeneration.SharedReadOptions
	var q fundinggeneration.Query
	var passwordEnv, expected string
	registerFundingReadFlags(f, &o, &passwordEnv)
	registerSharedReadFlags(f, &shared)
	if gate {
		f.StringVar(&expected, "expected-gate-id", "", "optional exact replay identity")
	} else {
		f.StringVar(&q.Entity, "entity", "", "exact FEC committee or candidate ID")
		f.StringVar(&q.Family, "family", "", "one declared family; default all")
		f.IntVar(&q.Limit, "limit", 2, "items per family, from 1 to 10")
		f.StringVar(&q.Cursor, "cursor", "", "family-scoped continuation")
	}
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
	if !gate {
		if err := q.Validate(); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	for _, v := range []string{o.Generation, o.GenerationSHA256, o.StorageRoot, o.GraphManifest, o.Participants, o.Conduits, o.Endpoint, passwordEnv} {
		if v == "" {
			fmt.Fprintln(stderr, "exact generation, retained locators and connection required")
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
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := fundinggeneration.OpenQueryReader(ctx, o, shared)
	if err != nil {
		fmt.Fprintln(stderr, "funding neighborhood:", err)
		return 1
	}
	var out any
	if gate {
		out, err = r.ValidateNeighborhoods(ctx, expected, o.Progress)
	} else {
		out, err = r.Neighborhood(ctx, q)
	}
	if err != nil {
		fmt.Fprintln(stderr, "funding neighborhood:", err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func registerSharedReadFlags(f *flag.FlagSet, o *fundinggeneration.SharedReadOptions) {
	f.StringVar(&o.BaseGeneration, "base-generation", "", "exact retained base generation; required for an extended generation")
	f.StringVar(&o.GraphManifest, "shared-graph-manifest", "", "exact retained shared-conduit graph manifest")
	f.StringVar(&o.Conduits, "shared-conduits", "", "exact retained shared-conduit calculation manifest")
}

func registerFundingReadFlags(f *flag.FlagSet, o *fundinggeneration.ReadOptions, passwordEnv *string) {
	f.StringVar(&o.Generation, "generation", "", "retained typed generation result")
	f.StringVar(&o.GenerationSHA256, "expected-generation-sha256", "", "exact generation file checksum")
	f.StringVar(&o.StorageRoot, "storage-root", "", "retained evidence root")
	f.StringVar(&o.GraphManifest, "graph-manifest", "", "retained receipt graph manifest locator")
	f.StringVar(&o.Participants, "participants", "", "retained participant manifest locator")
	f.StringVar(&o.Conduits, "conduits", "", "retained conduit manifest locator")
	f.StringVar(&o.Endpoint, "endpoint", "", "Arango endpoint")
	f.StringVar(&o.Username, "username", "root", "Arango username")
	f.StringVar(passwordEnv, "password-env", "ARANGO_PASSWORD", "configured password environment variable")
}
