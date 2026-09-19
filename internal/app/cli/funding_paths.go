package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
)

func runFundingPaths(ctx context.Context, args []string, stdout, stderr io.Writer, gate bool) int {
	name := "inspect-funding-paths"
	if gate {
		name = "validate-funding-paths"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o fundinggeneration.ReadOptions
	var shared fundinggeneration.SharedReadOptions
	var q fundinggeneration.PathQuery
	var passwordEnv, ledger, expected string
	registerFundingReadFlags(f, &o, &passwordEnv)
	registerSharedReadFlags(f, &shared)
	if gate {
		f.StringVar(&expected, "expected-gate-id", "", "exact fresh replay identity")
	} else {
		f.StringVar(&q.From, "from-committee", "", "starting committee; excludes receipt ordinal")
		f.Uint64Var(&q.ReceiptOrdinal, "receipt-ordinal", 0, "exact pinned Schedule A source row")
		f.StringVar(&q.EntryFamily, "entry-family", "", "reported_receipt, conduit_association or shared_conduit_association")
		f.StringVar(&ledger, "ledger", "", "one selected schedule_a or schedule_b ledger")
		f.StringVar(&q.Target, "target", "", "exact committee or candidate ID")
		f.StringVar(&q.Ending, "ending-family", "", "candidate_authorization_context, independent_support or independent_opposition")
		f.IntVar(&q.MaxHops, "max-committee-hops", 4, "0 through 8; excludes entry and ending")
		f.IntVar(&q.Limit, "max-paths", 3, "1 through 10; more paths remain explicit")
		f.Uint64Var(&q.Budget, "max-expansions", 10000, "1 through 100000 examined topology links")
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
	q.Ledger = flow.Ledger(ledger)
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
		fmt.Fprintln(stderr, "funding paths:", err)
		return 1
	}
	var out any
	if gate {
		out, err = r.ValidatePaths(ctx, expected, o.Progress)
	} else {
		out, err = r.Paths(ctx, q)
	}
	if err != nil {
		fmt.Fprintln(stderr, "funding paths:", err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
