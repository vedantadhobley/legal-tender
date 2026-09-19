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

func runFundingWindow(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runFundingWindowQuery(ctx, args, stdout, stderr, false)
}

func runFundingWindowQuery(ctx context.Context, args []string, stdout, stderr io.Writer, connections bool) int {
	name := "inspect-funding-window-paths"
	if connections {
		name = "inspect-funding-window-connections"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o fundinggeneration.WindowOpenOptions
	var q fundinggeneration.WindowPathQuery
	var connection fundinggeneration.WindowConnectionQuery
	var spec, specSHA, passwordEnv, ledger, start, end, expected string
	f.StringVar(&spec, "inputs", "", "explicit generation/receipt locator specification")
	f.StringVar(&specSHA, "expected-inputs-sha256", "", "exact specification checksum")
	f.StringVar(&o.StorageRoot, "storage-root", "", "retained evidence root")
	f.StringVar(&o.Endpoint, "endpoint", "", "Arango endpoint")
	f.StringVar(&o.Username, "username", "root", "Arango username")
	f.StringVar(&passwordEnv, "password-env", "ARANGO_PASSWORD", "configured password environment variable")
	f.StringVar(&q.From, "from-committee", "", "exact starting committee ID")
	f.StringVar(&q.Target, "target", "", "exact target ID; candidate requires connection mode and ending family")
	if connections {
		f.StringVar(&connection.EntryGeneration, "receipt-generation", "", "exact supplied generation ID for receipt occurrence")
		f.Uint64Var(&connection.ReceiptOrdinal, "receipt-ordinal", 0, "exact Schedule A source row within receipt generation")
		f.StringVar(&connection.EntryFamily, "entry-family", "", "reported_receipt, conduit_association or shared_conduit_association")
		f.StringVar(&connection.Ending, "ending-family", "", "candidate_authorization_context, independent_support or independent_opposition")
		f.StringVar(&connection.SpendingDate, "spending-date-field", "", "required for Schedule E: expenditure or dissemination; no fallback")
	}
	f.StringVar(&ledger, "ledger", "", "one selected schedule_a or schedule_b ledger")
	f.StringVar(&start, "start-date", "", "inclusive reported date; requires end-date")
	f.StringVar(&end, "end-date", "", "inclusive reported date; requires start-date")
	f.IntVar(&q.MaxHops, "max-committee-hops", 4, "0 through 8")
	f.IntVar(&q.Limit, "max-paths", 3, "1 through 10")
	f.Uint64Var(&q.Budget, "max-expansions", 10000, "1 through 100000")
	f.StringVar(&expected, "expected-result-id", "", "exact fresh replay identity")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	q.Ledger = flow.Ledger(ledger)
	if start != "" || end != "" {
		q.Window = &fundinggeneration.DateWindow{Start: start, End: end}
	}
	if f.NArg() != 0 || spec == "" || specSHA == "" || o.StorageRoot == "" || o.Endpoint == "" || passwordEnv == "" {
		fmt.Fprintln(stderr, "explicit input specification, storage and connection required")
		return 2
	}
	validate := q.Validate
	if connections {
		connection.From, connection.Target, connection.Ledger, connection.Window = q.From, q.Target, q.Ledger, q.Window
		connection.MaxHops, connection.Limit, connection.Budget = q.MaxHops, q.Limit, q.Budget
		validate = connection.Validate
	}
	if err := validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	inputs, err := fundinggeneration.ReadWindowInputs(spec, specSHA)
	if err != nil {
		fmt.Fprintln(stderr, "window inputs:", err)
		return 2
	}
	o.Inputs = inputs.Inputs
	o.Password = os.Getenv(passwordEnv)
	if o.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := fundinggeneration.OpenWindowReader(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "funding window:", err)
		return 1
	}
	var out any
	var resultID string
	if connections {
		var result fundinggeneration.WindowConnectionsResult
		result, err = r.ConnectionPaths(ctx, connection)
		out, resultID = result, result.ResultID
	} else {
		var result fundinggeneration.WindowPathsResult
		result, err = r.Paths(ctx, q)
		out, resultID = result, result.ResultID
	}
	if err != nil {
		fmt.Fprintln(stderr, "funding window:", err)
		return 1
	}
	if expected != "" && resultID != expected {
		fmt.Fprintln(stderr, "funding window replay identity differs")
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
