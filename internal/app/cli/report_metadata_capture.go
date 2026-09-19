package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

func runCaptureReportMetadata(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("capture-report-metadata", flag.ContinueOnError)
	f.SetOutput(stderr)
	path := f.String("request", "", "closed metadata scope and explicit budgets JSON")
	dir := f.String("output-dir", "", "new private capture directory; parent must exist")
	keyEnv := f.String("api-key-env", "ELECTION_API_KEY", "environment variable name, never the key itself")
	demo := f.Bool("demo-key", false, "use the public DEMO_KEY for a small manual test")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	customEnv := false
	f.Visit(func(f *flag.Flag) {
		if f.Name == "api-key-env" {
			customEnv = true
		}
	})
	if f.NArg() != 0 || *path == "" || *dir == "" || (*demo && customEnv) || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(*keyEnv) {
		fmt.Fprintln(stderr, "require --request, --output-dir, and either --demo-key or an API key environment variable")
		return 2
	}
	r, err := reportmetadata.ReadFetchRequest(*path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	key := "DEMO_KEY"
	if !*demo {
		key = os.Getenv(*keyEnv)
	}
	result, err := reportmetadata.Fetch(ctx, *dir, r, key)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, "cannot write metadata capture result")
		return 1
	}
	if result.State != "captured" {
		fmt.Fprintln(stderr, "metadata capture did not complete; inspect its retained result")
		return 1
	}
	return 0
}
