package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	view "github.com/vedantadhobley/legal-tender/internal/presentation/candidateevidence"
)

func runCandidateConnection(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("inspect-candidate-connection", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var root, parent, expected, report string
	var ordinal uint64
	flags.StringVar(&root, "storage-root", "", "published source storage root (read-only)")
	flags.StringVar(&parent, "candidate-report", "", "retained v2 candidate report JSON")
	flags.StringVar(&expected, "expected-report-id", "", "exact report ID to inspect; never substitute a newer report")
	flags.Uint64Var(&ordinal, "source-row-ordinal", 0, "exact Schedule A witness or candidate-boundary ordinal in the report")
	flags.StringVar(&report, "report", "", "optional new Markdown drilldown path; never overwritten")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || root == "" || parent == "" || expected == "" || ordinal == 0 {
		fmt.Fprintln(stderr, "storage, candidate report, expected report ID and source row ordinal required")
		return 2
	}
	if report != "" {
		if _, err := os.Lstat(report); !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(stderr, "report path already exists or cannot be inspected")
			return 1
		}
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	fmt.Fprintln(stderr, "checking pinned report and published source ancestry; then seeking the selected row")
	c, err := view.InspectConnection(ctx, root, parent, expected, ordinal, build)
	if err != nil {
		fmt.Fprintln(stderr, "connection source:", err)
		return 1
	}
	if report != "" {
		if err := writeNewReport(report, func(w io.Writer) error { return view.WriteConnectionMarkdown(w, c) }); err != nil {
			fmt.Fprintln(stderr, "connection report:", err)
			return 1
		}
	}
	if err := encodeJSON(stdout, c); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: one verified source row; connection %s; no financial-use promotion\n", c.ConnectionID)
	return 0
}
