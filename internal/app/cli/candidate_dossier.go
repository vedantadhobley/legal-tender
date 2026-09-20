package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	view "github.com/vedantadhobley/legal-tender/internal/presentation/candidateevidence"
)

func runCandidateDossier(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("build-candidate-dossier", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var root, parent, expected, interpretations string
	flags.StringVar(&root, "storage-root", "", "published source storage root (read-only)")
	flags.StringVar(&parent, "candidate-report", "", "retained v2 candidate report JSON")
	flags.StringVar(&expected, "expected-report-id", "", "exact v2 report ID; never substitute a newer report")
	flags.StringVar(&interpretations, "candidate-interpretations", "", "exact published candidate-interpretation manifest or verified current pointer")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if root == "" || parent == "" || expected == "" || interpretations == "" {
		fmt.Fprintln(stderr, "storage, candidate report, expected report ID, and candidate interpretations required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	fmt.Fprintln(stderr, "verifying the exact receipt report and complete candidate-interpretation publication")
	dossier, err := view.BuildDossier(ctx, root, parent, expected, interpretations, build)
	if err != nil {
		fmt.Fprintln(stderr, "candidate dossier:", err)
		return 1
	}
	if err := encodeJSON(stdout, dossier); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d relevant independent-expenditure rows; dossier %s; terminal attribution remains unset\n", dossier.OutsideSpending.RelevantRows, dossier.DossierID)
	return 0
}
