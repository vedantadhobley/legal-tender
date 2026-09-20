package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	comparison "github.com/vedantadhobley/legal-tender/internal/calculation/fec/terminalpolicycomparison"
	view "github.com/vedantadhobley/legal-tender/internal/presentation/candidateevidence"
)

func runTerminalPolicyComparison(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("compare-terminal-policies", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var dossierPath, expectedID string
	flags.StringVar(&dossierPath, "candidate-dossier", "", "exact candidate dossier JSON")
	flags.StringVar(&expectedID, "expected-dossier-id", "", "exact dossier ID; never substitute another dossier")
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
	if dossierPath == "" || expectedID == "" {
		fmt.Fprintln(stderr, "candidate dossier and expected dossier ID required")
		return 2
	}
	dossier, dossierSHA, err := view.OpenDossier(ctx, dossierPath, expectedID)
	if err != nil {
		fmt.Fprintln(stderr, "terminal-policy comparison dossier:", err)
		return 1
	}
	executable, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	committees := make([]comparison.CommitteeInput, 0, len(dossier.Receipts.CandidateLinkedCommittees))
	for _, committee := range dossier.Receipts.CandidateLinkedCommittees {
		committees = append(committees, comparison.CommitteeInput{
			CommitteeID: committee.CommitteeID, Authorization: committee.Authorization, Receipts: committee.Receipts,
		})
	}
	result, err := comparison.Compare(ctx, comparison.Input{
		CandidateID: dossier.CandidateID, Cycle: dossier.Cycle, ExecutableSHA256: executable,
		Reference: comparison.InputReference{
			DossierID: dossier.DossierID, DossierSHA256: dossierSHA,
			ReceiptFactSetID: dossier.Inputs.ReceiptFactSetID, ReceiptManifestSHA: dossier.Inputs.ReceiptManifestSHA256,
			ReceiptSourceRelease: dossier.Inputs.ReceiptFECSourceReleaseID,
		},
		Committees: committees,
	})
	if err != nil {
		fmt.Fprintln(stderr, "terminal-policy comparison:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d conserving scenarios; comparison %s; no terminal or allocation policy selected\n", len(result.Scenarios), result.ComparisonID)
	return 0
}
