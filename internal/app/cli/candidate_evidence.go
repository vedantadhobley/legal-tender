package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	view "github.com/vedantadhobley/legal-tender/internal/presentation/candidateevidence"
)

func runCandidateEvidence(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("build-candidate-evidence", flag.ContinueOnError)
	flags.SetOutput(stderr)
	o := fundingbasis.CandidateEvidenceOptions{}
	var report, version, candidateNames string
	flags.StringVar(&o.StorageRoot, "storage-root", "", "published source storage root (read-only)")
	flags.StringVar(&o.BasisResult, "basis-result", "", "exact verified receipt inventory JSON")
	flags.StringVar(&o.Bundle, "observation-bundle", "", "exact committee-flow evidence bundle")
	flags.StringVar(&o.Linkages, "linkage-facts", "", "exact source-matched linkage manifest")
	flags.StringVar(&o.Cycle, "cycle", "", "expected FEC source cycle")
	flags.StringVar(&o.Candidate, "candidate", "", "exact candidate ID")
	flags.StringVar(&o.SummaryFacts, "committee-summary-facts", "", "optional pinned summary context, never replacement receipts")
	flags.StringVar(&report, "report", "", "optional new Markdown report path; never overwritten")
	flags.StringVar(&version, "view-version", "v1", "v1 evidence or v2 evidence with source-backed names and path examples")
	flags.StringVar(&candidateNames, "candidate-master-facts", "", "optional pinned candidate-name context (v2 only)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if version != "v1" && version != "v2" || version != "v2" && candidateNames != "" {
		fmt.Fprintln(stderr, "view version must be v1 or v2; candidate-name context requires v2")
		return 2
	}
	if flags.NArg() != 0 || o.StorageRoot == "" || o.BasisResult == "" || o.Bundle == "" || o.Linkages == "" || !validPeriod(o.Cycle) || o.Candidate == "" {
		fmt.Fprintln(stderr, "storage root, inventory, observation bundle, linkages, cycle, and candidate required")
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
	o.ExecutableSHA256 = build
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := fundingbasis.BuildCandidateEvidence(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "candidate evidence:", err)
		return 1
	}
	var result any = r
	render := func(w io.Writer) error { return fundingbasis.WriteCandidateEvidenceReport(w, r) }
	if version == "v2" {
		v, err := view.Build(ctx, o.StorageRoot, candidateNames, r)
		if err != nil {
			fmt.Fprintln(stderr, "candidate presentation:", err)
			return 1
		}
		result = v
		render = func(w io.Writer) error { return view.WriteMarkdown(w, v) }
	}
	if report != "" {
		if err := writeNewReport(report, render); err != nil {
			fmt.Fprintln(stderr, "candidate report:", err)
			return 1
		}
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "complete: %d reached committees; integrated evidence %s; terminal and allocation policies not selected\n", r.Overview.ReachedCommittees, r.ResultID)
	return 0
}

func executableDigest(ctx context.Context) (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buffer := make([]byte, 256<<10)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buffer)
		if n > 0 {
			h.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if n == 0 {
			return "", io.ErrNoProgress
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeNewCandidateReport(path string, r fundingbasis.CandidateEvidence) error {
	return writeNewReport(path, func(w io.Writer) error { return fundingbasis.WriteCandidateEvidenceReport(w, r) })
}

func writeNewReport(path string, render func(io.Writer) error) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	writeErr := render(f)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		// Only remove the incomplete file created by this invocation.
		return errors.Join(err, os.Remove(path))
	}
	return nil
}
