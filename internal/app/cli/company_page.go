package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

func runCompanyPage(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("extract-company-page", flag.ContinueOnError)
	f.SetOutput(stderr)
	var body string
	var propose bool
	var source companypage.Source
	f.StringVar(&body, "body", "", "retained company HTML; offline read only")
	f.StringVar(&source.SHA256, "expected-body-sha256", "", "exact retained body digest")
	f.StringVar(&source.URL, "source-url", "", "explicit page provenance, not a fetch target or verified identity")
	f.StringVar(&source.ObservedOn, "observed-on", "", "page observation YYYY-MM-DD, not role validity")
	f.BoolVar(&propose, "propose-relationships", false, "opt-in bounded prose role/alias syntax candidates; no identity acceptance")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || body == "" || source.Validate() != nil {
		fmt.Fprintln(stderr, "body, exact SHA-256, source URL and observation day required")
		return 2
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	st, err := os.Lstat(body)
	if err != nil || !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > companypage.MaxBody {
		fmt.Fprintln(stderr, "company page requires a bounded regular body file")
		return 1
	}
	fp, err := os.Open(body)
	if err != nil {
		fmt.Fprintln(stderr, "cannot open company page")
		return 1
	}
	raw, readErr := io.ReadAll(io.LimitReader(fp, companypage.MaxBody+1))
	closeErr := fp.Close()
	if readErr != nil || closeErr != nil {
		fmt.Fprintln(stderr, "cannot read company page")
		return 1
	}
	evidence, err := companypage.Extract(ctx, raw, source)
	if err != nil {
		fmt.Fprintln(stderr, "company page:", err)
		return 1
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	result := struct {
		BuildSHA256 string                         `json:"build_sha256"`
		Evidence    companypage.Evidence           `json:"evidence"`
		Proposals   *personaffiliation.ProseResult `json:"proposals,omitempty"`
	}{BuildSHA256: build, Evidence: evidence}
	if propose {
		proposals, err := personaffiliation.ProposeProse(ctx, evidence)
		if err != nil {
			fmt.Fprintln(stderr, "prose candidates:", err)
			return 1
		}
		result.Proposals = &proposals
	}
	if err = encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
