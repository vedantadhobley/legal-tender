package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	evaluation "github.com/vedantadhobley/legal-tender/internal/audit/organizationresolution"
	resolver "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func runOrganizationEvaluation(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("evaluate-organizations", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o evaluation.Options
	f.StringVar(&o.ProposalPolicy, "proposal-policy", resolver.Policy, "versioned name-proposal policy; not identity approval")
	f.StringVar(&o.CaptureDirectory, "capture", "", "saved capture directory; offline")
	f.StringVar(&o.CaptureSHA256, "expected-capture-sha256", "", "exact capture.json digest")
	f.StringVar(&o.CorpusPath, "corpus", "", "reviewed evaluation corpus JSON")
	f.StringVar(&o.CorpusSHA256, "expected-corpus-sha256", "", "exact corpus digest")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.CaptureDirectory == "" || o.CorpusPath == "" || !wikimedia.Digest(o.CaptureSHA256) || !wikimedia.Digest(o.CorpusSHA256) {
		fmt.Fprintln(stderr, "capture, corpus and both exact digests required")
		return 2
	}
	if !resolver.ValidPolicy(o.ProposalPolicy) {
		fmt.Fprintln(stderr, "unsupported organization proposal policy")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.BuildSHA256 = build
	r, err := evaluation.Run(o)
	if err != nil {
		fmt.Fprintln(stderr, "organization evaluation:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	// A completed diagnostic is not a quality gate or identity approval. Source
	// failures, incorrect proposals and unreviewed cases remain in the output.
	return 0
}
