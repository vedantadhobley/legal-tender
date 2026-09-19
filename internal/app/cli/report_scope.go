package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

func runReportScope(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-report-scope", flag.ContinueOnError)
	f.SetOutput(stderr)
	var r reportscope.Request
	registerReportEvidenceFlags(f, &r)
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || !hasReportEvidence(r) {
		fmt.Fprintln(stderr, "require --source-url, --body, --body-sha256, --headers, --headers-sha256 and no positional arguments")
		return 2
	}
	out, err := reportscope.Assess(ctx, r)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	// Unresolved is a valid assessment, not transport success or financial readiness.
	return 0
}

func hasReportEvidence(r reportscope.Request) bool {
	return r.SourceURL != "" && r.BodyPath != "" && r.BodySHA256 != "" && r.HeadersPath != "" && r.HeadersSHA256 != ""
}

func registerReportEvidenceFlags(f *flag.FlagSet, r *reportscope.Request) {
	registerDocumentFlags(f, r)
	f.Func("metadata-capture", "validated local metadata capture; repeat at most four times", func(s string) error { r.MetadataCaptures = append(r.MetadataCaptures, s); return nil })
}

func registerDocumentFlags(f *flag.FlagSet, r *reportscope.Request) {
	f.StringVar(&r.SourceURL, "source-url", "", "recorded public docquery posted-file URL; never fetched")
	f.StringVar(&r.BodyPath, "body", "", "retained local document body")
	f.StringVar(&r.BodySHA256, "body-sha256", "", "expected exact body SHA-256")
	f.StringVar(&r.HeadersPath, "headers", "", "retained local HTTP response headers")
	f.StringVar(&r.HeadersSHA256, "headers-sha256", "", "expected exact header SHA-256")
}
