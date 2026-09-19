package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	audit "github.com/vedantadhobley/legal-tender/internal/audit/personaffiliation"
	affiliation "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

type affiliationReport struct {
	Selection audit.AppearanceSelection    `json:"selection"`
	Report    affiliation.EnrichmentResult `json:"report"`
}

func runAffiliationEnrichment(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("enrich-affiliations", flag.ContinueOnError)
	f.SetOutput(stderr)
	var corpus, corpusPin, capture, capturePin, output, agent, policy string
	var sampleSize int
	f.StringVar(&corpus, "corpus", "", "retained FEC source corpus directory")
	f.StringVar(&corpusPin, "expected-corpus-sha256", "", "exact corpus digest")
	f.StringVar(&capture, "capture", "", "existing discovery capture; offline replay")
	f.StringVar(&capturePin, "expected-capture-sha256", "", "exact capture digest for offline replay")
	f.StringVar(&output, "output", "", "new discovery capture directory for a live run")
	f.StringVar(&agent, "user-agent", "", "descriptive Wikimedia user agent for a live run")
	f.StringVar(&policy, "query-policy", wikimedia.DiscoveryPolicy, "retrieval policy for a live run")
	f.IntVar(&sampleSize, "sample-size", 0, "0 uses reviewed FEC rows; 1..5 selects additional name/employer pairs by hash")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	live := output != ""
	policySpecified := false
	f.Visit(func(flag *flag.Flag) {
		if flag.Name == "query-policy" {
			policySpecified = true
		}
	})
	if f.NArg() != 0 || corpus == "" || !wikimedia.Digest(corpusPin) || sampleSize < 0 || sampleSize > 5 ||
		(live && (capture != "" || capturePin != "" || agent == "")) ||
		(!live && (capture == "" || !wikimedia.Digest(capturePin) || agent != "" || policySpecified)) ||
		(policy != wikimedia.DiscoveryPolicy && policy != wikimedia.DiscoveryVariantsPolicy) {
		fmt.Fprintln(stderr, "use a verified FEC corpus and either capture+digest for replay or output+user-agent for live enrichment; sample-size 0..5")
		return 2
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	selection, err := audit.SelectAppearances(ctx, corpus, corpusPin, sampleSize)
	if err != nil {
		fmt.Fprintln(stderr, "affiliation source selection:", err)
		return 1
	}
	if live {
		plan, err := selection.DiscoveryPlan(build, policy)
		if err != nil {
			fmt.Fprintln(stderr, "affiliation discovery plan:", err)
			return 1
		}
		raw, err := json.Marshal(plan)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		capturePin, err = wikimedia.CaptureDiscovery(ctx, raw, wikimedia.CaptureOptions{Directory: output, UserAgent: agent, BuildSHA256: build})
		if err != nil {
			fmt.Fprintln(stderr, "affiliation capture:", err)
			return 1
		}
		capture = output
	}
	discovery, err := wikimedia.ReadDiscovery(capture, capturePin)
	if err != nil {
		fmt.Fprintln(stderr, "affiliation replay:", err)
		return 1
	}
	report, err := affiliation.EnrichAppearances(selection.Appearances, discovery, build)
	if err != nil {
		fmt.Fprintln(stderr, "affiliation enrichment:", err)
		return 1
	}
	if err := encodeJSON(stdout, affiliationReport{Selection: selection, Report: report}); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if !report.CaptureUsable {
		return 1
	}
	return 0
}
