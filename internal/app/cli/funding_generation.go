package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
)

func runFundingGeneration(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("verify-funding-generation", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o fundinggeneration.Options
	var passwordEnv string
	f.StringVar(&o.ReceiptManifest, "graph-manifest", "", "exact completed receipt graph manifest")
	f.StringVar(&o.ReceiptSHA256, "expected-graph-sha256", "", "receipt manifest checksum")
	f.StringVar(&o.FlowBundle, "flow-bundle", "", "immutable committee-flow bundle")
	f.StringVar(&o.FlowBundleSHA256, "expected-flow-bundle-sha256", "", "flow bundle checksum")
	f.StringVar(&o.OutsideBundle, "outside-bundle", "", "immutable resolved independent-expenditure bundle")
	f.StringVar(&o.OutsideBundleSHA256, "expected-outside-bundle-sha256", "", "outside bundle checksum")
	f.StringVar(&o.ExpectedGenerationID, "expected-generation-id", "", "optional replay identity")
	registerReceiptSourceFlags(f, &o.Receipt, &passwordEnv)
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected arguments")
		return 2
	}
	for _, v := range []string{o.ReceiptManifest, o.ReceiptSHA256, o.FlowBundle, o.FlowBundleSHA256, o.OutsideBundle, o.OutsideBundleSHA256, o.Receipt.StorageRoot, o.Receipt.Participants, o.Receipt.ParticipantID, o.Receipt.Conduits, o.Receipt.ConduitID, o.Receipt.Facts, o.Receipt.Committees, o.Receipt.Candidates, o.Receipt.Linkages, o.Receipt.Endpoint} {
		if v == "" {
			fmt.Fprintln(stderr, "all exact receipt, flow and outside-spending inputs required")
			return 2
		}
	}
	o.Receipt.Password = os.Getenv(passwordEnv)
	if o.Receipt.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	o.Receipt.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	out, err := fundinggeneration.Verify(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "funding generation:", err)
		return 1
	}
	if err = encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
