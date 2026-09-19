package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
)

func runSharedReferenceReview(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("review-shared-receipt-references", flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptconduits.SharedReviewOptions
	f.StringVar(&o.StorageRoot, "storage-root", "", "exact source storage root")
	f.StringVar(&o.Facts, "schedule-a-facts", "", "immutable Schedule A fact manifest")
	f.StringVar(&o.Profile, "profile", "", "exact shared-reference-profile.json")
	f.StringVar(&o.ProfileID, "expected-profile-id", "", "required profile identity")
	f.StringVar(&o.Topology, "topology-manifest", "", "exact topology manifest")
	f.StringVar(&o.References, "reference-manifest", "", "exact reference join manifest")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.StorageRoot == "" || o.Facts == "" || o.Profile == "" || o.ProfileID == "" || o.Topology == "" || o.References == "" {
		fmt.Fprintln(stderr, "exact profile, topology, reference and original-source inputs required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := receiptconduits.ReviewSharedReferences(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "shared reference source review:", err)
		return 1
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil || len(b) > 32<<20 {
		fmt.Fprintln(stderr, "review output exceeds 32MiB or cannot encode:", err)
		return 1
	}
	if _, err = stdout.Write(append(b, '\n')); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
