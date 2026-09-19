package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func runReceiptWindow(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runReceiptComparison(ctx, "compare-receipt-report-window", args, stdout, stderr)
}

func runReceiptComparison(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var r fundingbasis.ReceiptWindowRequest
	f.StringVar(&r.Summary.StorageRoot, "storage-root", "", "published storage root (read-only)")
	f.StringVar(&r.Summary.SummaryManifest, "summary-facts", "", "exact published summary manifest")
	f.StringVar(&r.Summary.Window.Membership.CapturePath, "capture", "", "retained single-committee report capture")
	f.StringVar(&r.Summary.Window.Membership.Start, "start", "", "inclusive requested start, YYYY-MM-DD")
	f.StringVar(&r.Summary.Window.Membership.End, "end", "", "inclusive requested end, YYYY-MM-DD")
	f.StringVar(&r.Summary.Window.DocumentsPath, "documents", "", "pinned document-set descriptor")
	f.StringVar(&r.ProfilePath, "profile", "", "retained complete v2 occurrence profile")
	f.StringVar(&r.ProfileSHA256, "profile-sha256", "", "expected profile byte digest")
	version := "v1"
	if command == "compare-receipt-families" {
		f.StringVar(&version, "comparison-version", "v1", "reviewed report comparison: v1 or v2 (includes thresholded components)")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if version != "v1" && version != "v2" {
		fmt.Fprintln(stderr, "--comparison-version must be v1 or v2")
		return 2
	}
	if f.NArg() != 0 || r.Summary.StorageRoot == "" || r.Summary.SummaryManifest == "" || r.Summary.Window.Membership.CapturePath == "" || r.Summary.Window.Membership.Start == "" || r.Summary.Window.Membership.End == "" || r.Summary.Window.DocumentsPath == "" || r.ProfilePath == "" || r.ProfileSHA256 == "" {
		fmt.Fprintln(stderr, "require --storage-root, --summary-facts, --capture, --documents, --start, --end, --profile, --profile-sha256 and no positional arguments")
		return 2
	}
	var rv any
	var err error
	if command == "compare-receipt-family-summary" {
		rv, err = fundingbasis.CompareReceiptFamilySummary(ctx, r)
	} else if command == "compare-receipt-family-window" {
		rv, err = fundingbasis.CompareReceiptFamilyWindow(ctx, r)
	} else if command == "review-receipt-family-absence" {
		rv, err = fundingbasis.ReviewFamilyAbsence(ctx, r)
	} else if command == "compare-receipt-families" {
		if version == "v2" {
			rv, err = fundingbasis.CompareReceiptFamiliesV2(ctx, r)
		} else {
			rv, err = fundingbasis.CompareReceiptFamilies(ctx, r)
		}
	} else {
		rv, err = fundingbasis.CompareReceiptWindow(ctx, r)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := encodeJSON(stdout, rv); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
