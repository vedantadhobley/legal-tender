package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/summarypublication"
)

func runVerifyCommitteeSummary(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify-committee-summary", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var input string
	var expected committeesummary.Expected
	flags.StringVar(&input, "input", "", "captured committee-summary CSV (read-only)")
	flags.StringVar(&expected.Cycle, "cycle", "", "expected source cycle")
	flags.StringVar(&expected.SHA256, "expected-sha256", "", "SHA-256 from capture evidence")
	flags.Int64Var(&expected.Bytes, "expected-bytes", 0, "byte count from capture evidence (at most 16 MiB)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || input == "" || committeesummary.ValidateExpected(expected) != nil {
		fmt.Fprintln(stderr, "require --input, --cycle, --expected-sha256, and --expected-bytes")
		return 2
	}
	file, err := os.Open(input)
	if err != nil {
		fmt.Fprintln(stderr, "committee summary:", err)
		return 1
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() != expected.Bytes {
		fmt.Fprintln(stderr, "committee summary input must be a regular file of the expected size")
		return 1
	}
	result, err := committeesummary.Verify(ctx, file, expected, nil)
	if err != nil {
		fmt.Fprintln(stderr, "committee summary:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stderr, "complete: committee-summary source scan; typed issues retained; no publication or terminal allocation")
	return 0
}

func runPublishCommitteeSummary(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-committee-summary", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var options summarypublication.Options
	flags.StringVar(&options.StorageRoot, "storage-root", "", "root of durable Legal Tender storage")
	flags.StringVar(&options.ReleasePath, "release", "", "exact published coordinated v4 release manifest")
	flags.StringVar(&options.Cycle, "cycle", "", "source cycle contained in the release")
	flags.StringVar(&options.RunID, "run-id", "", "publication run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || options.StorageRoot == "" || options.ReleasePath == "" || options.Cycle == "" || !release.ValidAcquisitionRunID(options.RunID) {
		fmt.Fprintln(stderr, "require --storage-root, --release, --cycle, and --run-id")
		return 2
	}
	result, err := summarypublication.Publish(ctx, options)
	if err != nil {
		fmt.Fprintln(stderr, "publish committee summary:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stderr, "published: immutable committee-summary occurrences and facts; no financial grouping or terminal allocation")
	return 0
}
