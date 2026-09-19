package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/recovery/funding"
)

func runFundingRecoveryVerify(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("verify-funding-recovery-files", flag.ContinueOnError)
	f.SetOutput(stderr)
	var inputs, sha, expected string
	var o funding.VerifyOptions
	f.StringVar(&inputs, "inputs", "", "exact generation and dependency locator specification")
	f.StringVar(&sha, "expected-inputs-sha256", "", "required specification byte digest")
	f.StringVar(&o.StorageRoot, "storage-root", "", "read-only retained publication root")
	f.Uint64Var(&o.MaxBytes, "max-bytes", 0, "required ceiling for deduplicated execution/comparison bytes")
	f.IntVar(&o.Workers, "workers", 0, "required bounded hashing concurrency (1..8)")
	f.StringVar(&expected, "expected-verification-id", "", "optional exact replay identity")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || inputs == "" || sha == "" || o.StorageRoot == "" || o.MaxBytes == 0 || o.Workers < 1 || o.Workers > 8 {
		fmt.Fprintln(stderr, "exact inputs, digest, storage root, positive byte ceiling and 1..8 workers required")
		return 2
	}
	in, err := funding.ReadInputs(inputs, sha)
	if err != nil {
		fmt.Fprintln(stderr, "recovery verification inputs:", err)
		return 1
	}
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	result, verifyErr := funding.VerifyFiles(ctx, in, sha, o)
	if result.Version != "" {
		if err = encodeJSON(stdout, result); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	if verifyErr != nil {
		fmt.Fprintln(stderr, "recovery file verification:", verifyErr)
		return 1
	}
	if expected != "" && expected != result.ID {
		fmt.Fprintln(stderr, "verification replay identity differs")
		return 1
	}
	if !result.Complete {
		fmt.Fprintln(stderr, "selected recovery file verification incomplete:", result.State)
		return 1
	}
	return 0
}
