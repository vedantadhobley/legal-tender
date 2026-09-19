package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/recovery/funding"
)

func runFundingRecovery(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("inspect-funding-recovery-inventory", flag.ContinueOnError)
	f.SetOutput(stderr)
	var inputs, sha, expected string
	var o funding.Options
	f.StringVar(&inputs, "inputs", "", "exact generation and dependency locator specification")
	f.StringVar(&sha, "expected-inputs-sha256", "", "required exact specification byte digest")
	f.StringVar(&o.StorageRoot, "storage-root", "", "read-only retained publication root")
	f.BoolVar(&o.HashBlobs, "hash-blobs", false, "also hash all source and artifact bytes; may scan hundreds of GB")
	f.StringVar(&expected, "expected-inventory-id", "", "optional exact replay result identity")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || inputs == "" || sha == "" || o.StorageRoot == "" {
		fmt.Fprintln(stderr, "exact inputs, input digest and storage root required")
		return 2
	}
	in, err := funding.ReadInputs(inputs, sha)
	if err != nil {
		fmt.Fprintln(stderr, "recovery inventory inputs:", err)
		return 1
	}
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	result, err := funding.Inspect(ctx, in, sha, o)
	if err != nil {
		fmt.Fprintln(stderr, "recovery inventory:", err)
		return 1
	}
	if err = encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if expected != "" && expected != result.ID {
		fmt.Fprintln(stderr, "inventory replay identity differs")
		return 1
	}
	if !result.Complete {
		fmt.Fprintln(stderr, "inventory incomplete; inspect dependency verification states")
		return 1
	}
	fmt.Fprintln(stderr, "file dependency inventory complete; recovery readiness remains unverified")
	return 0
}
