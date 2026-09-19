package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func runReviewReleaseStorage(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("review-release-storage", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "path to saved update_available plan")
	currentPath := flags.String("current", "", "path to that plan's prior release manifest")
	storageRoot := flags.String("storage-root", "", "existing durable storage root (read-only)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *planPath == "" || *storageRoot == "" {
		_, _ = io.WriteString(stderr, "--plan and --storage-root are required; positional arguments are not accepted\n")
		return 2
	}
	plan, err := decodeJSONFile[fecrelease.ReleasePlan](*planPath)
	if err != nil {
		fmt.Fprintf(stderr, "read release plan: %v\n", err)
		return 1
	}
	inventory, known := fecrelease.InventoryForVersion(plan.InventoryVersion)
	if !known {
		fmt.Fprintf(stderr, "unknown FEC inventory version %q\n", plan.InventoryVersion)
		return 1
	}
	var current *fecrelease.ReleaseManifest
	if *currentPath != "" {
		manifest, err := decodeJSONFile[fecrelease.ReleaseManifest](*currentPath)
		if err != nil {
			fmt.Fprintf(stderr, "read prior release: %v\n", err)
			return 1
		}
		current = &manifest
	}
	result, err := fecrelease.ReviewStorage(inventory, plan, current, *storageRoot, nil)
	if err != nil {
		fmt.Fprintf(stderr, "review release storage: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode storage review: %v\n", err)
		return 1
	}
	if !result.Acquisition.Passed || !result.Scenario.FitsBudget {
		_, _ = io.WriteString(stderr, "storage review: acquisition blocked or staging scenario incomplete/over budget\n")
		return 1
	}
	_, _ = io.WriteString(stderr, "storage scenario fits; prior output sizes are not bounds or acquisition authorization\n")
	return 0
}
