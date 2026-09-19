package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func runOrganizationRegistry(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var capture, pin, registry, registryPin, output, agent string
	f.StringVar(&capture, "capture", "", "saved Wikimedia capture directory")
	f.StringVar(&pin, "expected-capture-sha256", "", "exact Wikimedia capture digest")
	if command == "capture-organization-registry" {
		f.StringVar(&output, "output", "", "new registry capture directory; never overwritten")
		f.StringVar(&agent, "user-agent", "", "descriptive user agent with contact")
	} else {
		f.StringVar(&registry, "registry-capture", "", "saved exact-LEI GLEIF capture directory; offline")
		f.StringVar(&registryPin, "expected-registry-sha256", "", "exact registry capture digest")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || capture == "" || !wikimedia.Digest(pin) || (command == "capture-organization-registry" && (output == "" || agent == "")) || (command == "corroborate-organizations" && (registry == "" || !wikimedia.Digest(registryPin))) {
		fmt.Fprintln(stderr, "pinned capture and command-specific registry inputs required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	w, err := wikimedia.Read(capture, pin)
	if err == nil && command == "capture-organization-registry" {
		var requests gleif.RequestSet
		requests, err = org.RegistryRequests(w)
		if err == nil {
			registry = output
			registryPin, err = gleif.Capture(ctx, requests, gleif.Options{Directory: output, BuildSHA256: build, UserAgent: agent})
		}
	}
	var g gleif.Replay
	if err == nil {
		g, err = gleif.Read(registry, registryPin)
	}
	var r org.Corroboration
	if err == nil {
		r, err = org.Corroborate(w, g, build)
	}
	if err != nil {
		fmt.Fprintln(stderr, "organization registry evidence:", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if !r.Proposals.Complete {
		return 1
	}
	for _, o := range g.Observations {
		if o.Issue != "" {
			return 1
		}
	}
	return 0
}
