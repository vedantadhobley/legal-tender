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

func runTerminalAssessment(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runTerminalEvidence(ctx, args, stdout, stderr, false)
}

func runTerminalEvidence(ctx context.Context, args []string, stdout, stderr io.Writer, roles bool) int {
	name := "assess-terminal-sources"
	if roles {
		name = "profile-terminal-receipt-roles"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o fundinggeneration.ReadOptions
	var passwordEnv, expected string
	var workers int
	registerFundingReadFlags(f, &o, &passwordEnv)
	if roles {
		f.StringVar(&expected, "expected-profile-id", "", "exact fresh profile replay identity")
		f.IntVar(&workers, "workers", 4, "1..8 compact participant readers; no semantic effect")
	} else {
		f.StringVar(&expected, "expected-assessment-id", "", "exact fresh replay identity")
	}
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
	if roles && (workers < 1 || workers > 8) {
		fmt.Fprintln(stderr, "1..8 workers required")
		return 2
	}
	for _, v := range []string{o.Generation, o.GenerationSHA256, o.StorageRoot, o.GraphManifest, o.Participants, o.Conduits, o.Endpoint, passwordEnv} {
		if v == "" {
			fmt.Fprintln(stderr, "exact generation, retained locators and connection required")
			return 2
		}
	}
	o.Password = os.Getenv(passwordEnv)
	if o.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	r, err := fundinggeneration.OpenReader(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "terminal assessment:", err)
		return 1
	}
	var out any
	if roles {
		out, err = r.ProfileReceiptRoles(ctx, workers, expected, o.Progress)
	} else {
		out, err = r.AssessTerminalSources(ctx, expected, o.Progress)
	}
	if err != nil {
		fmt.Fprintln(stderr, "terminal assessment:", err)
		return 1
	}
	if err := encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
