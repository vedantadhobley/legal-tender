package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/audit/personaffiliation"
	affiliation "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func runAffiliationDiscovery(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var corpus, plan, pin, capture, output, agent, policy string
	switch command {
	case "plan-affiliation-discovery":
		f.StringVar(&corpus, "corpus", "", "retained diagnostic person-affiliation corpus; offline")
		f.StringVar(&pin, "expected-corpus-sha256", "", "exact reviewed corpus digest")
		f.StringVar(&policy, "query-policy", wikimedia.DiscoveryPolicy, "versioned retrieval policy; v1 is the default")
	case "capture-affiliation-candidates":
		f.StringVar(&plan, "plan", "", "source-derived discovery plan JSON")
		f.StringVar(&pin, "expected-plan-sha256", "", "exact plan digest")
		f.StringVar(&output, "output", "", "new capture directory")
		f.StringVar(&agent, "user-agent", "", "descriptive Wikimedia user agent")
	case "replay-affiliation-candidates", "assess-affiliation-candidates":
		f.StringVar(&capture, "capture", "", "retained capture directory; offline")
		f.StringVar(&pin, "expected-capture-sha256", "", "exact capture.json digest")
	default:
		return 2
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || !wikimedia.Digest(pin) || (command == "plan-affiliation-discovery" && corpus == "") || (command == "capture-affiliation-candidates" && (plan == "" || output == "" || agent == "")) || ((command == "replay-affiliation-candidates" || command == "assess-affiliation-candidates") && capture == "") {
		fmt.Fprintln(stderr, "required paths, exact digest and capture user-agent missing")
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
	var result any
	code := 0
	if command == "plan-affiliation-discovery" {
		result, err = personaffiliation.DiscoveryPlanWithPolicy(ctx, corpus, pin, build, policy)
	} else {
		if command == "capture-affiliation-candidates" {
			var raw []byte
			raw, err = readDiscoveryPlan(plan, pin)
			if err == nil {
				pin, err = wikimedia.CaptureDiscovery(ctx, raw, wikimedia.CaptureOptions{Directory: output, UserAgent: agent, BuildSHA256: build})
				capture = output
			}
		}
		if err == nil {
			var r wikimedia.DiscoveryResult
			r, err = wikimedia.ReadDiscovery(capture, pin)
			result = r
			if err == nil && command == "assess-affiliation-candidates" {
				result = affiliation.AssessDiscovery(r, build)
			}
			if !r.CaptureUsable {
				code = 1
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "affiliation discovery:", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return code
}

func readDiscoveryPlan(path, pin string) ([]byte, error) {
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Size() > wikimedia.MaxBody {
		return nil, fmt.Errorf("discovery plan requires bounded regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, wikimedia.MaxBody+1))
	if err != nil {
		return nil, err
	}
	if len(b) > wikimedia.MaxBody || wikimedia.Hash(b) != pin {
		return nil, fmt.Errorf("discovery plan pin or size mismatch")
	}
	return b, nil
}
