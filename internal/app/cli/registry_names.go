package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/audit/personaffiliation"
	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func runRegistryNames(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var corpus, planPath, capture, pin, output, agent string
	switch command {
	case "plan-employer-registry":
		f.StringVar(&corpus, "corpus", "", "retained diagnostic FEC corpus")
		f.StringVar(&pin, "expected-corpus-sha256", "", "exact corpus digest")
	case "capture-registry-names":
		f.StringVar(&planPath, "plan", "", "source-derived registry name plan")
		f.StringVar(&pin, "expected-plan-sha256", "", "exact plan digest")
		f.StringVar(&output, "output", "", "new capture directory")
		f.StringVar(&agent, "user-agent", "", "descriptive GLEIF user agent")
	case "replay-registry-names":
		f.StringVar(&capture, "capture", "", "retained registry name capture; offline")
		f.StringVar(&pin, "expected-capture-sha256", "", "exact capture digest")
	default:
		return 2
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || !wikimedia.Digest(pin) || (command == "plan-employer-registry" && corpus == "") || (command == "capture-registry-names" && (planPath == "" || output == "" || agent == "")) || (command == "replay-registry-names" && capture == "") {
		fmt.Fprintln(stderr, "pinned command-specific inputs required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var result any
	code := 0
	if command == "plan-employer-registry" {
		result, err = personaffiliation.EmployerRegistryPlan(ctx, corpus, pin, build)
	} else {
		if command == "capture-registry-names" {
			var raw []byte
			raw, err = readDiscoveryPlan(planPath, pin)
			var p gleif.NamePlan
			if err == nil {
				err = strictjson.Decode(raw, &p)
			}
			if err == nil {
				pin, err = gleif.CaptureNames(ctx, p, gleif.Options{Directory: output, UserAgent: agent, BuildSHA256: build})
				capture = output
			}
		}
		if err == nil {
			var r gleif.NameReplay
			r, err = gleif.ReadNames(capture, pin)
			if err == nil {
				result = org.DiscoverRegistryNames(r, build)
				if !r.CaptureUsable {
					code = 1
				}
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "registry name discovery:", err)
		return 1
	}
	if err = encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return code
}
