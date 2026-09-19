package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	evaluation "github.com/vedantadhobley/legal-tender/internal/audit/organizationresolution"
	resolver "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
)

func TestOrganizationCLIValidation(t *testing.T) {
	for _, command := range []string{"build-organization-queries", "capture-organizations", "replay-organizations", "evaluate-organizations", "capture-organization-registry", "corroborate-organizations"} {
		prefix := []string{"pipeline", "entities", command}
		if command == "build-organization-queries" {
			prefix[1] = "fec"
		}
		var out, err bytes.Buffer
		if code := RunContext(context.Background(), append(prefix, "--help"), &out, &err); code != 0 || !strings.Contains(err.String(), "Usage") {
			t.Fatal(command, code, err.String())
		}
		out.Reset()
		err.Reset()
		if code := RunContext(context.Background(), prefix, &out, &err); code != 2 || out.Len() != 0 {
			t.Fatal(command, code, err.String())
		}
		out.Reset()
		err.Reset()
		if code := RunContext(context.Background(), append(prefix, "--arbitrary-endpoint=https://example.org"), &out, &err); code != 2 {
			t.Fatal(command, code)
		}
	}
}

func TestOrganizationCLIPolicySelection(t *testing.T) {
	root := filepath.Join("..", "..", "..", "tests", "fixtures", "organization-resolution")
	for _, command := range []string{"replay-organizations", "evaluate-organizations"} {
		args := []string{"pipeline", "entities", command, "--capture", filepath.Join(root, "capture-v1"), "--expected-capture-sha256", "6ef200cd16c436544a350c2940e70f677aa35b60ce3da9aacbd522de6e30ade8"}
		if command == "evaluate-organizations" {
			args = append(args, "--corpus", filepath.Join(root, "corpus-v1.json"), "--expected-corpus-sha256", "6475f3beaefc2ab64f4ce72cfb5b4135e6a7084ffd2c0ba7c1ce6cf494415792")
		}
		for _, policy := range []string{"", resolver.Policy, resolver.ExpandedPolicy, "latest"} {
			t.Run(command+"/"+policy, func(t *testing.T) {
				argv := append([]string{}, args...)
				if policy != "" {
					argv = append(argv, "--proposal-policy", policy)
				}
				var out, stderr bytes.Buffer
				code := RunContext(context.Background(), argv, &out, &stderr)
				if policy == "latest" {
					if code != 2 || out.Len() != 0 || !strings.Contains(stderr.String(), "policy") {
						t.Fatal(code, stderr.String())
					}
					return
				}
				wantPolicy := policy
				if wantPolicy == "" {
					wantPolicy = resolver.Policy
				}
				if command == "evaluate-organizations" {
					var r evaluation.Result
					if err := json.Unmarshal(out.Bytes(), &r); err != nil {
						t.Fatal(err, code, stderr.String())
					}
					want := 0
					if policy == resolver.ExpandedPolicy {
						want = 2
					}
					if code != 0 || r.ResolverPolicy != wantPolicy || r.Counts.SupportedProposals != want || r.IdentityPublicationApproved {
						t.Fatal("evaluation policy selection", code, r.Counts)
					}
				} else {
					var r resolver.Result
					if err := json.Unmarshal(out.Bytes(), &r); err != nil {
						t.Fatal(err, code, stderr.String())
					}
					if code != 1 || r.Policy != wantPolicy || r.Complete || r.IdentityResolved {
						t.Fatal("replay source/policy boundary", code)
					}
					if (r.Decisions[0].Baseline != nil) != (policy == resolver.ExpandedPolicy) {
						t.Fatal("baseline not policy scoped")
					}
				}
			})
		}
	}
	// Invalid policy must fail before reading queries or issuing requests.
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), []string{"pipeline", "entities", "capture-organizations", "--proposal-policy=latest"}, &out, &stderr); code != 2 || out.Len() != 0 || !strings.Contains(stderr.String(), "policy") {
		t.Fatal("capture policy validation", code, stderr.String())
	}
}
