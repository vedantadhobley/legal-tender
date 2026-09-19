package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func runOrganizations(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	if command == "enrich-affiliations" {
		return runAffiliationEnrichment(ctx, args, stdout, stderr)
	}
	if command == "query-relationships" {
		return runRelationships(ctx, args, stdout, stderr)
	}
	if command == "extract-company-page" {
		return runCompanyPage(ctx, args, stdout, stderr)
	}
	if command == "plan-employer-registry" || command == "capture-registry-names" || command == "replay-registry-names" {
		return runRegistryNames(ctx, command, args, stdout, stderr)
	}
	if command == "plan-affiliation-discovery" || command == "capture-affiliation-candidates" || command == "replay-affiliation-candidates" || command == "assess-affiliation-candidates" {
		return runAffiliationDiscovery(ctx, command, args, stdout, stderr)
	}
	if command == "extract-role-evidence" {
		return runRoleEvidence(ctx, args, stdout, stderr)
	}
	if command == "capture-issuer-filing" || command == "inspect-issuer-filing" {
		return runIssuerFiling(ctx, command, args, stdout, stderr)
	}
	if command == "capture-issuer-directory" || command == "discover-issuer-organizations" {
		return runOrganizationIssuers(ctx, command, args, stdout, stderr)
	}
	if command == "capture-organization-registry" || command == "corroborate-organizations" {
		return runOrganizationRegistry(ctx, command, args, stdout, stderr)
	}
	if command == "evaluate-organizations" {
		return runOrganizationEvaluation(ctx, args, stdout, stderr)
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var root, cm, receipts, ordinals, queries, pin, capture, output, agent string
	policy := org.Policy
	var limit int
	switch command {
	case "build-organization-queries":
		f.StringVar(&root, "storage-root", "", "read-only retained FEC storage root")
		f.StringVar(&cm, "committee-facts", "", "immutable committee-master manifest; selects connected organizations")
		f.StringVar(&receipts, "schedule-a-facts", "", "immutable receipt manifest; verifies all shards")
		f.StringVar(&ordinals, "receipt-ordinals", "", "1..20 comma-separated source ordinals for employer queries")
		f.IntVar(&limit, "limit", 5, "maximum distinct committee-connected names, 1..20")
	case "capture-organizations":
		f.StringVar(&policy, "proposal-policy", org.Policy, "versioned name-proposal policy; not identity approval")
		f.StringVar(&queries, "queries", "", "saved Go organization-query JSON")
		f.StringVar(&pin, "expected-queries-sha256", "", "required exact query-file digest")
		f.StringVar(&output, "output", "", "new capture directory; never overwritten")
		f.StringVar(&agent, "user-agent", "", "descriptive Wikimedia-compliant user agent with contact")
	case "replay-organizations":
		f.StringVar(&policy, "proposal-policy", org.Policy, "versioned name-proposal policy; not identity approval")
		f.StringVar(&capture, "capture", "", "saved capture directory; offline read only")
		f.StringVar(&pin, "expected-capture-sha256", "", "required capture.json digest")
	default:
		fmt.Fprintln(stderr, "unknown organization command")
		return 2
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected organization command arguments")
		return 2
	}
	if !org.ValidPolicy(policy) {
		fmt.Fprintln(stderr, "unsupported organization proposal policy")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var result any
	code := 0
	switch command {
	case "build-organization-queries":
		if root == "" || (cm == "") == (receipts == "") || (receipts == "") != (ordinals == "") {
			fmt.Fprintln(stderr, "use storage-root and exactly one of committee-facts or schedule-a-facts with receipt-ordinals")
			return 2
		}
		if cm != "" {
			result, err = org.CommitteeQueries(ctx, root, cm, build, limit)
		} else {
			var ids []uint64
			for _, s := range strings.Split(ordinals, ",") {
				n, e := strconv.ParseUint(s, 10, 64)
				if e != nil {
					fmt.Fprintln(stderr, "invalid receipt ordinal")
					return 2
				}
				ids = append(ids, n)
			}
			result, err = org.EmployerQueries(ctx, root, receipts, build, ids)
		}
	case "capture-organizations":
		if queries == "" || output == "" || agent == "" || !wikimedia.Digest(pin) {
			fmt.Fprintln(stderr, "queries, exact digest, new output directory and user-agent required")
			return 2
		}
		var b []byte
		file, e := os.Open(queries)
		if e != nil {
			err = e
			break
		}
		b, err = io.ReadAll(io.LimitReader(file, wikimedia.MaxBody+1))
		file.Close()
		if err != nil {
			break
		}
		if len(b) > wikimedia.MaxBody || wikimedia.Hash(b) != pin {
			err = fmt.Errorf("query byte budget or digest mismatch")
			break
		}
		var sha string
		sha, err = wikimedia.Capture(ctx, b, wikimedia.CaptureOptions{Directory: output, UserAgent: agent, BuildSHA256: build})
		if err == nil {
			var replay wikimedia.Replay
			replay, err = wikimedia.Read(output, sha)
			if err == nil {
				var r org.Result
				r, err = org.ResolveWithPolicy(replay, build, policy)
				result = r
				if !r.Complete {
					code = 1
				}
			}
		}
	case "replay-organizations":
		if capture == "" || !wikimedia.Digest(pin) {
			fmt.Fprintln(stderr, "capture directory and exact digest required")
			return 2
		}
		var replay wikimedia.Replay
		replay, err = wikimedia.Read(capture, pin)
		if err == nil {
			var r org.Result
			r, err = org.ResolveWithPolicy(replay, build, policy)
			result = r
			if !r.Complete {
				code = 1
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "organization evidence:", err)
		return 1
	}
	if err = encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return code
}
