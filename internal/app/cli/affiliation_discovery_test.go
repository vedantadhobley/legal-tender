package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	affiliation "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestAffiliationDiscoveryCLI(t *testing.T) {
	for _, command := range []string{"plan-affiliation-discovery", "capture-affiliation-candidates", "replay-affiliation-candidates", "assess-affiliation-candidates"} {
		for _, flag := range []string{"--help", "--unknown", ""} {
			args := []string{"pipeline", "entities", command}
			if flag != "" {
				args = append(args, flag)
			}
			var out, stderr bytes.Buffer
			want := 2
			if flag == "--help" {
				want = 0
			}
			if got := RunContext(context.Background(), args, &out, &stderr); got != want {
				t.Fatal(command, flag, got, stderr.String())
			}
		}
	}
	args := []string{"pipeline", "entities", "plan-affiliation-discovery", "--corpus", "../../../tests/fixtures/person-affiliation", "--expected-corpus-sha256", "7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4"}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
			t.Fatal(code, stderr.String())
		}
		var p wikimedia.DiscoveryPlan
		if err := json.Unmarshal(out.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		if len(p.Appearances) != 4 || len(p.Searches) != 9 || !wikimedia.Digest(p.BuildSHA256) {
			t.Fatal("bad plan")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("plan replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
	dir := t.TempDir()
	plan := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(plan, first, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "plan-link.json")
	if err := os.Symlink(plan, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readDiscoveryPlan(link, wikimedia.Hash(first)); err == nil {
		t.Fatal("plan symlink accepted")
	}
	if _, err := readDiscoveryPlan(plan, wikimedia.Hash([]byte("wrong"))); err == nil {
		t.Fatal("bad plan pin accepted")
	}
	// Invalid plans must fail before any network call or capture-directory creation.
	bad := []byte(`{"policy":"unreviewed"}`)
	if err := os.WriteFile(plan, bad, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "capture")
	args = []string{"pipeline", "entities", "capture-affiliation-candidates", "--plan", plan, "--expected-plan-sha256", wikimedia.Hash(bad), "--output", output, "--user-agent", "LegalTender/test (test@local)"}
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("invalid plan accepted", code)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("created capture before validating plan")
	}
}

func TestAffiliationVariantsAndAssessmentCLI(t *testing.T) {
	args := []string{"pipeline", "entities", "plan-affiliation-discovery", "--corpus", "../../../tests/fixtures/person-affiliation", "--expected-corpus-sha256", "7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4", "--query-policy", wikimedia.DiscoveryVariantsPolicy}
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
		t.Fatal(code, stderr.String())
	}
	var p wikimedia.DiscoveryPlan
	if err := json.Unmarshal(out.Bytes(), &p); err != nil || len(p.Searches) != 15 || p.Policy != wikimedia.DiscoveryVariantsPolicy {
		t.Fatal("v2 plan", err)
	}
	args = []string{"pipeline", "entities", "assess-affiliation-candidates", "--capture", "../../../tests/fixtures/person-affiliation/discovery-v1", "--expected-capture-sha256", "c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c"}
	var first []byte
	for range 2 {
		out.Reset()
		stderr.Reset()
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
			t.Fatal(code, stderr.String())
		}
		var r affiliation.DiscoveryRelevance
		if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.Policy != affiliation.DiscoveryRelevancePolicy || len(r.Observations) != 12 || r.IdentityApproved || r.EmploymentVerified {
			t.Fatal("assessment", err)
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("assessment replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
	args[len(args)-1] = wikimedia.Hash([]byte("wrong"))
	out.Reset()
	stderr.Reset()
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("assessment skipped pin verification")
	}
}

func TestRetainedAffiliationDiscoveryCLIReplay(t *testing.T) {
	args := []string{"pipeline", "entities", "replay-affiliation-candidates", "--capture", "../../../tests/fixtures/person-affiliation/discovery-v1", "--expected-capture-sha256", "c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c"}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
			t.Fatal(code, stderr.String())
		}
		if wikimedia.Hash(out.Bytes()) != "bfd1b2a9fe871a051fbdc71e8c7beda74b1b6cd3c3e4a03c2fa381f94e7c3975" {
			t.Fatal("retained real result changed")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("offline CLI replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
}

func TestRetainedAffiliationVariantsCLIReplay(t *testing.T) {
	args := []string{"pipeline", "entities", "replay-affiliation-candidates", "--capture", "../../../tests/fixtures/person-affiliation/discovery-v2", "--expected-capture-sha256", "5ff30d1c86bd6e4b2b7746f88628057f30d03220a6b27a6853fc8124e04a9c81"}
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
		t.Fatal(code, stderr.String())
	}
	if wikimedia.Hash(out.Bytes()) != "633c0e7b692dcefc26027ee4d26b880b55111856bfb01488a41beb163211c193" {
		t.Fatal("retained v2 result changed")
	}
}
