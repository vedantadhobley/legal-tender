package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestRegistryNameCLIPlanAndReplay(t *testing.T) {
	for _, command := range []string{"plan-employer-registry", "capture-registry-names", "replay-registry-names"} {
		for _, flag := range []string{"", "--help", "--unknown"} {
			args := []string{"pipeline", "entities", command}
			if flag != "" {
				args = append(args, flag)
			}
			var out, stderr bytes.Buffer
			want := 2
			if flag == "--help" {
				want = 0
			}
			if code := RunContext(context.Background(), args, &out, &stderr); code != want {
				t.Fatal(command, flag, code, stderr.String())
			}
		}
	}
	args := []string{"pipeline", "entities", "plan-employer-registry", "--corpus", "../../../tests/fixtures/person-affiliation", "--expected-corpus-sha256", "7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4"}
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
		t.Fatal(code, stderr.String())
	}
	var p gleif.NamePlan
	if err := json.Unmarshal(out.Bytes(), &p); err != nil || p.Validate() != nil || len(p.Inputs) != 4 || len(p.Queries) != 3 {
		t.Fatal("plan", err)
	}
	args = []string{"pipeline", "entities", "replay-registry-names", "--capture", "../../../tests/fixtures/person-affiliation/gleif-names-v1", "--expected-capture-sha256", "759cc1be8ff0713b1bd57655e081edc8b3820661f1af7049d9ddd62038e25e51"}
	var first []byte
	for range 2 {
		out.Reset()
		stderr.Reset()
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
			t.Fatal(code, stderr.String())
		}
		var r org.RegistryNameResult
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Policy != org.RegistryNamePolicy || !r.Evidence.CaptureUsable || len(r.Decisions) != 4 || r.Decisions[1].State != "ambiguous_name_correspondences" || r.IdentityApproved || r.EmploymentVerified || r.GraphPublicationApproved || r.FinancialAttribution || r.ExhaustiveDiscovery {
			t.Fatal("wrong real result")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("offline replay changed")
		}
		first = bytes.Clone(out.Bytes())
	}
	args[len(args)-1] = wikimedia.Hash([]byte("wrong"))
	out.Reset()
	stderr.Reset()
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("pin not checked")
	}
}

func TestRegistryNameCLIRejectsTamperingBeforeNetwork(t *testing.T) {
	dir := t.TempDir()
	plan := filepath.Join(dir, "plan.json")
	body := []byte(`{"policy":"unreviewed"}`)
	if err := os.WriteFile(plan, body, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "capture")
	args := []string{"pipeline", "entities", "capture-registry-names", "--plan", plan, "--expected-plan-sha256", wikimedia.Hash(body), "--output", output, "--user-agent", "LegalTender/test (test@local)"}
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 {
		t.Fatal("tampered plan accepted")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("network/capture work started")
	}
}
