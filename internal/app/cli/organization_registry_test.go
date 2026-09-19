package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
)

func TestCorroborationCLIUsesExactRetainedSources(t *testing.T) {
	root := filepath.Join("..", "..", "..", "tests", "fixtures", "organization-resolution")
	args := []string{"pipeline", "entities", "corroborate-organizations", "--capture", filepath.Join(root, "capture-v1"), "--expected-capture-sha256", "6ef200cd16c436544a350c2940e70f677aa35b60ce3da9aacbd522de6e30ade8", "--registry-capture", filepath.Join(root, "gleif-capture-v1"), "--expected-registry-sha256", "da1a319d7a91d87f6e0d2dcbe9ab10443f8e1beee890a0fd31ac966e922bd027"}
	var first []byte
	for range 2 {
		var out, stderr bytes.Buffer
		code := RunContext(context.Background(), args, &out, &stderr)
		if code != 1 || stderr.Len() != 0 {
			t.Fatal("retained Wikimedia source failures must keep result partial", code, stderr.String())
		}
		var r org.Corroboration
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Policy != org.CorroborationPolicy || r.IdentityPublicationApproved || len(r.Registry.Observations) != 1 {
			t.Fatal("corroboration boundary")
		}
		if first != nil && !bytes.Equal(first, out.Bytes()) {
			t.Fatal("CLI replay changed")
		}
		first = append([]byte(nil), out.Bytes()...)
	}
	args[len(args)-1] = "6ef200cd16c436544a350c2940e70f677aa35b60ce3da9aacbd522de6e30ade8"
	var out, stderr bytes.Buffer
	if code := RunContext(context.Background(), args, &out, &stderr); code != 1 || out.Len() != 0 || stderr.Len() == 0 {
		t.Fatal("substituted registry capture accepted")
	}
}
