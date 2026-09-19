package cli

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreserveReleaseControlInputIsImmutableAndIdempotent(t *testing.T) {
	root := t.TempDir()
	content := []byte("{\"status\":\"staged\"}\n")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	path, err := preserveReleaseControlInput(root, "stages", digest, content)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(root, "control", "fec", "release", "stages", digest+".json") {
		t.Fatalf("unexpected control path %s", path)
	}
	if _, err := preserveReleaseControlInput(root, "stages", digest, content); err != nil {
		t.Fatalf("idempotent preserve failed: %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", len(content))), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := preserveReleaseControlInput(root, "stages", digest, content); err == nil || !strings.Contains(err.Error(), "unexpected bytes") {
		t.Fatalf("corrupt immutable control artifact was not rejected: %v", err)
	}
}

func TestPreserveReleaseControlInputRejectsUnboundInput(t *testing.T) {
	content := []byte("{}\n")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	if _, err := preserveReleaseControlInput(t.TempDir(), "unknown", digest, content); err == nil {
		t.Fatal("unsupported category was accepted")
	}
	if _, err := preserveReleaseControlInput(t.TempDir(), "plans", strings.Repeat("0", 64), content); err == nil {
		t.Fatal("wrong content digest was accepted")
	}
}
