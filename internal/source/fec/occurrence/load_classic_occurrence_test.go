package occurrence

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
)

func TestLoadPublishedClassicOccurrencePinsExactBytes(t *testing.T) {
	root := t.TempDir()
	rows := classicFixture(t, classic.CommitteeMaster, "connected-organization.txt")
	release, digest := classicSourceReleaseFixture(t, root, "load-master", "", classic.CommitteeMaster, "2024", rows)
	m, err := PublishClassic(context.Background(), release, digest, string(classic.CommitteeMaster), "2024", "load-master", Options{StorageRoot: root, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, classicEvidenceBase(mustClassicSpec(t, classic.CommitteeMaster)), "current/2024.json")
	loaded, sha, err := LoadPublishedClassicOccurrenceManifest(root, path, "committee-master")
	if err != nil || loaded.OccurrenceSetID != m.OccurrenceSetID || sha == "" {
		t.Fatal("load failed", err)
	}
	if _, _, err := LoadPublishedClassicOccurrenceManifest(root, path, "candidate-master"); err == nil {
		t.Fatal("wrong dataset accepted")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadPublishedClassicOccurrenceManifest(root, path, "committee-master"); err == nil {
		t.Fatal("pointer byte drift accepted")
	}
	if _, _, err := LoadPublishedClassicOccurrenceManifest(root, filepath.Join(root, "..", "outside.json"), "committee-master"); err == nil {
		t.Fatal("path escape accepted")
	}
}
