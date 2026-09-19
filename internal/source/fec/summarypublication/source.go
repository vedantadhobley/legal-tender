package summarypublication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

var releaseIDPattern = regexp.MustCompile(`^fec-[a-f0-9]{64}$`)

// loadSource accepts a pointer only when it is byte-identical to the immutable
// release manifest. It revalidates just the selected raw source on consumption;
// it does not rescan hundreds of GiB of unrelated release artifacts.
func loadSource(root, path, cycle string) (release.ReleaseManifest, string, release.PublishedArtifact, error) {
	var manifest release.ReleaseManifest
	raw, err := readJSON(path, &manifest)
	if err != nil {
		return manifest, "", release.PublishedArtifact{}, err
	}
	if !releaseIDPattern.MatchString(manifest.ReleaseID) {
		return manifest, "", release.PublishedArtifact{}, fmt.Errorf("invalid source release ID")
	}
	if issues := release.ValidateKnownManifest(manifest); len(issues) != 0 {
		return manifest, "", release.PublishedArtifact{}, fmt.Errorf("invalid source release: %s", issues[0].Code)
	}
	if manifest.InventoryVersion != release.CommitteeSummaryInventoryVersion || !slices.Contains(manifest.Periods, cycle) {
		return manifest, "", release.PublishedArtifact{}, fmt.Errorf("committee summary requires a v4 release containing the requested cycle")
	}
	var backing release.ReleaseManifest
	immutable, err := readJSON(filepath.Join(root, "releases", "fec", "manifests", manifest.ReleaseID+".json"), &backing)
	if err != nil {
		return manifest, "", release.PublishedArtifact{}, err
	}
	if !bytes.Equal(raw, immutable) {
		return manifest, "", release.PublishedArtifact{}, fmt.Errorf("source release differs from immutable backing")
	}
	for _, source := range manifest.Artifacts {
		if source.SourceID != "fec:committee-summary:"+cycle {
			continue
		}
		expected := committeesummary.Expected{Cycle: cycle, Bytes: source.ByteCount, SHA256: source.SHA256}
		if err := committeesummary.ValidateExpected(expected); err != nil {
			return manifest, "", source, err
		}
		key := filepath.ToSlash(filepath.Join("raw", "fec", "artifacts", "sha256", source.SHA256[:2], source.SHA256))
		if source.StorageKey != key {
			return manifest, "", source, fmt.Errorf("noncanonical committee-summary raw artifact")
		}
		return manifest, digest(raw), source, nil
	}
	return manifest, "", release.PublishedArtifact{}, fmt.Errorf("release has no selected committee-summary artifact")
}

// Control manifests are small. Bound reads and reject unknown fields/trailing
// JSON so a partial or incompatible control artifact cannot authorize writes.
func readJSON(path string, value any) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("manifest must be a regular file")
	}
	const limit = 4 << 20
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, fmt.Errorf("manifest exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("manifest contains trailing JSON")
	}
	return raw, nil
}
