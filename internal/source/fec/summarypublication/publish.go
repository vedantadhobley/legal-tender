package summarypublication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

type Options struct {
	StorageRoot   string
	ReleasePath   string
	Cycle         string
	RunID         string
	Clock         func() time.Time
	DiskAvailable func(string) (uint64, error)
}

// Publish writes an immutable, release-bound fact manifest only after exact
// row readback. There is deliberately no mutable current pointer: a historical
// replay cannot advance or roll back another release's facts.
func Publish(ctx context.Context, options Options) (Manifest, error) {
	if options.StorageRoot == "" || options.ReleasePath == "" || !release.ValidAcquisitionRunID(options.RunID) {
		return Manifest{}, fmt.Errorf("storage root, release path, and valid run ID are required")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	sourceRelease, releaseSHA, source, err := loadSource(options.StorageRoot, options.ReleasePath, options.Cycle)
	if err != nil {
		return Manifest{}, err
	}
	setID := setIdentity(releaseSHA, source.SHA256, options.Cycle)
	path := ManifestPath(options.StorageRoot, setID)
	if _, err := os.Stat(path); err == nil {
		return loadExpected(ctx, options.StorageRoot, path, setID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	expected := committeesummary.Expected{Cycle: options.Cycle, Bytes: source.ByteCount, SHA256: source.SHA256}
	input, err := openSource(options.StorageRoot, expected)
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = input.Close() }()
	available := options.DiskAvailable
	if available == nil {
		available = func(path string) (uint64, error) {
			var stat syscall.Statfs_t
			if err := syscall.Statfs(path, &stat); err != nil {
				return 0, err
			}
			return stat.Bavail * uint64(stat.Bsize), nil
		}
	}
	free, err := available(options.StorageRoot)
	if err != nil {
		return Manifest{}, err
	}
	// Share the accepted FEC free-space floor; a bounded summary publication
	// needs a 1 GiB working allowance, not another large-schedule extract.
	if free < release.AcquisitionFreeFloorBytes+(1<<30) {
		return Manifest{}, fmt.Errorf("committee-summary publication storage preflight failed")
	}
	descriptor, verification, err := buildRows(ctx, options.StorageRoot, input, expected)
	if err != nil {
		return Manifest{}, err
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	manifest := Manifest{
		SchemaVersion: SchemaVersion, FactSetID: setID, FactType: FactType, State: "published",
		Cycle: options.Cycle, SourceContract: committeesummary.SourceContract, ParserVersion: committeesummary.ParserVersion,
		SourceReleaseID: sourceRelease.ReleaseID, SourceReleaseManifestSHA256: releaseSHA,
		SourceArtifact: source, RunID: options.RunID, PublishedAt: clock().UTC(),
		Verification: verification, Facts: descriptor, ReadbackVerified: true,
	}
	if manifest.PublishedAt.IsZero() {
		return Manifest{}, fmt.Errorf("publication time is required")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := writeImmutable(path, manifest); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return Manifest{}, err
		}
		// A concurrent winner may have a different run/time, but must verify
		// against the same release and raw source before its manifest is reused.
		return loadExpected(ctx, options.StorageRoot, path, setID)
	}
	return manifest, nil
}

func loadExpected(ctx context.Context, root, path, setID string) (Manifest, error) {
	m, err := Load(ctx, root, path)
	if err == nil && m.FactSetID != setID {
		return Manifest{}, fmt.Errorf("existing manifest belongs to another committee-summary fact set")
	}
	return m, err
}

func ManifestPath(root, setID string) string {
	return filepath.Join(root, basePath, "manifests", setID+".json")
}

// Load validates immutable manifest identity, exact release membership, source
// bytes, and every normalized record. It never trusts a stored success flag.
func Load(ctx context.Context, root, path string) (Manifest, error) {
	var m Manifest
	raw, err := readJSON(path, &m)
	if err != nil {
		return m, err
	}
	if err := ctx.Err(); err != nil {
		return m, err
	}
	if m.SchemaVersion != SchemaVersion || m.FactType != FactType || m.State != "published" || m.SourceContract != committeesummary.SourceContract || m.ParserVersion != committeesummary.ParserVersion || !m.ReadbackVerified || m.TerminalAttributionEligible || !release.ValidAcquisitionRunID(m.RunID) || m.PublishedAt.IsZero() || !releaseIDPattern.MatchString(m.SourceReleaseID) {
		return m, fmt.Errorf("invalid committee-summary fact manifest")
	}
	if m.FactSetID != setIdentity(m.SourceReleaseManifestSHA256, m.SourceArtifact.SHA256, m.Cycle) {
		return m, fmt.Errorf("committee-summary fact set identity mismatch")
	}
	var immutable Manifest
	backing, err := readJSON(ManifestPath(root, m.FactSetID), &immutable)
	if err != nil {
		return m, err
	}
	if digest(raw) != digest(backing) {
		return m, fmt.Errorf("committee-summary manifest differs from immutable backing")
	}
	_, releaseSHA, source, err := loadSource(root, filepath.Join(root, "releases", "fec", "manifests", m.SourceReleaseID+".json"), m.Cycle)
	if err != nil {
		return m, err
	}
	if releaseSHA != m.SourceReleaseManifestSHA256 || !reflect.DeepEqual(source, m.SourceArtifact) {
		return m, fmt.Errorf("committee-summary source release binding mismatch")
	}
	expected := committeesummary.Expected{Cycle: m.Cycle, Bytes: source.ByteCount, SHA256: source.SHA256}
	input, err := openSource(root, expected)
	if err != nil {
		return m, err
	}
	defer func() { _ = input.Close() }()
	if err := verifyRows(ctx, root, input, expected, m.Facts, m.Verification); err != nil {
		return m, err
	}
	return m, nil
}

func writeImmutable(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".pending-*.json")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if err := file.Chmod(0o640); err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}
