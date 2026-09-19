package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	"os"
	"path/filepath"
	"reflect"
)

// LoadPublishedClassicOccurrenceManifest validates the occurrence manifest and
// its immutable backing. It does not decode occurrence rows; callers consuming
// an artifact must additionally verify that artifact's content.
func LoadPublishedClassicOccurrenceManifest(storageRoot, path, expectedDataset string) (ClassicManifest, string, error) {
	if err := requirePathInside(storageRoot, path); err != nil {
		return ClassicManifest{}, "", err
	}
	m, digest, err := readClassicManifestWithSHA256(path)
	if err != nil {
		return m, "", err
	}
	if m.Dataset != expectedDataset {
		return m, "", fmt.Errorf("unexpected classic occurrence dataset")
	}
	if err := validateClassicManifest(m); err != nil {
		return m, "", err
	}
	spec, err := classic.Lookup(m.Dataset)
	if err != nil {
		return m, "", err
	}
	backing, backingDigest, err := readClassicManifestWithSHA256(filepath.Join(storageRoot, classicEvidenceBase(spec), "manifests", m.OccurrenceSetID+".json"))
	if err != nil {
		return m, "", err
	}
	if digest != backingDigest || !reflect.DeepEqual(m, backing) {
		return m, "", fmt.Errorf("classic occurrence pointer differs from immutable bytes")
	}
	return m, digest, nil
}

// LoadPublishedClassicFactManifest reads and validates a published classic
// fact manifest and its immutable pointer relationship. Artifact content is
// verified when a consumer streams Facts through the shared artifact reader.
func LoadPublishedClassicFactManifest(storageRoot, path, expectedDataset string) (ClassicFactManifest, string, error) {
	manifestPointer, err := readClassicFactManifestIfPresent(path)
	if err != nil {
		return ClassicFactManifest{}, "", err
	}
	if manifestPointer == nil {
		return ClassicFactManifest{}, "", os.ErrNotExist
	}
	manifest := *manifestPointer
	if expectedDataset != "" && manifest.Dataset != expectedDataset {
		return ClassicFactManifest{}, "", fmt.Errorf("classic fact manifest is %s, expected %s", manifest.Dataset, expectedDataset)
	}
	if err := validateClassicFactManifest(manifest); err != nil {
		return ClassicFactManifest{}, "", fmt.Errorf("validate classic fact manifest: %w", err)
	}
	if err := validateClassicFactManifestBacking(storageRoot, manifest); err != nil {
		return ClassicFactManifest{}, "", fmt.Errorf("validate classic fact backing: %w", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return ClassicFactManifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:]), nil
}

// LoadPublishedScheduleAColumnarManifest validates one active or immutable
// Schedule A Parquet fact manifest and rehashes every referenced shard before
// returning it to a calculation or audit consumer.
func LoadPublishedScheduleAColumnarManifest(ctx context.Context, storageRoot, path string) (ScheduleAColumnarManifest, string, error) {
	if storageRoot == "" || path == "" {
		return ScheduleAColumnarManifest{}, "", fmt.Errorf("storage root and Schedule A columnar fact manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return ScheduleAColumnarManifest{}, "", err
	}
	pointer, err := readScheduleAColumnarManifestIfPresent(path)
	if err != nil {
		return ScheduleAColumnarManifest{}, "", err
	}
	if pointer == nil {
		return ScheduleAColumnarManifest{}, "", os.ErrNotExist
	}
	if err := validateScheduleAColumnarManifest(*pointer); err != nil {
		return ScheduleAColumnarManifest{}, "", fmt.Errorf("validate Schedule A columnar manifest: %w", err)
	}
	immutablePath := filepath.Join(storageRoot, scheduleAColumnarBase(), "manifests", pointer.FactSetID+".json")
	immutable, err := readScheduleAColumnarManifestIfPresent(immutablePath)
	if err != nil {
		return ScheduleAColumnarManifest{}, "", err
	}
	if immutable == nil {
		return ScheduleAColumnarManifest{}, "", os.ErrNotExist
	}
	if !reflect.DeepEqual(*pointer, *immutable) {
		return ScheduleAColumnarManifest{}, "", fmt.Errorf("Schedule A columnar pointer differs from immutable manifest")
	}
	if err := validateScheduleAColumnarManifestBacking(storageRoot, *immutable); err != nil {
		return ScheduleAColumnarManifest{}, "", fmt.Errorf("validate Schedule A columnar backing: %w", err)
	}
	content, err := os.ReadFile(immutablePath)
	if err != nil {
		return ScheduleAColumnarManifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return *immutable, hex.EncodeToString(digest[:]), nil
}

// LoadPublishedScheduleEFactManifest verifies an active or immutable Schedule
// E fact manifest, its immutable pointer relationship, and its complete
// backing artifact before returning it to a downstream calculation.
func LoadPublishedScheduleEFactManifest(ctx context.Context, storageRoot, path string) (ScheduleEFactManifest, string, error) {
	if storageRoot == "" || path == "" {
		return ScheduleEFactManifest{}, "", fmt.Errorf("storage root and Schedule E fact manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return ScheduleEFactManifest{}, "", err
	}
	pointer, err := readScheduleEFactManifestIfPresent(path)
	if err != nil {
		return ScheduleEFactManifest{}, "", err
	}
	if pointer == nil {
		return ScheduleEFactManifest{}, "", os.ErrNotExist
	}
	if err := validateScheduleEFactManifest(*pointer); err != nil {
		return ScheduleEFactManifest{}, "", fmt.Errorf("validate Schedule E fact manifest: %w", err)
	}
	immutablePath := filepath.Join(storageRoot, scheduleEFactBase(), "manifests", pointer.FactSetID+".json")
	immutablePointer, err := readScheduleEFactManifestIfPresent(immutablePath)
	if err != nil {
		return ScheduleEFactManifest{}, "", err
	}
	if immutablePointer == nil {
		return ScheduleEFactManifest{}, "", os.ErrNotExist
	}
	immutable := *immutablePointer
	if !reflect.DeepEqual(*pointer, immutable) {
		return ScheduleEFactManifest{}, "", fmt.Errorf("Schedule E fact pointer differs from immutable manifest")
	}
	if err := validateScheduleEFactManifestBacking(ctx, storageRoot, immutable); err != nil {
		return ScheduleEFactManifest{}, "", fmt.Errorf("validate Schedule E fact backing: %w", err)
	}
	content, err := os.ReadFile(immutablePath)
	if err != nil {
		return ScheduleEFactManifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return immutable, hex.EncodeToString(digest[:]), nil
}
