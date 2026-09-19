package receipts

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// LoadPublishedFactBundle reads and validates one immutable-ready receipt fact
// bundle. The returned digest identifies the exact manifest bytes supplied by
// the caller.
func LoadPublishedFactBundle(storageRoot, path string) (FactBundleManifest, string, error) {
	manifest, digest, err := readStrictJSON[FactBundleManifest](path)
	if err != nil {
		return FactBundleManifest{}, "", err
	}
	if err := validateFactBundleManifest(manifest); err != nil {
		return FactBundleManifest{}, "", fmt.Errorf("validate fact bundle: %w", err)
	}
	if err := validateFactBundleManifestBacking(storageRoot, manifest); err != nil {
		return FactBundleManifest{}, "", fmt.Errorf("validate fact bundle backing: %w", err)
	}
	return manifest, digest, nil
}

// LoadPublishedCompactManifest reads one published compact calculation and
// verifies its immutable manifest plus the result artifact consumed by graph
// projection. The sparse exception artifact remains bound by the immutable
// manifest but is not reread by a consumer that does not use it.
func LoadPublishedCompactManifest(ctx context.Context, storageRoot, path string) (CompactManifest, string, error) {
	manifest, digest, err := readStrictJSON[CompactManifest](path)
	if err != nil {
		return CompactManifest{}, "", err
	}
	if err := validateCompactManifest(manifest); err != nil {
		return CompactManifest{}, "", fmt.Errorf("validate compact calculation: %w", err)
	}
	immutablePath := filepath.Join(storageRoot, compactCalculationBase(), "manifests", manifest.CalculationSetID+".json")
	immutable, _, err := readStrictJSON[CompactManifest](immutablePath)
	if err != nil {
		return CompactManifest{}, "", fmt.Errorf("read immutable compact calculation: %w", err)
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return CompactManifest{}, "", fmt.Errorf("compact calculation pointer differs from immutable manifest")
	}
	resultPath, err := storageartifact.Resolve(storageRoot, manifest.Results.StorageKey)
	if err != nil {
		return CompactManifest{}, "", err
	}
	if err := storageartifact.Verify(ctx, resultPath, manifest.Results); err != nil {
		return CompactManifest{}, "", fmt.Errorf("verify compact calculation results: %w", err)
	}
	return manifest, digest, nil
}

// CompactManifestUsesBundle reports whether a calculation consumed exactly
// the immutable fact-set references frozen by a ready bundle.
func CompactManifestUsesBundle(manifest CompactManifest, bundle FactBundleManifest) bool {
	return manifest.Cycle == bundle.Cycle &&
		manifest.SourceReleaseID == bundle.SourceReleaseID &&
		reflect.DeepEqual(manifest.InputFactSets, bundle.InputFactSets)
}
