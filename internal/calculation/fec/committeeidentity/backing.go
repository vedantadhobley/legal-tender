package committeeidentity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func validateEvidenceBacking(ctx context.Context, storageRoot string, manifest Manifest) error {
	basePath := filepath.Join(
		storageRoot, "bundles", "fec", "receiver-reported-committee-flow-projection",
		"manifests", manifest.Inputs.ReadinessBundle.BundleID+".json",
	)
	bundle, bundleDigest, resolved, err := fecflows.LoadProjectionBundle(ctx, storageRoot, basePath)
	if err != nil {
		return fmt.Errorf("verify committee-identity base bundle: %w", err)
	}
	if bundle.BundleID != manifest.Inputs.ReadinessBundle.BundleID || bundleDigest != manifest.Inputs.ReadinessBundle.ManifestSHA256 ||
		bundle.Cycle != manifest.Cycle || bundle.SourceReleaseID != manifest.SourceReleaseID {
		return fmt.Errorf("committee-identity base bundle differs from manifest inputs")
	}
	calculation, calculationDigest, err := fecflows.LoadPublishedManifest(ctx, storageRoot, resolved.CalculationManifestPath)
	if err != nil {
		return fmt.Errorf("verify committee-identity flow calculation: %w", err)
	}
	if calculation.CalculationSetID != manifest.Inputs.Calculation.CalculationSetID ||
		calculationDigest != manifest.Inputs.Calculation.ManifestSHA256 ||
		calculation.Results.CompressedSHA256 != manifest.Inputs.Calculation.ResultsSHA256 {
		return fmt.Errorf("committee-identity flow calculation differs from manifest inputs")
	}
	current, currentDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, resolved.CommitteeManifestPath, "committee-master")
	if err != nil {
		return fmt.Errorf("verify selected committee master: %w", err)
	}
	if err := verifyFactReference(ctx, storageRoot, manifest.Inputs.CurrentCommitteeMaster, current, currentDigest); err != nil {
		return fmt.Errorf("verify selected committee master: %w", err)
	}
	for _, reference := range append(
		append([]fecflowmastergaps.FactSetReference(nil), manifest.Inputs.SameCycleComparisons...),
		manifest.Inputs.NormalizedHistory...,
	) {
		path := filepath.Join(storageRoot, "facts", "fec", "classic", "committee-master", "manifests", reference.FactSetID+".json")
		loaded, digest, err := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, path, "committee-master")
		if err != nil {
			return fmt.Errorf("verify committee master %s: %w", reference.FactSetID, err)
		}
		if err := verifyFactReference(ctx, storageRoot, reference, loaded, digest); err != nil {
			return fmt.Errorf("verify committee master %s: %w", reference.FactSetID, err)
		}
	}
	for _, archive := range manifest.Inputs.RawHistory {
		path, err := storageartifact.Resolve(storageRoot, archive.StorageKey)
		if err != nil {
			return err
		}
		if err := verifyRawArchive(ctx, path, archive.ArchiveBytes, archive.ArchiveSHA256); err != nil {
			return fmt.Errorf("verify raw committee history %s: %w", archive.Cycle, err)
		}
	}
	return nil
}

func verifyFactReference(ctx context.Context, storageRoot string, reference fecflowmastergaps.FactSetReference, manifest fecoccurrence.ClassicFactManifest, manifestDigest string) error {
	if manifest.Dataset != reference.Dataset || manifest.Cycle != reference.Cycle ||
		manifest.SourceReleaseID != reference.SourceReleaseID || manifest.FactSetID != reference.FactSetID ||
		manifestDigest != reference.ManifestSHA256 || manifest.Facts.CompressedSHA256 != reference.FactsSHA256 {
		return fmt.Errorf("fact reference differs from immutable manifest")
	}
	path, err := storageartifact.Resolve(storageRoot, manifest.Facts.StorageKey)
	if err != nil {
		return err
	}
	return storageartifact.Verify(ctx, path, storageartifact.Descriptor{
		RecordCount: manifest.Facts.RecordCount, UncompressedBytes: manifest.Facts.UncompressedBytes,
		UncompressedSHA256: manifest.Facts.UncompressedSHA256, CompressedBytes: manifest.Facts.CompressedBytes,
		CompressedSHA256: manifest.Facts.CompressedSHA256, Compression: manifest.Facts.Compression,
		StorageKey: manifest.Facts.StorageKey,
	})
}

func verifyRawArchive(ctx context.Context, path string, expectedBytes uint64, expectedDigest string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	written, err := io.Copy(hash, &contextReader{ctx: ctx, reader: file})
	if err != nil {
		return err
	}
	if written < 0 || uint64(written) != expectedBytes || hex.EncodeToString(hash.Sum(nil)) != expectedDigest {
		return fmt.Errorf("archive bytes or SHA-256 differ")
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}
