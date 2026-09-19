package flowreconciliation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const committeeFactBase = "facts/fec/classic/committee-master"

// Master facts can predate the coordinated release, but neither same-cycle
// naming nor release ancestry alone is enough: all selected bytes must agree.
func loadCommittee(ctx context.Context, root, path string, r Result) (FactReference, error) {
	return loadClassicReference(ctx, root, path, r, "committee-master", "cm", "cm.txt")
}

// LoadLinkageReference applies the same source-byte ancestry gate as committee
// masters. Older facts may be reused only if this release selects their exact
// archive and staged member. It does not create or advance a publication.
func LoadLinkageReference(ctx context.Context, root, path string, r Result) (FactReference, error) {
	return loadClassicReference(ctx, root, path, r, "candidate-committee-linkage", "ccl", "ccl.txt")
}

func loadClassicReference(ctx context.Context, root, path string, r Result, dataset, code, member string) (FactReference, error) {
	base := filepath.Join("facts/fec/classic", dataset)
	path, err := inside(root, path)
	if err != nil {
		return FactReference{}, err
	}
	m, digest, err := occurrence.LoadPublishedClassicFactManifest(root, path, dataset)
	if err != nil {
		return FactReference{}, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return FactReference{}, err
	}
	if hashBytes(content) != digest {
		return FactReference{}, fmt.Errorf("committee pointer changed while loading")
	}
	if err := verifyPinnedBytes(root, base, m.FactSetID, content); err != nil {
		return FactReference{}, err
	}
	if m.Cycle != r.Cycle || m.Counts.InvalidFacts != 0 || m.Counts.ExcludedOccurrences != 0 || m.Counts.SourceOccurrences != m.Counts.Facts {
		return FactReference{}, fmt.Errorf("committee cycle or fact conservation mismatch")
	}
	o, od, err := occurrence.LoadPublishedClassicOccurrenceManifest(root, filepath.Join(root, "evidence/fec/classic", dataset, "manifests", m.OccurrenceSetID+".json"), dataset)
	if err != nil {
		return FactReference{}, err
	}
	if od != m.OccurrenceManifestSHA256 || o.Cycle != m.Cycle || o.Counts.Total != m.Counts.SourceOccurrences {
		return FactReference{}, fmt.Errorf("committee fact/occurrence ancestry mismatch")
	}
	var selected committeeSource
	for i, ref := range []struct{ id, digest string }{{r.Input.ReleaseID, r.Input.ReleaseSHA256}, {m.SourceReleaseID, m.SourceReleaseManifestSHA256}, {o.SourceReleaseID, o.SourceReleaseManifestSHA256}} {
		manifest, digest, err := loadRelease(root, filepath.Join(root, "releases/fec/manifests", ref.id+".json"))
		if err != nil {
			return FactReference{}, err
		}
		if digest != ref.digest {
			return FactReference{}, fmt.Errorf("committee source release digest mismatch")
		}
		bound, err := bindClassicSource(manifest, o, code, member)
		if err != nil {
			return FactReference{}, err
		}
		if i > 0 && bound != selected {
			return FactReference{}, fmt.Errorf("coordinated release replaced committee-master source bytes")
		}
		selected = bound
	}
	d := artifact.Descriptor{RecordCount: m.Facts.RecordCount, UncompressedBytes: m.Facts.UncompressedBytes, UncompressedSHA256: m.Facts.UncompressedSHA256, CompressedBytes: m.Facts.CompressedBytes, CompressedSHA256: m.Facts.CompressedSHA256, Compression: m.Facts.Compression, StorageKey: m.Facts.StorageKey}
	reader, err := artifact.Open[occurrence.ClassicFact](ctx, root, d)
	if err != nil {
		return FactReference{}, err
	}
	defer reader.Abort()
	for {
		f, ok, err := reader.Next()
		if err != nil {
			return FactReference{}, err
		}
		if !ok {
			break
		}
		if f.SchemaVersion != m.FactSchemaVersion || f.FactType != m.FactType || f.Dataset != m.Dataset || f.Cycle != m.Cycle || f.OccurrenceSetID != m.OccurrenceSetID || f.SourceReleaseID != m.SourceReleaseID || f.SourceContract != m.SourceContract || f.State != "valid" {
			return FactReference{}, fmt.Errorf("committee fact row differs from pinned manifest")
		}
	}
	if err := reader.Close(); err != nil {
		return FactReference{}, err
	}
	return FactReference{m.FactSetID, digest, m.SourceReleaseID, selected.ArchiveSHA256, m.Counts.Facts}, nil
}

type committeeSource struct {
	ArchiveSHA256, CompressedSHA256, UncompressedSHA256 string
	ArchiveBytes                                        int64
	CompressedBytes, UncompressedBytes                  uint64
}

func bindCommittee(r release.ReleaseManifest, o occurrence.ClassicManifest) (committeeSource, error) {
	return bindClassicSource(r, o, "cm", "cm.txt")
}

func bindClassicSource(r release.ReleaseManifest, o occurrence.ClassicManifest, code, member string) (committeeSource, error) {
	var result committeeSource
	artifacts, outputs := 0, 0
	for _, a := range r.Artifacts {
		if a.SourceID == "fec:"+code+":"+o.Cycle {
			artifacts++
			result.ArchiveSHA256, result.ArchiveBytes = a.SHA256, a.ByteCount
		}
	}
	for _, s := range r.StagedOutputs {
		if s.SourceID != "fec:"+code+":"+o.Cycle || s.Period != o.Cycle {
			continue
		}
		outputs++
		if s.SelectionKind != "member" || s.Selection != member || s.SourceArtifactSHA256 != result.ArchiveSHA256 || s.CompressedSHA256 != o.StagedOutputSHA256 || s.SourceArtifactSHA256 != o.SourceArtifactSHA256 {
			return result, fmt.Errorf("release does not select exact committee-master occurrence bytes")
		}
		result.CompressedSHA256, result.CompressedBytes = s.CompressedSHA256, s.CompressedByteCount
		result.UncompressedSHA256, result.UncompressedBytes = s.UncompressedSHA256, s.UncompressedByteCount
	}
	if artifacts != 1 || outputs != 1 {
		return result, fmt.Errorf("release must select one exact committee-master source")
	}
	return result, nil
}
