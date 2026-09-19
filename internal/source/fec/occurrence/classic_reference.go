package occurrence

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	release "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const ClassicReferencePolicy = "fec/classic-reference-content-equivalence@1.0.0"
const CandidateReferencePolicy = "fec/candidate-reference-content-equivalence@1.0.0"
const referenceByteLimit = 64 << 20
const referenceRowLimit = 1_000_000

type ReferenceIdentity struct {
	ID     string `json:"id"`
	SHA256 string `json:"manifest_sha256"`
}

type ReferenceArchive struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// A proof permits reuse of reference content, never substitution of source
// occurrence identities. The two immutable acquisition histories stay separate.
type ClassicReferenceProof struct {
	Policy              string            `json:"policy"`
	ProofID             string            `json:"proof_id"`
	Dataset             string            `json:"dataset"`
	Cycle               string            `json:"cycle"`
	FactSet             ReferenceIdentity `json:"fact_set"`
	Occurrences         ReferenceIdentity `json:"occurrence_set"`
	SourceRelease       ReferenceIdentity `json:"source_release"`
	TargetRelease       ReferenceIdentity `json:"target_release"`
	SourceArchive       ReferenceArchive  `json:"source_archive"`
	TargetArchive       ReferenceArchive  `json:"target_archive"`
	SourceContract      string            `json:"source_contract"`
	ParserVersion       string            `json:"parser_version"`
	NormalizerVersion   string            `json:"normalizer_version"`
	FactSchemaVersion   string            `json:"fact_schema_version"`
	Member              string            `json:"member"`
	MemberSHA256        string            `json:"member_sha256"`
	MemberBytes         uint64            `json:"member_bytes"`
	Rows                uint64            `json:"rows"`
	ContentValuesSHA256 string            `json:"ordered_content_values_sha256"`
}

// ProveClassicReference verifies both archive bodies, both staged members and
// every stored fact against complete normalization replay. It reads no mutable
// pointer and writes no publication. Scope is CN/CM/CCL reference content only.
func ProveClassicReference(ctx context.Context, root, facts, dataset, targetID, targetSHA string) (ClassicReferenceProof, error) {
	var out ClassicReferenceProof
	if err := requirePathInside(root, facts); err != nil {
		return out, err
	}
	policy := referencePolicy(dataset)
	if policy == "" {
		return out, fmt.Errorf("unsupported reference dataset")
	}
	spec, err := classic.Lookup(dataset)
	if err != nil {
		return out, err
	}
	m, md, err := LoadPublishedClassicFactManifest(root, facts, dataset)
	if err != nil {
		return out, err
	}
	o, od, err := LoadPublishedClassicOccurrenceManifest(root, filepath.Join(root, classicEvidenceBase(spec), "manifests", m.OccurrenceSetID+".json"), dataset)
	if err != nil {
		return out, err
	}
	if od != m.OccurrenceManifestSHA256 || o.SourceReleaseID != m.SourceReleaseID || o.SourceReleaseManifestSHA256 != m.SourceReleaseManifestSHA256 || o.Cycle != m.Cycle || o.SourceContract != m.SourceContract || o.Counts.Total == 0 || o.Counts.Total > referenceRowLimit || o.Counts.UniqueKeys != o.Counts.Total || o.Counts.Invalid != 0 || m.Counts.SourceOccurrences != o.Counts.Total || m.Counts.Facts != o.Counts.Total || m.Counts.ValidFacts != o.Counts.Total || m.Counts.InvalidFacts != 0 || m.Counts.ExcludedOccurrences != 0 {
		return out, fmt.Errorf("reference schema, ancestry or complete valid-row coverage mismatch")
	}
	origin, err := referenceRelease(root, m.SourceReleaseID, m.SourceReleaseManifestSHA256)
	if err != nil {
		return out, err
	}
	target, err := referenceRelease(root, targetID, targetSHA)
	if err != nil {
		return out, err
	}
	a, asha, err := selectedClassicOutput(origin, spec, m.Cycle)
	if err != nil {
		return out, err
	}
	b, _, err := selectedClassicOutput(target, spec, m.Cycle)
	if err != nil {
		return out, err
	}
	if asha != o.SourceArtifactSHA256 || a.CompressedSHA256 != o.StagedOutputSHA256 || a.Selection != o.Member || a.UncompressedSHA256 != b.UncompressedSHA256 || a.UncompressedByteCount != b.UncompressedByteCount || a.Selection != b.Selection || a.UncompressedByteCount > referenceByteLimit || a.CompressedByteCount > referenceByteLimit || b.CompressedByteCount > referenceByteLimit {
		return out, fmt.Errorf("reference source binding or selected content differs")
	}
	sa, err := verifyReferenceArchive(ctx, root, origin, a)
	if err != nil {
		return out, err
	}
	ta, err := verifyReferenceArchive(ctx, root, target, b)
	if err != nil {
		return out, err
	}
	left, err := replayReferenceFacts(ctx, root, m, o, a, spec)
	if err != nil {
		return out, err
	}
	right, err := replayReferenceFacts(ctx, root, m, o, b, spec)
	if err != nil {
		return out, err
	}
	if left != right {
		return out, fmt.Errorf("reference normalization replay differs")
	}
	out = ClassicReferenceProof{Policy: policy, Dataset: dataset, Cycle: m.Cycle,
		FactSet: ReferenceIdentity{m.FactSetID, md}, Occurrences: ReferenceIdentity{m.OccurrenceSetID, od},
		SourceRelease: ReferenceIdentity{m.SourceReleaseID, m.SourceReleaseManifestSHA256}, TargetRelease: ReferenceIdentity{targetID, targetSHA},
		SourceArchive: sa, TargetArchive: ta, SourceContract: m.SourceContract, ParserVersion: o.ParserVersion,
		NormalizerVersion: m.NormalizerVersion, FactSchemaVersion: m.FactSchemaVersion,
		Member: a.Selection, MemberSHA256: a.UncompressedSHA256, MemberBytes: a.UncompressedByteCount, Rows: m.Counts.Facts, ContentValuesSHA256: left}
	raw, err := json.Marshal(out)
	if err != nil {
		return ClassicReferenceProof{}, err
	}
	out.ProofID = referenceHash(raw)
	return out, nil
}

// SameReferenceContent compares content/schema evidence without conflating
// occurrence, publication or archive identities. Both proofs must be freshly
// verified by ProveClassicReference before this predicate can authorize reuse.
func SameReferenceContent(a, b ClassicReferenceProof) bool {
	return referencePolicy(a.Dataset) != "" && a.Policy == referencePolicy(a.Dataset) && b.Policy == a.Policy && a.Dataset == b.Dataset && a.Cycle == b.Cycle && a.TargetRelease == b.TargetRelease && a.SourceContract == b.SourceContract && a.ParserVersion == b.ParserVersion && a.NormalizerVersion == b.NormalizerVersion && a.FactSchemaVersion == b.FactSchemaVersion && a.Member == b.Member && a.MemberSHA256 == b.MemberSHA256 && a.MemberBytes == b.MemberBytes && a.Rows == b.Rows && a.ContentValuesSHA256 == b.ContentValuesSHA256
}

func referencePolicy(dataset string) string {
	switch classic.Dataset(dataset) {
	case classic.CommitteeMaster, classic.CandidateCommitteeLinkage:
		return ClassicReferencePolicy
	case classic.CandidateMaster:
		return CandidateReferencePolicy
	default:
		return ""
	}
}

func referenceHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func referenceRelease(root, id, expected string) (release.ReleaseManifest, error) {
	var r release.ReleaseManifest
	if !releaseIDPattern.MatchString(id) || !digestPattern.MatchString(expected) {
		return r, fmt.Errorf("exact reference release identity required")
	}
	p, err := artifact.Resolve(root, filepath.Join("releases/fec/manifests", id+".json"))
	if err != nil {
		return r, err
	}
	f, err := os.Open(p)
	if err != nil {
		return r, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if err != nil {
		return r, err
	}
	if len(raw) > 8<<20 || referenceHash(raw) != expected {
		return r, fmt.Errorf("reference release bytes mismatch")
	}
	if err := referenceJSON(raw, &r); err != nil {
		return r, err
	}
	if r.ReleaseID != id || len(release.ValidateKnownManifest(r)) != 0 {
		return r, fmt.Errorf("invalid reference source release")
	}
	return r, nil
}

func referenceJSON(raw []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(into); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("trailing reference JSON")
	}
	return nil
}

func verifyReferenceArchive(ctx context.Context, root string, r release.ReleaseManifest, s release.StagedOutput) (ReferenceArchive, error) {
	var a *release.PublishedArtifact
	for i := range r.Artifacts {
		if r.Artifacts[i].SourceID != s.SourceID {
			continue
		}
		if a != nil {
			return ReferenceArchive{}, fmt.Errorf("duplicate reference archive")
		}
		a = &r.Artifacts[i]
	}
	if a == nil || a.SHA256 != s.SourceArtifactSHA256 || a.ByteCount <= 0 || a.ByteCount > referenceByteLimit {
		return ReferenceArchive{}, fmt.Errorf("invalid or oversized reference archive")
	}
	p, err := artifact.Resolve(root, a.StorageKey)
	if err != nil {
		return ReferenceArchive{}, err
	}
	f, err := os.Open(p)
	if err != nil {
		return ReferenceArchive{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return ReferenceArchive{}, err
	}
	if !info.Mode().IsRegular() || info.Size() != a.ByteCount {
		return ReferenceArchive{}, fmt.Errorf("reference archive size mismatch")
	}
	h := sha256.New()
	if _, err := io.Copy(h, &contextReader{ctx: ctx, reader: f}); err != nil {
		return ReferenceArchive{}, err
	}
	if hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return ReferenceArchive{}, fmt.Errorf("reference archive hash mismatch")
	}
	z, err := zip.NewReader(f, info.Size())
	if err != nil {
		return ReferenceArchive{}, err
	}
	var member *zip.File
	for _, v := range z.File {
		if v.Name != s.Selection {
			continue
		}
		if member != nil {
			return ReferenceArchive{}, fmt.Errorf("duplicate selected ZIP member")
		}
		member = v
	}
	if member == nil || member.UncompressedSize64 != s.UncompressedByteCount || member.UncompressedSize64 > referenceByteLimit {
		return ReferenceArchive{}, fmt.Errorf("reference ZIP member size or membership mismatch")
	}
	reader, err := member.Open()
	if err != nil {
		return ReferenceArchive{}, err
	}
	defer reader.Close()
	h = sha256.New()
	n, err := io.Copy(h, io.LimitReader(&contextReader{ctx: ctx, reader: reader}, int64(s.UncompressedByteCount)+1))
	if err != nil {
		return ReferenceArchive{}, err
	}
	if uint64(n) != s.UncompressedByteCount || hex.EncodeToString(h.Sum(nil)) != s.UncompressedSHA256 {
		return ReferenceArchive{}, fmt.Errorf("reference ZIP body differs from selected content")
	}
	return ReferenceArchive{a.SHA256, a.ByteCount}, nil
}

func replayReferenceFacts(ctx context.Context, root string, m ClassicFactManifest, o ClassicManifest, output release.StagedOutput, spec classic.Spec) (string, error) {
	a := m.Facts
	r, err := artifact.Open[json.RawMessage](ctx, root, artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey})
	if err != nil {
		return "", err
	}
	defer r.Abort()
	h := sha256.New()
	counts, err := scanClassicFacts(ctx, o, output, spec, root, func(want ClassicFact) error {
		raw, ok, err := r.Next()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("reference facts end before source")
		}
		var got, normalized ClassicFact
		if err := referenceJSON(raw, &got); err != nil {
			return err
		}
		b, err := json.Marshal(want)
		if err != nil {
			return err
		}
		if err := referenceJSON(b, &normalized); err != nil {
			return err
		}
		if !reflect.DeepEqual(got, normalized) {
			return fmt.Errorf("reference fact differs from full source replay")
		}
		// Exclude only archive occurrence provenance; all content and semantic
		// identities remain. The complete original row was compared above.
		want.OccurrenceSetID, want.OccurrenceID, want.SourceReleaseID = "", "", ""
		b, err = json.Marshal(want)
		if err != nil {
			return err
		}
		h.Write(append(b, '\n'))
		return nil
	}, nil)
	if err != nil {
		return "", err
	}
	if counts != m.Counts {
		return "", fmt.Errorf("reference fact census differs from replay")
	}
	if _, more, err := r.Next(); err != nil || more {
		return "", fmt.Errorf("reference facts have extra rows or invalid backing: %v", err)
	}
	if err := r.Close(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
