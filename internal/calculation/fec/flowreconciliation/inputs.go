package flowreconciliation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	occurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	release "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func loadInputs(ctx context.Context, o Options) (Inputs, occurrence.ScheduleAColumnarManifest, occurrence.ScheduleBColumnarManifest, error) {
	var inputs Inputs
	a, ad, err := occurrence.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.ScheduleA)
	if err != nil {
		return inputs, a, occurrence.ScheduleBColumnarManifest{}, err
	}
	b, bd, err := occurrence.LoadPublishedScheduleBColumnarManifest(ctx, o.StorageRoot, o.ScheduleB)
	if err != nil {
		return inputs, a, b, err
	}
	if a.Cycle != o.Cycle || b.Cycle != o.Cycle || a.Counts.Facts != a.Counts.SourceOccurrences || a.Counts.InvalidFacts != 0 || a.Counts.ExcludedOccurrences != 0 {
		return inputs, a, b, fmt.Errorf("cycle mismatch or non-dense Schedule A facts")
	}
	current, rd, err := loadRelease(o.StorageRoot, o.Release)
	if err != nil {
		return inputs, a, b, err
	}
	priorA, pad, err := loadRelease(o.StorageRoot, filepath.Join(o.StorageRoot, "releases/fec/manifests", a.SourceReleaseID+".json"))
	if err != nil {
		return inputs, a, b, err
	}
	priorB, pbd, err := loadRelease(o.StorageRoot, filepath.Join(o.StorageRoot, "releases/fec/manifests", b.SourceReleaseID+".json"))
	if err != nil {
		return inputs, a, b, err
	}
	if pad != a.SourceReleaseManifestSHA256 || pbd != b.SourceReleaseManifestSHA256 {
		return inputs, a, b, fmt.Errorf("fact release ancestry digest mismatch")
	}
	aArtifact, err := bindA(current, a)
	if err != nil {
		return inputs, a, b, err
	}
	oldA, err := bindA(priorA, a)
	if err != nil {
		return inputs, a, b, err
	}
	if aArtifact != oldA {
		return inputs, a, b, fmt.Errorf("coordinated release replaced Schedule A source bytes")
	}
	if err := bindB(current, b); err != nil {
		return inputs, a, b, err
	}
	if err := bindB(priorB, b); err != nil {
		return inputs, a, b, err
	}
	inputs = Inputs{current.ReleaseID, rd, FactReference{a.FactSetID, ad, a.SourceReleaseID, aArtifact, a.Counts.Facts}, FactReference{b.FactSetID, bd, b.SourceReleaseID, b.SourceArtifactSHA256, b.Counts.Facts}}
	return inputs, a, b, nil
}

func bindA(r release.ReleaseManifest, a occurrence.ScheduleAColumnarManifest) (string, error) {
	var archive string
	for _, v := range r.Artifacts {
		if v.SourceID == release.ScheduleASourceID {
			archive = v.SHA256
		}
	}
	matches := 0
	for _, v := range r.StagedOutputs {
		if v.SourceID != release.ScheduleASourceID || v.Period != a.Cycle {
			continue
		}
		if v.SourceArtifactSHA256 != archive || v.CompressedSHA256 != a.SourceReplay.CompressedSHA256 || v.CompressedByteCount != a.SourceReplay.CompressedBytes || v.UncompressedSHA256 != a.SourceReplay.UncompressedSHA256 || v.UncompressedByteCount != a.SourceReplay.UncompressedBytes {
			return "", fmt.Errorf("release does not select the exact Schedule A fact input")
		}
		matches++
	}
	if archive == "" || matches != 1 {
		return "", fmt.Errorf("release must select one exact Schedule A relation")
	}
	return archive, nil
}
func bindB(r release.ReleaseManifest, b occurrence.ScheduleBColumnarManifest) error {
	inventory, known := release.InventoryForVersion(r.InventoryVersion)
	if !known {
		return fmt.Errorf("unknown coordinated inventory")
	}
	selected := false
	for _, source := range inventory.Sources {
		if source.SourceID != release.ScheduleBSourceID {
			continue
		}
		for _, relation := range source.RelationSelections {
			if relation.Scope == b.Cycle && relation.Name == b.Relation && relation.Materialization == release.RelationMaterializationArchiveDirect {
				selected = true
			}
		}
	}
	if !selected {
		return fmt.Errorf("release does not select the exact Schedule B cycle relation")
	}
	for _, v := range r.Artifacts {
		if v.SourceID == release.ScheduleBSourceID {
			if v.SHA256 == b.SourceArtifactSHA256 && v.ByteCount == b.SourceArtifactByteCount {
				return nil
			}
			return fmt.Errorf("release does not select the exact Schedule B source")
		}
	}
	return fmt.Errorf("release lacks Schedule B")
}

func loadRelease(root, path string) (release.ReleaseManifest, string, error) {
	var r release.ReleaseManifest
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return r, "", err
	}
	resolved, err := artifact.Resolve(root, relative)
	if err != nil {
		return r, "", err
	}
	content, err := os.ReadFile(resolved)
	if err != nil {
		return r, "", err
	}
	if err := strictJSON(content, &r); err != nil {
		return r, "", err
	}
	if digest, err := hex.DecodeString(strings.TrimPrefix(r.ReleaseID, "fec-")); err != nil || len(digest) != 32 || !strings.HasPrefix(r.ReleaseID, "fec-") {
		return r, "", fmt.Errorf("invalid release identity")
	}
	if issues := release.ValidateKnownManifest(r); len(issues) != 0 {
		return r, "", fmt.Errorf("invalid coordinated release: %s", issues[0].Message)
	}
	immutable, err := os.ReadFile(filepath.Join(root, "releases/fec/manifests", r.ReleaseID+".json"))
	if err != nil {
		return r, "", err
	}
	if !bytes.Equal(immutable, content) {
		return r, "", fmt.Errorf("release pointer differs from immutable bytes")
	}
	return r, hashBytes(content), nil
}
func hashBytes(b []byte) string { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func hashJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return hashBytes(b)
}
func strictJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}
