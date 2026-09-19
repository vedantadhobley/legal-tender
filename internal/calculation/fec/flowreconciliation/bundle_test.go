package flowreconciliation

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestCommitteeSourceBinding(t *testing.T) {
	o := occurrence.ClassicManifest{Cycle: "2024", SourceArtifactSHA256: "archive", StagedOutputSHA256: "selected"}
	r := release.ReleaseManifest{Artifacts: []release.PublishedArtifact{{SelectedSource: release.SelectedSource{SourceID: "fec:cm:2024"}, SHA256: "archive", ByteCount: 10}}, StagedOutputs: []release.StagedOutput{{SourceID: "fec:cm:2024", Period: "2024", SelectionKind: "member", Selection: "cm.txt", SourceArtifactSHA256: "archive", CompressedSHA256: "selected", CompressedByteCount: 20, UncompressedSHA256: "copy", UncompressedByteCount: 30}}}
	first, err := bindCommittee(r, o)
	if err != nil {
		t.Fatal(err)
	}
	r.ReleaseID = "different-release-same-selected-bytes"
	second, err := bindCommittee(r, o)
	if err != nil || first != second {
		t.Fatal("unchanged source rejected", err)
	}
	// A repackaged archive is still a different occurrence ancestry even when
	// its selected member is byte-identical. Publish release-matched facts;
	// do not silently relabel old occurrences with the new archive identity.
	repacked := r
	repacked.Artifacts = append([]release.PublishedArtifact(nil), r.Artifacts...)
	repacked.StagedOutputs = append([]release.StagedOutput(nil), r.StagedOutputs...)
	repacked.Artifacts[0].SHA256 = "repacked"
	repacked.StagedOutputs[0].SourceArtifactSHA256 = "repacked"
	if _, err := bindCommittee(repacked, o); err == nil {
		t.Fatal("archive ancestry change accepted as old occurrence")
	}
	for _, change := range []func(*release.StagedOutput){func(s *release.StagedOutput) { s.CompressedSHA256 = "changed" }, func(s *release.StagedOutput) { s.Selection = "other.txt" }, func(s *release.StagedOutput) { s.Period = "2022" }, func(s *release.StagedOutput) { s.SourceArtifactSHA256 = "changed" }} {
		changed := r
		changed.StagedOutputs = append([]release.StagedOutput(nil), r.StagedOutputs...)
		change(&changed.StagedOutputs[0])
		if _, err := bindCommittee(changed, o); err == nil {
			t.Fatal("wrong source accepted")
		}
	}
	r.StagedOutputs[0].UncompressedByteCount++
	changed, err := bindCommittee(r, o)
	if err != nil || changed == first {
		t.Fatal("uncompressed source change hidden")
	}
	r.StagedOutputs = append(r.StagedOutputs, r.StagedOutputs[0])
	if _, err := bindCommittee(r, o); err == nil {
		t.Fatal("ambiguous source accepted")
	}
}

func TestLinkageSourceBindingUsesExactCCLNotCommitteeMaster(t *testing.T) {
	o := occurrence.ClassicManifest{Cycle: "2024", SourceArtifactSHA256: "archive", StagedOutputSHA256: "member"}
	r := release.ReleaseManifest{Artifacts: []release.PublishedArtifact{{SelectedSource: release.SelectedSource{SourceID: "fec:ccl:2024"}, SHA256: "archive", ByteCount: 10}}, StagedOutputs: []release.StagedOutput{{SourceID: "fec:ccl:2024", Period: "2024", SelectionKind: "member", Selection: "ccl.txt", SourceArtifactSHA256: "archive", CompressedSHA256: "member", UncompressedSHA256: "body"}}}
	if _, err := bindClassicSource(r, o, "ccl", "ccl.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := bindCommittee(r, o); err == nil {
		t.Fatal("linkage source accepted as committee master")
	}
	r.Artifacts[0].SHA256 = "repacked-archive"
	r.StagedOutputs[0].SourceArtifactSHA256 = "repacked-archive"
	if _, err := bindClassicSource(r, o, "ccl", "ccl.txt"); err == nil {
		t.Fatal("unchanged member hid replaced archive ancestry")
	}
}

func TestBundlePublicationIdentityAndGuards(t *testing.T) {
	o, i, a, b := publicationFixture(t)
	r, err := publishLoaded(context.Background(), o, i, a, b)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := jsonBytes(r)
	bundle := newBundle(r, hashBytes(data), i.A)
	ctx := context.Background()
	first, err := commitBundle(ctx, o.StorageRoot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	second, err := commitBundle(ctx, o.StorageRoot, bundle)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("bundle replay differs", err)
	}
	path := filepath.Join(o.StorageRoot, BundleBase, "current", o.Cycle+".json")
	if loaded, _, err := readBundle(o.StorageRoot, path); err != nil || !reflect.DeepEqual(bundle, loaded) {
		t.Fatal("bundle readback differs", err)
	}
	// A stored ready flag is insufficient: source and master backing are absent.
	if _, _, _, err := LoadBundle(ctx, o.StorageRoot, path); err == nil {
		t.Fatal("ready flag bypassed source checks")
	}
	if metadata, _, err := LoadBundleMetadata(o.StorageRoot, path); err != nil || !reflect.DeepEqual(metadata, bundle) {
		t.Fatal("metadata-only rejection preflight requires a valid pinned bundle", err)
	}
	for _, change := range []func(*Bundle){func(v *Bundle) { v.Committee.ManifestSHA256 = strings.Repeat("f", 64) }, func(v *Bundle) { v.Calculation.ManifestSHA256 = strings.Repeat("f", 64) }, func(v *Bundle) { v.Input.B.ManifestSHA256 = strings.Repeat("f", 64) }} {
		v := bundle
		change(&v)
		if bundleIdentity(v) == bundle.BundleID {
			t.Fatal("input omitted from bundle identity")
		}
	}
	for _, change := range []func(*Bundle){func(v *Bundle) { v.Consumer = "combined_money_graph" }, func(v *Bundle) { v.IdentityScope = "historical_automatic" }, func(v *Bundle) { v.EconomicFlowEligible = true }, func(v *Bundle) { v.Checks = v.Checks[:2] }, func(v *Bundle) { v.Committee.FactSetID = "../../other" }} {
		v := bundle
		change(&v)
		v.BundleID = bundleIdentity(v)
		content, _ := jsonBytes(v)
		if err := os.WriteFile(path, content, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readBundle(o.StorageRoot, path); err == nil {
			t.Fatal("unsafe bundle accepted")
		}
	}
}
