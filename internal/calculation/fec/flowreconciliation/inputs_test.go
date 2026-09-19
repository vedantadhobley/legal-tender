package flowreconciliation

import (
	occurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	release "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"testing"
)

func TestCoordinatedSourceBinding(t *testing.T) {
	a := occurrence.ScheduleAColumnarManifest{Cycle: "2024"}
	a.SourceReplay.CompressedSHA256 = "compressed"
	a.SourceReplay.CompressedBytes = 10
	a.SourceReplay.UncompressedSHA256 = "copy"
	a.SourceReplay.UncompressedBytes = 100
	r := release.ReleaseManifest{InventoryVersion: release.ActiveInventoryVersion, Artifacts: []release.PublishedArtifact{{SelectedSource: release.SelectedSource{SourceID: release.ScheduleASourceID}, SHA256: "a"}, {SelectedSource: release.SelectedSource{SourceID: release.ScheduleBSourceID}, SHA256: "b", ByteCount: 20}}, StagedOutputs: []release.StagedOutput{{SourceID: release.ScheduleASourceID, Period: "2024", SourceArtifactSHA256: "a", CompressedSHA256: "compressed", CompressedByteCount: 10, UncompressedSHA256: "copy", UncompressedByteCount: 100}}}
	b := occurrence.ScheduleBColumnarManifest{Cycle: "2024", Relation: "disclosure.fec_fitem_sched_b_2023_2024", SourceArtifactSHA256: "b", SourceArtifactByteCount: 20}
	if sha, err := bindA(r, a); err != nil || sha != "a" {
		t.Fatal(sha, err)
	}
	if err := bindB(r, b); err != nil {
		t.Fatal(err)
	}
	r.ReleaseID = "different-coordinated-release-same-source-bytes"
	if _, err := bindA(r, a); err != nil {
		t.Fatal(err)
	}
	if err := bindB(r, b); err != nil {
		t.Fatal(err)
	}
	b.SourceArtifactSHA256 = "changed"
	if err := bindB(r, b); err == nil {
		t.Fatal("changed B archive accepted")
	}
	b.SourceArtifactSHA256 = "b"
	b.Relation = "wrong"
	if err := bindB(r, b); err == nil {
		t.Fatal("wrong B relation accepted")
	}
	a.SourceReplay.CompressedSHA256 = "changed"
	if _, err := bindA(r, a); err == nil {
		t.Fatal("changed A input accepted")
	}
	a.SourceReplay.CompressedSHA256 = "compressed"
	r.StagedOutputs = append(r.StagedOutputs, r.StagedOutputs[0])
	if _, err := bindA(r, a); err == nil {
		t.Fatal("ambiguous A selection")
	}
}
