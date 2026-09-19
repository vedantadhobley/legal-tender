package occurrence

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	release "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestScheduleEExactMembershipPreservesAncestry(t *testing.T) {
	for _, mode := range []string{"same_version", "changed_archive", "changed_relation", "changed_size", "wrong_target_sha", "corrupt_fact", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			root := t.TempDir()
			r, rd := sourceReleaseV2Fixture(t, root, "membership-origin", "", exactScheduleEFixture(t, "escaped-copy-text.copy"))
			o, err := PublishScheduleEOccurrences(ctx, r, rd, "2024", "membership-occurrences", Options{StorageRoot: root, DiskAvailable: fixtureDiskAvailable})
			if err != nil {
				t.Fatal(err)
			}
			m, err := PublishScheduleEFacts(ctx, r, rd, filepath.Join(root, scheduleEEvidenceBase(), "manifests", o.OccurrenceSetID+".json"), "membership-facts", Options{StorageRoot: root, DiskAvailable: fixtureDiskAvailable})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, scheduleEFactBase(), "manifests", m.FactSetID+".json")
			b, _ := json.Marshal(r)
			var target release.ReleaseManifest
			_ = json.Unmarshal(b, &target)
			target.ReleaseID = "fec-" + referenceHash([]byte("target"))
			target.PriorReleaseID = r.ReleaseID
			for i := range target.Artifacts {
				if target.Artifacts[i].SourceID == release.ScheduleESourceID && mode == "changed_archive" {
					target.Artifacts[i].SHA256 = referenceHash([]byte("other archive"))
				}
			}
			for i := range target.StagedOutputs {
				s := &target.StagedOutputs[i]
				if s.SourceID != release.ScheduleESourceID {
					continue
				}
				switch mode {
				case "changed_archive":
					s.SourceArtifactSHA256 = referenceHash([]byte("other archive"))
				case "changed_relation":
					s.UncompressedSHA256 = referenceHash([]byte("other relation"))
				case "changed_size":
					s.UncompressedByteCount++
				}
			}
			b, _ = json.Marshal(target)
			sha := referenceHash(b)
			writeFile(t, filepath.Join(root, "releases/fec/manifests", target.ReleaseID+".json"), b)
			if mode == "wrong_target_sha" {
				sha = referenceHash([]byte("wrong"))
			}
			if mode == "corrupt_fact" {
				writeFile(t, filepath.Join(root, m.Facts.StorageKey), []byte("corrupt"))
			}
			if mode == "cancelled" {
				cancel()
			}
			got, err := ProveScheduleEReleaseMembership(ctx, root, path, target.ReleaseID, sha)
			if mode != "same_version" {
				if err == nil {
					t.Fatal("accepted", mode)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.SourceRelease.ID == got.TargetRelease.ID || got.FactSet.ID != m.FactSetID || got.Rows != 1 {
				t.Fatal(got)
			}
			again, err := ProveScheduleEReleaseMembership(ctx, root, path, target.ReleaseID, sha)
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatal("unstable proof", err)
			}
		})
	}
}
