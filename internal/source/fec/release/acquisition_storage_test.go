package release

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryBytesCountsInodesNotPathsOrContents(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	original := filepath.Join(root, "original")
	writeStorageTestFile(t, original, 123)
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(original, filepath.Join(root, "nested", "alias")); err != nil {
		t.Fatal(err)
	}
	// Identical data on a different inode still occupies a separate file.
	writeStorageTestFile(t, filepath.Join(root, "copy"), 123)
	writeStorageTestFile(t, filepath.Join(root, "empty"), 0)
	// Sparse logical bytes remain fully budgeted; this is not st_blocks/du.
	writeStorageTestFile(t, filepath.Join(root, "sparse"), 1<<30)
	outside := filepath.Join(t.TempDir(), "outside")
	writeStorageTestFile(t, outside, 999)
	if err := os.Symlink(outside, filepath.Join(root, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "absent"), filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root, filepath.Join(root, "loop")); err != nil {
		t.Fatal(err)
	}
	got, err := directoryBytes(root)
	if err != nil || got != 246+1<<30 {
		t.Fatalf("bytes=%d err=%v", got, err)
	}
	// A link outside the counted tree must not make its in-tree inode disappear.
	if err := os.Link(original, filepath.Join(t.TempDir(), "external-alias")); err != nil {
		t.Fatal(err)
	}
	again, err := directoryBytes(root)
	if err != nil || again != got {
		t.Fatalf("external link changed bytes=%d err=%v", again, err)
	}
}

func TestDirectoryBytesMissingRootFails(t *testing.T) {
	t.Parallel()
	if _, err := directoryBytes(filepath.Join(t.TempDir(), "missing")); !os.IsNotExist(err) {
		t.Fatalf("error=%v", err)
	}
}

func TestAcquisitionAndStageShareInodeAccounting(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	scheduleRoot := scheduleAStorageRoot(root)
	if err := os.MkdirAll(scheduleRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(scheduleRoot, "original")
	writeStorageTestFile(t, original, 300<<30)
	if err := os.Link(original, filepath.Join(scheduleRoot, "alias")); err != nil {
		t.Fatal(err)
	}
	length := int64(50 << 30)
	plan := ReleasePlan{CandidateReleaseID: "fixture", ChangedSourceIDs: []string{ScheduleASourceID}}
	selected := map[string]SelectedSource{ScheduleASourceID: {ContentLength: &length}}
	disk := func(string) (DiskSpace, error) { return DiskSpace{AvailableBytes: 2 << 40}, nil }
	acquisition, issues, err := preflightStorage(root, scheduleRoot, plan, selected, disk)
	if err != nil || len(issues) != 0 || !acquisition.Passed || acquisition.ScheduleAHotBytesBefore != 300<<30 {
		t.Fatalf("preflight=%+v issues=%v err=%v", acquisition, issues, err)
	}
	var baseline StageStorage
	configureStageStorage(&baseline, StageOptions{})
	stage, issues, err := inspectStageStorage(root, baseline, StageOptions{DiskUsage: disk}, true)
	if err != nil || len(issues) != 0 || !stage.Passed || stage.ScheduleAHotBytesAfter != acquisition.ScheduleAHotBytesBefore {
		t.Fatalf("stage=%+v issues=%v err=%v", stage, issues, err)
	}
}

func TestStageStorageRejectsWorkspaceOverflow(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(scheduleAStorageRoot(root), 0o750); err != nil {
		t.Fatal(err)
	}
	baseline := StageStorage{LargestExtractWorkingBytes: ^uint64(0), WorkingMarginBytes: 1}
	if _, _, err := inspectStageStorage(root, baseline, StageOptions{}, true); err == nil {
		t.Fatal("overflow accepted")
	}
}

func writeStorageTestFile(t *testing.T, path string, size int64) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
}
