package release

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func storageTestBudget(t *testing.T, ctx context.Context, root string, hot, free uint64) *writeBudget {
	t.Helper()
	if err := os.MkdirAll(scheduleAStorageRoot(root), 0o750); err != nil {
		t.Fatal(err)
	}
	budget, err := newWriteBudget(ctx, root, StageStorage{ScheduleAHotCapBytes: hot + 10, FreeFloorBytes: 100, WorkingMarginBytes: 10}, func(string) (DiskSpace, error) {
		return DiskSpace{AvailableBytes: free + 110}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return budget
}

func storageTestWriter(t *testing.T, budget *writeBudget, name string, hot bool) (*os.File, io.Writer) {
	t.Helper()
	path := filepath.Join(budget.root, name)
	if hot {
		path = filepath.Join(scheduleAStorageRoot(budget.root), name)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	writer, err := budget.writer(file, hot)
	if err != nil {
		t.Fatal(err)
	}
	return file, writer
}

func TestWriteBudgetExactBoundariesAndStickyStop(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		hot, free uint64
		chargeHot bool
	}{
		{"hot", 7, 100, true},
		{"free", 100, 7, true},
		{"other source does not consume hot", 0, 7, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := storageTestBudget(t, context.Background(), t.TempDir(), tc.hot, tc.free)
			file, writer := storageTestWriter(t, budget, "partial", tc.chargeHot)
			if n, err := writer.Write([]byte("1234567")); n != 7 || err != nil {
				t.Fatalf("exact boundary: n=%d err=%v", n, err)
			}
			for range 2 {
				if n, err := writer.Write([]byte("8")); n != 0 || !errors.Is(err, errStorageBudget) {
					t.Fatalf("stop: n=%d err=%v", n, err)
				}
			}
			info, _ := file.Stat()
			if info.Size() != 7 {
				t.Fatalf("grew past budget: %d", info.Size())
			}
		})
	}
}

func TestWriteBudgetChunksConcurrentGrowthWithoutDiskFeedback(t *testing.T) {
	t.Parallel()
	const allowance = 3 * storageWriteChunk
	budget := storageTestBudget(t, context.Background(), t.TempDir(), allowance, allowance)
	var group sync.WaitGroup
	var written atomic.Int64
	for i := range 4 {
		_, writer := storageTestWriter(t, budget, string(rune('a'+i)), true)
		group.Go(func() {
			n, err := writer.Write(bytes.Repeat([]byte("x"), 2*storageWriteChunk))
			written.Add(int64(n))
			if err != nil && !errors.Is(err, errStorageBudget) {
				t.Errorf("write: %v", err)
			}
		})
	}
	group.Wait()
	hot, err := directoryBytes(scheduleAStorageRoot(budget.root))
	if err != nil || written.Load() != allowance || hot != allowance {
		t.Fatalf("shared allowance: written=%d hot=%d err=%v", written.Load(), hot, err)
	}
}

func TestWriteBudgetRefreshExternalUsageAndCancellation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"free", "hot", "cancel", "stat error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			budget := storageTestBudget(t, ctx, t.TempDir(), 32<<20, 32<<20)
			file, writer := storageTestWriter(t, budget, "partial", true)
			if _, err := writer.Write(make([]byte, storageHotRefreshBytes)); err != nil {
				t.Fatal(err)
			}
			want := errStorageBudget
			switch mode {
			case "free":
				budget.diskUsage = func(string) (DiskSpace, error) { return DiskSpace{AvailableBytes: 109}, nil }
			case "hot":
				writeStorageTestFile(t, filepath.Join(scheduleAStorageRoot(budget.root), "external"), 24<<20)
			case "cancel":
				cancel()
				want = context.Canceled
			case "stat error":
				want = errors.New("stat unavailable")
				budget.diskUsage = func(string) (DiskSpace, error) { return DiskSpace{}, want }
			}
			if n, err := writer.Write([]byte("x")); n != 0 || !errors.Is(err, want) {
				t.Fatalf("mode=%s n=%d err=%v", mode, n, err)
			}
			info, _ := file.Stat()
			if info.Size() != storageHotRefreshBytes {
				t.Fatal("write happened after stop")
			}
		})
	}
}

func TestWriteBudgetCountsOrphansAndRejectsLegacyWorkspace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	orphan := filepath.Join(stageTemporaryDirectory(root, "old-candidate", "old-run"), ".selected-orphan.zst")
	if err := os.MkdirAll(filepath.Dir(orphan), 0o750); err != nil {
		t.Fatal(err)
	}
	writeStorageTestFile(t, orphan, 30)
	budget := storageTestBudget(t, context.Background(), root, 30, 100)
	_, writer := storageTestWriter(t, budget, "partial", true)
	if n, err := writer.Write([]byte("x")); n != 0 || !errors.Is(err, errStorageBudget) {
		t.Fatalf("orphan ignored: n=%d err=%v", n, err)
	}
	legacy := filepath.Join(root, "raw", "fec", "staging", "old", "selected", "run", ".selected-legacy.zst")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o750); err != nil {
		t.Fatal(err)
	}
	writeStorageTestFile(t, legacy, 1)
	if err := checkLegacyWorkspace(root); err == nil {
		t.Fatal("uncounted legacy workspace accepted")
	}
	for _, path := range []string{orphan, legacy} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("removed retained temporary evidence: %v", err)
		}
	}
}

func TestStorageLockAndUnsafePartialPaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	lock, err := lockSourceStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lockSourceStorage(root); !errors.Is(err, errStorageBusy) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("concurrent lock: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err = lockSourceStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	_ = lock.Close()
	if _, err := os.Stat(filepath.Join(root, "raw", "fec", ".storage.lock")); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(root, "immutable")
	if err := os.WriteFile(original, []byte("preserve"), 0o640); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"hardlink", "symlink"} {
		path := filepath.Join(root, mode)
		link := os.Link
		if mode == "symlink" {
			link = os.Symlink
		}
		if err := link(original, path); err != nil {
			t.Fatal(err)
		}
		if file, _, _, err := openPartial(path, 1); err == nil {
			_ = file.Close()
			t.Fatal("unsafe partial accepted")
		}
	}
	content, err := os.ReadFile(original)
	if err != nil || string(content) != "preserve" {
		t.Fatalf("immutable changed: %q %v", content, err)
	}
	budget := storageTestBudget(t, context.Background(), root, 100, 100)
	file, err := os.OpenFile(original, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := budget.writer(file, false); err == nil {
		t.Fatal("shared output accepted")
	}
}

func TestWriteBudgetOverflowFailsClosed(t *testing.T) {
	t.Parallel()
	_, err := newWriteBudget(context.Background(), t.TempDir(), StageStorage{
		ScheduleAHotCapBytes: ^uint64(0), WorkingMarginBytes: ^uint64(0), LargestExtractWorkingBytes: 1,
	}, func(string) (DiskSpace, error) { return DiskSpace{AvailableBytes: ^uint64(0)}, nil })
	if err == nil {
		t.Fatal("overflow accepted")
	}
}
