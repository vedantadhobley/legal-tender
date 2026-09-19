package externalsort

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSharedWorkspaceConcurrentSortsAndAssembly(t *testing.T) {
	s, err := NewWorkspace(filepath.Join(t.TempDir(), "work"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]File, 8)
	var wg sync.WaitGroup
	for i := range files {
		wg.Go(func() {
			sorter, err := New(context.Background(), s, 32, 4)
			if err != nil {
				t.Error(err)
				return
			}
			for j := 999; j >= 0; j-- {
				if err = sorter.Add(Record{Key: fmt.Sprintf("%04d", j), Ordinal: uint64(i*1000 + j)}); err != nil {
					t.Error(err)
					return
				}
			}
			files[i], err = sorter.Finish()
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if _, err = MergeRuns(context.Background(), s, []File{files[0], files[0]}, 4); err == nil {
		t.Fatal("repeated merge input accepted")
	}
	f, err := MergeRuns(context.Background(), s, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	if f.Rows != 8000 || len(readAll(t, s, f)) != 8000 {
		t.Fatal("concurrent conservation")
	}
	peak, live := s.Stats()
	if live != f.Bytes || peak > s.Limit {
		t.Fatal("shared budget accounting")
	}
}

func TestConcurrentWritersShareOneHardCap(t *testing.T) {
	s, err := NewWorkspace(filepath.Join(t.TempDir(), "work"), 96)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			w, err := s.Writer(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			defer w.Abort()
			if err = w.Add(Record{Key: "concurrent", Ordinal: uint64(i)}); err != nil {
				t.Error(err)
				return
			}
			// Some complete streams fit; other writes must stop before the cap.
			w.Finish()
		})
	}
	wg.Wait()
	var physical uint64
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		physical += uint64(info.Size())
	}
	peak, live := s.Stats()
	if live != physical || peak > s.Limit || live > s.Limit {
		t.Fatal("concurrent cap overshoot or lost partial accounting", live, physical, peak)
	}
}
