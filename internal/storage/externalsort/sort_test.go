package externalsort

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func readAll(t *testing.T, s *Workspace, f File) []Record {
	t.Helper()
	r, err := Open(context.Background(), s.Dir, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := []Record{}
	for {
		v, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
}
func TestExternalMergeConservesDuplicatesAcrossGeometry(t *testing.T) {
	want := []Record{}
	for i := 300; i > 0; i-- {
		want = append(want, Record{Key: string([]byte{byte(i % 4), 0, 'x'}), Tag: byte(i % 3), Ordinal: uint64(i % 7), Data: []byte{byte(i % 13)}})
	}
	want = append(want, want[0])
	slices.SortFunc(want, Compare)
	var digest string
	for _, rows := range []int{1, 7, 100} {
		s, err := NewWorkspace(filepath.Join(t.TempDir(), "work"), 16<<20)
		if err != nil {
			t.Fatal(err)
		}
		sorter, _ := New(context.Background(), s, rows, 3)
		for i := len(want) - 1; i >= 0; i-- {
			if err = sorter.Add(want[i]); err != nil {
				t.Fatal(err)
			}
		}
		f, err := sorter.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, readAll(t, s, f)) {
			t.Fatal("lost/reordered records")
		}
		if digest != "" && digest != f.ValuesSHA256 {
			t.Fatal("geometry changed values")
		}
		digest = f.ValuesSHA256
		if s.Live != f.Bytes || s.Peak > s.Limit {
			t.Fatal("workspace accounting")
		}
		if err = s.Remove(File{Name: "../unowned"}); err == nil {
			t.Fatal("unowned deletion")
		}
	}
}
func TestSortFailuresAndEmptyInput(t *testing.T) {
	ctx := context.Background()
	s, _ := NewWorkspace(filepath.Join(t.TempDir(), "work"), 1<<20)
	sorter, _ := New(ctx, s, 2, 2)
	f, err := sorter.Finish()
	if err != nil || f.Rows != 0 {
		t.Fatal(err)
	}
	if err = sorter.Add(Record{Data: make([]byte, MaxRecordBytes)}); err == nil {
		t.Fatal("oversize")
	}
	if err = os.WriteFile(filepath.Join(s.Dir, f.Name), []byte("broken"), 0640); err != nil {
		t.Fatal(err)
	}
	if err = Verify(ctx, s.Dir, f); err == nil {
		t.Fatal("corruption accepted")
	}
	tiny, _ := NewWorkspace(filepath.Join(t.TempDir(), "work"), 1)
	limited, _ := New(ctx, tiny, 2, 2)
	limited.Add(Record{Key: "a"})
	if _, err = limited.Finish(); err == nil {
		t.Fatal("budget ignored")
	}
	if tiny.Live > tiny.Limit {
		t.Fatal("budget overshot")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	other, _ := New(cancelled, s, 2, 2)
	if err = other.Add(Record{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
