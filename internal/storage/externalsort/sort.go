package externalsort

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Sorter bounds both record count and encoded bytes, and merges with bounded
// fan-in. It never materializes an entire equal-key group.
type Sorter struct {
	ctx            context.Context
	space          *Workspace
	RunRows, FanIn int
	buffer         []Record
	bytes          int
	runs           []File
}

func New(ctx context.Context, space *Workspace, rows, fanIn int) (*Sorter, error) {
	if rows < 1 || rows > 100000 || fanIn < 2 || fanIn > 16 {
		return nil, fmt.Errorf("sort rows 1..100000 and fan-in 2..16 required")
	}
	return &Sorter{ctx: ctx, space: space, RunRows: rows, FanIn: fanIn}, nil
}
func (s *Sorter) Add(r Record) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	n := 17 + len(r.Key) + len(r.Data)
	if n > MaxRecordBytes {
		return fmt.Errorf("oversized sort record")
	}
	if len(s.buffer) > 0 && (len(s.buffer) >= s.RunRows || s.bytes+n > 32<<20) {
		if err := s.flush(); err != nil {
			return err
		}
	}
	r.Key = strings.Clone(r.Key)
	r.Data = slices.Clone(r.Data)
	s.buffer = append(s.buffer, r)
	s.bytes += n
	return nil
}
func (s *Sorter) flush() error {
	if len(s.buffer) == 0 {
		return nil
	}
	slices.SortFunc(s.buffer, Compare)
	w, err := s.space.Writer(s.ctx)
	if err != nil {
		return err
	}
	defer w.Abort()
	for _, r := range s.buffer {
		if err = w.Add(r); err != nil {
			return err
		}
	}
	d, err := w.Finish()
	if err != nil {
		return err
	}
	s.runs = append(s.runs, d)
	clear(s.buffer)
	s.buffer = s.buffer[:0]
	s.bytes = 0
	return nil
}
func (s *Sorter) Finish() (File, error) {
	if err := s.flush(); err != nil {
		return File{}, err
	}
	if len(s.runs) == 0 {
		w, err := s.space.Writer(s.ctx)
		if err != nil {
			return File{}, err
		}
		return w.Finish()
	}
	return MergeRuns(s.ctx, s.space, s.runs, s.FanIn)
}

// MergeRuns combines verified workspace-owned streams. It removes inputs only
// after the replacement passes full readback and count conservation. Distinct
// calls may run concurrently, but must own disjoint input files.
func MergeRuns(ctx context.Context, space *Workspace, runs []File, fanIn int) (File, error) {
	if len(runs) == 0 || fanIn < 2 || fanIn > 16 {
		return File{}, fmt.Errorf("nonempty runs and fan-in 2..16 required")
	}
	seen := map[string]bool{}
	for _, f := range runs {
		if !space.owns(f) || seen[f.Name] {
			return File{}, fmt.Errorf("merge inputs must be distinct owned files")
		}
		seen[f.Name] = true
	}
	for len(runs) > 1 {
		next := []File{}
		for start := 0; start < len(runs); start += fanIn {
			group := runs[start:min(start+fanIn, len(runs))]
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			d, err := merge(ctx, space, group)
			if err != nil {
				return File{}, err
			}
			next = append(next, d)
			for _, f := range group {
				if err = space.Remove(f); err != nil {
					return File{}, err
				}
			}
		}
		runs = next
	}
	return runs[0], nil
}

type item struct {
	record Record
	reader *Reader
}
type queue []item

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return Compare(q[i].record, q[j].record) < 0 }
func (q queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(v any)        { *q = append(*q, v.(item)) }
func (q *queue) Pop() any          { v := (*q)[len(*q)-1]; *q = (*q)[:len(*q)-1]; return v }
func merge(ctx context.Context, s *Workspace, files []File) (File, error) {
	var q queue
	readers := []*Reader{}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()
	var expected uint64
	for _, f := range files {
		expected += f.Rows
		r, err := Open(ctx, s.Dir, f)
		if err != nil {
			return File{}, err
		}
		readers = append(readers, r)
		v, err := r.Next()
		if err == nil {
			heap.Push(&q, item{v, r})
		} else if err != io.EOF {
			return File{}, err
		}
	}
	w, err := s.Writer(ctx)
	if err != nil {
		return File{}, err
	}
	defer w.Abort()
	for len(q) > 0 {
		v := heap.Pop(&q).(item)
		if err = w.Add(v.record); err != nil {
			return File{}, err
		}
		next, err := v.reader.Next()
		if err == nil {
			heap.Push(&q, item{next, v.reader})
		} else if err != io.EOF {
			return File{}, err
		}
	}
	d, err := w.Finish()
	if err == nil && d.Rows != expected {
		return File{}, fmt.Errorf("merge count conservation")
	}
	return d, err
}
