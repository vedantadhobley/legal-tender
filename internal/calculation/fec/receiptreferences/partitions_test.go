package receiptreferences

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func sliceSource(rows []Row) batchSource {
	return func(ctx context.Context, visit func([]Row) error) error {
		for start := 0; start < len(rows); start += 8192 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(rows[start:min(start+8192, len(rows))]); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestPartitionedCalculationMatchesSerialEngine(t *testing.T) {
	rows := []Row{}
	for report := 1; report <= 10; report++ {
		part := fixture()
		for i := range part {
			part[i].Ordinal += int64(len(rows))
			if part[i].File != nil {
				part[i].File = ptr(fmt.Sprintf("%d%s", report, *part[i].File))
			}
		}
		rows = append(rows, part...)
	}
	serial, _, _ := runRows(t, rows, 17, 256)
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "work"), 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			r, err := calculateReferences(context.Background(), s, Options{Workers: workers, RunRows: 17, FanIn: 8, FilterBytes: 256}, sliceSource(rows), func(string) {})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(serial.out.States, r.States) || r.SourceRows != uint64(len(rows)) || r.Decisions != (xsort.File{Name: r.Decisions.Name, Rows: serial.out.Decisions.Rows, Bytes: serial.out.Decisions.Bytes, SHA256: serial.out.Decisions.SHA256, ValuesSHA256: serial.out.Decisions.ValuesSHA256}) {
				t.Fatal("partition changed decisions or state counts")
			}
			if serial.out.ExactIncidences.ValuesSHA256 != r.ExactIncidences.ValuesSHA256 || serial.out.Neighbors.ValuesSHA256 != r.Neighbors.ValuesSHA256 || logicalID(serial.out) != logicalID(r) {
				t.Fatal("partition changed canonical evidence identity")
			}
			_, live := s.Stats()
			if live != r.LookupEvidence.Bytes+r.Decisions.Bytes+r.ExactIncidences.Bytes+r.Neighbors.Bytes {
				t.Fatal("unaccounted retained files")
			}
		})
	}
}

func TestPartitionDispatchOwnershipCancellationAndActualOverlap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows := []Row{}
	seenPartition := map[int]bool{}
	for file := 1; len(rows) < 4; file++ {
		r := Row{Ordinal: int64(file), Recipient: ptr("C00000001"), File: ptr(fmt.Sprint(file))}
		i := reportPartition(r, 4)
		if !seenPartition[i] {
			seenPartition[i] = true
			rows = append(rows, r)
		}
	}
	var arrived sync.WaitGroup
	arrived.Add(4)
	visitors := make([]func(Row) error, 4)
	for i := range visitors {
		visitors[i] = func(r Row) error {
			arrived.Done()
			arrived.Wait() // A serial dispatcher cannot pass this barrier.
			if r.Ordinal <= 0 || reportPartition(r, 4) != i {
				return fmt.Errorf("borrowed row corrupted")
			}
			return nil
		}
	}
	source := func(ctx context.Context, visit func([]Row) error) error {
		err := visit(rows)
		for i := range rows {
			rows[i].Ordinal = -1
		}
		return err
	}
	if err := dispatchScan(ctx, source, visitors); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("worker failure")
	for i := range visitors {
		visitors[i] = func(Row) error { return failure }
	}
	if err := dispatchScan(ctx, sliceSource(rows), visitors); !errors.Is(err, failure) {
		t.Fatal("worker error lost", err)
	}
	cancel()
	if err := dispatchScan(ctx, sliceSource(rows), visitors); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
}

func TestPartitionedEmptyReferencesAndFailure(t *testing.T) {
	for _, rows := range [][]Row{nil, {{Ordinal: 1}, {Ordinal: 2, Recipient: ptr("C00000001"), File: ptr("1")}}} {
		s, _ := xsort.NewWorkspace(filepath.Join(t.TempDir(), "work"), 16<<20)
		r, err := calculateReferences(context.Background(), s, Options{Workers: 8, RunRows: 20, FanIn: 8, FilterBytes: 8}, sliceSource(rows), func(string) {})
		if err != nil || r.SourceRows != uint64(len(rows)) || r.ReferenceRows != 0 || r.Decisions.Rows != 0 || r.Neighbors.Rows != 0 {
			t.Fatal("empty partition handling", err)
		}
	}
	s, _ := xsort.NewWorkspace(filepath.Join(t.TempDir(), "work"), 1)
	if _, err := calculateReferences(context.Background(), s, Options{Workers: 8, RunRows: 20, FanIn: 8, FilterBytes: 8}, sliceSource(fixture()), func(string) {}); err == nil {
		t.Fatal("parallel workspace failure accepted")
	}
}
