package receiptparticipants

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestParallelScanValidatesCompleteShardsAndWorkerOwnership(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 5)
	dir := t.TempDir()
	r, e := publish(context.Background(), Options{StorageRoot: root, Workers: 3, MaxOutputBytes: 64 << 20}, m, []int{0, 1, 2}, dir, func(string) {})
	if e != nil {
		t.Fatal(e)
	}
	r.SourceRows = uint64(len(rows))
	r.State = "complete_cycle_participant_index"
	i := Inspector{participant: r, dir: dir}
	for _, workers := range []int{1, 4, 8} {
		seen := make([][]Row, workers)
		c, e := i.Scan(context.Background(), workers, func(w int, row Row) error { seen[w] = append(seen[w], cloneOwned(row)); return nil }, nil)
		if e != nil || !equalCensus(c, r.Census) {
			t.Fatal(e)
		}
		all := []Row{}
		for _, rs := range seen {
			all = append(all, rs...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].Ordinal < all[j].Ordinal })
		for j, row := range rows {
			if !reflect.DeepEqual(all[j], project(row)) {
				t.Fatal("changed source role row")
			}
		}
	}
	i.participant.Census.Rows++
	if _, e := i.Scan(context.Background(), 2, func(int, Row) error { return nil }, nil); e == nil {
		t.Fatal("accepted altered publication census")
	}
	i.participant = r
	if e := os.WriteFile(filepath.Join(dir, r.Files[0].Name), []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := i.Scan(context.Background(), 2, func(int, Row) error { return nil }, nil); e == nil {
		t.Fatal("accepted corrupt shard")
	}
}
func TestScanRefusesSampleCancellationAndConsumerFailure(t *testing.T) {
	i := Inspector{participant: Result{State: "sample"}}
	if _, e := i.Scan(context.Background(), 1, func(int, Row) error { return nil }, nil); e == nil {
		t.Fatal("sample accepted")
	}
	i.participant.State = "complete_cycle_participant_index"
	for _, workers := range []int{0, 9} {
		if _, e := i.Scan(context.Background(), workers, func(int, Row) error { return nil }, nil); e == nil {
			t.Fatal("bad workers")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := i.Scan(ctx, 1, func(int, Row) error { t.Fatal("visited canceled scan"); return nil }, nil); e == nil {
		t.Fatal("ignored cancellation")
	}
	failure := errors.New("consumer failed")
	_, e := scanFiles(context.Background(), Result{Files: make([]File, 8)}, 4, func(context.Context, File, int) (Census, error) { return Census{}, failure }, nil)
	if !errors.Is(e, failure) {
		t.Fatal("lost scan failure", e)
	}
}
