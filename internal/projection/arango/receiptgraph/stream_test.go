package receiptgraph

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	xs "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func fixtureStream(t *testing.T, ordinals []uint64) (Options, loaded) {
	t.Helper()
	dir := t.TempDir()
	w, e := xs.NewWorkspace(filepath.Join(dir, "data"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.Writer(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range ordinals {
		// Pinned v1 wire code zero: incident/transaction uniqueness unassessed.
		key := binary.BigEndian.AppendUint64(nil, n)
		payload := make([]byte, 10)
		if e = out.Add(xs.Record{Key: string(key), Ordinal: n, Data: payload}); e != nil {
			t.Fatal(e)
		}
	}
	file, e := out.Finish()
	if e != nil {
		t.Fatal(e)
	}
	return Options{Conduits: filepath.Join(dir, "manifest.json"), First: 2, Rows: 3}, loaded{p: p.Result{SourceRows: 10, Files: []p.File{{First: 1, Last: 3}, {First: 4, Last: 10}}}, c: c.Result{Decisions: file, EligibleRoleRows: uint64(len(ordinals)), States: map[string]uint64{c.Unassessed: uint64(len(ordinals))}, Amounts: map[string]uint64{"not_assessed": uint64(len(ordinals))}}}
}
func TestStreamingJoinWholeArtifactAndSelectedMembership(t *testing.T) {
	for _, scenario := range []string{"valid", "missing", "inapplicable", "duplicate", "tail_corrupt", "wrong_census"} {
		t.Run(scenario, func(t *testing.T) {
			ordinals := []uint64{1, 2, 4, 9}
			if scenario == "missing" {
				ordinals = []uint64{1, 4, 9}
			}
			if scenario == "duplicate" {
				ordinals = []uint64{1, 2, 2, 4, 9}
			}
			o, l := fixtureStream(t, ordinals)
			if scenario == "wrong_census" {
				l.c.EligibleRoleRows++
			}
			if scenario == "tail_corrupt" {
				f, e := os.OpenFile(filepath.Join(filepath.Dir(o.Conduits), "data", l.c.Decisions.Name), os.O_WRONLY|os.O_APPEND, 0)
				if e != nil {
					t.Fatal(e)
				}
				_, _ = f.Write([]byte("corrupt tail"))
				_ = f.Close()
			}
			readRows := 0
			visited := 0
			e := streamWith(context.Background(), o, l, func(f p.File, visit func(p.Row) error) error {
				for n := f.First; n <= f.Last; n++ {
					readRows++
					row := p.Row{Ordinal: int64(n)}
					if n == 1 || n == 2 || n == 4 || n == 9 {
						row.ReceiptType = ptr("15E")
					}
					if scenario == "inapplicable" && n == 2 {
						row.Memo = true
					}
					if e := visit(row); e != nil {
						return e
					}
				}
				return nil
			}, func(row p.Row, d *c.Decision) error {
				visited++
				if uint64(row.Ordinal) < o.First || uint64(row.Ordinal) >= o.First+o.Rows {
					t.Fatal("sample range leaked")
				}
				return nil
			})
			if scenario == "valid" {
				if e != nil || visited != 3 || readRows != 10 {
					t.Fatalf("stream failed conservation/full selected-shard scan: %v %d %d", e, visited, readRows)
				}
			} else if e == nil {
				t.Fatal("accepted invalid membership/backing")
			}
		})
	}
}
