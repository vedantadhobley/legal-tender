package receiptparticipants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Opt-in, read-only gate. All output rows are checked. Bounded benchmark runs
// also compare every projected value with the old generic source reader; a full
// publication checks eight explicit whole shards by that separate reader path.
func TestRetainedParticipantCorpus(t *testing.T) {
	path := os.Getenv("LT_PARTICIPANT_MANIFEST")
	if path == "" {
		t.Skip("retained participant manifest not requested")
	}
	r, err := Load(path, os.Getenv("LT_PARTICIPANT_ID"))
	if err != nil {
		t.Fatal(err)
	}
	root := os.Getenv("LT_PARTICIPANT_SOURCE_ROOT")
	m, sha, err := occ.LoadPublishedScheduleAColumnarManifest(context.Background(), root, os.Getenv("LT_PARTICIPANT_SOURCE_MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	if m.FactSetID != r.FactSetID || sha != r.ManifestSHA256 || m.Cycle != r.Cycle || m.Counts.Facts != r.SourceRows {
		t.Fatal("source ancestry")
	}
	dir := filepath.Join(filepath.Dir(path), "data")
	var next atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := newCensus()
	var compared uint64
	for range 8 {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(r.Files) {
					return
				}
				d := r.Files[i]
				var source occ.ScheduleAColumnarShard
				found := false
				for _, s := range m.Shards {
					if s.FirstSourceRowOrdinal == d.First && s.LastSourceRowOrdinal == d.Last && s.SHA256 == d.SourceSHA256 {
						source, found = s, true
						break
					}
				}
				if !found {
					t.Error("unbacked output shard", i)
					return
				}
				c, err := ReadShard(context.Background(), dir, d, nil)
				if err != nil {
					t.Error(i, err)
					return
				}
				// These are corpus sample positions, never production routing rules.
				compare := r.Scope == "selected_whole_shards_not_complete_cycle"
				for _, n := range []uint64{0, 33, 66, 99, 132, 165, 198, 231} {
					compare = compare || source.Index == n
				}
				if compare {
					compareOldReader(t, root, m, source, d, r, dir)
				}
				mu.Lock()
				total.merge(c)
				if compare {
					compared += d.Rows
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if !equalCensus(total, r.Census) {
		t.Fatal("complete output census disagreement")
	}
	t.Logf("verified all %d participant rows; old-reader equivalence over %d rows; complete source inspection at each compared shard boundary", total.Rows, compared)
}

func compareOldReader(t *testing.T, root string, m occ.ScheduleAColumnarManifest, s occ.ScheduleAColumnarShard, d File, result Result, dir string) {
	t.Helper()
	path, err := artifact.Resolve(root, s.StorageKey)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = verifyOpen(context.Background(), f, s.Bytes, s.SHA256); err != nil {
		t.Fatal(err)
	}
	p, err := parquet.OpenFile(f, int64(s.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[fundingbasis.SourceEvidenceRow](p)
	defer r.Close()
	buffer := make([]fundingbasis.SourceEvidenceRow, 8192)
	var count uint64
	h := sha256.New()
	canonical := make([]byte, 0, 2048)
	for {
		n, e := r.Read(buffer)
		for _, row := range buffer[:n] {
			if uint64(row.Ordinal) != d.First+count {
				t.Fatal("old reader occurrence order")
			}
			canonical = project(row).canonical(canonical)
			h.Write(canonical)
			count++
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if n == 0 {
			t.Fatal(io.ErrNoProgress)
		}
	}
	if count != d.Rows || hex.EncodeToString(h.Sum(nil)) != d.ValuesSHA256 {
		t.Fatal("old-reader value disagreement", s.Index)
	}
	for _, ordinal := range []uint64{d.First, d.Last} {
		opened, err := inspectVerified(context.Background(), root, m, result, dir, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		if len(opened.Source.Fields) != 99 || opened.IdentityResolved || opened.FinancialEligibility {
			t.Fatal("full source inspection boundary")
		}
	}
}
