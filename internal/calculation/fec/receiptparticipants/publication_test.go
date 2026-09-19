package receiptparticipants

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/narrowparquet"
)

func ptr[T any](v T) *T { return &v }
func fixtureRows() []fundingbasis.SourceEvidenceRow {
	rows := make([]fundingbasis.SourceEvidenceRow, 12)
	for i := range rows {
		rows[i] = fundingbasis.SourceEvidenceRow{Ordinal: int64(i + 1), Cycle: 2024, Normalization: "valid", Recipient: ptr("C00000001"), Individual: ptr(true), ReceiptType: ptr("15"), Amount: ptr(int64(123)), AmountState: "reported_value"}
	}
	rows[1].Amount = ptr(int64(-456))
	rows[2].Amount = ptr(int64(0))
	rows[3].Amount = nil
	rows[3].AmountState = "source_null"
	rows[4].Memo = true
	rows[4].MemoText = ptr("memo text")
	rows[4].ConduitName = ptr("reported name")
	rows[5].ReceiptType = ptr("15K")
	rows[5].Contributor = ptr("C00000002")
	rows[5].CleanContributor = ptr("C00000002")
	rows[5].Entity = ptr("IND")
	rows[6].Recipient = nil
	rows[7].Contributor = ptr("")
	rows[7].ReceiptType = ptr("UNKNOWN")
	rows[7].Individual = ptr(false)
	rows[8].ReceiptType = ptr("15E")
	rows[8].ConduitID = ptr("C00000003")
	rows[8].Entity = ptr("IND")
	rows[8].File = ptr("42")
	rows[8].Transaction = ptr("original")
	rows[8].BackReference = ptr("memo")
	rows[8].BackSchedule = ptr("SA")
	rows[9].Contributor = ptr("C00000004")
	rows[9].Individual = ptr(true)
	rows[9].ConduitID = ptr("bad")
	rows[9].Entity = ptr("É中")
	rows[10].Individual = nil
	rows[10].Amount = ptr(int64(math.MinInt64))
	rows[11].Amount = ptr(int64(math.MaxInt64))
	return rows
}

func sourceFixture(t *testing.T, rows []fundingbasis.SourceEvidenceRow, size int) (string, occ.ScheduleAColumnarManifest) {
	t.Helper()
	root := t.TempDir()
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	selected := parquet.SchemaOf(new(fundingbasis.SourceEvidenceRow))
	m := occ.ScheduleAColumnarManifest{Cycle: "2024", FactSetID: strings.Repeat("a", 64), Counts: occ.ScheduleAFactCounts{Facts: uint64(len(rows)), ValidFacts: uint64(len(rows)), SourceOccurrences: uint64(len(rows))}}
	for start := 0; start < len(rows); start += size {
		end := min(start+size, len(rows))
		var b bytes.Buffer
		w := parquet.NewWriter(&b, schema.Parquet(), parquet.MaxRowsPerRowGroup(3), parquet.PageBufferSize(512))
		for _, input := range rows[start:end] {
			r := make(parquet.Row, schema.ColumnCount())
			for i, path := range schema.Parquet().Columns() {
				c, _ := schema.Parquet().Lookup(path...)
				v := parquet.NullValue()
				if c.MaxDefinitionLevel == 0 {
					switch c.Node.Type().Kind() {
					case parquet.ByteArray:
						v = parquet.ByteArrayValue(nil)
					case parquet.Int64:
						v = parquet.Int64Value(0)
					case parquet.Int32:
						v = parquet.Int32Value(0)
					case parquet.Boolean:
						v = parquet.BooleanValue(false)
					default:
						t.Fatal("unexpected fixture type")
					}
				}
				r[i] = v.Level(0, 0, i)
			}
			for i, v := range selected.Deconstruct(nil, input) {
				at, ok := schema.ColumnIndex(selected.Columns()[i][0])
				if !ok {
					t.Fatal("fixture field binding")
				}
				r[at] = v.Level(v.RepetitionLevel(), v.DefinitionLevel(), at)
			}
			if _, err = w.WriteRows([]parquet.Row{r}); err != nil {
				t.Fatal(err)
			}
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("source-%d.parquet", len(m.Shards))
		if err = os.WriteFile(filepath.Join(root, name), b.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(b.Bytes())
		count := uint64(end - start)
		m.Shards = append(m.Shards, occ.ScheduleAColumnarShard{Index: uint64(len(m.Shards)), StorageKey: name, Bytes: uint64(b.Len()), SHA256: hex.EncodeToString(h[:]), Facts: count, SourceRows: count, ValidFacts: count, FirstSourceRowOrdinal: uint64(start + 1), LastSourceRowOrdinal: uint64(end)})
	}
	return root, m
}

func TestCompleteParticipantPublicationPreservesGrainAndWorkerReplay(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 5)
	var expected []Row
	for _, r := range rows {
		expected = append(expected, project(r))
	}
	var previous []File
	for _, workers := range []int{1, 3, 8} {
		dir := t.TempDir()
		r, err := publish(context.Background(), Options{StorageRoot: root, Workers: workers, MaxOutputBytes: 64 << 20}, m, []int{0, 1, 2}, dir, func(string) {})
		if err != nil {
			t.Fatal(err)
		}
		var got []Row
		for _, file := range r.Files {
			_, err = ReadShard(context.Background(), dir, file, func(v Row) error {
				got = append(got, cloneOwned(v))
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("participant values changed\n%#v\n%#v", got, expected)
		}
		if previous != nil && !reflect.DeepEqual(previous, r.Files) {
			t.Fatal("worker count changed file identity")
		}
		previous = r.Files
		if r.Census.Rows != 12 || r.Census.UnknownAmounts != 1 || r.Census.NegativeAmounts != 2 || r.Census.ZeroAmounts != 1 || r.Census.MemoRows != 1 || r.Census.Components["overlapping_individual_and_committee"] != 1 {
			t.Fatal(r.Census)
		}
		if got[5].SourceRoute != "reported_committee_observation" || !got[5].IndividualOverlap || !got[5].EntityConflict {
			t.Fatal("overlap became a second donor", got[5])
		}
		if got[8].ConduitState != "reported_structured_id_identity_unverified" || got[8].ReportedSourceID != nil {
			t.Fatal("conduit became source", got[8])
		}
	}
}

func TestNarrowSourceMappingAgainstGenericReader(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 12)
	s := m.Shards[0]
	f, err := os.Open(filepath.Join(root, s.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	p, err := parquet.OpenFile(f, int64(s.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := narrowparquet.New[fundingbasis.SourceEvidenceRow](p)
	if err != nil {
		t.Fatal(err)
	}
	defer narrow.Close()
	old := parquet.NewGenericReader[fundingbasis.SourceEvidenceRow](p)
	defer old.Close()
	a, b := make([]fundingbasis.SourceEvidenceRow, 1), make([]fundingbasis.SourceEvidenceRow, 1)
	for i := range rows {
		if _, err = narrow.Read(a); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if _, err = old.Read(b); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(a[0], rows[i]) {
			t.Fatal("source field mapping differs", i)
		}
	}
}

func TestFailureCancellationAndCorruption(t *testing.T) {
	for _, kind := range []string{"cap", "cancel", "source bytes", "ordinal", "cycle", "normalization", "amount", "long id", "callback"} {
		t.Run(kind, func(t *testing.T) {
			rows := fixtureRows()
			switch kind {
			case "ordinal":
				rows[1].Ordinal = 99
			case "cycle":
				rows[1].Cycle = 2026
			case "normalization":
				rows[1].Normalization = "invalid"
			case "amount":
				rows[1].Amount = nil
			case "long id":
				rows[1].Contributor = ptr(strings.Repeat("x", 4097))
			}
			root, m := sourceFixture(t, rows, 5)
			ctx := context.Background()
			if kind == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if kind == "source bytes" {
				path := filepath.Join(root, m.Shards[0].StorageKey)
				b, _ := os.ReadFile(path)
				b[len(b)/2] ^= 1
				if err := os.WriteFile(path, b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "callback" {
				want := errors.New("consumer failed")
				if err := scanSource(ctx, root, m.Shards[0], 2024, func([]fundingbasis.SourceEvidenceRow) error { return want }); !errors.Is(err, want) {
					t.Fatal(err)
				}
				return
			}
			cap := uint64(64 << 20)
			if kind == "cap" {
				cap = 1
			}
			if _, err := publish(ctx, Options{StorageRoot: root, Workers: 3, MaxOutputBytes: cap}, m, []int{0, 1, 2}, t.TempDir(), func(string) {}); err == nil {
				t.Fatal("invalid publication succeeded")
			}
		})
	}
	root, m := sourceFixture(t, fixtureRows(), 12)
	dir := t.TempDir()
	r, err := publish(context.Background(), Options{StorageRoot: root, Workers: 1, MaxOutputBytes: 64 << 20}, m, []int{0}, dir, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	bad := r.Files[0]
	bad.ValuesSHA256 = strings.Repeat("0", 64)
	if _, err = ReadShard(context.Background(), dir, bad, nil); err == nil {
		t.Fatal("changed values accepted")
	}
	bad = r.Files[0]
	bad.Rows--
	if _, err = ReadShard(context.Background(), dir, bad, nil); err == nil {
		t.Fatal("changed row count accepted")
	}
}

func TestCanonicalNullEmptySignedAndAppearanceIdentity(t *testing.T) {
	r := project(fixtureRows()[0])
	a := r.canonical(nil)
	r.Contributor = ptr("")
	if bytes.Equal(a, r.canonical(nil)) {
		t.Fatal("null/empty collapsed")
	}
	r.Amount = ptr(int64(math.MinInt64))
	a = r.canonical(nil)
	r.Amount = ptr(int64(math.MaxInt64))
	if bytes.Equal(a, r.canonical(nil)) {
		t.Fatal("signed amount collapsed")
	}
	aID, _ := AppearanceID(strings.Repeat("a", 64), 1)
	bID, _ := AppearanceID(strings.Repeat("a", 64), 2)
	cID, _ := AppearanceID(strings.Repeat("b", 64), 1)
	if aID == bID || aID == cID {
		t.Fatal("source appearances collapsed")
	}
	if _, err := AppearanceID("bad", 0); err == nil {
		t.Fatal("unbound appearance accepted")
	}
}
