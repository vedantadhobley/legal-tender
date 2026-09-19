package identityassertions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func ptr(s string) *string { return &s }

func fixtureRows() []Receipt {
	rows := make([]Receipt, 12)
	for i := range rows {
		r := Receipt{Ordinal: int64(i + 1), Cycle: 2024, Normalization: "valid"}
		v := reflect.ValueOf(&r).Elem()
		for field := 3; field < v.NumField(); field++ {
			v.Field(field).Set(reflect.ValueOf(ptr(fmt.Sprintf("%d:%d É中", i, field))))
		}
		rows[i] = r
	}
	rows[0].Employer = nil
	rows[1].Employer = ptr("")
	rows[2].Employer = ptr("  \t")
	rows[3].Employer = ptr("RETIRED")
	rows[4].Employer = ptr("SELF-EMPLOYED")
	rows[5].Employer = ptr("  EXAMPLE, INC.  ")
	rows[6].Name = nil
	rows[7].Name = ptr("")
	rows[8].ReceiptDate = nil
	rows[9].ReceiptDate = ptr("2023-06-04 00:00:00")
	rows[10] = rows[9]
	rows[10].Ordinal = 11 // Duplicate-looking source appearances remain distinct.
	rows[11].Entity = ptr("UNKNOWN")
	return rows
}

// Write full original-schema rows, not a narrow-schema stand-in. Each selected
// field is unique in the fixture, so swapping a mapping cannot pass silently.
func sourceFixture(t *testing.T, rows []Receipt, size int) (string, occ.ScheduleAColumnarManifest) {
	t.Helper()
	root := t.TempDir()
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	selected := parquet.SchemaOf(new(Receipt))
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
		count := uint64(end - start)
		m.Shards = append(m.Shards, occ.ScheduleAColumnarShard{Index: uint64(len(m.Shards)), StorageKey: name, Bytes: uint64(b.Len()), SHA256: hashBytes(b.Bytes()), Facts: count, SourceRows: count, ValidFacts: count, FirstSourceRowOrdinal: uint64(start + 1), LastSourceRowOrdinal: uint64(end)})
	}
	return root, m
}

func owned(r Receipt) Receipt {
	b, _ := json.Marshal(r)
	var out Receipt
	_ = json.Unmarshal(b, &out)
	return out
}

func TestReceiptExactFieldsWorkerReplayAndNoResolution(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 5)
	var previous []ReceiptProof
	for _, workers := range []int{1, 3, 8} {
		got := make([]Receipt, len(rows))
		proofs, err := scanReceipts(context.Background(), root, m, 2024, workers, func(worker int, row Receipt) error {
			if worker < 0 || worker >= workers {
				return fmt.Errorf("worker ownership")
			}
			got[row.Ordinal-1] = owned(row)
			return nil
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, rows) {
			t.Fatal("reported fields changed")
		}
		if previous != nil && !reflect.DeepEqual(previous, proofs) {
			t.Fatal("workers changed result")
		}
		previous = proofs
		if proofs[0].Fields[15] != (FieldCounts{Null: 1, Empty: 1, Nonempty: 3}) {
			t.Fatal("employer state loss", proofs[0])
		}
	}
	a, _ := receiptparticipants.AppearanceID(m.FactSetID, 10)
	b, _ := receiptparticipants.AppearanceID(m.FactSetID, 11)
	if a == b || bytes.Equal(rows[9].canonical(nil), rows[10].canonical(nil)) {
		t.Fatal("appearance collapse")
	}
	if reflect.TypeOf(Receipt{}).NumField() != len(ReceiptColumns())+3 {
		t.Fatal("unmapped receipt fields")
	}
	for i, name := range ReceiptColumns() {
		if reflect.TypeOf(Receipt{}).Field(i+3).Tag.Get("parquet") != name {
			t.Fatal("field mapping order")
		}
	}
}

func TestReceiptFailuresNeverReturnAcceptedProof(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := scanReceipts(ctx, root, m, 2024, 2, nil, nil); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatal("canceled result", got, err)
	}
	want := errors.New("consumer rejected")
	if got, err := scanReceipts(context.Background(), root, m, 2024, 3, func(int, Receipt) error { return want }, nil); !errors.Is(err, want) || got != nil {
		t.Fatal("consumer failure", got, err)
	}
	if _, err := scanReceipts(context.Background(), root, m, 2022, 3, nil, nil); err == nil {
		t.Fatal("cross-cycle assertion")
	}
	for _, workers := range []int{0, 9} {
		if _, err := scanReceipts(context.Background(), root, m, 2024, workers, nil, nil); err == nil {
			t.Fatal("worker cap")
		}
	}
	m.Shards[1].FirstSourceRowOrdinal++
	if _, err := scanReceipts(context.Background(), root, m, 2024, 3, nil, nil); err == nil {
		t.Fatal("ordinal gap")
	}
	m.Shards[1].FirstSourceRowOrdinal--
	p := filepath.Join(root, m.Shards[0].StorageKey)
	body, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)/2] ^= 1
	if err = os.WriteFile(p, body, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = scanReceipts(context.Background(), root, m, 2024, 3, nil, nil); err == nil {
		t.Fatal("corrupt backing accepted")
	}
	for _, mutate := range []func(*Receipt){func(r *Receipt) { r.Ordinal = 0 }, func(r *Receipt) { r.Normalization = "invalid" }, func(r *Receipt) { r.Employer = ptr(strings.Repeat("x", maxTextBytes+1)) }} {
		r := rows[0]
		mutate(&r)
		if r.validate(1, 2024) == nil {
			t.Fatal("invalid row accepted")
		}
	}
}

func committeeFixture(t *testing.T) (string, occ.ClassicFactManifest, []Committee) {
	t.Helper()
	root := t.TempDir()
	m := occ.ClassicFactManifest{FactSetID: strings.Repeat("b", 64), FactSchemaVersion: occ.ClassicFactSchemaVersion, FactType: "fec.committee.v1", Dataset: "committee-master", Cycle: "2024", OccurrenceSetID: "occurrences", SourceReleaseID: "release", SourceContract: "contract", Counts: occ.ClassicFactCounts{Facts: 3, ValidFacts: 3, SourceOccurrences: 4, ExcludedOccurrences: 1}}
	w, err := artifact.NewWriter(context.Background(), root, filepath.Join(root, "tmp"), "fixtures", "committees")
	if err != nil {
		t.Fatal(err)
	}
	var rows []Committee
	for i, name := range []string{"", "ACME CORPORATION", "  ACME CORPORATION  "} {
		row := Committee{FactID: fmt.Sprint(i), OccurrenceID: fmt.Sprintf("occ-%d", i), CommitteeID: "C00000001", Name: "Committee", OrganizationType: "?", ConnectedOrganization: name}
		fields := map[string]string{}
		for j, p := range row.fields() {
			fields[CommitteeColumns()[j]] = *p
		}
		f := occ.ClassicFact{FactID: row.FactID, OccurrenceID: row.OccurrenceID, SchemaVersion: m.FactSchemaVersion, FactType: m.FactType, Dataset: m.Dataset, Cycle: m.Cycle, OccurrenceSetID: m.OccurrenceSetID, SourceReleaseID: m.SourceReleaseID, SourceContract: m.SourceContract, State: "valid", SourceFields: fields,
			TypedFields: occ.CommitteeTypedFields{CommitteeID: row.CommitteeID, Name: row.Name, OrganizationTypeCode: row.OrganizationType, ConnectedOrganization: name, SourceCycle: 2024}}
		if err = w.WriteJSON(f); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	a, err := w.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	m.Facts = occ.Artifact{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	return root, m, rows
}

func TestCommitteeAssertionsRetainFactGrainAndBlankText(t *testing.T) {
	root, m, expected := committeeFixture(t)
	var got []Committee
	p, err := scanCommittee(context.Background(), root, m, func(r Committee) error { got = append(got, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, expected) || p.Rows != 3 || p.Fields[3] != (FieldCounts{Empty: 1, Nonempty: 2}) {
		t.Fatal("committee fact grain lost", p, got)
	}
	if _, err = scanCommittee(context.Background(), root, m, func(Committee) error { return errors.New("consumer failure") }); err == nil {
		t.Fatal("consumer failure ignored")
	}
	m.Cycle = "2022"
	if _, err = scanCommittee(context.Background(), root, m, nil); err == nil {
		t.Fatal("committee cycle ignored")
	}
	m.Cycle = "2024"
	m.Counts.Facts++
	if _, err = scanCommittee(context.Background(), root, m, nil); err == nil {
		t.Fatal("committee count ignored")
	}
	f := occ.ClassicFact{Cycle: "2024", SourceFields: map[string]string{"CMTE_ID": "", "CMTE_NM": "", "ORG_TP": "", "CONNECTED_ORG_NM": ""}, TypedFields: occ.CommitteeTypedFields{SourceCycle: 2024}}
	if _, err = committeeFields(f); err != nil {
		t.Fatal(err)
	}
	delete(f.SourceFields, "CONNECTED_ORG_NM")
	if _, err = committeeFields(f); err == nil {
		t.Fatal("missing collapsed to empty")
	}
	f.SourceFields["CONNECTED_ORG_NM"] = "not the typed value"
	if _, err = committeeFields(f); err == nil {
		t.Fatal("typed mismatch accepted")
	}
}
