package identityassertions

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// Independent full-width reader: select physical values by original column
// names, not by the Receipt Go struct or the optimized narrow reader.
func genericValues(t *testing.T, root string, shard occ.ScheduleAColumnarShard) string {
	t.Helper()
	f, err := os.Open(filepath.Join(root, shard.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := parquet.NewReader(f)
	defer r.Close()
	columns := map[string]int{}
	for i, c := range r.Schema().Columns() {
		columns[c[0]] = i
	}
	value := func(row parquet.Row, name string) parquet.Value {
		i, ok := columns[name]
		if !ok {
			t.Fatal("missing generic column", name)
		}
		if len(row) != len(columns) || row[i].Column() != i {
			t.Fatal("full-width row layout")
		}
		return row[i]
	}
	h := sha256.New()
	var count uint64
	buf := make([]parquet.Row, 128)
	b := make([]byte, 0, 4096)
	for {
		n, err := r.ReadRows(buf)
		for _, row := range buf[:n] {
			b = binary.BigEndian.AppendUint64(b[:0], uint64(value(row, "lt_source_row_ordinal").Int64()))
			b = binary.BigEndian.AppendUint64(b, uint64(value(row, "lt_two_year_transaction_period").Int64()))
			text := func(v parquet.Value) {
				if v.IsNull() {
					b = append(b, 0)
					return
				}
				b = append(b, 1)
				p := v.ByteArray()
				b = binary.BigEndian.AppendUint64(b, uint64(len(p)))
				b = append(b, p...)
			}
			text(value(row, "lt_normalization_state"))
			for _, name := range ReceiptColumns() {
				text(value(row, name))
			}
			h.Write(b)
			count++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal(io.ErrNoProgress)
		}
	}
	if count != shard.Facts {
		t.Fatal("full-width comparison row count")
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestFullWidthEquivalenceAndFullSchemaGate(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 5)
	for _, s := range m.Shards {
		p, err := scanReceipt(context.Background(), root, s, 2024, nil)
		if err != nil {
			t.Fatal(err)
		}
		if genericValues(t, root, s) != p.ValuesSHA256 {
			t.Fatal("full-width source value disagreement")
		}
	}
	// Valid narrow data, but not the accepted full 99-column source schema.
	var b bytes.Buffer
	w := parquet.NewGenericWriter[Receipt](&b)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s := m.Shards[0]
	s.Bytes, s.SHA256, s.Facts = uint64(b.Len()), hashBytes(b.Bytes()), uint64(len(rows))
	if err := os.WriteFile(filepath.Join(root, s.StorageKey), b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := scanReceipt(context.Background(), root, s, 2024, nil); err == nil {
		t.Fatal("narrow-schema source substitution")
	}
}

func TestManifestGuardsAndSourcePopulations(t *testing.T) {
	root, m := sourceFixture(t, fixtureRows(), 5)
	proofs, err := scanReceipts(context.Background(), root, m, 2024, 3, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	croot, cm, _ := committeeFixture(t)
	cp, err := scanCommittee(context.Background(), croot, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	source := func(n uint64) Source {
		return Source{strings.Repeat("a", 64), strings.Repeat("b", 64), "fec-" + strings.Repeat("c", 64), strings.Repeat("d", 64), n, n, 0}
	}
	r := Result{SchemaVersion: Version, Policy: Policy, State: "complete_published_fact_assertion_view", Storage: "references_immutable_source_facts", Cycle: "2024", BuildSHA256: strings.Repeat("e", 64), ReceiptSource: source(12), CommitteeSource: source(3), ReceiptColumns: ReceiptColumns(), CommitteeColumns: CommitteeColumns(), Receipts: proofs, Committee: cp}
	r.CommitteeSource.SourceOccurrences = 4
	r.CommitteeSource.ExcludedOccurrences = 1
	for _, p := range proofs {
		for j, f := range p.Fields {
			r.ReceiptFields[j].Null += f.Null
			r.ReceiptFields[j].Empty += f.Empty
			r.ReceiptFields[j].Nonempty += f.Nonempty
		}
	}
	r.ViewID = logicalID(r)
	b, _ := json.Marshal(r)
	if _, err = Decode(b, r.ViewID); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*Result){
		func(r *Result) { r.IdentityResolved = true }, func(r *Result) { r.EmploymentVerified = true }, func(r *Result) { r.OwnershipVerified = true }, func(r *Result) { r.TerminalPolicyAdopted = true }, func(r *Result) { r.FinancialAttribution = true },
		func(r *Result) { r.ReceiptColumns[15] = "contributor_employer_text" },
		func(r *Result) { r.Receipts[0].Rows-- }, func(r *Result) { r.Receipts[0].Fields[0].Nonempty++ },
		func(r *Result) { r.ReceiptSource.ExcludedOccurrences = 1 },
		func(r *Result) { r.ReceiptSource.SourceReleaseID = strings.Repeat("c", 64) },
		func(r *Result) { r.CommitteeSource.SourceOccurrences = 3 },
		func(r *Result) { r.Committee.Fields[0].Null = 1 },
	} {
		var got Result
		_ = json.Unmarshal(b, &got)
		mutation(&got)
		got.ViewID = logicalID(got) // Self-consistent hash is not enough.
		v, _ := json.Marshal(got)
		if _, err = Decode(v, got.ViewID); err == nil {
			t.Fatal("invalid evidence accepted", string(v))
		}
	}
	if _, err = Decode(append(b, []byte(" {}")...), r.ViewID); err == nil {
		t.Fatal("trailing data")
	}
	if _, err = Decode(b, strings.Repeat("f", 64)); err == nil {
		t.Fatal("expected ID ignored")
	}
	if _, err = Scan(context.Background(), Options{}, Consumer{}); err == nil {
		t.Fatal("missing inputs")
	}
}

// Opt-in gate against a completed retained view. Full CLI replay verifies all
// shards; this adds a separate physical reader for first/middle/last whole
// shards, selected by layout rather than any donor or committee identity.
func TestRetainedIdentityAssertionView(t *testing.T) {
	path := os.Getenv("LT_ASSERTION_VIEW")
	if path == "" {
		t.Skip("retained identity assertion gate not requested")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Decode(b, os.Getenv("LT_ASSERTION_VIEW_ID"))
	if err != nil {
		t.Fatal(err)
	}
	root := os.Getenv("LT_ASSERTION_ROOT")
	seen := map[int]bool{}
	for _, i := range []int{0, len(r.Receipts) / 2, len(r.Receipts) - 1} {
		if seen[i] {
			continue
		}
		seen[i] = true
		p := r.Receipts[i]
		if genericValues(t, root, p.Source) != p.ValuesSHA256 {
			t.Fatal("retained full-width equivalence", i)
		}
		t.Logf("full-width source agreement: shard %d, %d rows", i, p.Rows)
	}
}
