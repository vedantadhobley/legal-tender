package receiptreferences

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// Full artifact readback plus source-backed witnesses. This does not claim a
// second independent decoder or a second independent full-cycle calculation.
func TestRetainedCycle(t *testing.T) {
	dir, root := os.Getenv("LT_REFERENCE_RESULT"), os.Getenv("LT_REFERENCE_STORAGE")
	if dir == "" || root == "" {
		t.Skip("retained full-cycle reference result not configured")
	}
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	if err = json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "complete_cycle_reference_join" || result.FinancialEligibility || result.ConduitEligibilityEvaluated || result.CalculationID != logicalID(result) {
		t.Fatal("invalid completed result")
	}
	ctx := context.Background()
	data := filepath.Join(dir, "data")
	var bytes uint64
	for _, f := range []xsort.File{result.LookupEvidence, result.Decisions, result.ExactIncidences, result.Neighbors} {
		t.Logf("verifying %s: %d records", f.Name, f.Rows)
		if err = xsort.Verify(ctx, data, f); err != nil {
			t.Fatal(err)
		}
		bytes += f.Bytes
	}
	if bytes != result.RetainedBytes || result.PeakWorkspaceBytes < bytes {
		t.Fatal("retained file conservation")
	}
	counts := map[string]uint64{"no_report_reference": result.SourceRows - result.ReferenceRows}
	witnesses := []Decision{}
	byState := map[string]int{}
	var previous uint64
	r, err := xsort.Open(ctx, data, result.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	for {
		record, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var d Decision
		if err = json.Unmarshal(record.Data, &d); err != nil {
			t.Fatal(err)
		}
		if record.Ordinal <= previous || record.Ordinal > result.SourceRows || uint64(d.Source.Ordinal) != record.Ordinal || record.Key != ordinalKey(record.Ordinal) {
			t.Fatal("decision occurrence order")
		}
		previous = record.Ordinal
		counts[d.State]++
		if (d.Target != nil) != (d.State == "exact_same_report_reference") {
			t.Fatal("target eligibility")
		}
		if byState[d.State] < 3 {
			witnesses = append(witnesses, d)
			byState[d.State]++
		}
	}
	r.Close()
	if !reflect.DeepEqual(counts, result.States) {
		t.Fatal("state conservation", counts, result.States)
	}
	t.Logf("all %d reference decisions conserve states; checking lookup witnesses", result.ReferenceRows)
	// Count selected source/target keys from the complete lookup evidence, not
	// from the decision's stored multiplicities. Retain bounded member witnesses.
	keys := map[string]*lookup{}
	members := map[uint64]memberWitness{}
	for _, d := range witnesses {
		if !scope(d.Source) {
			continue
		}
		for _, tx := range []*string{d.Source.Transaction, d.Source.BackReference} {
			if present(tx) {
				keys[key(d.Source.Recipient, d.Source.File, tx)] = &lookup{}
			}
		}
	}
	r, err = xsort.Open(ctx, data, result.LookupEvidence)
	if err != nil {
		t.Fatal(err)
	}
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if v.Tag != 0 {
			continue
		}
		if k := keys[v.Key]; k != nil {
			var m member
			if err = json.Unmarshal(v.Data, &m); err != nil {
				t.Fatal(err)
			}
			k.Count++
			if k.Count == 1 {
				k.First = m
			}
			if k.Count <= 3 {
				members[m.Ordinal] = memberWitness{Member: m, Key: v.Key}
			}
		}
	}
	r.Close()
	for _, d := range witnesses {
		for i, tx := range []*string{d.Source.Transaction, d.Source.BackReference} {
			count := d.SourceMultiplicity
			if i == 1 {
				count = d.TargetMultiplicity
			}
			if count != nil {
				if !present(tx) || keys[key(d.Source.Recipient, d.Source.File, tx)].Count != *count {
					t.Fatal("lookup multiplicity witness")
				}
			}
		}
	}
	// Independently check outgoing/incoming degree conservation and enforce
	// reciprocal references are not counted twice as distinct peers.
	r, err = xsort.Open(ctx, data, result.Neighbors)
	if err != nil {
		t.Fatal(err)
	}
	var incoming, outgoing uint64
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var n Neighbors
		if err = json.Unmarshal(v.Data, &n); err != nil {
			t.Fatal(err)
		}
		if n.Ordinal != v.Ordinal || n.Peers == 0 || n.Peers > n.Incoming+n.Outgoing {
			t.Fatal("invalid degree")
		}
		incoming += n.Incoming
		outgoing += n.Outgoing
	}
	r.Close()
	if incoming != result.States["exact_same_report_reference"] || outgoing != incoming || result.ExactIncidences.Rows != incoming+outgoing {
		t.Fatal("inverse-reference conservation")
	}
	t.Log("lookup witnesses and global incidence totals pass; verifying full source rows")
	manifest, digest, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, root, filepath.Join(root, "facts/fec/schedule-a/columnar/manifests", result.FactSetID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if digest != result.ManifestSHA256 || manifest.Counts.Facts != result.SourceRows || manifest.Cycle != result.Cycle {
		t.Fatal("source boundary mismatch")
	}
	wanted := map[uint64]*Row{}
	for i := range witnesses {
		d := &witnesses[i]
		wanted[uint64(d.Source.Ordinal)] = &d.Source
	}
	for ordinal := range members {
		if _, ok := wanted[ordinal]; !ok {
			wanted[ordinal] = nil
		}
	}
	verifyPhysicalWitnesses(t, root, manifest, wanted, members)
	t.Logf("all four artifacts verified; %d reference decisions; %d source/target row witnesses across %d observed states", result.ReferenceRows, len(wanted), len(byState))
}

type memberWitness struct {
	Member member
	Key    string
}

func verifyPhysicalWitnesses(t *testing.T, root string, m occ.ScheduleAColumnarManifest, wanted map[uint64]*Row, members map[uint64]memberWitness) {
	t.Helper()
	ordinals := make([]uint64, 0, len(wanted))
	for ordinal := range wanted {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	for _, shard := range m.Shards {
		var selected []uint64
		for _, ordinal := range ordinals {
			if ordinal >= shard.FirstSourceRowOrdinal && ordinal <= shard.LastSourceRowOrdinal {
				selected = append(selected, ordinal)
			}
		}
		if len(selected) == 0 {
			continue
		}
		f, err := os.Open(filepath.Join(root, shard.StorageKey))
		if err != nil {
			t.Fatal(err)
		}
		p, err := parquet.OpenFile(f, int64(shard.Bytes))
		if err != nil {
			t.Fatal(err)
		}
		reader := parquet.NewReader(p)
		for _, ordinal := range selected {
			if err = reader.SeekToRow(int64(ordinal - shard.FirstSourceRowOrdinal)); err != nil {
				t.Fatal(err)
			}
			rows := make([]parquet.Row, 1)
			n, err := reader.ReadRows(rows)
			if n != 1 || (err != nil && err != io.EOF) {
				t.Fatal("physical witness", err)
			}
			fields := map[string]parquet.Value{}
			for _, v := range rows[0] {
				fields[p.Schema().Columns()[v.Column()][0]] = v
			}
			str := func(name string) *string {
				v, ok := fields[name]
				if !ok {
					t.Fatal("missing source field", name)
				}
				if v.IsNull() {
					return nil
				}
				s := string(v.ByteArray())
				return &s
			}
			v := fields["lt_two_year_transaction_period"]
			cycle := v.Int64()
			if v.Kind() == parquet.Int32 {
				cycle = int64(v.Int32())
			}
			normalization := str("lt_normalization_state")
			if normalization == nil {
				t.Fatal("missing source normalization")
			}
			raw := Row{Ordinal: fields["lt_source_row_ordinal"].Int64(), Cycle: cycle, Normalization: *normalization, Recipient: str("cmte_id"), File: str("file_num"), Transaction: str("tran_id"), BackReference: str("back_ref_tran_id"), BackSchedule: str("back_ref_sched_nm"), Schedule: str("schedule_type"), Line: str("line_num")}
			if expected := wanted[ordinal]; expected != nil && !reflect.DeepEqual(raw, *expected) {
				t.Fatal("physical source decision mismatch", ordinal)
			}
			if witness, ok := members[ordinal]; ok {
				member := witness.Member
				if uint64(raw.Ordinal) != member.Ordinal || !reflect.DeepEqual(raw.Schedule, member.Schedule) || !reflect.DeepEqual(raw.Line, member.Line) || key(raw.Recipient, raw.File, raw.Transaction) != witness.Key {
					t.Fatal("physical target member mismatch", ordinal)
				}
			}
		}
		reader.Close()
		f.Close()
	}
}
