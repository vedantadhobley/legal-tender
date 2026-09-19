package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func allFieldsRow() p.Row {
	return p.Row{Ordinal: 12, Recipient: ptr("C00000001"), Component: "reported_receipt", IndividualDecision: "included", CommitteeDecision: "excluded", ReceiptRole: "earmarked", SourceRoute: "publisher_individual_identity_unresolved", ReportedSourceID: nil, IndividualOverlap: true, EntityConflict: true, EarmarkState: "reported_earmark", ConduitState: "no_structured_conduit", ReportedConduitID: nil, ReferenceState: "reported_back_reference", MemoTextPresent: true, ConduitNamePresent: false, Amount: ptr(int64(-9007199254740993)), AmountState: "known", Memo: false, Entity: ptr("IND"), Contributor: ptr(""), CleanContributor: nil, ConduitID: ptr(""), ReceiptType: ptr("15E")}
}

func TestCompactReconstructsEveryParticipantField(t *testing.T) {
	for _, amount := range []*int64{nil, ptr(int64(0)), ptr(int64(-123)), ptr(int64(9007199254740993))} {
		for _, memo := range []bool{false, true} {
			r := allFieldsRow()
			r.Amount = amount
			r.Memo = memo
			for _, d := range []*c.Decision{nil, {Ordinal: 12, State: c.Unassessed, AmountComparison: "not_assessed"}, {Ordinal: 12, Related: 13, State: "shared_related_record_unresolved", AmountComparison: "different_reported_amount"}, {Ordinal: 12, Related: 13, State: "reported_earmark_memo_association", AmountComparison: "same_reported_amount", ConduitID: ptr("C00000002")}} {
				a, edge, association, e := project(strings.Repeat("a", 64), r, d)
				if e != nil {
					t.Fatal(e)
				}
				v := compact(a)
				back, e := expand(v, r)
				if e != nil || !reflect.DeepEqual(a, back) {
					t.Fatalf("lost source grain: %v", e)
				}
				raw, _ := json.Marshal(v)
				source, _ := json.Marshal(a)
				if e = verifyReconstruction(raw, source); e != nil {
					t.Fatal(e)
				}
				if strings.Contains(string(raw), `"participant"`) || strings.Contains(string(raw), `"raw_contributor_id"`) {
					t.Fatal("duplicated full row still in graph")
				}
				if edge == nil || (d != nil && d.ConduitID != nil) != (association != nil) {
					t.Fatal("edges changed")
				}
			}
		}
	}
}

func TestCompactRejectsChangedFieldsAndWrongSource(t *testing.T) {
	r := allFieldsRow()
	a, _, _, _ := project(strings.Repeat("a", 64), r, nil)
	v := compact(a)
	source, _ := json.Marshal(a)
	for _, mutate := range []func(*compactAppearance){func(v *compactAppearance) { v.Ordinal++ }, func(v *compactAppearance) { v.Key = strings.Repeat("b", 64) }, func(v *compactAppearance) { v.FactSet = strings.Repeat("b", 64) }, func(v *compactAppearance) { v.SourceRoute = "changed" }, func(v *compactAppearance) { v.Component = "changed" }, func(v *compactAppearance) { v.IdentityResolved = true }, func(v *compactAppearance) { v.RecipientState = "changed" }, func(v *compactAppearance) { v.ConduitState = "changed" }, func(v *compactAppearance) { v.Conduit = &c.Decision{Ordinal: 12, State: c.Unassessed} }} {
		bad := v
		mutate(&bad)
		raw, _ := json.Marshal(bad)
		if e := verifyReconstruction(raw, source); e == nil {
			t.Fatalf("mutated graph fields accepted: %+v", bad)
		}
	}
	raw, _ := json.Marshal(v)
	var generic map[string]any
	_ = json.Unmarshal(raw, &generic)
	delete(generic, "identity_resolved")
	omitted, _ := json.Marshal(generic)
	if verifyReconstruction(omitted, source) == nil {
		t.Fatal("omitted false accepted")
	}
	generic["identity_resolved"] = false
	generic["extra"] = nil
	extra, _ := json.Marshal(generic)
	if verifyReconstruction(extra, source) == nil {
		t.Fatal("extra field accepted")
	}
	r.Ordinal++
	if _, e := expand(v, r); e == nil {
		t.Fatal("different occurrence substituted")
	}
}

func TestCompactBatchSourceAndPhysicalDigests(t *testing.T) {
	var physical, source map[string]string
	for _, size := range []int{1, 7, 1000} {
		b := newBatches(size, func(v batch) error {
			if len(v.data)+len(v.evidence) > 8<<20 {
				t.Fatal("combined batch cap exceeded")
			}
			return nil
		})
		old := newBatches(size, func(batch) error { return nil })
		for n := 1; n <= 99; n++ {
			r := allFieldsRow()
			r.Ordinal = int64(n)
			a, _, _, _ := project(strings.Repeat("a", 64), r, nil)
			if e := b.addEvidence(appearances, a.Key, compact(a), a); e != nil {
				t.Fatal(e)
			}
			if e := old.add(appearances, a.Key, a); e != nil {
				t.Fatal(e)
			}
		}
		if e := b.finish(); e != nil {
			t.Fatal(e)
		}
		if e := old.finish(); e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(b.sourceDigests(), old.digests()) {
			t.Fatal("source equivalent document hashes changed")
		}
		if b.bytes[appearances] >= old.bytes[appearances] {
			t.Fatal("compact representation grew")
		}
		if physical == nil {
			physical, source = b.digests(), b.sourceDigests()
		} else if !reflect.DeepEqual(physical, b.digests()) || !reflect.DeepEqual(source, b.sourceDigests()) {
			t.Fatal("batch layout changed either digest")
		}
	}
}

func TestCompactLiveReadbackReconstructsSource(t *testing.T) {
	r := allFieldsRow()
	a, _, _, _ := project(strings.Repeat("a", 64), r, nil)
	physical, _ := json.Marshal(compact(a))
	source, _ := json.Marshal(a)
	for _, bad := range []bool{false, true} {
		proof := source
		if bad {
			a.Row.Ordinal++
			proof, _ = json.Marshal(a)
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, `{"result":[%s],"hasMore":false}`, physical)
		}))
		cl, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p"}, "lt_receipt_sample_test")
		e := cl.verifyBatch(context.Background(), appearances, batch{keys: []string{a.Key}, data: append(physical, '\n'), evidence: append(proof, '\n')})
		s.Close()
		if (e != nil) != bad {
			t.Fatalf("wrong reconstruction outcome: %v", e)
		}
	}
}

func TestComparisonRejectsWrongInputScope(t *testing.T) {
	d := definition{Version: Version, State: "verified_bounded_sample_not_complete_cycle", Build: strings.Repeat("a", 64), First: 1, Rows: 1, SourceRows: 2, Inputs: Inputs{Cycle: "2024"}}
	b, _ := json.Marshal(d)
	d.Key = digest(b)
	r := Result{Definition: d, Database: "lt_receipt_sample_2024_" + d.Key[:32], Counts: map[string]uint64{appearances: 1, receipts: 1}, PayloadSHA256: map[string]string{}}
	for _, name := range collections {
		r.PayloadSHA256[name] = digest(nil)
	}
	if e := validateBaseline(r, d); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*definition){func(v *definition) { v.Rows++ }, func(v *definition) { v.First++ }, func(v *definition) { v.SourceRows++ }, func(v *definition) { v.Inputs.Cycle = "2026" }, func(v *definition) { v.Inputs.Facts.ID = "other" }} {
		other := d
		mutate(&other)
		if e := validateBaseline(r, other); e == nil {
			t.Fatal("different baseline scope accepted")
		}
	}
	r.Definition.FinancialEligibility = true
	if e := validateBaseline(r, d); e == nil {
		t.Fatal("changed baseline identity accepted")
	}
}

func TestCompactFlagsRejectAmbiguity(t *testing.T) {
	for _, o := range []Options{{Layout: "mystery"}, {Layout: ExpandedLayout, CompareResult: "old", CompareSHA256: strings.Repeat("a", 64)}, {Layout: CompactLayout, CompareResult: "old"}, {Layout: CompactLayout, CompareSHA256: strings.Repeat("a", 64)}} {
		if _, e := Run(context.Background(), o); e == nil {
			t.Fatal("invalid layout/comparison flags accepted")
		}
	}
}
