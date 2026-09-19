package receiptroles

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"testing"

	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func str(s string) *string { return &s }
func money(n int64) *int64 { return &n }
func fixture() []p.Row {
	return []p.Row{
		{Ordinal: 1, Recipient: str("C00000001"), SourceRoute: "publisher_individual_identity_unresolved", Entity: str("IND"), Amount: money(5)},
		{Ordinal: 2, Recipient: str("C00000001"), SourceRoute: "reported_committee_observation", ReportedSourceID: str("C00000002"), Entity: str("IND"), IndividualOverlap: true, EntityConflict: true, Amount: money(-10)},
		{Ordinal: 3, Recipient: str("C00000002"), SourceRoute: "reported_committee_observation", ReportedSourceID: str("C00000099"), Amount: money(0)},
		{Ordinal: 4, Recipient: str("C00000002"), SourceRoute: "memo_only_receipt", Memo: true, Amount: money(40), ConduitState: "name_only_unresolved", ReferenceState: "reported_reference_present"},
		{Ordinal: 5, Recipient: str("C00000001"), SourceRoute: "other_or_unresolved_receipt"},
		{Ordinal: 6, Recipient: str("C00000004"), SourceRoute: "other", Amount: money(40)},
		{Ordinal: 7, Recipient: nil, SourceRoute: "unresolved_recipient", Amount: money(9)},
		{Ordinal: 8, Recipient: str("C00000001"), SourceRoute: "publisher_individual_identity_unresolved", Entity: str("IND"), Amount: money(5)},
	}
}
func newFixture(t *testing.T, n int) *Collector {
	t.Helper()
	c, e := New([]string{"C00000003", "C00000001", "C00000002"}, map[string]string{"C00000002": strings.Repeat("a", 64)}, n)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func TestProfilesConserveRolesSignsNullsIdentityAndScope(t *testing.T) {
	c := newFixture(t, 1)
	rows := fixture()
	for _, r := range rows {
		if e := c.Observe(0, r); e != nil {
			t.Fatal(e)
		}
	}
	v, e := c.Finish(uint64(len(rows)))
	if e != nil {
		t.Fatal(e)
	}
	if v.Rows != 8 || v.Scoped.Rows != 6 || v.Outside.Rows != 1 || v.Unresolved.Rows != 1 || v.IdentityResolved || v.TerminalEligible {
		t.Fatal(v)
	}
	if v.Scoped.Positive != 3 || v.Scoped.Negative != 1 || v.Scoped.Zero != 1 || v.Scoped.Unknown != 1 {
		t.Fatal(v.Scoped)
	}
	if len(v.Profiles) != 3 || v.Profiles[2].Counts.Rows != 0 || v.Profiles[2].State != "no_reported_occurrences_in_exact_participant_publication" {
		t.Fatal(v.Profiles)
	}
	if len(v.Sources) != 2 || v.Sources[0].MasterFactID == nil || v.Sources[1].MasterFactID != nil || v.Sources[1].State != "not_in_pinned_same_cycle_master" {
		t.Fatal(v.Sources)
	}
	if v.Profiles[0].Counts.Rows != 4 || len(v.Profiles[0].Groups) != 3 {
		t.Fatal(v.Profiles[0])
	}
	if _, e := c.Finish(9); e == nil {
		t.Fatal("partial scan accepted")
	}
}
func TestWorkerAndOrderIndependentReplayMatchesSerial(t *testing.T) {
	rows := []p.Row{}
	base := fixture()
	for i := 0; i < 3000; i++ {
		r := base[i%len(base)]
		r.Ordinal = int64(i + 1)
		rows = append(rows, r)
	}
	serial := newFixture(t, 1)
	for _, r := range rows {
		if e := serial.Observe(0, r); e != nil {
			t.Fatal(e)
		}
	}
	want, e := serial.Finish(uint64(len(rows)))
	if e != nil {
		t.Fatal(e)
	}
	rng := rand.New(rand.NewPCG(17, 33))
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	for _, workers := range []int{2, 4, 8} {
		c := newFixture(t, workers)
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				for i := w; i < len(rows); i += workers {
					if e := c.Observe(w, rows[i]); e != nil {
						t.Error(e)
						return
					}
				}
			})
		}
		wg.Wait()
		got, e := c.Finish(uint64(len(rows)))
		if e != nil || !reflect.DeepEqual(want, got) {
			t.Fatal("worker/order changed semantics", workers, e)
		}
	}
}
func TestNullAndEmptyEntityRemainDistinctAndReportedIDsAreNotNames(t *testing.T) {
	c := newFixture(t, 1)
	for i, label := range []*string{nil, str(""), str("IND"), str("ORG")} {
		r := p.Row{Ordinal: int64(i + 1), Recipient: str("C00000001"), SourceRoute: "publisher_individual_identity_unresolved", Entity: label, Contributor: str("C00000002")}
		if e := c.Observe(0, r); e != nil {
			t.Fatal(e)
		}
	}
	v, e := c.Finish(4)
	if e != nil || len(v.Profiles[0].Groups) != 4 || len(v.Sources) != 0 {
		t.Fatal(v, e)
	}
	for _, g := range v.Profiles[0].Groups {
		if g.Key.SourceIdentity != "no_routed_source_committee_assertion" {
			t.Fatal("raw ID became accepted role")
		}
	}
}
func TestProfileRefusesBadScopeAndResourceOverflow(t *testing.T) {
	for _, ids := range [][]string{{"C00000001", "C00000001"}, {"person-name"}} {
		if _, e := New(ids, nil, 1); e == nil {
			t.Fatal("bad scope")
		}
	}
	for _, n := range []int{0, 9} {
		if _, e := New(nil, nil, n); e == nil {
			t.Fatal("bad workers")
		}
	}
	if _, e := New(nil, map[string]string{"C00000001": "not-a-fact"}, 1); e == nil {
		t.Fatal("bad master")
	}
	c := newFixture(t, 1)
	c.limit = 1
	if e := c.Observe(0, fixture()[0]); e == nil {
		t.Fatal("silent map cap truncation")
	}
	if _, e := c.Finish(1); e == nil {
		t.Fatal("partial capped profile accepted")
	}
	c = newFixture(t, 1)
	if e := c.Observe(1, fixture()[0]); e == nil {
		t.Fatal("bad slot")
	}
	if _, e := Classify(p.Row{Ordinal: 1, ReportedSourceID: str("a name")}, nil); e == nil {
		t.Fatal("name resolved as ID")
	}
}
func TestProfileOwnsBorrowedParquetKeys(t *testing.T) {
	c := newFixture(t, 1)
	// Reusing the nullable field pointers must not change stored keys/evidence.
	id, route, entity := "C00000001", "route", "IND"
	r := p.Row{Ordinal: 1, Recipient: &id, SourceRoute: route, Entity: &entity}
	if e := c.Observe(0, r); e != nil {
		t.Fatal(e)
	}
	id, entity = "C00000002", "ORG"
	r.Ordinal = 2
	r.SourceRoute = fmt.Sprint("other")
	if e := c.Observe(0, r); e != nil {
		t.Fatal(e)
	}
	v, e := c.Finish(2)
	if e != nil || v.Profiles[0].Groups[0].Key.Entity != "IND" || v.Profiles[1].Groups[0].Key.Entity != "ORG" {
		t.Fatal(v, e)
	}
}
