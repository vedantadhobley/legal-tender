package fundinggeneration

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	roles "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptroles"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	r "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func TestRoleWitnessSelectionUsesEarliestOccurrencesAndDeduplicates(t *testing.T) {
	first := roles.Key{Route: "individual", SourceIdentity: "unresolved"}
	other := roles.Key{Route: "committee", SourceIdentity: "master", Conflict: true, Overlap: true}
	v := roles.Result{Profiles: []roles.Profile{{CommitteeID: "C00000002", Groups: []roles.Group{{Key: first, Counts: roles.Counts{Rows: 2, First: 7}}, {Key: other, Counts: roles.Counts{Rows: 3, First: 9}}}}, {CommitteeID: "C00000001", Groups: []roles.Group{{Key: first, Counts: roles.Counts{Rows: 1, First: 2}}}}}}
	got := roleSelections(v)
	if len(got) != 2 || got[0].ordinal != 2 || got[1].ordinal != 9 || got[0].recipient != "C00000001" || len(got[1].kinds) != 4 {
		t.Fatal(got)
	}
	v.Profiles[0], v.Profiles[1] = v.Profiles[1], v.Profiles[0]
	if !reflect.DeepEqual(got, roleSelections(v)) {
		t.Fatal("selection depends on iteration order")
	}
}
func TestRoleWitnessMustMatchSourceRoleAndReceiptGraph(t *testing.T) {
	id := "C00000001"
	source := p.Inspection{AppearanceID: "appearance", Participant: p.Row{Ordinal: 5, Recipient: &id, SourceRoute: "reported_source"}}
	k, e := roles.Classify(source.Participant, nil)
	if e != nil {
		t.Fatal(e)
	}
	pick := rolePick{ordinal: 5, recipient: id, key: k}
	makeEntry := func() r.PathEntry {
		b, _ := json.Marshal(source)
		return r.PathEntry{State: "available", Source: b, Link: &graphread.Link{From: "appearance", To: id}, Item: &graphread.Item{Key: "appearance"}}
	}
	if e := checkRoleWitness(pick, makeEntry(), nil); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*r.PathEntry){func(e *r.PathEntry) { e.State = "missing" }, func(e *r.PathEntry) { e.Link.To = "C00000002" }, func(e *r.PathEntry) { e.Item.Key = "other" }, func(e *r.PathEntry) { e.Source = json.RawMessage(`{}`) }, func(e *r.PathEntry) { e.Link = nil }} {
		entry := makeEntry()
		change(&entry)
		if e := checkRoleWitness(pick, entry, nil); e == nil {
			t.Fatal("accepted changed witness")
		}
	}
	source.IdentityResolved = true
	if e := checkRoleWitness(pick, makeEntry(), nil); e == nil {
		t.Fatal("promoted source identity")
	}
}
func TestRoleProfileValidatesControlsBeforeOpeningEvidence(t *testing.T) {
	reader := Reader{}
	for _, workers := range []int{0, 9} {
		if _, e := reader.ProfileReceiptRoles(context.Background(), workers, "", nil); e == nil {
			t.Fatal("invalid worker count")
		}
	}
	if _, e := reader.ProfileReceiptRoles(context.Background(), 4, "bad", nil); e == nil {
		t.Fatal("invalid replay identity")
	}
}
