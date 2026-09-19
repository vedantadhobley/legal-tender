package flowevidence

import "testing"

func TestCommitteeIdentitiesAreExactIndependentValueCopies(t *testing.T) {
	id := "original-fact"
	r := Reader{m: model{entities: []entity{
		{CommitteeID: "C00000001", IdentityState: "same_cycle_master", MasterFactSetID: "master-set", MasterFactID: &id},
		{CommitteeID: "C00000002", IdentityState: "unresolved_same_cycle_master", MasterFactSetID: "master-set"},
	}}}
	got := r.CommitteeIdentities()
	if len(got) != 2 || got[0].FactID != id || got[1].FactID != "" || got[1].State != "unresolved_same_cycle_master" || got[1].FactSetID != "master-set" {
		t.Fatal(got)
	}
	got[0].FactID = "mutated"
	got[1].State = "same_cycle_master"
	if *r.m.entities[0].MasterFactID != id || r.m.entities[1].IdentityState != "unresolved_same_cycle_master" {
		t.Fatal("consumer mutated verified source model")
	}
}
