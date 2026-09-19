package flowevidence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	fc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func TestCrossPartitionModelPreservesIdentityDatesAndHistoricalFacets(t *testing.T) {
	// Instantiate the actual stored-model types, not a live/verified Reader.
	// These synthetic readers call only pure accessors; no backing verification
	// or Arango acceptance is implied by constructing them in this test.
	day := int32(19356)
	olderMasterID, newerMasterID := hash("older-master"), hash("newer-master")
	older := &Reader{m: model{
		a:        []observation{{Key: edgeKey(ScheduleA, hash("older-facts"), 1), From: entityID("C00000001"), To: entityID("C00000002"), Cycle: "2022", Ledger: ScheduleA, FactSetID: hash("older-facts"), EconomicFlowStatus: "not_established", Observation: fc.Observation{Ordinal: 1, Sender: "C00000001", Recipient: "C00000002", Date: &day, Amount: 101}}},
		entities: []entity{{Key: "C00000002", CommitteeID: "C00000002", Cycle: "2022", IdentityState: "same_cycle_master", MasterFactSetID: hash("older-masters"), MasterFactID: &olderMasterID, Master: &occ.CommitteeTypedFields{CommitteeID: "C00000002", Name: "Reported old name", SourceCycle: 2022}}},
	}}
	newer := &Reader{m: model{
		a:        []observation{{Key: edgeKey(ScheduleA, hash("newer-facts"), 1), From: entityID("C00000002"), To: entityID("C00000003"), Cycle: "2024", Ledger: ScheduleA, FactSetID: hash("newer-facts"), EconomicFlowStatus: "not_established", Observation: fc.Observation{Ordinal: 1, Sender: "C00000002", Recipient: "C00000003", Date: nil, Amount: -9}}},
		entities: []entity{{Key: "C00000002", CommitteeID: "C00000002", Cycle: "2024", IdentityState: "same_cycle_master", MasterFactSetID: hash("newer-masters"), MasterFactID: &newerMasterID, Master: &occ.CommitteeTypedFields{CommitteeID: "C00000002", Name: "Reported new name", SourceCycle: 2024}}},
	}}
	links := []graphread.Link{}
	facets := map[string][]entity{}
	for _, reader := range []*Reader{older, newer} {
		if err := reader.VisitLinks(context.Background(), ScheduleA, func(link graphread.Link) error {
			links = append(links, link)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := reader.VisitCommitteeDocuments(context.Background(), func(id string, raw json.RawMessage) error {
			var facet entity
			if err := json.Unmarshal(raw, &facet); err != nil {
				return err
			}
			facets[id] = append(facets[id], facet)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		row := reader.m.a[0]
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		var replay observation
		if err := json.Unmarshal(raw, &replay); err != nil || !reflect.DeepEqual(row, replay) || replay.TerminalEligible || replay.EconomicFlowStatus != "not_established" {
			t.Fatal("stored model lost exact evidence on replay", err)
		}
	}
	if len(links) != 2 || links[0].To != links[1].From || links[0].ID() == links[1].ID() {
		t.Fatal("shared endpoint did not compose or equal ordinals collided", links)
	}
	if edgeKey(ScheduleB, hash("older-facts"), 1) == links[0].Key {
		t.Fatal("sender and receiver ledgers share an occurrence identity")
	}
	profiles := facets["C00000002"]
	if len(facets) != 1 || len(profiles) != 2 || profiles[0].Cycle != "2022" || profiles[1].Cycle != "2024" || profiles[0].Master.Name != "Reported old name" || profiles[1].Master.Name != "Reported new name" || *profiles[0].MasterFactID == *profiles[1].MasterFactID {
		t.Fatal("shared entity erased historical source assertions")
	}
	// A same-cycle-master profile supplies reported context, not an invented
	// day-level employment/ownership/authorization interval. A future temporal
	// consumer must retain both source facets rather than pick the latest globally.
	if older.m.a[0].Date == nil || newer.m.a[0].Date != nil {
		t.Fatal("unknown date was filled from a source cycle")
	}
}
