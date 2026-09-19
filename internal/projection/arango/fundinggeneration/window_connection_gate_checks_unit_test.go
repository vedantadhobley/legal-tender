package fundinggeneration

import (
	"encoding/json"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func checkedConnectionFixture(t *testing.T) (WindowConnectionsResult, connectionCounts) {
	t.Helper()
	marshal := func(v any) json.RawMessage { b, err := json.Marshal(v); liveCheck(t, err); return b }
	day := int32(19000)
	g := Result{GenerationID: valueID("generation")}
	g.Receipts.Inputs.Facts.ID = valueID("A facts")
	g.Receipts.Inputs.Linkages.ID = valueID("linkage facts")
	g.CommitteeFlow.Inputs.A.FactSetID = g.Receipts.Inputs.Facts.ID
	key := valueID("appearance")
	entryLink := graphread.Link{Family: "reported_receipt", Key: key, From: key, To: "C00000001"}
	flowLink := graphread.Link{Family: "receiver_reported_committee_observation", Key: valueID("flow"), From: "C00000001", To: "C00000002"}
	authLink := graphread.Link{Family: "candidate_authorization_context", Key: valueID("authorization"), From: "C00000002", To: "H0CA00001"}
	source := marshal(map[string]any{"fact_set_id": g.Receipts.Inputs.Facts.ID, "source": map[string]any{"source_row_ordinal": 1, "fields": map[string]any{"lt_receipt_date": day}}})
	item := graphread.Item{Key: key, Evidence: source}
	out := WindowConnectionsResult{SchemaVersion: WindowConnectionsVersion, Policy: WindowConnectionsPolicy, BuildSHA256: valueID("build"),
		Inputs:   []WindowPublication{{GenerationID: g.GenerationID, GenerationSHA256: valueID("bytes"), Generation: g}},
		Query:    WindowConnectionQuery{PathQuery: PathQuery{ReceiptOrdinal: 1, EntryFamily: "reported_receipt", Ledger: flow.ScheduleA, Target: authLink.To, Ending: "candidate_authorization_context", MaxHops: 1, Limit: 1, Budget: 100}, EntryGeneration: g.GenerationID},
		Entry:    &WindowReceiptEntry{GenerationID: g.GenerationID, Selection: "included", DatedPathEntry: receipt.DatedPathEntry{Date: &day, Entry: receipt.PathEntry{State: "available", Link: &entryLink, Item: &item, Source: source}}},
		Coverage: []WindowCoverage{{GenerationID: g.GenerationID, Rows: 3, Included: 3, UndatedIncluded: 1}},
		Contexts: []WindowAuthorizationContext{{GenerationID: g.GenerationID, Links: 1, TemporalBasis: authorizationTimeBasis}},
	}
	out.Links = []WindowConnectionLink{
		{ID: connectionLinkID(g.GenerationID, entryLink), GenerationID: g.GenerationID, Topology: entryLink, Date: &day, TemporalBasis: "underlying_receipt_reported_date", Evidence: item},
		{ID: connectionLinkID(g.GenerationID, flowLink), GenerationID: g.GenerationID, Topology: flowLink, Date: &day, TemporalBasis: "selected_committee_observation_reported_date", Evidence: graphread.Item{Key: flowLink.Key, Document: marshal(map[string]any{"fact_set_id": g.CommitteeFlow.Inputs.A.FactSetID, "source_row_ordinal": 7, "date_days": day, "ledger": "schedule_a"})}},
		{ID: connectionLinkID(g.GenerationID, authLink), GenerationID: g.GenerationID, Topology: authLink, TemporalBasis: authorizationTimeBasis, Evidence: graphread.Item{Key: authLink.Key, Evidence: marshal(map[string]any{"fact_set_id": g.Receipts.Inputs.Linkages.ID, "supporting_fact_ids": []string{valueID("assertion")}})}},
	}
	out.Paths = []EvidencePath{{ID: valueID("path"), Links: []string{out.Links[0].ID, out.Links[1].ID, out.Links[2].ID}}}
	out.ResultID = valueID(out)
	return out, connectionCounts{g.GenerationID: {flow.ScheduleA: {days: map[int32]uint64{day: 2}, unknown: 1}}}
}

func TestWindowConnectionGateRejectsResealedIncorrectResults(t *testing.T) {
	out, counts := checkedConnectionFixture(t)
	liveCheck(t, checkConnectionGateResult(out, counts))
	for _, mutate := range []func(*WindowConnectionsResult){
		func(v *WindowConnectionsResult) { v.FinancialEligibility = true },
		func(v *WindowConnectionsResult) { v.TerminalEligible = true },
		func(v *WindowConnectionsResult) { v.Coverage[0].Included-- },
		func(v *WindowConnectionsResult) { v.Coverage[0].UndatedIncluded = 0 },
		func(v *WindowConnectionsResult) { v.Entry.GenerationID = valueID("foreign") },
		func(v *WindowConnectionsResult) { v.Entry.Selection = "unknown_date_excluded" },
		func(v *WindowConnectionsResult) { v.Entry.Date = nil },
		func(v *WindowConnectionsResult) { v.Entry.Entry.Link = nil },
		func(v *WindowConnectionsResult) { v.Links[1].Date = dayPtr(19001) },
		func(v *WindowConnectionsResult) { v.Links[1].Topology.Family = "sender_reported_committee_observation" },
		func(v *WindowConnectionsResult) { v.Links[2].Date = dayPtr(19000) },
		func(v *WindowConnectionsResult) {
			v.Links[2].Evidence.Evidence = json.RawMessage(`{"fact_set_id":"foreign","supporting_fact_ids":[]}`)
		},
		func(v *WindowConnectionsResult) { v.Paths[0].Links[0] = v.Links[1].ID },
		func(v *WindowConnectionsResult) { v.Links = append(v.Links, v.Links[0]) },
		func(v *WindowConnectionsResult) { v.Contexts[0].TemporalBasis = "valid_through_cycle" },
	} {
		v, counts := checkedConnectionFixture(t)
		mutate(&v)
		v.ResultID = ""
		v.ResultID = valueID(v)
		if checkConnectionGateResult(v, counts) == nil {
			t.Fatal("gate accepted incorrect values after resealing")
		}
	}
}
