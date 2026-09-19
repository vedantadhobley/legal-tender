package fundinggeneration

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	fc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	r "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	o "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func TestGenerationFamiliesPreserveGrainAndReplayAcrossCycles(t *testing.T) {
	for _, cycle := range []string{"2020", "2022", "2024", "2026", "2028"} {
		rv := r.CycleView{Projection: r.Reference{ID: "receipt"}, Database: "receipt_db", Inputs: r.Inputs{Cycle: cycle, Facts: r.Reference{ID: "facts-a"}, Linkages: r.Reference{ID: "linkages"}}, SourceRows: 100, Counts: map[string]uint64{"reported_receipts": 99, "reported_conduit_associations": 3, "candidate_authorization_context": 5}, Unrouted: 1}
		fv := flow.View{ProjectionID: "flow", Cycle: cycle, Inputs: fc.Inputs{A: fc.FactReference{FactSetID: "facts-a", Facts: 100}, B: fc.FactReference{FactSetID: "facts-b", Facts: 200}}, A: fc.Measures{Rows: 10}, B: fc.Measures{Rows: 12}, Unresolved: 4}
		ev := ie.ResolvedView{ProjectionID: "outside", Database: "outside_db", Cycle: cycle, Inputs: ie.InputReferences{ScheduleEFactSetID: "facts-e"}, Counts: ie.ProjectionCounts{SupportEdges: 7, OppositionEdges: 2}, Coverage: ie.CandidateResolutionCoverage{UnprojectableDecisions: 3, UnprojectableMinorUnits: "-500"}}
		v := assemble("build", rv, fv, "flow_db", 9, ev, nil, o.ScheduleEReleaseMembership{})
		if v.Cycle != cycle || len(v.Families) != 8 || v.FinancialEligibility || v.TerminalEligible || len(v.EndpointNamespaces) != 5 {
			t.Fatal(v)
		}
		if v.Families[0].OverlapGroup != v.Families[3].OverlapGroup || v.Families[0].FactSetID != v.Families[3].FactSetID {
			t.Fatal("Schedule A overlap hidden")
		}
		if v.Families[1].AmountMeaning != "no_additional_money" || v.Families[2].AmountMeaning != "not_a_payment" || v.Families[5].AmountMeaning != "not_an_additional_payment" {
			t.Fatal("supporting evidence became money")
		}
		if v.Families[6].Membership != 7 || v.Families[7].Membership != 2 || v.OutsideSpending.Coverage != ev.Coverage || v.CommitteeFlow.Unresolved != 4 {
			t.Fatal("stance or gaps collapsed")
		}
		v.GenerationID = identity(v)
		raw, _ := json.Marshal(v)
		var replay Result
		_ = json.Unmarshal(raw, &replay)
		if identity(replay) != v.GenerationID || !reflect.DeepEqual(v, replay) {
			t.Fatal("unstable generation")
		}
		for _, change := range []func(*Result){func(v *Result) { v.BuildSHA256 = "changed" }, func(v *Result) { v.Cycle = "2030" }, func(v *Result) { v.Receipts.Inputs.Facts.SHA256 = "changed" }, func(v *Result) { v.OutsideSpending.Inputs.ScheduleEManifestSHA256 = "changed" }, func(v *Result) { v.CommitteeFlow.Inputs.B.FactSetID = "changed" }, func(v *Result) { v.OutsideSpending.Coverage.UnprojectableDecisions++ }} {
			var changed Result
			_ = json.Unmarshal(raw, &changed)
			change(&changed)
			if identity(changed) == v.GenerationID {
				t.Fatal("changed input did not invalidate generation")
			}
		}
	}
}

func TestGenerationSharedReceiptIdentityRejectsDrift(t *testing.T) {
	rv := r.CycleView{Inputs: r.Inputs{Cycle: "2028", Facts: r.Reference{ID: "a", SHA256: "sha"}, SourceRelease: "old-release"}, SourceRows: 12}
	b := fc.Bundle{Cycle: "2028", Input: fc.Inputs{ReleaseID: "coordinated-release", A: fc.FactReference{FactSetID: "a", ManifestSHA256: "sha", SourceReleaseID: "old-release", Facts: 12}}}
	if err := compatibleReceipt(rv, b); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*fc.Bundle){func(b *fc.Bundle) { b.Cycle = "2026" }, func(b *fc.Bundle) { b.Input.A.FactSetID = "b" }, func(b *fc.Bundle) { b.Input.A.ManifestSHA256 = "other" }, func(b *fc.Bundle) { b.Input.A.SourceReleaseID = "new" }, func(b *fc.Bundle) { b.Input.A.Facts-- }} {
		changed := b
		change(&changed)
		if compatibleReceipt(rv, changed) == nil {
			t.Fatal("accepted mixed receipt ancestry")
		}
	}
}

func TestGenerationRequiresFreshInputsNotSerializedApproval(t *testing.T) {
	for _, opt := range []Options{{}, {BuildSHA256: strings.Repeat("a", 64), ExpectedGenerationID: "passed"}} {
		got, err := Verify(context.Background(), opt)
		if err == nil || got.GenerationID != "" {
			t.Fatal("accepted missing inputs", got, err)
		}
	}
}
