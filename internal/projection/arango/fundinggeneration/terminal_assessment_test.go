package fundinggeneration

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	a "github.com/vedantadhobley/legal-tender/internal/calculation/fec/terminalassessment"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

func TestTerminalWitnessesSelectFromCompleteTopologyNotOperatorIDs(t *testing.T) {
	cm := func(i int) string { return fmt.Sprintf("C%08d", i) }
	key := func(i int) string { return fmt.Sprintf("%064x", i) }
	ids := []a.Committee{}
	for i := 1; i <= 6; i++ {
		id := key(i)
		ids = append(ids, a.Committee{ID: cm(i), IdentityState: a.SameCycleMaster, MasterFactSetID: key(99), MasterFactID: &id})
	}
	ids[1].IdentityState, ids[1].MasterFactID = a.MissingMaster, nil
	obs := []a.Observation{{Key: key(2), From: cm(1), To: cm(3)}, {Key: key(1), From: cm(1), To: cm(3)}, {Key: key(3), From: cm(2), To: cm(3)}, {Key: key(4), From: cm(3), To: cm(3)}, {Key: key(5), From: cm(4), To: cm(5)}, {Key: key(6), From: cm(5), To: cm(4)}}
	b := []a.Observation{{Key: key(1), From: cm(3), To: cm(1)}, {Key: key(2), From: cm(6), To: cm(3)}}
	v, err := a.Analyze(context.Background(), ids, obs, b)
	if err != nil {
		t.Fatal(err)
	}
	want := []terminalSelection{{"identified_frontier", cm(1)}, {"missing_master_frontier", cm(2)}, {"cyclic_root", cm(4)}, {"nonroot_cycle", cm(3)}, {"cross_ledger_frontier_disagreement", cm(1)}}
	if got := terminalSelections(v, flow.ScheduleA); !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	selected, err := selectTerminalIncident(flow.ScheduleA, cm(1), obs)
	if err != nil || selected.Key != key(1) || selected.Family != "receiver_reported_committee_observation" {
		t.Fatal(selected, err)
	}
	obs[0], obs[1] = obs[1], obs[0]
	replay, err := selectTerminalIncident(flow.ScheduleA, cm(1), obs)
	if err != nil || selected != replay {
		t.Fatal("unstable incident")
	}
	if _, err := selectTerminalIncident(flow.ScheduleA, cm(6), obs); err == nil {
		t.Fatal("manufactured witness")
	}
	if got := terminalSelections(v, flow.ScheduleB); got[1].id != "" || got[2].id != "" || got[3].id != "" {
		t.Fatal("invented missing/cyclic witness", got)
	}
	// Inbound only in the other ledger does not qualify as disagreement.
	if got := terminalSelections(v, flow.ScheduleB); got[4].id != cm(1) {
		t.Fatal(got)
	}
}

func TestTerminalAssessmentIdentityIncludesScopeDefinitionsAndConsumer(t *testing.T) {
	v := TerminalAssessment{SchemaVersion: TerminalAssessmentVersion, GenerationID: strings.Repeat("a", 64), GenerationSHA256: strings.Repeat("b", 64), BuildSHA256: strings.Repeat("c", 64), Cycle: "2028", Assessment: a.Result{Policy: a.Policy, OriginState: "not_established"}}
	id := valueID(v)
	for _, change := range []func(*TerminalAssessment){
		func(v *TerminalAssessment) { v.BuildSHA256 = "other" },
		func(v *TerminalAssessment) { v.GenerationSHA256 = "other" },
		func(v *TerminalAssessment) { v.Cycle = "2030" },
		func(v *TerminalAssessment) { v.Assessment.Policy = "other" },
		func(v *TerminalAssessment) { v.Assessment.A.Observations = 1 },
		func(v *TerminalAssessment) { v.TerminalEligible = true },
	} {
		copy := v
		change(&copy)
		if valueID(copy) == id {
			t.Fatal("assessment did not invalidate")
		}
	}
	r := Reader{}
	if _, err := r.AssessTerminalSources(context.Background(), "invalid", nil); err == nil {
		t.Fatal("invalid replay identity accessed backend")
	}
}
