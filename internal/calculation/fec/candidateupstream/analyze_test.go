package candidateupstream

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

const candidate = "H0AA00001"

func cm(n int) string { return fmt.Sprintf("C%08d", n) }
func obs(ord uint64, from, to int, amount int64) flow.Observation {
	d := int32(20000)
	return flow.Observation{Ordinal: ord, SubID: fmt.Sprint(ord), Sender: cm(from), Recipient: cm(to), Type: "15K", Role: "contribution", ReportingRole: "registered_filer_contribution", Date: &d, Amount: amount}
}
func links() []receipts.LinkageFact {
	return []receipts.LinkageFact{{FactID: "link1", State: "valid", CandidateID: candidate, CommitteeID: cm(1), DesignationCode: "P"}, {FactID: "link2", State: "valid", CandidateID: candidate, CommitteeID: cm(2), DesignationCode: "A"}}
}
func fixture() []flow.Observation {
	return []flow.Observation{obs(1, 3, 1, 10000), obs(2, 1, 2, 10000), obs(3, 4, 3, 8000), obs(4, 3, 4, 3000), obs(5, 5, 4, 6000), obs(6, 3, 1, -500), obs(7, 3, 1, 10000), obs(8, 3, 1, 0), obs(9, 6, 3, 500), obs(10, 6, 6, 100), obs(11, 7, 8, 90000)}
}
func TestScopeConservationCyclesAndUnknownLeaf(t *testing.T) {
	r, err := analyze(context.Background(), "2024", candidate, Inputs{}, links(), map[string]string{cm(3): "master3"}, fixture())
	if err != nil {
		t.Fatal(err)
	}
	if r.Accounting.External.Signed != "19500" || r.Accounting.Internal.Signed != "10000" || r.Accounting.CandidateLinked.Signed != "29500" || r.Accounting.External.Positive != "20000" || r.Accounting.External.Negative != "-500" || r.Accounting.External.ZeroRecords != 1 {
		t.Fatal(r.Accounting)
	}
	if len(r.CandidateObservations) != 5 || len(r.UpstreamOrdinals) != 5 || len(r.Nodes) != 6 || len(r.CyclicComponents) != 2 {
		t.Fatal("wrong scoped topology", len(r.Nodes), r.CyclicComponents)
	}
	if r.Accounting.TerminalAllocated != "0" || r.Accounting.UnresolvedAttribution != r.Accounting.External || r.TerminalEligible {
		t.Fatal("unsupported money allocation")
	}
	for _, node := range r.Nodes {
		if node.TerminalEligible {
			t.Fatal("terminal inferred from topology")
		}
		if node.CommitteeID == cm(5) && (node.Incoming != 0 || node.MasterFactID != nil || node.Hops != 3 || node.WitnessOrdinal == nil || *node.WitnessOrdinal != 5 || !contains(node.Reasons, "no_selected_incoming_committee_observations")) {
			t.Fatal(node)
		}
		if node.CommitteeID == cm(3) && (node.MasterFactID == nil || *node.MasterFactID != "master3" || node.CyclicComponent == nil) {
			t.Fatal(node)
		}
	}
	verifyWitnesses(t, r, fixture())
}

func TestReportedRolesAndDatesStayEvidenceNotCashAllocation(t *testing.T) {
	rows := []flow.Observation{obs(1, 3, 1, 100), obs(2, 4, 3, -50), obs(3, 5, 4, 0)}
	rows[0].Type, rows[0].Role, rows[0].ReportingRole = "15Z", "in_kind", "registered_filer_in_kind_contribution"
	rows[1].Type, rows[1].Role, rows[1].ReportingRole = "20R", "refund_or_repayment", "refund_or_repayment_received"
	rows[2].Type, rows[2].Role, rows[2].ReportingRole = "18G", "affiliated_transfer", "affiliated_transfer_in"
	rows[0].Date = nil
	r, err := analyze(context.Background(), "2024", candidate, Inputs{}, links()[:1], nil, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Nodes) != 4 || !reflect.DeepEqual(r.CandidateObservations[0].Observation, rows[0]) || !reflect.DeepEqual(r.UpstreamOrdinals, []uint64{2, 3}) || r.Accounting.TerminalAllocated != "0" {
		t.Fatal("source role, sign, or date changed")
	}
	rows[1].ReportingRole = "registered_filer_contribution"
	if _, err := analyze(context.Background(), "2024", candidate, Inputs{}, links(), nil, rows); err == nil {
		t.Fatal("inconsistent reporting/flow roles accepted")
	}
}

func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}

func verifyWitnesses(t *testing.T, r Result, source []flow.Observation) {
	t.Helper()
	nodes := map[string]Node{}
	rows := map[uint64]flow.Observation{}
	available := map[uint64]flow.Observation{}
	for _, v := range source {
		available[v.Ordinal] = v
	}
	for _, n := range r.Nodes {
		nodes[n.CommitteeID] = n
	}
	for _, v := range r.CandidateObservations {
		rows[v.Observation.Ordinal] = v.Observation
	}
	for _, ordinal := range r.UpstreamOrdinals {
		v, ok := available[ordinal]
		if !ok {
			t.Fatal("upstream ordinal outside source membership")
		}
		if _, exists := rows[ordinal]; exists {
			t.Fatal("duplicated source occurrence")
		}
		rows[ordinal] = v
	}
	for _, n := range r.Nodes {
		if n.Authorized {
			if n.Hops != 0 || n.WitnessOrdinal != nil {
				t.Fatal(n)
			}
			continue
		}
		if n.WitnessOrdinal == nil {
			t.Fatal("missing source witness")
		}
		o, ok := rows[*n.WitnessOrdinal]
		if !ok || o.Sender != n.CommitteeID {
			t.Fatal("wrong witness source")
		}
		parent, ok := nodes[o.Recipient]
		if !ok || parent.Hops != n.Hops-1 {
			t.Fatal("witness does not advance to candidate scope")
		}
	}
}

func TestUnresolvedAuthorizationNeverBecomesExternalFunding(t *testing.T) {
	l := links()
	l = append(l, receipts.LinkageFact{FactID: "other", State: "valid", CandidateID: "H0AA00002", CommitteeID: cm(2), DesignationCode: "P"})
	r, err := analyze(context.Background(), "2024", candidate, Inputs{}, l, nil, []flow.Observation{obs(1, 2, 1, 500), obs(2, 3, 2, 700), obs(3, 4, 1, 300)})
	if err != nil {
		t.Fatal(err)
	}
	if r.Accounting.External.Signed != "300" || r.Accounting.UnresolvedScope.Signed != "1200" || r.Accounting.Internal.Records != 0 {
		t.Fatal(r.Accounting)
	}
	if r.Relationships[1].State != "unresolved" {
		t.Fatal(r.Relationships)
	}
	verifyWitnesses(t, r, []flow.Observation{obs(1, 2, 1, 500), obs(2, 3, 2, 700), obs(3, 4, 1, 300)})
	l = append(l, receipts.LinkageFact{FactID: "conflict", State: "valid", CandidateID: candidate, CommitteeID: cm(1), DesignationCode: "U"})
	if _, err := analyze(context.Background(), "2024", candidate, Inputs{}, l, nil, nil); err == nil {
		t.Fatal("fabricated zero for no authorized scope")
	}
}

func TestDeterminismInputIdentityAndExactLargeSignedMoney(t *testing.T) {
	rows := []flow.Observation{obs(1, 3, 1, math.MaxInt64), obs(2, 3, 1, math.MaxInt64), obs(3, 3, 1, math.MinInt64)}
	r, err := analyze(context.Background(), "2024", candidate, Inputs{}, links(), nil, rows)
	if err != nil {
		t.Fatal(err)
	}
	if r.Accounting.External.Positive != "18446744073709551614" || r.Accounting.External.Signed != "9223372036854775806" {
		t.Fatal(r.Accounting)
	}
	rows[0], rows[2] = rows[2], rows[0]
	l := links()
	l[0], l[1] = l[1], l[0]
	replay, err := analyze(context.Background(), "2024", candidate, Inputs{}, l, nil, rows)
	if err != nil || !reflect.DeepEqual(r, replay) {
		t.Fatal("replay depends on input ordering", err)
	}
	for _, input := range []Inputs{{BundleSHA256: strings.Repeat("a", 64)}, {Linkages: flow.FactReference{ManifestSHA256: strings.Repeat("a", 64)}}} {
		changed, err := analyze(context.Background(), "2024", candidate, input, l, nil, rows)
		if err != nil || changed.CalculationID == r.CalculationID {
			t.Fatal("unpinned input", err)
		}
	}
	b, err := json.Marshal(r)
	if err != nil || !strings.Contains(string(b), `"terminal_attribution_eligible":false`) || !strings.Contains(string(b), `"18446744073709551614"`) {
		t.Fatal("money/guard wire contract", err)
	}
}

func TestInvalidRowsSelectionAndCancellation(t *testing.T) {
	for _, change := range []func(*flow.Observation){func(v *flow.Observation) { v.Ordinal = 0 }, func(v *flow.Observation) { v.Sender = "not-a-committee" }, func(v *flow.Observation) { v.Type = "24K" }, func(v *flow.Observation) { v.Role = "invented" }, func(v *flow.Observation) { v.SubID = "" }} {
		v := obs(1, 3, 1, 1)
		change(&v)
		if _, err := analyze(context.Background(), "2024", candidate, Inputs{}, links(), nil, []flow.Observation{v}); err == nil {
			t.Fatal("invalid observation accepted")
		}
	}
	if _, err := analyze(context.Background(), "2024", candidate, Inputs{}, links(), nil, []flow.Observation{obs(1, 3, 1, 1), obs(1, 4, 1, 2)}); err == nil {
		t.Fatal("duplicate ordinal accepted")
	}
	for _, selection := range [][2]string{{"2023", candidate}, {"2024", "../../candidate"}, {"2024", ""}} {
		if _, err := analyze(context.Background(), selection[0], selection[1], Inputs{}, links(), nil, nil); err == nil {
			t.Fatal(selection)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := analyze(ctx, "2024", candidate, Inputs{}, links(), nil, fixture()); err != context.Canceled {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Options{Cycle: "2024", Candidate: candidate}); err == nil {
		t.Fatal("missing publications accepted")
	}
}

func TestCompleteReachabilityAgainstIndependentClosure(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 7))
	for trial := 0; trial < 40; trial++ {
		const n = 15
		var reach [n][n]bool
		rows := []flow.Observation{}
		for i := 0; i < n; i++ {
			reach[i][i] = true
			for j := 0; j < n; j++ {
				if rng.IntN(8) == 0 {
					reach[i][j] = true
					rows = append(rows, obs(uint64(len(rows)+1), i+1, j+1, int64(rng.IntN(201)-100)))
				}
			}
		}
		for k := 0; k < n; k++ {
			for i := 0; i < n; i++ {
				for j := 0; j < n; j++ {
					reach[i][j] = reach[i][j] || reach[i][k] && reach[k][j]
				}
			}
		}
		r, err := analyze(context.Background(), "2024", candidate, Inputs{}, links()[:1], nil, rows)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		components := map[string]string{}
		for _, v := range r.Nodes {
			seen[v.CommitteeID] = true
			if v.CyclicComponent != nil {
				components[v.CommitteeID] = *v.CyclicComponent
			}
		}
		for i := 0; i < n; i++ {
			if seen[cm(i+1)] != reach[i][0] {
				t.Fatal("incomplete selected-cohort ancestry")
			}
			for j := i + 1; j < n; j++ {
				if reach[i][0] && reach[j][0] {
					same := components[cm(i+1)] != "" && components[cm(i+1)] == components[cm(j+1)]
					if same != (reach[i][j] && reach[j][i]) {
						t.Fatal("incorrect strong component")
					}
				}
			}
		}
		verifyWitnesses(t, r, rows)
	}
}

func TestLongChainHasNoHiddenDepthCutoff(t *testing.T) {
	rows := make([]flow.Observation, 10000)
	for i := range rows {
		rows[i] = obs(uint64(i+1), i+2, i+1, 1)
	}
	r, err := analyze(context.Background(), "2024", candidate, Inputs{}, links()[:1], nil, rows)
	if err != nil || len(r.Nodes) != 10001 || r.Nodes[len(r.Nodes)-1].Hops != 10000 || len(r.CyclicComponents) != 0 {
		t.Fatal("chain truncated", err)
	}
	verifyWitnesses(t, r, rows)
}
