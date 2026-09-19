package candidateupstream

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"testing"
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

// Opt-in, read-only corpus gate. Sample selection is data-driven, not a rule
// in the calculation: choose two candidates with the most linked observations.
func TestLiveCandidateUpstream(t *testing.T) {
	root := os.Getenv("LT_UPSTREAM_STORAGE")
	if root == "" {
		t.Skip("opt-in published-data gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	o := Options{StorageRoot: root, Bundle: os.Getenv("LT_UPSTREAM_BUNDLE"), Linkages: os.Getenv("LT_UPSTREAM_LINKAGES"), Cycle: os.Getenv("LT_UPSTREAM_CYCLE")}
	start := time.Now()
	l, err := load(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	loadSeconds := time.Since(start).Seconds()
	counts := map[string]int{}
	byCommittee := map[string]map[string]bool{}
	for _, v := range l.linkages {
		if v.DesignationCode != "A" && v.DesignationCode != "P" {
			continue
		}
		if byCommittee[v.CommitteeID] == nil {
			byCommittee[v.CommitteeID] = map[string]bool{}
		}
		byCommittee[v.CommitteeID][v.CandidateID] = true
	}
	for _, v := range l.observations {
		for id := range byCommittee[v.Recipient] {
			counts[id]++
		}
	}
	ids := []string{}
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if counts[ids[i]] == counts[ids[j]] {
			return ids[i] < ids[j]
		}
		return counts[ids[i]] > counts[ids[j]]
	})
	if len(ids) < 2 {
		t.Fatal("insufficient real candidate scopes")
	}
	out := os.Getenv("LT_UPSTREAM_OUTPUT")
	if out == "" {
		t.Fatal("isolated audit output required")
	}
	if err := os.MkdirAll(out, 0750); err != nil {
		t.Fatal(err)
	}
	type sample struct {
		Candidate        string     `json:"candidate_id"`
		CalculationID    string     `json:"calculation_id"`
		Seconds          float64    `json:"calculation_seconds"`
		ReplaySeconds    float64    `json:"replay_seconds"`
		Accounting       Accounting `json:"accounting"`
		Nodes            int        `json:"reachable_committees"`
		MissingMasters   int        `json:"missing_masters"`
		Leaves           int        `json:"no_selected_incoming"`
		CyclicComponents int        `json:"cyclic_components"`
		UpstreamRows     int        `json:"upstream_rows"`
		ResultBytes      int        `json:"result_bytes"`
	}
	samples := []sample{}
	for _, id := range ids[:2] {
		started := time.Now()
		r, err := analyze(ctx, o.Cycle, id, l.inputs, l.linkages, l.masters, l.observations)
		if err != nil {
			t.Fatal(err)
		}
		seconds := time.Since(started).Seconds()
		verifyWitnesses(t, r, l.observations)
		verifyCorpusMembership(t, r, l.observations)
		started = time.Now()
		replay, err := analyze(ctx, o.Cycle, id, l.inputs, l.linkages, l.masters, l.observations)
		if err != nil || !reflect.DeepEqual(r, replay) {
			t.Fatal("real replay differs", err)
		}
		s := sample{Candidate: id, CalculationID: r.CalculationID, Seconds: seconds, ReplaySeconds: time.Since(started).Seconds(), Accounting: r.Accounting, Nodes: len(r.Nodes), CyclicComponents: len(r.CyclicComponents), UpstreamRows: len(r.UpstreamOrdinals)}
		for _, n := range r.Nodes {
			if n.MasterFactID == nil {
				s.MissingMasters++
			}
			if !n.Authorized && n.Incoming == 0 {
				s.Leaves++
			}
		}
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		s.ResultBytes = len(data)
		if err := os.WriteFile(filepath.Join(out, id+".json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, s)
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	result := struct {
		State       string   `json:"state"`
		Inputs      Inputs   `json:"inputs"`
		LoadSeconds float64  `json:"load_seconds"`
		Seconds     float64  `json:"gate_seconds"`
		PeakRSS     int64    `json:"peak_rss_bytes"`
		Samples     []sample `json:"samples"`
	}{"passed", l.inputs, loadSeconds, time.Since(start).Seconds(), usage.Maxrss * 1024, samples}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "result.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
}

func verifyCorpusMembership(t *testing.T, r Result, source []flow.Observation) {
	t.Helper()
	linked, auth, uncertain := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, v := range r.Relationships {
		if v.State == "authorized" {
			linked[v.CommitteeID] = true
			auth[v.CommitteeID] = true
		}
		if v.State == "unresolved" {
			linked[v.CommitteeID] = true
			uncertain[v.CommitteeID] = true
		}
	}
	nodes := map[string]bool{}
	for _, n := range r.Nodes {
		nodes[n.CommitteeID] = true
		if n.TerminalEligible {
			t.Fatal("terminal eligibility fabricated")
		}
	}
	want := map[uint64]flow.Observation{}
	rootCount := 0
	total := new(big.Int)
	for _, v := range source {
		if linked[v.Recipient] {
			rootCount++
		}
		if nodes[v.Recipient] || linked[v.Recipient] {
			want[v.Ordinal] = v
		}
		if nodes[v.Recipient] && !nodes[v.Sender] {
			t.Fatal("ancestry is not closed over selected ledger")
		}
		if auth[v.Recipient] && !auth[v.Sender] && !uncertain[v.Sender] {
			total.Add(total, big.NewInt(v.Amount))
		}
	}
	if rootCount != len(r.CandidateObservations) || total.String() != r.Accounting.External.Signed || r.Accounting.UnresolvedAttribution != r.Accounting.External || r.Accounting.TerminalAllocated != "0" {
		t.Fatal("candidate boundary does not conserve")
	}
	check := func(v flow.Observation) {
		if expected, ok := want[v.Ordinal]; !ok || !reflect.DeepEqual(expected, v) {
			t.Fatal("missing, repeated, or changed source observation")
		}
		delete(want, v.Ordinal)
	}
	for _, v := range r.CandidateObservations {
		check(v.Observation)
	}
	for _, ordinal := range r.UpstreamOrdinals {
		v, ok := want[ordinal]
		if !ok {
			t.Fatal("missing or repeated upstream ordinal")
		}
		check(v)
	}
	if len(want) != 0 {
		t.Fatal("incomplete scoped evidence")
	}
}
