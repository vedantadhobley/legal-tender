package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	fc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

// This is a composition experiment, not a replacement generation loader. It
// exercises the production topology/search with source-shaped synthetic inputs.
// Selection and provenance routing below are TEST scaffolding, not a published
// temporal query API or proof that real cross-cycle backing has been verified.
type partitionProbe struct {
	Cycle   string
	FactSet string
	Rows    []fc.Observation
}

type partitionProbeEvidence struct {
	Cycle, FactSet string
	Row            fc.Observation
}

func probeDay(t *testing.T, date string) *int32 {
	t.Helper()
	v, err := time.Parse(time.DateOnly, date)
	if err != nil {
		t.Fatal(err)
	}
	day := int32(v.Unix() / 86400)
	return &day
}

func probePartitions(t *testing.T, firstYear int) []partitionProbe {
	t.Helper()
	firstCycle, nextCycle := fmt.Sprint(firstYear), fmt.Sprint(firstYear+2)
	return []partitionProbe{
		{Cycle: firstCycle, FactSet: valueID("fixture-facts-" + firstCycle), Rows: []fc.Observation{
			{Ordinal: 1, Sender: "C00000001", Recipient: "C00000002", Date: probeDay(t, firstCycle+"-12-30"), Amount: 101},
		}},
		{Cycle: nextCycle, FactSet: valueID("fixture-facts-" + nextCycle), Rows: []fc.Observation{
			{Ordinal: 1, Sender: "C00000002", Recipient: "C00000003", Date: probeDay(t, fmt.Sprintf("%d-01-02", firstYear+1)), Amount: 71},
			// Parallel source appearances remain distinct, even with equal values.
			{Ordinal: 2, Sender: "C00000002", Recipient: "C00000003", Date: probeDay(t, fmt.Sprintf("%d-01-02", firstYear+1)), Amount: 71},
			{Ordinal: 3, Sender: "C00000003", Recipient: "C00000004", Date: nil, Amount: -9},
		}},
	}
}

// A nil window means all supplied observations, including unknown dates. A
// bounded window uses inclusive reported dates, with unknowns returned separately.
// This deliberately does not infer dates from source cycles, financial membership,
// chronological feasibility, or an employment/authorization validity interval.
func probeTopology(partitions []partitionProbe, window *[2]int32) (pathTopology, map[string]partitionProbeEvidence, []partitionProbeEvidence, error) {
	topology := pathTopology{}
	evidence := map[string]partitionProbeEvidence{}
	unknown := []partitionProbeEvidence{}
	for _, partition := range partitions {
		for _, row := range partition.Rows {
			proof := partitionProbeEvidence{partition.Cycle, partition.FactSet, row}
			if window != nil {
				if row.Date == nil {
					unknown = append(unknown, proof)
					continue
				}
				if *row.Date < window[0] || *row.Date > window[1] {
					continue
				}
			}
			// Synthetic keys pin exact occurrence identity; production key scoping
			// is checked separately in flowevidence's cross-partition model test.
			link := graphread.Link{Family: "receiver_reported_committee_observation", Key: valueID([]any{partition.FactSet, row.Ordinal}), From: row.Sender, To: row.Recipient}
			if _, exists := evidence[link.ID()]; exists {
				return nil, nil, nil, fmt.Errorf("overlapping source membership")
			}
			evidence[link.ID()] = proof
			if err := topology.add(link); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return topology, evidence, unknown, topology.order()
}

func probePaths(t *testing.T, topology pathTopology, start, target string) ([][]graphread.Link, PathSearch) {
	t.Helper()
	paths, state, err := searchPaths(context.Background(), topology, nil, start, target, 4, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	return paths, state
}

func TestCrossPartitionSearchComposesDetailedEvidenceNotCycleAnswers(t *testing.T) {
	for _, year := range []int{2022, 2030} {
		t.Run(fmt.Sprint(year), func(t *testing.T) {
			partitions := probePartitions(t, year)
			before, err := json.Marshal(partitions)
			if err != nil {
				t.Fatal(err)
			}
			// Neither partition contains the route. Adding their path counts would
			// incorrectly yield zero; the combined detailed topology has two paths.
			for _, partition := range partitions {
				topology, _, _, err := probeTopology([]partitionProbe{partition}, nil)
				if err != nil {
					t.Fatal(err)
				}
				paths, _ := probePaths(t, topology, "C00000001", "C00000003")
				if len(paths) != 0 {
					t.Fatal("fixture unexpectedly has a within-partition path")
				}
			}
			topology, evidence, _, err := probeTopology(partitions, nil)
			if err != nil {
				t.Fatal(err)
			}
			paths, state := probePaths(t, topology, "C00000001", "C00000003")
			if len(paths) != 2 || state.State != "complete_within_hop_bound" || len(evidence) != 4 {
				t.Fatal("cross-partition connectivity or source grain lost", paths, state)
			}
			for _, path := range paths {
				if len(path) != 2 || path[0].To != path[1].From {
					t.Fatal("shared committee identity did not connect")
				}
				first, second := evidence[path[0].ID()], evidence[path[1].ID()]
				if first.Cycle != partitions[0].Cycle || second.Cycle != partitions[1].Cycle || first.FactSet == second.FactSet || first.Row.Amount != 101 || second.Row.Amount != 71 {
					t.Fatal("lost original evidence or collapsed amounts")
				}
			}
			// Reordering partitions/rows must not alter membership, ordering or scope.
			reordered := []partitionProbe{partitions[1], partitions[0]}
			reordered[0].Rows = slices.Clone(reordered[0].Rows)
			slices.Reverse(reordered[0].Rows)
			again, proof, _, err := probeTopology(reordered, nil)
			if err != nil {
				t.Fatal(err)
			}
			replay, replayState := probePaths(t, again, "C00000001", "C00000003")
			if !reflect.DeepEqual(paths, replay) || state != replayState || !reflect.DeepEqual(evidence, proof) {
				t.Fatal("input order changed the answer")
			}
			after, err := json.Marshal(partitions)
			if err != nil || string(before) != string(after) {
				t.Fatal("composition mutated source observations", err)
			}
		})
	}
}

func TestCrossPartitionDateSelectionIsNotASourceCycleFilter(t *testing.T) {
	partitions := probePartitions(t, 2022)
	window := [2]int32{*probeDay(t, "2022-12-30"), *probeDay(t, "2023-01-02")}
	topology, evidence, unknown, err := probeTopology(partitions, &window)
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := probePaths(t, topology, "C00000001", "C00000003")
	if len(paths) != 2 || len(evidence) != 3 || len(unknown) != 1 || unknown[0].Row.Date != nil || unknown[0].Row.Amount != -9 {
		t.Fatal("arbitrary date window lost a boundary date or hid unknown time")
	}
	// Same source partitions, narrower dates: the first edge is outside scope.
	window[0]++
	narrow, _, _, err := probeTopology(partitions, &window)
	if err != nil {
		t.Fatal(err)
	}
	paths, state := probePaths(t, narrow, "C00000001", "C00000003")
	if len(paths) != 0 || state.NoOutgoing != 1 {
		t.Fatal("window did not change path membership")
	}
	// The same endpoint has outgoing evidence when the filter is removed. A
	// no-outgoing counter in the narrower query cannot classify a terminal source.
	all, _, _, err := probeTopology(partitions, nil)
	if err != nil {
		t.Fatal(err)
	}
	paths, _ = probePaths(t, all, "C00000001", "C00000004")
	if len(paths) != 2 || partitions[1].Rows[2].Date != nil {
		t.Fatal("all-supplied-evidence topology lost an undated observation")
	}
}

func TestCrossPartitionTopologyDoesNotClaimChronologicalMoneyFlow(t *testing.T) {
	partitions := probePartitions(t, 2022)
	partitions[0].Rows[0].Date = probeDay(t, "2022-12-31")
	partitions[1].Rows[0].Date = probeDay(t, "2021-01-01")
	partitions[1].Rows[1].Date = probeDay(t, "2021-01-01")
	topology, evidence, _, err := probeTopology(partitions, nil)
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := probePaths(t, topology, "C00000001", "C00000003")
	if len(paths) != 2 {
		t.Fatal("observation search silently introduced a chronology policy")
	}
	for _, path := range paths {
		first, second := evidence[path[0].ID()], evidence[path[1].ID()]
		if *first.Row.Date <= *second.Row.Date || second.Cycle != "2024" {
			t.Fatal("reported dates were overwritten from partition labels")
		}
	}
}

func TestCrossPartitionDuplicateInputsAreNotAdditionalEvidence(t *testing.T) {
	partitions := probePartitions(t, 2022)
	if _, _, _, err := probeTopology(append(partitions, partitions[0]), nil); err == nil {
		t.Fatal("double-counted the same source publication")
	}
	// The existing topology boundary independently rejects duplicate link IDs.
	topology, _, _, err := probeTopology(partitions, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := topology.add(topology["C00000001"][0]); err != nil {
		t.Fatal(err)
	}
	if topology.order() == nil {
		t.Fatal("production topology accepted duplicate occurrence links")
	}
}
