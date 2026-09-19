package fundingbasis

import (
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
)

type CommitteeInventory struct {
	CommitteeID      string            `json:"committee_id"`
	Authorized       bool              `json:"candidate_authorized"`
	HasCurrentMaster bool              `json:"has_current_master"`
	Components       []ComponentAmount `json:"components"`
	Coverage         string            `json:"coverage"`
	TerminalEligible bool              `json:"terminal_attribution_eligible"`
}

type ComponentAmount struct {
	Component string   `json:"component"`
	Measures  Measures `json:"measures"`
}

type Assessment struct {
	SchemaVersion    string               `json:"schema_version"`
	InventoryID      string               `json:"inventory_calculation_id"`
	UpstreamID       string               `json:"upstream_calculation_id"`
	CandidateID      string               `json:"candidate_id"`
	Cycle            string               `json:"cycle"`
	State            string               `json:"state"`
	Committees       []CommitteeInventory `json:"committees"`
	TerminalEligible bool                 `json:"terminal_attribution_eligible"`
	NotCovered       []string             `json:"not_covered"`
}

// Assess attaches inventories to a freshly computed upstream trace. It does
// not add their subtotals across the network: those can contain the same funds.
func (r *Reader) Assess(trace candidateupstream.Result) (Assessment, error) {
	if err := r.matchTrace(trace); err != nil {
		return Assessment{}, err
	}
	return r.assess(trace)
}

func (r *Reader) matchTrace(trace candidateupstream.Result) error {
	a := trace.Inputs.Sources.A
	if trace.Cycle != r.result.Cycle || a.FactSetID != r.result.Input.FactSetID || a.ManifestSHA256 != r.result.Input.ManifestSHA256 || a.Facts != r.result.Input.Facts || trace.CalculationID == "" {
		return fmt.Errorf("candidate trace and receipt inventory must bind the exact same Schedule A facts")
	}
	return nil
}

func (r *Reader) assess(trace candidateupstream.Result) (Assessment, error) {
	byCommittee := make(map[string][]Bucket)
	for _, b := range r.result.Buckets {
		if b.Key.Recipient.Present {
			byCommittee[b.Key.Recipient.Value] = append(byCommittee[b.Key.Recipient.Value], b)
		}
	}
	out := Assessment{SchemaVersion: "legal-tender.fec.candidate-funding-basis-assessment.v1", InventoryID: r.result.CalculationID,
		UpstreamID: trace.CalculationID, CandidateID: trace.Candidate, Cycle: trace.Cycle, State: "reported_receipts_only_allocation_unresolved", Committees: []CommitteeInventory{}, NotCovered: limitations()}
	for _, node := range trace.Nodes {
		c := CommitteeInventory{CommitteeID: node.CommitteeID, Authorized: node.Authorized, HasCurrentMaster: node.MasterFactID != nil,
			Coverage: "no_schedule_a_rows_in_exact_snapshot", Components: []ComponentAmount{}}
		groups := make(map[string]Measures)
		for _, b := range byCommittee[node.CommitteeID] {
			m := groups[b.Key.Component]
			if err := m.merge(b.Measures); err != nil {
				return Assessment{}, err
			}
			groups[b.Key.Component] = m
		}
		if len(groups) > 0 {
			c.Coverage = "reported_schedule_a_rows_not_complete_funding"
		}
		for component, m := range groups {
			c.Components = append(c.Components, ComponentAmount{component, m})
		}
		sort.Slice(c.Components, func(i, j int) bool { return c.Components[i].Component < c.Components[j].Component })
		out.Committees = append(out.Committees, c)
	}
	sort.Slice(out.Committees, func(i, j int) bool { return out.Committees[i].CommitteeID < out.Committees[j].CommitteeID })
	return out, nil
}
