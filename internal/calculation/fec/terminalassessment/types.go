// Package terminalassessment compares provisional topology hypotheses. It does
// not classify financial origins, resolve people, or allocate terminal dollars.
package terminalassessment

const Policy = "fec/selected-committee-boundary-assessment@1.0.0"

const (
	SameCycleMaster = "same_cycle_master"
	MissingMaster   = "unresolved_same_cycle_master"
	Absent          = "not_in_selected_ledger"
	Frontier        = "no_selected_incoming_observations"
	Inbound         = "selected_incoming_observations_present"
)

type Committee struct {
	ID              string  `json:"committee_id"`
	IdentityState   string  `json:"identity_state"`
	MasterFactSetID string  `json:"master_fact_set_id"`
	MasterFactID    *string `json:"master_fact_id"`
}

// Observation intentionally has no amount: all selected source occurrences,
// including parallel, signed, zero and in-kind rows, retain their topology.
type Observation struct{ Key, From, To string }

type Definition struct {
	ID        string `json:"hypothesis_id"`
	Predicate string `json:"predicate"`
	Status    string `json:"status"`
}

func Definitions() []Definition {
	return []Definition{
		{"selected_inbound_frontier@1", "present endpoint with zero selected incoming observations", "provisional_not_terminal_policy"},
		{"same_cycle_master_frontier@1", "selected inbound frontier with a same-cycle master fact", "provisional_not_terminal_policy"},
		{"selected_condensation_root@1", "member of a strongly connected component with zero external incoming observations", "provisional_not_terminal_policy"},
	}
}

type Node struct {
	CommitteeID string `json:"committee_id"`
	State       string `json:"selected_ledger_state"`
	Incoming    uint64 `json:"incoming_observations"`
	Outgoing    uint64 `json:"outgoing_observations"`
	SelfLoops   uint64 `json:"self_loop_observations"`
	ComponentID string `json:"component_id"`
	// Nil means not applicable: an absent node is not a zero-inbound frontier.
	Frontier         *bool    `json:"matches_inbound_frontier"`
	IdentityFrontier *bool    `json:"matches_same_cycle_master_frontier"`
	RootComponent    *bool    `json:"matches_condensation_root"`
	Blockers         []string `json:"additional_origin_blockers"`
}

type Component struct {
	ID             string   `json:"component_id"`
	Members        []string `json:"members"`
	Cyclic         bool     `json:"cyclic"`
	Internal       uint64   `json:"internal_observations"`
	Incoming       uint64   `json:"external_incoming_observations"`
	Outgoing       uint64   `json:"external_outgoing_observations"`
	MissingMasters uint64   `json:"unresolved_same_cycle_masters"`
}

type RuleCount struct {
	ID            string `json:"hypothesis_id"`
	Matched       uint64 `json:"matched_nodes"`
	NotMatched    uint64 `json:"not_matched_nodes"`
	NotApplicable uint64 `json:"not_applicable_nodes"`
}

type Ledger struct {
	Name                 string      `json:"ledger"`
	State                string      `json:"state"`
	Observations         uint64      `json:"observations"`
	Present              uint64      `json:"present_nodes"`
	MissingMasters       uint64      `json:"present_unresolved_same_cycle_masters"`
	CyclicComponents     uint64      `json:"cyclic_components"`
	RootComponents       uint64      `json:"root_components"`
	CyclicRootComponents uint64      `json:"cyclic_root_components"`
	Nodes                []Node      `json:"nodes"`
	Components           []Component `json:"components"`
	Rules                []RuleCount `json:"hypotheses"`
}

type Comparison struct {
	A          string `json:"schedule_a_state"`
	B          string `json:"schedule_b_state"`
	Committees uint64 `json:"committees"`
}

type Result struct {
	Policy           string       `json:"policy"`
	Definitions      []Definition `json:"definitions"`
	Committees       []Committee  `json:"committees"`
	A                Ledger       `json:"schedule_a"`
	B                Ledger       `json:"schedule_b"`
	Comparison       []Comparison `json:"frontier_comparison"`
	OriginState      string       `json:"financial_origin_state"`
	Blockers         []string     `json:"origin_blockers_for_all_nodes"`
	TerminalEligible bool         `json:"terminal_attribution_eligible"`
}
