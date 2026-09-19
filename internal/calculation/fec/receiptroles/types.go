// Package receiptroles profiles reported occurrence roles without resolving
// people, asserting corporate ownership, or allocating financial amounts.
package receiptroles

import "strings"

const Policy = "fec/committee-receipt-role-profile@1.0.0"
const MaxCells = 1000000 // Shared worker map-entry circuit breaker, never truncation.

type Key struct {
	Route              string `json:"source_route"`
	Component          string `json:"inventory_component"`
	IndividualDecision string `json:"individual_decision"`
	CommitteeDecision  string `json:"committee_decision"`
	ReceiptRole        string `json:"receipt_role"`
	EntityPresent      bool   `json:"reported_entity_type_present"`
	Entity             string `json:"reported_entity_type"`
	Memo               bool   `json:"memo_subtotal"`
	Conflict           bool   `json:"entity_conflict"`
	Overlap            bool   `json:"publisher_individual_overlap"`
	SourceIdentity     string `json:"reported_source_committee_identity_state"`
}

func (k Key) owned() Key {
	for _, p := range []*string{&k.Route, &k.Component, &k.IndividualDecision, &k.CommitteeDecision, &k.ReceiptRole, &k.Entity, &k.SourceIdentity} {
		*p = strings.Clone(*p)
	}
	return k
}

type Counts struct {
	Rows     uint64 `json:"source_occurrences"`
	Positive uint64 `json:"positive_amount_occurrences"`
	Negative uint64 `json:"negative_amount_occurrences"`
	Zero     uint64 `json:"zero_amount_occurrences"`
	Unknown  uint64 `json:"unknown_amount_occurrences"`
	First    uint64 `json:"first_source_row_ordinal"`
}

func (c *Counts) merge(v Counts) {
	c.Rows += v.Rows
	c.Positive += v.Positive
	c.Negative += v.Negative
	c.Zero += v.Zero
	c.Unknown += v.Unknown
	if v.First > 0 && (c.First == 0 || v.First < c.First) {
		c.First = v.First
	}
}

type Group struct {
	Key    Key    `json:"role"`
	Counts Counts `json:"counts"`
}
type Profile struct {
	CommitteeID string            `json:"committee_id"`
	State       string            `json:"state"`
	Counts      Counts            `json:"counts"`
	Groups      []Group           `json:"role_groups"`
	Earmarks    map[string]uint64 `json:"earmark_state_marginal"`
	Conduits    map[string]uint64 `json:"structured_conduit_state_marginal"`
	References  map[string]uint64 `json:"reported_reference_state_marginal"`
}
type SourceIdentity struct {
	ID           string  `json:"reported_source_committee_id"`
	State        string  `json:"state"`
	MasterFactID *string `json:"master_fact_id"`
	Rows         uint64  `json:"source_occurrences_in_scoped_recipients"`
	First        uint64  `json:"first_source_row_ordinal"`
}
type Result struct {
	Policy           string           `json:"policy"`
	Rows             uint64           `json:"complete_participant_rows"`
	Scoped           Counts           `json:"scoped_recipient_occurrences"`
	Outside          Counts           `json:"outside_selected_committee_union"`
	Unresolved       Counts           `json:"unresolved_reported_recipient"`
	Profiles         []Profile        `json:"committee_profiles"`
	Sources          []SourceIdentity `json:"reported_source_committee_identities"`
	IdentityResolved bool             `json:"person_corporation_identity_resolved"`
	TerminalEligible bool             `json:"terminal_attribution_eligible"`
}
