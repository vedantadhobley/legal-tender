// Package candidateupstream traces the accepted receiver-reported committee
// cohort into a candidate scope. Connectivity is not pooled-dollar attribution.
package candidateupstream

import (
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const Version = "legal-tender.fec.candidate-committee-upstream.v1"
const Policy = "fec/candidate-committee-upstream-evidence@1.0.0"

type Options struct {
	StorageRoot, Bundle, Linkages, Cycle, Candidate string
	Progress                                        func(string)
}

type Inputs struct {
	BundleID             string                             `json:"bundle_id"`
	BundleSHA256         string                             `json:"bundle_sha256"`
	Reconciliation       flow.CalculationReference          `json:"reconciliation"`
	Sources              flow.Inputs                        `json:"sources"`
	CommitteeMaster      flow.FactReference                 `json:"committee_master"`
	Linkages             flow.FactReference                 `json:"linkages"`
	ReferenceEquivalence []occurrence.ClassicReferenceProof `json:"reference_equivalence,omitempty"`
}

// Amount is a signed observation subtotal. Positive and negative populations
// remain visible even when their net is zero. It is not an economic cash total.
type Amount struct {
	Records         uint64 `json:"records"`
	PositiveRecords uint64 `json:"positive_records"`
	NegativeRecords uint64 `json:"negative_records"`
	ZeroRecords     uint64 `json:"zero_records"`
	Signed          string `json:"signed_minor_units"`
	Positive        string `json:"positive_minor_units"`
	Negative        string `json:"negative_minor_units"`
}

type Accounting struct {
	CandidateLinked       Amount `json:"candidate_linked_observations"`
	External              Amount `json:"external_to_authorized_scope"`
	Internal              Amount `json:"within_authorized_scope"`
	UnresolvedScope       Amount `json:"unresolved_authorization_boundary"`
	TerminalAllocated     string `json:"terminal_allocated_minor_units"`
	UnresolvedAttribution Amount `json:"unresolved_attribution"`
}

type CandidateObservation struct {
	Observation flow.Observation `json:"observation"`
	Disposition string           `json:"disposition"`
}

type Node struct {
	CommitteeID      string   `json:"committee_id"`
	Authorized       bool     `json:"candidate_authorized"`
	MasterFactID     *string  `json:"master_fact_id"`
	Hops             int      `json:"minimum_hops_to_authorized_scope"`
	WitnessOrdinal   *uint64  `json:"witness_source_row_ordinal"`
	Incoming         uint64   `json:"selected_incoming_observations"`
	CyclicComponent  *string  `json:"cyclic_component_id"`
	Reasons          []string `json:"attribution_boundary_reasons"`
	TerminalEligible bool     `json:"terminal_attribution_eligible"`
}

type Component struct {
	ID         string   `json:"component_id"`
	Committees []string `json:"committee_ids"`
}

type Result struct {
	SchemaVersion         string                           `json:"schema_version"`
	CalculationID         string                           `json:"calculation_id"`
	Policy                string                           `json:"policy"`
	Cycle                 string                           `json:"cycle"`
	Candidate             string                           `json:"candidate_id"`
	State                 string                           `json:"state"`
	Scope                 string                           `json:"scope"`
	Ledger                string                           `json:"ledger"`
	TimeSemantics         string                           `json:"time_semantics"`
	AttributionState      string                           `json:"attribution_state"`
	TerminalEligible      bool                             `json:"terminal_attribution_eligible"`
	Inputs                Inputs                           `json:"inputs"`
	Relationships         []receipts.CommitteeRelationship `json:"committee_relationships"`
	Accounting            Accounting                       `json:"accounting"`
	CandidateObservations []CandidateObservation           `json:"candidate_observations"`
	UpstreamOrdinals      []uint64                         `json:"upstream_source_row_ordinals"`
	Nodes                 []Node                           `json:"nodes"`
	CyclicComponents      []Component                      `json:"cyclic_components"`
	Exclusions            []string                         `json:"not_covered"`
}
