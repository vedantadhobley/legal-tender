// Package flowevidence projects source observations, not resolved payments.
package flowevidence

import (
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const Version = "legal-tender.arango.committee-flow-evidence.v1"
const GraphName = "committee_flow_evidence"
const entities = "entities"
const receivers = "receiver_reported_observations"
const senders = "sender_reported_observations"
const components = "reconciliation_components"
const metadata = "projection_metadata"

type Ledger string

const ScheduleA Ledger = "schedule_a"
const ScheduleB Ledger = "schedule_b"

type Options struct {
	StorageRoot, Bundle, Cycle, Endpoint, Username, Password string
	BatchSize                                                int
	Progress                                                 func(string)
}

type entity struct {
	Key              string                           `json:"_key"`
	Cycle            string                           `json:"cycle"`
	CommitteeID      string                           `json:"committee_id"`
	IdentityState    string                           `json:"identity_state"`
	MasterFactSetID  string                           `json:"master_fact_set_id"`
	MasterFactID     *string                          `json:"master_fact_id"`
	Master           *occurrence.CommitteeTypedFields `json:"master"`
	TerminalEligible bool                             `json:"terminal_attribution_eligible"`
}

type observation struct {
	Key                string `json:"_key"`
	From               string `json:"_from"`
	To                 string `json:"_to"`
	SchemaVersion      string `json:"schema_version"`
	Cycle              string `json:"cycle"`
	Ledger             Ledger `json:"ledger"`
	FactSetID          string `json:"fact_set_id"`
	CalculationID      string `json:"calculation_set_id"`
	SelectionPolicy    string `json:"selection_policy"`
	ReportingPolicy    string `json:"reporting_policy"`
	MatchingPolicy     string `json:"matching_policy"`
	ComponentID        string `json:"component_id"`
	EconomicFlowStatus string `json:"economic_flow_status"`
	TerminalEligible   bool   `json:"terminal_attribution_eligible"`
	flow.Observation
}

type component struct {
	Key                string `json:"_key"`
	Cycle              string `json:"cycle"`
	Sender             string `json:"sender_committee_id"`
	Recipient          string `json:"reported_recipient_committee_id"`
	CalculationID      string `json:"calculation_set_id"`
	MatchingPolicy     string `json:"matching_policy"`
	AFactSetID         string `json:"schedule_a_fact_set_id"`
	BFactSetID         string `json:"schedule_b_fact_set_id"`
	EconomicFlowStatus string `json:"economic_flow_status"`
	TerminalEligible   bool   `json:"terminal_attribution_eligible"`
	flow.Assertion
}

type SourceProof struct {
	EdgeID string             `json:"edge_id"`
	Source flow.SourceExample `json:"source"`
}
type QueryResult struct {
	Ledger       Ledger  `json:"ledger"`
	Kind         string  `json:"kind"`
	Rows         int     `json:"rows"`
	Milliseconds float64 `json:"milliseconds"`
	Status       string  `json:"status"`
}
type Storage struct {
	Collection string `json:"collection"`
	Documents  uint64 `json:"document_bytes"`
	Indexes    uint64 `json:"index_bytes"`
	Cache      uint64 `json:"cache_bytes"`
}
type Result struct {
	SchemaVersion        string        `json:"schema_version"`
	ProjectionID         string        `json:"projection_id"`
	Database             string        `json:"database"`
	State                string        `json:"state"`
	Reused               bool          `json:"reused"`
	Cycle                string        `json:"cycle"`
	BundleID             string        `json:"bundle_id"`
	BundleSHA256         string        `json:"bundle_sha256"`
	Entities             int           `json:"entities"`
	Unresolved           int           `json:"unresolved_same_cycle_masters"`
	A                    flow.Measures `json:"schedule_a"`
	B                    flow.Measures `json:"schedule_b"`
	Components           int           `json:"components"`
	Sources              []SourceProof `json:"source_drilldown"`
	VerifiedSourceShards uint64        `json:"verified_source_shards"`
	Queries              []QueryResult `json:"queries"`
	Storage              []Storage     `json:"storage"`
	Checks               []string      `json:"checks"`
	ElapsedSeconds       float64       `json:"elapsed_seconds"`
	ProcessPeakRSSBytes  uint64        `json:"process_peak_rss_bytes"`
	CompletedAt          time.Time     `json:"completed_at"`
}

type model struct {
	id, database string
	bundle       flow.Bundle
	bundleSHA    string
	calculation  flow.Result
	entities     []entity
	a, b         []observation
	components   []component
	unresolved   int
}

func (m *model) edges(side Ledger) []observation {
	if side == ScheduleA {
		return m.a
	}
	return m.b
}
