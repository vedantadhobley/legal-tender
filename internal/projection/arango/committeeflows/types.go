// Package committeeflows projects one verified receiver-reported committee-
// flow calculation into an isolated ArangoDB graph.
package committeeflows

import "time"

const (
	ProjectionVersion   = "legal-tender.arango.receiver-reported-committee-flows-projection.v1"
	ResultSchemaVersion = "legal-tender.arango.receiver-reported-committee-flows-probe-result.v1"
	EntitySchemaVersion = "legal-tender.arango.entity.v1"
	EdgeSchemaVersion   = "legal-tender.arango.receiver-reported-committee-flow-edge.v1"
	MetadataSchema      = "legal-tender.arango.projection-metadata.v1"
	GraphName           = "receiver_reported_committee_flows"
)

const (
	entitiesCollection = "entities"
	edgesCollection    = "receiver_reported_flows"
	metadataCollection = "projection_metadata"
)

type Input struct {
	StorageRoot         string
	Cycle               string
	ReadinessBundlePath string
	Endpoint            string
	Username            string
	Password            string
	RunID               string
	BatchSize           int
	QueryRepetitions    int
}

type Options struct {
	Clock    func() time.Time
	Progress func(string)
}

type Result struct {
	SchemaVersion        string             `json:"schema_version"`
	ProjectionVersion    string             `json:"projection_version"`
	ProjectionID         string             `json:"projection_id"`
	State                string             `json:"state"`
	Cycle                string             `json:"cycle"`
	Database             string             `json:"database"`
	Graph                string             `json:"graph"`
	RunID                string             `json:"run_id"`
	ObservedAt           time.Time          `json:"observed_at"`
	Inputs               InputReferences    `json:"inputs"`
	ExpectedCounts       ProjectionCounts   `json:"expected_counts"`
	ObservedCounts       ProjectionCounts   `json:"observed_counts"`
	ExpectedAmounts      ProjectionAmounts  `json:"expected_amounts"`
	ObservedAmounts      ProjectionAmounts  `json:"observed_amounts"`
	Topology             TopologyMetrics    `json:"topology"`
	ReusedProjection     bool               `json:"reused_projection"`
	MissingMasterFacts   MissingMasterFacts `json:"missing_master_facts"`
	Storage              StorageMetrics     `json:"storage"`
	RepresentativeSource string             `json:"representative_source_committee_id"`
	RepresentativeTarget string             `json:"representative_target_committee_id"`
	RepresentativeCycle  string             `json:"representative_cycle_committee_id"`
	Queries              []QueryMetric      `json:"queries"`
	Checks               []Check            `json:"checks"`
}

type InputReferences struct {
	SourceReleaseID           string `json:"source_release_id"`
	ReadinessBundleID         string `json:"readiness_bundle_id"`
	ReadinessBundleSHA256     string `json:"readiness_bundle_manifest_sha256"`
	CalculationSetID          string `json:"calculation_set_id"`
	CalculationManifestSHA256 string `json:"calculation_manifest_sha256"`
	ScheduleAFactSetID        string `json:"schedule_a_fact_set_id"`
	ScheduleAManifestSHA256   string `json:"schedule_a_manifest_sha256"`
	CommitteeFactSetID        string `json:"committee_fact_set_id"`
	CommitteeManifestSHA256   string `json:"committee_manifest_sha256"`
}

type ProjectionCounts struct {
	Entities                    uint64 `json:"entities"`
	PresentCommitteeMasters     uint64 `json:"present_committee_masters"`
	MissingCommitteeMasters     uint64 `json:"missing_committee_masters"`
	Edges                       uint64 `json:"edges"`
	RegisteredFilerContribution uint64 `json:"registered_filer_contribution_edges"`
	InKindContribution          uint64 `json:"in_kind_contribution_edges"`
	AffiliatedTransferIn        uint64 `json:"affiliated_transfer_in_edges"`
	RefundRepaymentReceived     uint64 `json:"refund_repayment_received_edges"`
}

type ProjectionAmounts struct {
	TotalMinorUnits                       string `json:"total_minor_units"`
	RegisteredFilerContributionMinorUnits string `json:"registered_filer_contribution_minor_units"`
	InKindContributionMinorUnits          string `json:"in_kind_contribution_minor_units"`
	AffiliatedTransferInMinorUnits        string `json:"affiliated_transfer_in_minor_units"`
	RefundRepaymentReceivedMinorUnits     string `json:"refund_repayment_received_minor_units"`
}

type TopologyMetrics struct {
	WeakComponents          uint64 `json:"weak_components"`
	StrongComponents        uint64 `json:"strong_components"`
	CyclicStrongComponents  uint64 `json:"cyclic_strong_components"`
	CommitteesInCycles      uint64 `json:"committees_in_cycles"`
	RepresentativePathHops  int    `json:"representative_path_hops"`
	RepresentativeCycleHops int    `json:"representative_cycle_hops"`
}

type MissingMasterFacts struct {
	Committees uint64 `json:"committees"`
}

type StorageMetrics struct {
	Collections   []CollectionMetrics `json:"collections"`
	DocumentBytes uint64              `json:"document_bytes"`
	IndexBytes    uint64              `json:"index_bytes"`
	CombinedBytes uint64              `json:"combined_bytes"`
}

type CollectionMetrics struct {
	Name          string `json:"name"`
	Documents     uint64 `json:"documents"`
	DocumentBytes uint64 `json:"document_bytes"`
	IndexCount    uint64 `json:"index_count"`
	IndexBytes    uint64 `json:"index_bytes"`
	CacheBytes    uint64 `json:"cache_bytes"`
}

type QueryMetric struct {
	ID            string `json:"id"`
	Repetitions   int    `json:"repetitions"`
	ResultRows    int    `json:"result_rows"`
	MinimumMicros int64  `json:"minimum_micros"`
	MedianMicros  int64  `json:"median_micros"`
	P95Micros     int64  `json:"p95_micros"`
	MaximumMicros int64  `json:"maximum_micros"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type entityDocument struct {
	Key                   string  `json:"_key"`
	SchemaVersion         string  `json:"schema_version"`
	EntityType            string  `json:"entity_type"`
	EntityID              string  `json:"entity_id"`
	Cycle                 string  `json:"cycle"`
	SourceState           string  `json:"source_state"`
	SourceFactID          *string `json:"source_fact_id"`
	SourceFactSetID       *string `json:"source_fact_set_id"`
	Name                  string  `json:"name"`
	PartyAffiliation      string  `json:"party_affiliation,omitempty"`
	DesignationCode       string  `json:"designation_code,omitempty"`
	CommitteeTypeCode     string  `json:"committee_type_code,omitempty"`
	OrganizationTypeCode  string  `json:"organization_type_code,omitempty"`
	ConnectedOrganization string  `json:"connected_organization,omitempty"`
	DocumentDigest        string  `json:"document_digest"`
}

type flowEdge struct {
	Key                string `json:"_key"`
	From               string `json:"_from"`
	To                 string `json:"_to"`
	SchemaVersion      string `json:"schema_version"`
	RelationType       string `json:"relation_type"`
	Cycle              string `json:"cycle"`
	ReceiptRole        string `json:"receipt_role"`
	AmountMinorUnits   string `json:"amount_minor_units"`
	ReceiptCount       uint64 `json:"receipt_count"`
	PositiveCount      uint64 `json:"positive_count"`
	NegativeCount      uint64 `json:"negative_count"`
	ZeroCount          uint64 `json:"zero_count"`
	ResultID           string `json:"result_id"`
	CalculationSetID   string `json:"calculation_set_id"`
	ScheduleAFactSetID string `json:"schedule_a_fact_set_id"`
	SourceReleaseID    string `json:"source_release_id"`
	DocumentDigest     string `json:"document_digest"`
}

type projectionMetadata struct {
	Key               string             `json:"_key"`
	SchemaVersion     string             `json:"schema_version"`
	ProjectionVersion string             `json:"projection_version"`
	ProjectionID      string             `json:"projection_id"`
	Cycle             string             `json:"cycle"`
	Graph             string             `json:"graph"`
	Inputs            InputReferences    `json:"inputs"`
	Counts            ProjectionCounts   `json:"counts"`
	Amounts           ProjectionAmounts  `json:"amounts"`
	Topology          TopologyMetrics    `json:"topology"`
	Missing           MissingMasterFacts `json:"missing_master_facts"`
	DocumentDigest    string             `json:"document_digest"`
}

type projection struct {
	ID                   string
	Database             string
	Cycle                string
	Inputs               InputReferences
	Entities             []entityDocument
	Edges                []flowEdge
	Metadata             projectionMetadata
	Counts               ProjectionCounts
	Amounts              ProjectionAmounts
	Topology             TopologyMetrics
	Missing              MissingMasterFacts
	RepresentativeSource string
	RepresentativeTarget string
	RepresentativeCycle  string
}
