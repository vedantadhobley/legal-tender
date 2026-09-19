package committeeflows

import (
	"time"

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
)

const (
	ProjectionVersionV2   = "legal-tender.arango.receiver-reported-committee-flows-projection.v2"
	ResultSchemaVersionV2 = "legal-tender.arango.receiver-reported-committee-flows-probe-result.v2"
	EntitySchemaVersionV2 = "legal-tender.arango.committee-identity.v2"
	MetadataSchemaV2      = "legal-tender.arango.projection-metadata.v2"
)

type InputV2 struct {
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

type ResultV2 struct {
	SchemaVersion        string             `json:"schema_version"`
	ProjectionVersion    string             `json:"projection_version"`
	ProjectionID         string             `json:"projection_id"`
	State                string             `json:"state"`
	Cycle                string             `json:"cycle"`
	Database             string             `json:"database"`
	Graph                string             `json:"graph"`
	RunID                string             `json:"run_id"`
	ObservedAt           time.Time          `json:"observed_at"`
	Inputs               InputReferencesV2  `json:"inputs"`
	ExpectedCounts       ProjectionCountsV2 `json:"expected_counts"`
	ObservedCounts       ProjectionCountsV2 `json:"observed_counts"`
	ExpectedAmounts      ProjectionAmounts  `json:"expected_amounts"`
	ObservedAmounts      ProjectionAmounts  `json:"observed_amounts"`
	Topology             TopologyMetrics    `json:"topology"`
	ReusedProjection     bool               `json:"reused_projection"`
	Storage              StorageMetrics     `json:"storage"`
	RepresentativeSource string             `json:"representative_source_committee_id"`
	RepresentativeTarget string             `json:"representative_target_committee_id"`
	RepresentativeCycle  string             `json:"representative_cycle_committee_id"`
	Queries              []QueryMetric      `json:"queries"`
	Checks               []Check            `json:"checks"`
}

type InputReferencesV2 struct {
	SourceReleaseID           string `json:"source_release_id"`
	ReadinessBundleID         string `json:"readiness_bundle_id"`
	ReadinessBundleSHA256     string `json:"readiness_bundle_manifest_sha256"`
	BaseFlowBundleID          string `json:"base_flow_bundle_id"`
	BaseFlowBundleSHA256      string `json:"base_flow_bundle_manifest_sha256"`
	CalculationSetID          string `json:"calculation_set_id"`
	CalculationManifestSHA256 string `json:"calculation_manifest_sha256"`
	ScheduleAFactSetID        string `json:"schedule_a_fact_set_id"`
	ScheduleAManifestSHA256   string `json:"schedule_a_manifest_sha256"`
	CommitteeFactSetID        string `json:"committee_fact_set_id"`
	CommitteeManifestSHA256   string `json:"committee_manifest_sha256"`
	IdentityCalculationSetID  string `json:"identity_calculation_set_id"`
	IdentityManifestSHA256    string `json:"identity_manifest_sha256"`
	IdentityDecisionsSHA256   string `json:"identity_decisions_sha256"`
}

type ProjectionCountsV2 struct {
	Entities                      uint64 `json:"entities"`
	CurrentCycleMasters           uint64 `json:"current_cycle_masters"`
	HistoricalRegistrations       uint64 `json:"historical_registrations"`
	AlternateReleaseRegistrations uint64 `json:"alternate_release_registrations"`
	UnresolvedReportedIDs         uint64 `json:"unresolved_reported_ids"`
	TerminalIdentityEligible      uint64 `json:"terminal_identity_eligible"`
	TerminalIdentityIneligible    uint64 `json:"terminal_identity_ineligible"`
	Edges                         uint64 `json:"edges"`
	RegisteredFilerContribution   uint64 `json:"registered_filer_contribution_edges"`
	InKindContribution            uint64 `json:"in_kind_contribution_edges"`
	AffiliatedTransferIn          uint64 `json:"affiliated_transfer_in_edges"`
	RefundRepaymentReceived       uint64 `json:"refund_repayment_received_edges"`
}

type entityDocumentV2 struct {
	Key                      string                                           `json:"_key"`
	SchemaVersion            string                                           `json:"schema_version"`
	EntityType               string                                           `json:"entity_type"`
	EntityID                 string                                           `json:"entity_id"`
	Cycle                    string                                           `json:"cycle"`
	SourceState              string                                           `json:"source_state"`
	TerminalIdentityEligible bool                                             `json:"terminal_identity_eligible"`
	SourceFactID             *string                                          `json:"source_fact_id"`
	SourceFactSetID          *string                                          `json:"source_fact_set_id"`
	IdentityDecisionID       *string                                          `json:"identity_decision_id"`
	IdentityCalculationSetID *string                                          `json:"identity_calculation_set_id"`
	Name                     string                                           `json:"name"`
	PartyAffiliation         string                                           `json:"party_affiliation,omitempty"`
	DesignationCode          string                                           `json:"designation_code,omitempty"`
	CommitteeTypeCode        string                                           `json:"committee_type_code,omitempty"`
	OrganizationTypeCode     string                                           `json:"organization_type_code,omitempty"`
	ConnectedOrganization    string                                           `json:"connected_organization,omitempty"`
	SameCycleAssertions      []fecflowmastergaps.CommitteeHistoricalAssertion `json:"same_cycle_comparison_assertions"`
	HistoricalAssertions     []fecflowmastergaps.CommitteeHistoricalAssertion `json:"historical_assertions"`
	DocumentDigest           string                                           `json:"document_digest"`
}

type projectionMetadataV2 struct {
	Key               string             `json:"_key"`
	SchemaVersion     string             `json:"schema_version"`
	ProjectionVersion string             `json:"projection_version"`
	ProjectionID      string             `json:"projection_id"`
	Cycle             string             `json:"cycle"`
	Graph             string             `json:"graph"`
	Inputs            InputReferencesV2  `json:"inputs"`
	Counts            ProjectionCountsV2 `json:"counts"`
	Amounts           ProjectionAmounts  `json:"amounts"`
	Topology          TopologyMetrics    `json:"topology"`
	DocumentDigest    string             `json:"document_digest"`
}

type projectionV2 struct {
	ID                   string
	Database             string
	Cycle                string
	Inputs               InputReferencesV2
	Entities             []entityDocumentV2
	Edges                []flowEdge
	Metadata             projectionMetadataV2
	Counts               ProjectionCountsV2
	Amounts              ProjectionAmounts
	Topology             TopologyMetrics
	RepresentativeSource string
	RepresentativeTarget string
	RepresentativeCycle  string
}
