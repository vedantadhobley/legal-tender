// Package independentexpenditures projects one verified effective Schedule E
// calculation into an isolated ArangoDB graph for bounded model validation.
package independentexpenditures

import "time"

const (
	ProjectionVersion           = "legal-tender.arango.independent-expenditures-projection.v1"
	ResultSchemaVersion         = "legal-tender.arango.independent-expenditures-probe-result.v1"
	EntitySchemaVersion         = "legal-tender.arango.entity.v1"
	EdgeSchemaVersion           = "legal-tender.arango.independent-expenditure-edge.v1"
	MetadataSchema              = "legal-tender.arango.independent-expenditure-projection-metadata.v1"
	GraphName                   = "independent_expenditures"
	ResolvedProjectionVersion   = "legal-tender.arango.resolved-independent-expenditures-projection.v2"
	ResolvedResultSchemaVersion = "legal-tender.arango.resolved-independent-expenditures-probe-result.v2"
	ResolvedEdgeSchemaVersion   = "legal-tender.arango.resolved-independent-expenditure-edge.v2"
	ResolvedMetadataSchema      = "legal-tender.arango.resolved-independent-expenditure-projection-metadata.v2"
)

const (
	entitiesCollection = "entities"
	edgesCollection    = "independent_expenditure_edges"
	metadataCollection = "projection_metadata"
)

type Input struct {
	StorageRoot             string
	Cycle                   string
	ReadinessBundlePath     string
	CalculationManifestPath string
	CandidateManifestPath   string
	CommitteeManifestPath   string
	Endpoint                string
	Username                string
	Password                string
	RunID                   string
	BatchSize               int
	QueryRepetitions        int
}

type Options struct {
	Clock    func() time.Time
	Progress func(string)
}

type ResolvedInput struct {
	StorageRoot            string
	Cycle                  string
	ReadinessBundlePath    string
	AggregateManifestPath  string
	ResolutionManifestPath string
	CandidateManifestPath  string
	CommitteeManifestPath  string
	Endpoint               string
	Username               string
	Password               string
	RunID                  string
	BatchSize              int
	QueryRepetitions       int
}

type Result struct {
	SchemaVersion               string                       `json:"schema_version"`
	ProjectionVersion           string                       `json:"projection_version"`
	ProjectionID                string                       `json:"projection_id"`
	State                       string                       `json:"state"`
	Cycle                       string                       `json:"cycle"`
	Database                    string                       `json:"database"`
	Graph                       string                       `json:"graph"`
	RunID                       string                       `json:"run_id"`
	ObservedAt                  time.Time                    `json:"observed_at"`
	Inputs                      InputReferences              `json:"inputs"`
	ExpectedCounts              ProjectionCounts             `json:"expected_counts"`
	ObservedCounts              ProjectionCounts             `json:"observed_counts"`
	ExpectedAmounts             ProjectionAmounts            `json:"expected_amounts"`
	ObservedAmounts             ProjectionAmounts            `json:"observed_amounts"`
	ReusedProjection            bool                         `json:"reused_projection"`
	MissingMasterFacts          MissingMasterFacts           `json:"missing_master_facts"`
	Storage                     StorageMetrics               `json:"storage"`
	RepresentativeCandidate     string                       `json:"representative_candidate_id"`
	RepresentativeSpender       string                       `json:"representative_spender_committee_id"`
	Queries                     []QueryMetric                `json:"queries"`
	Checks                      []Check                      `json:"checks"`
	CandidateResolutionCoverage *CandidateResolutionCoverage `json:"candidate_resolution_coverage,omitempty"`
}

type InputReferences struct {
	SourceReleaseID                     string `json:"source_release_id"`
	CalculationSetID                    string `json:"calculation_set_id"`
	CalculationManifestSHA256           string `json:"calculation_manifest_sha256"`
	ScheduleEFactSetID                  string `json:"schedule_e_fact_set_id"`
	ScheduleEManifestSHA256             string `json:"schedule_e_manifest_sha256"`
	CandidateFactSetID                  string `json:"candidate_fact_set_id"`
	CandidateManifestSHA256             string `json:"candidate_manifest_sha256"`
	CommitteeFactSetID                  string `json:"committee_fact_set_id"`
	CommitteeManifestSHA256             string `json:"committee_manifest_sha256"`
	CandidateResolutionCalculationSetID string `json:"candidate_resolution_calculation_set_id,omitempty"`
	CandidateResolutionManifestSHA256   string `json:"candidate_resolution_manifest_sha256,omitempty"`
	CandidateResolutionDecisionsSHA256  string `json:"candidate_resolution_decisions_sha256,omitempty"`
}

type CandidateResolutionCoverage struct {
	SourceDecisions         uint64 `json:"source_decisions"`
	ProjectableDecisions    uint64 `json:"projectable_decisions"`
	UnprojectableDecisions  uint64 `json:"unprojectable_decisions"`
	ProjectableMinorUnits   string `json:"projectable_minor_units"`
	UnprojectableMinorUnits string `json:"unprojectable_minor_units"`
}

type EdgeResolutionCounts struct {
	Confirmed  uint64 `json:"confirmed"`
	Resolved   uint64 `json:"resolved"`
	Unverified uint64 `json:"unverified"`
}

type EdgeResolutionAmounts struct {
	ConfirmedMinorUnits  string `json:"confirmed_minor_units"`
	ResolvedMinorUnits   string `json:"resolved_minor_units"`
	UnverifiedMinorUnits string `json:"unverified_minor_units"`
}

type ProjectionCounts struct {
	Entities        uint64 `json:"entities"`
	Candidates      uint64 `json:"candidates"`
	Spenders        uint64 `json:"spenders"`
	Edges           uint64 `json:"edges"`
	SupportEdges    uint64 `json:"support_edges"`
	OppositionEdges uint64 `json:"opposition_edges"`
}

type ProjectionAmounts struct {
	AttributedMinorUnits string `json:"attributed_minor_units"`
	SupportMinorUnits    string `json:"support_minor_units"`
	OppositionMinorUnits string `json:"opposition_minor_units"`
}

type MissingMasterFacts struct {
	Candidates uint64 `json:"candidates"`
	Spenders   uint64 `json:"spenders"`
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
	Office                string  `json:"office,omitempty"`
	OfficeState           string  `json:"office_state,omitempty"`
	OfficeDistrict        string  `json:"office_district,omitempty"`
	DesignationCode       string  `json:"designation_code,omitempty"`
	CommitteeTypeCode     string  `json:"committee_type_code,omitempty"`
	OrganizationTypeCode  string  `json:"organization_type_code,omitempty"`
	ConnectedOrganization string  `json:"connected_organization,omitempty"`
	DocumentDigest        string  `json:"document_digest"`
}

type expenditureEdge struct {
	Key                         string `json:"_key"`
	From                        string `json:"_from"`
	To                          string `json:"_to"`
	SchemaVersion               string `json:"schema_version"`
	RelationType                string `json:"relation_type"`
	Cycle                       string `json:"cycle"`
	SupportOppose               string `json:"support_oppose"`
	AmountMinorUnits            string `json:"amount_minor_units"`
	ExpenditureCount            uint64 `json:"expenditure_count"`
	PositiveCount               uint64 `json:"positive_count"`
	NegativeCount               uint64 `json:"negative_count"`
	ZeroCount                   uint64 `json:"zero_count"`
	MissingExpenditureTypeCount uint64 `json:"missing_expenditure_type_count"`
	ResultID                    string `json:"result_id"`
	CalculationSetID            string `json:"calculation_set_id"`
	ScheduleEFactSetID          string `json:"schedule_e_fact_set_id"`
	SourceReleaseID             string `json:"source_release_id"`
	DocumentDigest              string `json:"document_digest"`
}

type projectionMetadata struct {
	Key               string                       `json:"_key"`
	SchemaVersion     string                       `json:"schema_version"`
	ProjectionVersion string                       `json:"projection_version"`
	ProjectionID      string                       `json:"projection_id"`
	Cycle             string                       `json:"cycle"`
	Graph             string                       `json:"graph"`
	Inputs            InputReferences              `json:"inputs"`
	Counts            ProjectionCounts             `json:"counts"`
	Amounts           ProjectionAmounts            `json:"amounts"`
	Missing           MissingMasterFacts           `json:"missing_master_facts"`
	Coverage          *CandidateResolutionCoverage `json:"candidate_resolution_coverage,omitempty"`
	DocumentDigest    string                       `json:"document_digest"`
}

type resolvedExpenditureEdge struct {
	Key                                 string                `json:"_key"`
	From                                string                `json:"_from"`
	To                                  string                `json:"_to"`
	SchemaVersion                       string                `json:"schema_version"`
	RelationType                        string                `json:"relation_type"`
	Cycle                               string                `json:"cycle"`
	SupportOppose                       string                `json:"support_oppose"`
	AmountMinorUnits                    string                `json:"amount_minor_units"`
	ExpenditureCount                    uint64                `json:"expenditure_count"`
	PositiveCount                       uint64                `json:"positive_count"`
	NegativeCount                       uint64                `json:"negative_count"`
	ZeroCount                           uint64                `json:"zero_count"`
	ResolutionCounts                    EdgeResolutionCounts  `json:"resolution_counts"`
	ResolutionAmounts                   EdgeResolutionAmounts `json:"resolution_amounts"`
	ResultID                            string                `json:"result_id"`
	CalculationSetID                    string                `json:"calculation_set_id"`
	CandidateResolutionCalculationSetID string                `json:"candidate_resolution_calculation_set_id"`
	ScheduleEFactSetID                  string                `json:"schedule_e_fact_set_id"`
	SourceReleaseID                     string                `json:"source_release_id"`
	DocumentDigest                      string                `json:"document_digest"`
}

type resolvedProjection struct {
	ID                      string
	Database                string
	Cycle                   string
	Inputs                  InputReferences
	Coverage                CandidateResolutionCoverage
	Entities                []entityDocument
	Edges                   []resolvedExpenditureEdge
	Metadata                projectionMetadata
	Counts                  ProjectionCounts
	Amounts                 ProjectionAmounts
	Missing                 MissingMasterFacts
	RepresentativeCandidate string
	RepresentativeSpender   string
}

type projection struct {
	ID                      string
	Database                string
	Cycle                   string
	Inputs                  InputReferences
	Entities                []entityDocument
	Edges                   []expenditureEdge
	Metadata                projectionMetadata
	Counts                  ProjectionCounts
	Amounts                 ProjectionAmounts
	Missing                 MissingMasterFacts
	RepresentativeCandidate string
	RepresentativeSpender   string
}
