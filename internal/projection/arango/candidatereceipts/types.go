// Package candidatereceipts projects one verified candidate-receipt
// calculation into an isolated ArangoDB graph for bounded model validation.
package candidatereceipts

import (
	"time"
)

const (
	ProjectionVersion    = "legal-tender.arango.candidate-receipts-projection.v1"
	ResultSchemaVersion  = "legal-tender.arango.candidate-receipts-probe-result.v1"
	EntitySchemaVersion  = "legal-tender.arango.entity.v1"
	EdgeSchemaVersion    = "legal-tender.arango.candidate-receipt-edge.v1"
	ResultDocumentSchema = "legal-tender.arango.candidate-receipt-result.v1"
	MetadataSchema       = "legal-tender.arango.projection-metadata.v1"
	GraphName            = "candidate_receipts"
)

const (
	entitiesCollection      = "entities"
	resultsCollection       = "candidate_results"
	relationshipsCollection = "candidate_committee_relationships"
	receiptsCollection      = "receipt_components"
	metadataCollection      = "projection_metadata"
)

type Input struct {
	StorageRoot             string
	Cycle                   string
	FactBundleManifestPath  string
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

type Result struct {
	SchemaVersion           string             `json:"schema_version"`
	ProjectionVersion       string             `json:"projection_version"`
	ProjectionID            string             `json:"projection_id"`
	State                   string             `json:"state"`
	Cycle                   string             `json:"cycle"`
	Database                string             `json:"database"`
	Graph                   string             `json:"graph"`
	RunID                   string             `json:"run_id"`
	ObservedAt              time.Time          `json:"observed_at"`
	Inputs                  InputReferences    `json:"inputs"`
	ExpectedCounts          ProjectionCounts   `json:"expected_counts"`
	ObservedCounts          ProjectionCounts   `json:"observed_counts"`
	ReusedProjection        bool               `json:"reused_projection"`
	MissingMasterFacts      MissingMasterFacts `json:"missing_master_facts"`
	Storage                 StorageMetrics     `json:"storage"`
	RepresentativeCandidate string             `json:"representative_candidate_id"`
	Queries                 []QueryMetric      `json:"queries"`
	Checks                  []Check            `json:"checks"`
}

type InputReferences struct {
	SourceReleaseID           string `json:"source_release_id"`
	FactBundleID              string `json:"fact_bundle_id"`
	FactBundleManifestSHA256  string `json:"fact_bundle_manifest_sha256"`
	CalculationSetID          string `json:"calculation_set_id"`
	CalculationManifestSHA256 string `json:"calculation_manifest_sha256"`
	CandidateFactSetID        string `json:"candidate_fact_set_id"`
	CandidateManifestSHA256   string `json:"candidate_manifest_sha256"`
	CommitteeFactSetID        string `json:"committee_fact_set_id"`
	CommitteeManifestSHA256   string `json:"committee_manifest_sha256"`
}

type ProjectionCounts struct {
	Entities                        uint64 `json:"entities"`
	Candidates                      uint64 `json:"candidates"`
	Committees                      uint64 `json:"committees"`
	CandidateResults                uint64 `json:"candidate_results"`
	CandidateCommitteeRelationships uint64 `json:"candidate_committee_relationships"`
	ReceiptComponents               uint64 `json:"receipt_components"`
}

type MissingMasterFacts struct {
	Candidates uint64 `json:"candidates"`
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
	Office                string  `json:"office,omitempty"`
	OfficeState           string  `json:"office_state,omitempty"`
	OfficeDistrict        string  `json:"office_district,omitempty"`
	DesignationCode       string  `json:"designation_code,omitempty"`
	CommitteeTypeCode     string  `json:"committee_type_code,omitempty"`
	OrganizationTypeCode  string  `json:"organization_type_code,omitempty"`
	ConnectedOrganization string  `json:"connected_organization,omitempty"`
	DocumentDigest        string  `json:"document_digest"`
}

type relationshipEdge struct {
	Key               string   `json:"_key"`
	From              string   `json:"_from"`
	To                string   `json:"_to"`
	SchemaVersion     string   `json:"schema_version"`
	RelationType      string   `json:"relation_type"`
	Cycle             string   `json:"cycle"`
	RelationshipState string   `json:"relationship_state"`
	DesignationCodes  []string `json:"designation_codes"`
	SupportingFactIDs []string `json:"supporting_fact_ids"`
	CalculationSetID  string   `json:"calculation_set_id"`
	DocumentDigest    string   `json:"document_digest"`
}

type receiptComponentEdge struct {
	Key                string `json:"_key"`
	From               string `json:"_from"`
	To                 string `json:"_to"`
	SchemaVersion      string `json:"schema_version"`
	RelationType       string `json:"relation_type"`
	Cycle              string `json:"cycle"`
	AmountMinorUnits   string `json:"amount_minor_units"`
	IncludedRecords    uint64 `json:"included_records"`
	PositiveRecords    uint64 `json:"positive_records"`
	NegativeRecords    uint64 `json:"negative_records"`
	ZeroRecords        uint64 `json:"zero_records"`
	CalculationSetID   string `json:"calculation_set_id"`
	CandidateResultKey string `json:"candidate_result_key"`
	DocumentDigest     string `json:"document_digest"`
}

type resultDocument struct {
	Key              string `json:"_key"`
	SchemaVersion    string `json:"schema_version"`
	Cycle            string `json:"cycle"`
	CandidateID      string `json:"candidate_id"`
	CalculationSetID string `json:"calculation_set_id"`
	SourceReleaseID  string `json:"source_release_id"`
	Result           any    `json:"result"`
	DocumentDigest   string `json:"document_digest"`
}

type projectionMetadata struct {
	Key               string           `json:"_key"`
	SchemaVersion     string           `json:"schema_version"`
	ProjectionVersion string           `json:"projection_version"`
	ProjectionID      string           `json:"projection_id"`
	Cycle             string           `json:"cycle"`
	Graph             string           `json:"graph"`
	Inputs            InputReferences  `json:"inputs"`
	Counts            ProjectionCounts `json:"counts"`
	DocumentDigest    string           `json:"document_digest"`
}

type projection struct {
	ID                      string
	Database                string
	Cycle                   string
	Inputs                  InputReferences
	Entities                []entityDocument
	Results                 []resultDocument
	Relationships           []relationshipEdge
	ReceiptComponents       []receiptComponentEdge
	Metadata                projectionMetadata
	Counts                  ProjectionCounts
	Missing                 MissingMasterFacts
	RepresentativeCandidate string
}
