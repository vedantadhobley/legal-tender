// Package occurrence publishes immutable processed Schedule A row evidence.
package occurrence

import "time"

const (
	ManifestSchemaVersion = "legal-tender.fec.schedule-a-occurrence-set.v1"
	ParserVersion         = "legal-tender.fec.schedule-a-occurrence-parser.v2"
	SemanticSchemaVersion = "legal-tender.fec.schedule-a-semantic-digest.v1"
)

type Manifest struct {
	Schema                      string       `json:"$schema"`
	SchemaVersion               string       `json:"schema_version"`
	OccurrenceSetID             string       `json:"occurrence_set_id"`
	PriorOccurrenceSetID        string       `json:"prior_occurrence_set_id,omitempty"`
	SourceReleaseID             string       `json:"source_release_id"`
	SourceReleaseManifestSHA256 string       `json:"source_release_manifest_sha256"`
	SourceArtifactSHA256        string       `json:"source_artifact_sha256"`
	StagedOutputSHA256          string       `json:"staged_output_sha256"`
	Relation                    string       `json:"relation"`
	Cycle                       string       `json:"cycle"`
	RunID                       string       `json:"run_id"`
	State                       string       `json:"state"`
	ParserVersion               string       `json:"parser_version"`
	SemanticSchemaVersion       string       `json:"semantic_schema_version"`
	PublishedAt                 time.Time    `json:"published_at"`
	Counts                      Counts       `json:"counts"`
	Changes                     ChangeCounts `json:"changes"`
	Artifacts                   ArtifactSet  `json:"artifacts"`
	Checks                      []Check      `json:"checks"`
}

type Counts struct {
	Total                uint64 `json:"total"`
	Valid                uint64 `json:"valid"`
	Invalid              uint64 `json:"invalid"`
	Keyed                uint64 `json:"keyed"`
	UniqueKeys           uint64 `json:"unique_keys"`
	InvalidKeys          uint64 `json:"invalid_keys"`
	DuplicateKeys        uint64 `json:"duplicate_keys"`
	DuplicateOccurrences uint64 `json:"duplicate_occurrences"`
}

type ChangeCounts struct {
	Added     uint64 `json:"added"`
	Changed   uint64 `json:"changed"`
	Absent    uint64 `json:"absent"`
	Unchanged uint64 `json:"unchanged"`
	Invalid   uint64 `json:"invalid"`
}

type ArtifactSet struct {
	Occurrences  Artifact `json:"occurrences"`
	Issues       Artifact `json:"issues"`
	NaturalIndex Artifact `json:"natural_index"`
	Changes      Artifact `json:"changes"`
}

type Artifact struct {
	RecordCount        uint64 `json:"record_count"`
	UncompressedBytes  uint64 `json:"uncompressed_byte_count"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
	CompressedBytes    uint64 `json:"compressed_byte_count"`
	CompressedSHA256   string `json:"compressed_sha256"`
	Compression        string `json:"compression"`
	StorageKey         string `json:"storage_key"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type Occurrence struct {
	OccurrenceID             string   `json:"occurrence_id"`
	RowOrdinal               uint64   `json:"row_ordinal"`
	RawByteOffset            uint64   `json:"raw_byte_offset"`
	RawByteLength            uint64   `json:"raw_byte_length"`
	RawContentSHA256         string   `json:"raw_content_sha256"`
	NaturalKey               *string  `json:"natural_key"`
	PublisherRecordReference string   `json:"publisher_record_reference"`
	RecordVersionID          string   `json:"record_version_id"`
	SemanticDigest           *string  `json:"semantic_digest"`
	State                    string   `json:"state"`
	IssueCodes               []string `json:"issue_codes"`
}

type Issue struct {
	IssueID                string  `json:"issue_id"`
	OccurrenceID           string  `json:"occurrence_id"`
	RowOrdinal             uint64  `json:"row_ordinal"`
	NaturalKey             *string `json:"natural_key,omitempty"`
	Code                   string  `json:"code"`
	Severity               string  `json:"severity"`
	Message                string  `json:"message"`
	RelatedOccurrenceCount uint64  `json:"related_occurrence_count,omitempty"`
}

type NaturalIndexEntry struct {
	NaturalKey      string `json:"natural_key"`
	State           string `json:"state"`
	OccurrenceCount uint64 `json:"occurrence_count"`
	StateDigest     string `json:"state_digest"`
	RowOrdinal      uint64 `json:"row_ordinal,omitempty"`
	OccurrenceID    string `json:"occurrence_id,omitempty"`
	RecordVersionID string `json:"record_version_id,omitempty"`
	SemanticDigest  string `json:"semantic_digest,omitempty"`
}

type Change struct {
	NaturalKey             string `json:"natural_key"`
	Change                 string `json:"change"`
	PriorState             string `json:"prior_state,omitempty"`
	CurrentState           string `json:"current_state"`
	PriorOccurrenceID      string `json:"prior_occurrence_id,omitempty"`
	CurrentOccurrenceID    string `json:"current_occurrence_id,omitempty"`
	PriorRecordVersionID   string `json:"prior_record_version_id,omitempty"`
	CurrentRecordVersionID string `json:"current_record_version_id,omitempty"`
	PriorSemanticDigest    string `json:"prior_semantic_digest,omitempty"`
	CurrentSemanticDigest  string `json:"current_semantic_digest,omitempty"`
}

type Options struct {
	StorageRoot             string
	CurrentManifestPath     string
	Clock                   func() time.Time
	ShardCount              int
	RowsPerColumnarShard    uint64
	RowsPerColumnarRowGroup uint64
	FreeFloorBytes          uint64
	WorkingMarginBytes      uint64
	DiskAvailable           func(string) (uint64, error)
	Progress                func(string)
}
