package occurrence

import "time"

const (
	ScheduleACompactManifestSchemaVersion = "legal-tender.fec.schedule-a-compact-occurrence-set.v1"
	ScheduleACompactPublisherVersion      = "legal-tender.fec.schedule-a-compact-occurrence-publisher.v1"
	ScheduleACompactIndexSchemaVersion    = "legal-tender.fec.schedule-a-compact-key-index.v1"
	ScheduleACompactIndexHash             = "fnv1a32-sub-id-decimal-mod-v1"
	ScheduleACompactOccurrenceEncoding    = "dense-source-row-ordinal-v1"
	ScheduleACompactIndexRecordBytes      = 48
	ScheduleACompactStageRecordBytes      = 65
)

// ScheduleACompactManifest preserves the logical Schedule A occurrence and
// natural-key change boundary without materializing one JSON object per row.
type ScheduleACompactManifest struct {
	Schema                      string                        `json:"$schema"`
	SchemaVersion               string                        `json:"schema_version"`
	OccurrenceSetID             string                        `json:"occurrence_set_id"`
	PriorOccurrenceSetID        string                        `json:"prior_occurrence_set_id,omitempty"`
	SourceReleaseID             string                        `json:"source_release_id"`
	SourceReleaseManifestSHA256 string                        `json:"source_release_manifest_sha256"`
	SourceArtifactSHA256        string                        `json:"source_artifact_sha256"`
	StagedOutputSHA256          string                        `json:"staged_output_sha256"`
	Relation                    string                        `json:"relation"`
	Cycle                       string                        `json:"cycle"`
	RunID                       string                        `json:"run_id"`
	State                       string                        `json:"state"`
	ParserVersion               string                        `json:"parser_version"`
	SemanticSchemaVersion       string                        `json:"semantic_schema_version"`
	PublisherVersion            string                        `json:"publisher_version"`
	IndexSchemaVersion          string                        `json:"index_schema_version"`
	OccurrenceEncoding          string                        `json:"occurrence_encoding"`
	ChangeMode                  string                        `json:"change_mode"`
	PublishedAt                 time.Time                     `json:"published_at"`
	Configuration               ScheduleACompactConfiguration `json:"configuration"`
	Counts                      Counts                        `json:"counts"`
	Changes                     ChangeCounts                  `json:"changes"`
	SourceReplay                ScheduleACompactSourceReplay  `json:"source_replay"`
	IndexPartitions             []ScheduleACompactPartition   `json:"index_partitions"`
	RowExceptions               Artifact                      `json:"row_exceptions"`
	Deltas                      Artifact                      `json:"deltas"`
	Checks                      []Check                       `json:"checks"`
}

type ScheduleACompactConfiguration struct {
	Partitions       int    `json:"partitions"`
	PartitionHash    string `json:"partition_hash"`
	IndexRecordBytes int    `json:"index_record_bytes"`
	IndexEncoding    string `json:"index_encoding"`
}

type ScheduleACompactSourceReplay struct {
	Rows               uint64 `json:"rows"`
	CompressedBytes    uint64 `json:"compressed_bytes"`
	CompressedSHA256   string `json:"compressed_sha256"`
	UncompressedBytes  uint64 `json:"uncompressed_bytes"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
}

type ScheduleACompactPartition struct {
	Partition     int                      `json:"partition"`
	FirstSubID    string                   `json:"first_sub_id,omitempty"`
	LastSubID     string                   `json:"last_sub_id,omitempty"`
	Index         ScheduleACompactArtifact `json:"index"`
	KeyExceptions Artifact                 `json:"key_exceptions"`
}

type ScheduleACompactArtifact struct {
	RecordCount        uint64 `json:"record_count"`
	RecordBytes        int    `json:"record_bytes"`
	UncompressedBytes  uint64 `json:"uncompressed_byte_count"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
	CompressedBytes    uint64 `json:"compressed_byte_count"`
	CompressedSHA256   string `json:"compressed_sha256"`
	Compression        string `json:"compression"`
	Encoding           string `json:"encoding"`
	StorageKey         string `json:"storage_key"`
}

type ScheduleACompactRowException struct {
	ExceptionID            string `json:"exception_id"`
	OccurrenceID           string `json:"occurrence_id"`
	RowOrdinal             uint64 `json:"row_ordinal"`
	RawByteOffset          uint64 `json:"raw_byte_offset"`
	RawByteLength          uint64 `json:"raw_byte_length"`
	RawContentSHA256       string `json:"raw_content_sha256,omitempty"`
	SubID                  string `json:"sub_id,omitempty"`
	Code                   string `json:"code"`
	Severity               string `json:"severity"`
	Message                string `json:"message"`
	RelatedOccurrenceCount uint64 `json:"related_occurrence_count,omitempty"`
}

type ScheduleACompactKeyException struct {
	Partition        int    `json:"partition"`
	SubID            string `json:"sub_id"`
	State            string `json:"state"`
	OccurrenceCount  uint64 `json:"occurrence_count"`
	RowOrdinal       uint64 `json:"row_ordinal,omitempty"`
	ComparisonDigest string `json:"comparison_digest"`
}

type ScheduleACompactDelta struct {
	SubID                   string `json:"sub_id"`
	Change                  string `json:"change"`
	PriorState              string `json:"prior_state"`
	CurrentState            string `json:"current_state"`
	PriorRowOrdinal         uint64 `json:"prior_row_ordinal,omitempty"`
	CurrentRowOrdinal       uint64 `json:"current_row_ordinal,omitempty"`
	PriorComparisonDigest   string `json:"prior_comparison_digest,omitempty"`
	CurrentComparisonDigest string `json:"current_comparison_digest,omitempty"`
}

type compactStageEntry struct {
	SubID         uint64
	RowOrdinal    uint64
	RawByteOffset uint64
	RawByteLength uint64
	Valid         bool
	Semantic      [32]byte
}

type compactKeyState struct {
	SubID            uint64
	State            string
	OccurrenceCount  uint64
	RowOrdinal       uint64
	ComparisonDigest [32]byte
}
