package occurrence

import "time"

const (
	ScheduleAColumnarFactSetSchemaVersion = "legal-tender.fec.schedule-a-columnar-fact-set.v1"
	ScheduleAColumnarPublisherVersion     = "legal-tender.fec.schedule-a-columnar-publisher.v1"
	ScheduleAColumnarCheckpointVersion    = "legal-tender.fec.schedule-a-columnar-checkpoint.v1"
)

// ScheduleAColumnarManifest publishes one immutable set of Parquet shards.
// The logical fact type remains fec.schedule_a_receipt.v1; this contract
// replaces only its rejected full-row JSON representation.
type ScheduleAColumnarManifest struct {
	Schema                      string                         `json:"$schema"`
	SchemaVersion               string                         `json:"schema_version"`
	FactSetID                   string                         `json:"fact_set_id"`
	FactType                    string                         `json:"fact_type"`
	Cycle                       string                         `json:"cycle"`
	SourceContract              string                         `json:"source_contract"`
	SourceReleaseID             string                         `json:"source_release_id"`
	SourceReleaseManifestSHA256 string                         `json:"source_release_manifest_sha256"`
	OccurrenceSetID             string                         `json:"occurrence_set_id"`
	OccurrenceManifestSHA256    string                         `json:"occurrence_manifest_sha256"`
	RunID                       string                         `json:"run_id"`
	State                       string                         `json:"state"`
	NormalizerVersion           string                         `json:"normalizer_version"`
	FactSchemaVersion           string                         `json:"fact_schema_version"`
	PhysicalSchemaVersion       string                         `json:"physical_schema_version"`
	PublisherVersion            string                         `json:"publisher_version"`
	ParquetLibrary              string                         `json:"parquet_library"`
	PublishedAt                 time.Time                      `json:"published_at"`
	Configuration               ScheduleAColumnarConfiguration `json:"configuration"`
	Counts                      ScheduleAFactCounts            `json:"counts"`
	SourceReplay                ScheduleAColumnarSourceReplay  `json:"source_replay"`
	Shards                      []ScheduleAColumnarShard       `json:"shards"`
	Checks                      []Check                        `json:"checks"`
}

type ScheduleAColumnarConfiguration struct {
	RowsPerShard    uint64 `json:"rows_per_shard"`
	RowsPerRowGroup uint64 `json:"rows_per_row_group"`
	ColumnCount     int    `json:"column_count"`
	Compression     string `json:"compression"`
	Locator         string `json:"locator"`
}

type ScheduleAColumnarSourceReplay struct {
	Rows               uint64 `json:"rows"`
	CompressedBytes    uint64 `json:"compressed_bytes"`
	CompressedSHA256   string `json:"compressed_sha256"`
	UncompressedBytes  uint64 `json:"uncompressed_bytes"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
	SemanticSHA256     string `json:"fact_semantic_sha256"`
}

// ScheduleAColumnarShard covers one deterministic source-row ordinal range.
// Facts may be fewer than source rows when the occurrence evidence excludes
// malformed or duplicate publisher keys.
type ScheduleAColumnarShard struct {
	Index                 uint64 `json:"index"`
	FirstSourceRowOrdinal uint64 `json:"first_source_row_ordinal"`
	LastSourceRowOrdinal  uint64 `json:"last_source_row_ordinal"`
	FirstRawByteOffset    uint64 `json:"first_raw_byte_offset"`
	LastRawByteEnd        uint64 `json:"last_raw_byte_end"`
	SourceRows            uint64 `json:"source_rows"`
	Facts                 uint64 `json:"facts"`
	ValidFacts            uint64 `json:"valid_facts"`
	InvalidFacts          uint64 `json:"invalid_facts"`
	RowGroups             uint64 `json:"row_groups"`
	Bytes                 uint64 `json:"bytes"`
	SHA256                string `json:"sha256"`
	SemanticSHA256        string `json:"semantic_sha256"`
	StorageKey            string `json:"storage_key"`
}

type scheduleAColumnarCheckpoint struct {
	SchemaVersion         string                         `json:"schema_version"`
	FactSetID             string                         `json:"fact_set_id"`
	Cycle                 string                         `json:"cycle"`
	OccurrenceSetID       string                         `json:"occurrence_set_id"`
	PhysicalSchemaVersion string                         `json:"physical_schema_version"`
	PublisherVersion      string                         `json:"publisher_version"`
	RunID                 string                         `json:"run_id"`
	Configuration         ScheduleAColumnarConfiguration `json:"configuration"`
	UpdatedAt             time.Time                      `json:"updated_at"`
	Shards                []ScheduleAColumnarShard       `json:"shards"`
}
