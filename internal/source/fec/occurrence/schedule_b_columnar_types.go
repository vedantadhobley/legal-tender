package occurrence

import "time"

const (
	ScheduleBColumnarFactSetSchemaVersion = "legal-tender.fec.schedule-b-columnar-fact-set.v1"
	ScheduleBColumnarPublisherVersion     = "legal-tender.fec.schedule-b-columnar-publisher.v1"
	ScheduleBColumnarCheckpointVersion    = "legal-tender.fec.schedule-b-columnar-checkpoint.v1"
	ScheduleBFactType                     = "fec.schedule_b_disbursement.v1"
	ScheduleBSourceContract               = "fec/schedule-b@1.0.0"
)

// ScheduleBColumnarManifest publishes one immutable, cycle-scoped set of
// Parquet facts directly from the coordinated release's Schedule B archive.
type ScheduleBColumnarManifest struct {
	Schema                      string                         `json:"$schema"`
	SchemaVersion               string                         `json:"schema_version"`
	FactSetID                   string                         `json:"fact_set_id"`
	FactType                    string                         `json:"fact_type"`
	Cycle                       string                         `json:"cycle"`
	Relation                    string                         `json:"relation"`
	SourceContract              string                         `json:"source_contract"`
	SourceReleaseID             string                         `json:"source_release_id"`
	SourceReleaseManifestSHA256 string                         `json:"source_release_manifest_sha256"`
	SourceArtifactSHA256        string                         `json:"source_artifact_sha256"`
	SourceArtifactByteCount     int64                          `json:"source_artifact_byte_count"`
	SourceArtifactStorageKey    string                         `json:"source_artifact_storage_key"`
	RunID                       string                         `json:"run_id"`
	State                       string                         `json:"state"`
	PhysicalSchemaVersion       string                         `json:"physical_schema_version"`
	PublisherVersion            string                         `json:"publisher_version"`
	ParquetLibrary              string                         `json:"parquet_library"`
	PublishedAt                 time.Time                      `json:"published_at"`
	Configuration               ScheduleBColumnarConfiguration `json:"configuration"`
	Counts                      ScheduleBColumnarCounts        `json:"counts"`
	SourceReplay                ScheduleBColumnarSourceReplay  `json:"source_replay"`
	Shards                      []ScheduleBColumnarShard       `json:"shards"`
	Checks                      []Check                        `json:"checks"`
}

type ScheduleBColumnarConfiguration struct {
	RowsPerShard    uint64 `json:"rows_per_shard"`
	RowsPerRowGroup uint64 `json:"rows_per_row_group"`
	ColumnCount     int    `json:"column_count"`
	Compression     string `json:"compression"`
	Locator         string `json:"locator"`
}

type ScheduleBColumnarCounts struct {
	SourceRows        uint64 `json:"source_rows"`
	Facts             uint64 `json:"facts"`
	ValidFacts        uint64 `json:"valid_facts"`
	InvalidSourceRows uint64 `json:"invalid_source_rows"`
	UniqueSubIDs      uint64 `json:"unique_sub_ids"`
	DuplicateSubIDs   uint64 `json:"duplicate_sub_ids"`
}

type ScheduleBColumnarSourceReplay struct {
	ArchiveBytes   int64  `json:"archive_bytes"`
	ArchiveSHA256  string `json:"archive_sha256"`
	COPYRows       uint64 `json:"copy_rows"`
	COPYBytes      uint64 `json:"copy_bytes"`
	COPYSHA256     string `json:"copy_sha256"`
	SemanticSHA256 string `json:"fact_semantic_sha256"`
}

type ScheduleBColumnarShard struct {
	Index                 uint64 `json:"index"`
	FirstSourceRowOrdinal uint64 `json:"first_source_row_ordinal"`
	LastSourceRowOrdinal  uint64 `json:"last_source_row_ordinal"`
	FirstRawByteOffset    uint64 `json:"first_raw_byte_offset"`
	LastRawByteEnd        uint64 `json:"last_raw_byte_end"`
	SourceRows            uint64 `json:"source_rows"`
	Facts                 uint64 `json:"facts"`
	RowGroups             uint64 `json:"row_groups"`
	Bytes                 uint64 `json:"bytes"`
	SHA256                string `json:"sha256"`
	SemanticSHA256        string `json:"semantic_sha256"`
	StorageKey            string `json:"storage_key"`
}

type scheduleBColumnarCheckpoint struct {
	SchemaVersion         string                         `json:"schema_version"`
	FactSetID             string                         `json:"fact_set_id"`
	Cycle                 string                         `json:"cycle"`
	SourceArtifactSHA256  string                         `json:"source_artifact_sha256"`
	PhysicalSchemaVersion string                         `json:"physical_schema_version"`
	PublisherVersion      string                         `json:"publisher_version"`
	RunID                 string                         `json:"run_id"`
	Configuration         ScheduleBColumnarConfiguration `json:"configuration"`
	UpdatedAt             time.Time                      `json:"updated_at"`
	Shards                []ScheduleBColumnarShard       `json:"shards"`
}

type ScheduleBColumnarOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	PGRestorePath       string
	WorkDir             string
	Clock               func() time.Time
	RowsPerShard        uint64
	RowsPerRowGroup     uint64
	FreeFloorBytes      uint64
	WorkingMarginBytes  uint64
	DiskAvailable       func(string) (uint64, error)
	Progress            func(string)
}
