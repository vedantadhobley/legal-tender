package release

import (
	"context"
	"io"
	"time"
)

const (
	StageSchemaVersion = "legal-tender.fec.staged-release.v1"

	StageStaged  = "staged"
	StageBlocked = "blocked"
	StageFailed  = "failed"
)

type StageResult struct {
	SchemaVersion      string         `json:"schema_version"`
	InventoryVersion   string         `json:"inventory_version"`
	CandidateReleaseID string         `json:"candidate_release_id"`
	PriorReleaseID     string         `json:"prior_release_id,omitempty"`
	PlanSHA256         string         `json:"plan_sha256"`
	AcquisitionSHA256  string         `json:"acquisition_sha256"`
	RunID              string         `json:"run_id"`
	Status             string         `json:"status"`
	StartedAt          time.Time      `json:"started_at"`
	CompletedAt        time.Time      `json:"completed_at"`
	Storage            StageStorage   `json:"storage"`
	Outputs            []StagedOutput `json:"outputs"`
	Checks             []ReleaseCheck `json:"checks"`
	Issues             []Issue        `json:"issues"`
}

type StageStorage struct {
	ScheduleAHotBytesBefore    uint64 `json:"schedule_a_hot_bytes_before"`
	ScheduleAHotBytesAfter     uint64 `json:"schedule_a_hot_bytes_after"`
	ScheduleAHotCapBytes       uint64 `json:"schedule_a_hot_cap_bytes"`
	FreeBytesBefore            uint64 `json:"free_bytes_before"`
	FreeBytesAfter             uint64 `json:"free_bytes_after"`
	FreeFloorBytes             uint64 `json:"free_floor_bytes"`
	WorkingMarginBytes         uint64 `json:"working_margin_bytes"`
	LargestExtractWorkingBytes uint64 `json:"largest_extract_working_bytes"`
	Passed                     bool   `json:"passed"`
}

type StagedOutput struct {
	SourceID               string    `json:"source_id"`
	Disposition            string    `json:"disposition"`
	SelectionKind          string    `json:"selection_kind"`
	Selection              string    `json:"selection"`
	Period                 string    `json:"period"`
	Representation         string    `json:"representation"`
	SourceArtifactSHA256   string    `json:"source_artifact_sha256"`
	RowCount               *uint64   `json:"row_count,omitempty"`
	ContractedFieldCount   *int      `json:"contracted_field_count,omitempty"`
	UncompressedByteCount  uint64    `json:"uncompressed_byte_count"`
	UncompressedSHA256     string    `json:"uncompressed_sha256"`
	Compression            string    `json:"compression"`
	CompressionLevel       int       `json:"compression_level"`
	CompressedByteCount    uint64    `json:"compressed_byte_count"`
	CompressedSHA256       string    `json:"compressed_sha256"`
	StorageKey             string    `json:"storage_key"`
	DecompressionValidated bool      `json:"decompression_validated"`
	StagedAt               time.Time `json:"staged_at"`
}

type ReleaseCheck struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type SelectedMemberExtractor func(context.Context, string, string, io.Writer) error

type SelectedRelationExtractor func(context.Context, string, string, string, io.Writer) (uint64, error)

// StageOptions exposes process and storage boundaries for tests. Production
// callers use the bounded-memory ZIP and pg_restore implementations.
type StageOptions struct {
	StorageRoot             string
	Clock                   func() time.Time
	DiskUsage               func(string) (DiskSpace, error)
	PGRestorePath           string
	MemberExtractor         SelectedMemberExtractor
	RelationExtractor       SelectedRelationExtractor
	ScheduleAHotCapBytes    uint64
	FreeFloorBytes          uint64
	WorkingMarginBytes      uint64
	LargestExtractWorkBytes uint64
	Progress                func(string)
}
