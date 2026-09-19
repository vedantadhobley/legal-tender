package release

import (
	"context"
	"time"
)

const (
	AcquisitionSchemaVersion = "legal-tender.fec.acquisition.v1"

	AcquisitionAcquired = "acquired"
	AcquisitionBlocked  = "blocked"
	AcquisitionFailed   = "failed"

	ScheduleASourceID = "fec:schedule-a:processed"
	ScheduleBSourceID = "fec:schedule-b:processed"
	ScheduleESourceID = "fec:schedule-e:processed"

	ScheduleAHotCapBytes          uint64 = 600 << 30
	AcquisitionFreeFloorBytes     uint64 = 500 << 30
	AcquisitionWorkingMarginBytes uint64 = 25 << 30
	// Historical reserve used by accepted pre-streaming-budget observations.
	// New acquisitions/stages record zero uncompressed working bytes.
	LargestScheduleAExtractBytes uint64 = 206_363_392_958
)

// AcquisitionResult is the durable result of acquiring one exact release
// plan. It is not a published release manifest.
type AcquisitionResult struct {
	SchemaVersion      string                `json:"schema_version"`
	InventoryVersion   string                `json:"inventory_version"`
	CandidateReleaseID string                `json:"candidate_release_id"`
	PriorReleaseID     string                `json:"prior_release_id,omitempty"`
	PlanSHA256         string                `json:"plan_sha256"`
	RunID              string                `json:"run_id"`
	Status             string                `json:"status"`
	StartedAt          time.Time             `json:"started_at"`
	CompletedAt        time.Time             `json:"completed_at"`
	Storage            StoragePreflight      `json:"storage"`
	Artifacts          []AcquisitionArtifact `json:"artifacts"`
	PostCapture        PostCaptureCheck      `json:"post_capture"`
	Issues             []Issue               `json:"issues"`
}

// StoragePreflight records the exact disk-budget decision made before any
// publisher body request.
type StoragePreflight struct {
	ScheduleAHotBytesBefore    uint64 `json:"schedule_a_hot_bytes_before"`
	CandidateDownloadBytes     uint64 `json:"candidate_download_bytes"`
	RemainingDownloadBytes     uint64 `json:"remaining_download_bytes"`
	ProjectedScheduleAHotBytes uint64 `json:"projected_schedule_a_hot_bytes"`
	ScheduleAHotCapBytes       uint64 `json:"schedule_a_hot_cap_bytes"`
	FreeBytesBefore            uint64 `json:"free_bytes_before"`
	FreeFloorBytes             uint64 `json:"free_floor_bytes"`
	WorkingMarginBytes         uint64 `json:"working_margin_bytes"`
	LargestExtractWorkingBytes uint64 `json:"largest_extract_working_bytes"`
	Passed                     bool   `json:"passed"`
}

// AcquisitionArtifact is either a newly captured immutable object or an
// unchanged object inherited from the prior published manifest.
type AcquisitionArtifact struct {
	SourceID        string    `json:"source_id"`
	Disposition     string    `json:"disposition"`
	VersionIdentity string    `json:"version_identity"`
	ByteCount       int64     `json:"byte_count"`
	SHA256          string    `json:"sha256"`
	StorageKey      string    `json:"storage_key"`
	AcquiredAt      time.Time `json:"acquired_at"`
	ContainerCheck  string    `json:"container_check"`
}

type PostCaptureCheck struct {
	StartedAt   *time.Time           `json:"started_at,omitempty"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
	Versions    []PostCaptureVersion `json:"versions"`
}

type PostCaptureVersion struct {
	SourceID        string    `json:"source_id"`
	ObservedAt      time.Time `json:"observed_at"`
	Status          string    `json:"status"`
	VersionIdentity string    `json:"version_identity,omitempty"`
	ProblemCode     string    `json:"problem_code,omitempty"`
	Problem         string    `json:"problem,omitempty"`
}

type DiskSpace struct {
	AvailableBytes uint64
}

type ContainerValidator func(context.Context, SourceSpec, string, string) (string, error)

// AcquisitionOptions provides process boundaries that tests can replace
// without weakening the production defaults.
type AcquisitionOptions struct {
	StorageRoot        string
	MaxConcurrent      int
	Clock              func() time.Time
	DiskUsage          func(string) (DiskSpace, error)
	ContainerValidator ContainerValidator
	PGRestorePath      string
	Progress           func(string)
}
