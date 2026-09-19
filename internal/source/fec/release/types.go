// Package release discovers and plans coordinated FEC bulk releases without
// downloading publisher bodies.
package release

import "time"

const (
	InventorySchemaVersionV1         = "legal-tender.fec.release-inventory.v1"
	InventorySchemaVersionV2         = "legal-tender.fec.release-inventory.v2"
	InventorySchemaVersionV3         = "legal-tender.fec.release-inventory.v3"
	InventorySchemaVersionV4         = "legal-tender.fec.release-inventory.v4"
	InitialInventoryVersion          = "legal-tender.fec.initial-release-inventory.v1"
	ScheduleEInventoryVersion        = "legal-tender.fec.initial-release-inventory.v2"
	ActiveInventoryVersion           = "legal-tender.fec.initial-release-inventory.v3"
	CommitteeSummaryInventoryVersion = "legal-tender.fec.initial-release-inventory.v4"
	DiscoverySchemaVersion           = "legal-tender.fec.discovery.v1"
	PlanSchemaVersion                = "legal-tender.fec.release-plan.v1"
	ManifestSchemaVersion            = "legal-tender.fec.release.v1"
)

// Inventory is the exact source membership of one release-contract version.
type Inventory struct {
	Schema           string       `json:"$schema,omitempty"`
	SchemaVersion    string       `json:"schema_version"`
	InventoryVersion string       `json:"inventory_version"`
	Periods          []string     `json:"periods"`
	Sources          []SourceSpec `json:"sources"`
}

// SourceSpec identifies one required publisher artifact and the source
// partitions selected from it after acquisition.
type SourceSpec struct {
	SourceID           string              `json:"source_id"`
	FactFamily         string              `json:"fact_family"`
	SourceContract     string              `json:"source_contract"`
	RequestURL         string              `json:"request_url"`
	Periods            []string            `json:"periods"`
	SelectedMembers    []string            `json:"selected_members"`
	SelectedRelations  []string            `json:"selected_relations"`
	RelationSelections []RelationSelection `json:"relation_selections,omitempty"`
	ArtifactFormat     string              `json:"artifact_format,omitempty"`
}

// RelationSelection describes one exact relation extracted from a PostgreSQL
// custom dump. Scope is either one two-year cycle or all_history; the source's
// Periods still declare which downstream cycle partitions consume the data.
type RelationSelection struct {
	Name            string `json:"name"`
	Scope           string `json:"scope"`
	FieldCount      int    `json:"field_count"`
	Materialization string `json:"materialization,omitempty"`
}

// Discovery is one metadata-only observation of the complete inventory.
type Discovery struct {
	SchemaVersion    string        `json:"schema_version"`
	InventoryVersion string        `json:"inventory_version"`
	StartedAt        time.Time     `json:"started_at"`
	CompletedAt      time.Time     `json:"completed_at"`
	Observations     []Observation `json:"observations"`
}

// Observation records HTTP metadata for one mutable publisher URL. It never
// contains source-body bytes.
type Observation struct {
	SourceID        string    `json:"source_id"`
	RequestMethod   string    `json:"request_method"`
	RequestURL      string    `json:"request_url"`
	FinalURL        string    `json:"final_url,omitempty"`
	ObservedAt      time.Time `json:"observed_at"`
	Status          string    `json:"status"`
	HTTPStatus      int       `json:"http_status,omitempty"`
	VersionIdentity string    `json:"version_identity,omitempty"`
	VersionBasis    string    `json:"version_basis,omitempty"`
	VersionID       string    `json:"version_id,omitempty"`
	ETag            string    `json:"etag,omitempty"`
	LastModified    string    `json:"last_modified,omitempty"`
	ContentLength   *int64    `json:"content_length,omitempty"`
	Digest          string    `json:"digest,omitempty"`
	AcceptRanges    string    `json:"accept_ranges,omitempty"`
	ProblemCode     string    `json:"problem_code,omitempty"`
	Problem         string    `json:"problem,omitempty"`
}

const (
	ObservationAvailable   = "available"
	ObservationUnavailable = "unavailable"

	PlanUpdateAvailable = "update_available"
	PlanNoChange        = "no_change"
	PlanSourceNotReady  = "source_not_ready"
	PlanInvalid         = "invalid"
)

// ReleasePlan is a pure decision over one saved discovery and an optional
// prior published manifest.
type ReleasePlan struct {
	SchemaVersion        string           `json:"schema_version"`
	InventoryVersion     string           `json:"inventory_version"`
	Status               string           `json:"status"`
	PlannedAt            time.Time        `json:"planned_at"`
	DiscoveryStartedAt   *time.Time       `json:"discovery_started_at,omitempty"`
	DiscoveryCompletedAt *time.Time       `json:"discovery_completed_at,omitempty"`
	PriorReleaseID       string           `json:"prior_release_id,omitempty"`
	CandidateReleaseID   string           `json:"candidate_release_id,omitempty"`
	SelectedSources      []SelectedSource `json:"selected_sources"`
	ChangedSourceIDs     []string         `json:"changed_source_ids"`
	ReusedSourceIDs      []string         `json:"reused_source_ids"`
	Issues               []Issue          `json:"issues"`
}

// SelectedSource freezes the exact metadata version that body acquisition may
// request after an update_available plan.
type SelectedSource struct {
	SourceID        string    `json:"source_id"`
	RequestURL      string    `json:"request_url"`
	FinalURL        string    `json:"final_url"`
	ObservedAt      time.Time `json:"observed_at"`
	VersionIdentity string    `json:"version_identity"`
	VersionBasis    string    `json:"version_basis"`
	VersionID       string    `json:"version_id,omitempty"`
	ETag            string    `json:"etag,omitempty"`
	LastModified    string    `json:"last_modified,omitempty"`
	ContentLength   *int64    `json:"content_length,omitempty"`
	Digest          string    `json:"digest,omitempty"`
	AcceptRanges    string    `json:"accept_ranges,omitempty"`
}

// Issue is a stable machine code plus human-readable planner diagnostic.
type Issue struct {
	SourceID string `json:"source_id,omitempty"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

// ReleaseManifest is the prior published baseline consumed by the planner.
type ReleaseManifest struct {
	Schema            string              `json:"$schema"`
	SchemaVersion     string              `json:"schema_version"`
	InventoryVersion  string              `json:"inventory_version"`
	ReleaseID         string              `json:"release_id"`
	PriorReleaseID    string              `json:"prior_release_id,omitempty"`
	RunID             string              `json:"run_id"`
	PlanSHA256        string              `json:"plan_sha256"`
	AcquisitionSHA256 string              `json:"acquisition_sha256"`
	StageSHA256       string              `json:"stage_sha256"`
	State             string              `json:"state"`
	SelectedAt        time.Time           `json:"selected_at"`
	PublishedAt       time.Time           `json:"published_at"`
	Periods           []string            `json:"periods"`
	Artifacts         []PublishedArtifact `json:"artifacts"`
	StagedOutputs     []StagedOutput      `json:"staged_outputs"`
	Checks            []ReleaseCheck      `json:"checks"`
}

// PublishedArtifact retains selection metadata plus the acquired content
// identity needed for reproducibility.
type PublishedArtifact struct {
	SelectedSource
	ByteCount  int64     `json:"byte_count"`
	SHA256     string    `json:"sha256"`
	StorageKey string    `json:"storage_key"`
	AcquiredAt time.Time `json:"acquired_at"`
}
