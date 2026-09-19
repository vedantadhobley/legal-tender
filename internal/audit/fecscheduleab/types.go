// Package fecscheduleab measures candidate reconciliation evidence between
// accepted receiver-side Schedule A flow observations and sender-side
// Schedule B observations. It does not publish reconciliation facts.
package fecscheduleab

import (
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
)

const (
	SchemaVersion = "legal-tender.audit.fec.schedule-ab-alignment.v1"
	ProbeVersion  = "legal-tender.audit.fec.schedule-ab-alignment.v1"
)

type Options struct {
	StorageRoot                  string
	ScheduleAFactManifestPath    string
	ScheduleAReleaseManifestPath string
	ScheduleBDumpPath            string
	ScheduleBObservationPath     string
	Cycle                        string
	WorkDir                      string
	Workers                      int
	MaxScheduleBRows             uint64
	PGRestorePath                string
	Clock                        func() time.Time
	Progress                     func(string)
}

type Result struct {
	SchemaVersion         string                 `json:"schema_version"`
	ProbeVersion          string                 `json:"probe_version"`
	Status                string                 `json:"status"`
	Cycle                 string                 `json:"cycle"`
	StartedAt             time.Time              `json:"started_at"`
	CompletedAt           time.Time              `json:"completed_at"`
	ElapsedSeconds        float64                `json:"elapsed_seconds"`
	Inputs                InputEvidence          `json:"inputs"`
	Configuration         Configuration          `json:"configuration"`
	Counts                Counts                 `json:"counts"`
	CandidateStates       []CandidateState       `json:"candidate_states"`
	ScheduleBDisposition  []NamedCount           `json:"schedule_b_disposition"`
	ScheduleBVerification scheduleb.Verification `json:"schedule_b_verification"`
	Checks                []Check                `json:"checks"`
	Caveats               []string               `json:"caveats"`
}

type InputEvidence struct {
	ScheduleA ScheduleAInput  `json:"schedule_a"`
	ScheduleB ScheduleBInput  `json:"schedule_b"`
	Alignment SourceAlignment `json:"alignment"`
}

type ScheduleAInput struct {
	FactSetID             string `json:"fact_set_id"`
	FactManifestSHA256    string `json:"fact_manifest_sha256"`
	SourceReleaseID       string `json:"source_release_id"`
	ReleaseManifestSHA256 string `json:"release_manifest_sha256"`
	SourceVersionID       string `json:"source_version_id"`
	SourceArtifactSHA256  string `json:"source_artifact_sha256"`
	LastModified          string `json:"last_modified"`
	Rows                  uint64 `json:"rows"`
	Shards                uint64 `json:"shards"`
}

type ScheduleBInput struct {
	ObservationSchemaVersion string `json:"observation_schema_version"`
	ObservedAt               string `json:"observed_at"`
	SourceVersionID          string `json:"source_version_id"`
	SourceArtifactSHA256     string `json:"source_artifact_sha256"`
	LastModified             string `json:"last_modified"`
	ArtifactBytes            int64  `json:"artifact_bytes"`
	Relation                 string `json:"relation"`
}

type SourceAlignment struct {
	ScheduleAPublisherDate string `json:"schedule_a_publisher_date"`
	ScheduleBPublisherDate string `json:"schedule_b_publisher_date"`
	SamePublisherDate      bool   `json:"same_publisher_date"`
	Basis                  string `json:"basis"`
}

type Configuration struct {
	ScheduleACohortPolicy string   `json:"schedule_a_cohort_policy"`
	ExactKey              string   `json:"exact_key"`
	CompatibleKey         string   `json:"compatible_key"`
	ConflictKey           string   `json:"conflict_key"`
	CandidatePrecedence   []string `json:"candidate_precedence"`
	Workers               int      `json:"workers"`
	MaxScheduleBRows      uint64   `json:"max_schedule_b_rows"`
}

type Counts struct {
	ScheduleASourceRows          uint64 `json:"schedule_a_source_rows"`
	ScheduleAFlowRows            uint64 `json:"schedule_a_flow_rows"`
	ScheduleAIndexableRows       uint64 `json:"schedule_a_indexable_rows"`
	ScheduleAWithoutDateRows     uint64 `json:"schedule_a_without_date_rows"`
	ScheduleAExactSignatures     uint64 `json:"schedule_a_exact_signatures"`
	ScheduleAEndpointPairs       uint64 `json:"schedule_a_endpoint_pairs"`
	ScheduleBSourceRows          uint64 `json:"schedule_b_source_rows"`
	ScheduleBEligibleRows        uint64 `json:"schedule_b_eligible_rows"`
	ScheduleBIneligibleRows      uint64 `json:"schedule_b_ineligible_rows"`
	ScheduleBKnownEndpointRows   uint64 `json:"schedule_b_known_endpoint_rows"`
	ScheduleBOutsideEndpointRows uint64 `json:"schedule_b_outside_endpoint_rows"`
}

type CandidateState struct {
	State                     string `json:"state"`
	SignatureGroups           uint64 `json:"signature_groups"`
	ScheduleARows             uint64 `json:"schedule_a_rows"`
	ScheduleBRows             uint64 `json:"schedule_b_rows"`
	ScheduleAAmountMinorUnits string `json:"schedule_a_amount_minor_units"`
	ScheduleBAmountMinorUnits string `json:"schedule_b_amount_minor_units"`
}

type NamedCount struct {
	Name string `json:"name"`
	Rows uint64 `json:"rows"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type scheduleBArchiveObservation struct {
	SchemaVersion  string `json:"schema_version"`
	ObservedAt     string `json:"observed_at"`
	RequestURL     string `json:"request_url"`
	LastModified   string `json:"last_modified"`
	VersionID      string `json:"version_id"`
	ContentLength  int64  `json:"content_length"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}
