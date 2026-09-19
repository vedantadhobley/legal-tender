package candidateresolution

import (
	"time"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	AggregateContractID             = "fec/resolved-independent-expenditures"
	AggregateContractVersion        = "1.0.0"
	AggregateManifestSchemaVersion  = "legal-tender.fec.resolved-independent-expenditure-set.v1"
	AggregateResultSchemaVersion    = "legal-tender.fec.resolved-independent-expenditure-spender-candidate.v1"
	AggregateExceptionSchemaVersion = "legal-tender.fec.resolved-independent-expenditure-exception.v1"
	AggregateGroupingPolicyVersion  = "legal-tender.fec.resolved-independent-expenditure-grouping-policy.v1"
	AggregatePublisherVersion       = "legal-tender.fec.resolved-independent-expenditure-publisher.v1"
)

type AggregatePublishInput struct {
	CandidateResolutionManifestPath string
}

type AggregatePublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

type AggregateManifest struct {
	Schema                 string                     `json:"$schema"`
	SchemaVersion          string                     `json:"schema_version"`
	CalculationSetID       string                     `json:"calculation_set_id"`
	Calculation            string                     `json:"calculation"`
	CalculationVersion     string                     `json:"calculation_version"`
	PublisherVersion       string                     `json:"publisher_version"`
	ResultSchemaVersion    string                     `json:"result_schema_version"`
	ExceptionSchemaVersion string                     `json:"exception_schema_version"`
	Cycle                  string                     `json:"cycle"`
	SourceReleaseID        string                     `json:"source_release_id"`
	InputResolution        AggregateInputReference    `json:"input_candidate_resolution"`
	RunID                  string                     `json:"run_id"`
	State                  string                     `json:"state"`
	PublishedAt            time.Time                  `json:"published_at"`
	GroupingPolicy         AggregateGroupingPolicy    `json:"grouping_policy"`
	Counts                 AggregateCounts            `json:"counts"`
	Amounts                AggregateAmounts           `json:"amounts"`
	Results                storageartifact.Descriptor `json:"results"`
	Exceptions             storageartifact.Descriptor `json:"exceptions"`
	Checks                 []Check                    `json:"checks"`
}

type AggregateInputReference struct {
	Role                  string `json:"role"`
	Calculation           string `json:"calculation"`
	CalculationVersion    string `json:"calculation_version"`
	CalculationSetID      string `json:"calculation_set_id"`
	ManifestSHA256        string `json:"manifest_sha256"`
	DecisionSchemaVersion string `json:"decision_schema_version"`
	DecisionsSHA256       string `json:"decisions_sha256"`
}

type AggregateGroupingPolicy struct {
	Version         string   `json:"version"`
	GroupBy         []string `json:"group_by"`
	ProjectedStates []string `json:"projected_states"`
	ExceptionStates []string `json:"exception_states"`
}

type AggregateCounts struct {
	SourceDecisions        uint64 `json:"source_decisions"`
	ProjectableDecisions   uint64 `json:"projectable_decisions"`
	UnprojectableDecisions uint64 `json:"unprojectable_decisions"`
	Confirmed              uint64 `json:"confirmed"`
	Resolved               uint64 `json:"resolved"`
	Unverified             uint64 `json:"unverified"`
	Ambiguous              uint64 `json:"ambiguous"`
	Unresolved             uint64 `json:"unresolved"`
	ResultGroups           uint64 `json:"result_groups"`
	Exceptions             uint64 `json:"exceptions"`
}

type AggregateAmounts struct {
	SourceMinorUnits        string `json:"source_minor_units"`
	ProjectableMinorUnits   string `json:"projectable_minor_units"`
	UnprojectableMinorUnits string `json:"unprojectable_minor_units"`
	ConfirmedMinorUnits     string `json:"confirmed_minor_units"`
	ResolvedMinorUnits      string `json:"resolved_minor_units"`
	UnverifiedMinorUnits    string `json:"unverified_minor_units"`
	AmbiguousMinorUnits     string `json:"ambiguous_minor_units"`
	UnresolvedMinorUnits    string `json:"unresolved_minor_units"`
}

type AggregateResolutionCounts struct {
	Confirmed  uint64 `json:"confirmed"`
	Resolved   uint64 `json:"resolved"`
	Unverified uint64 `json:"unverified"`
}

type AggregateResolutionAmounts struct {
	ConfirmedMinorUnits  string `json:"confirmed_minor_units"`
	ResolvedMinorUnits   string `json:"resolved_minor_units"`
	UnverifiedMinorUnits string `json:"unverified_minor_units"`
}

type AggregateResult struct {
	SchemaVersion          string                     `json:"schema_version"`
	ResultID               string                     `json:"result_id"`
	CalculationSetID       string                     `json:"calculation_set_id"`
	Cycle                  string                     `json:"cycle"`
	SpenderCommitteeID     string                     `json:"spender_committee_id"`
	CandidateID            string                     `json:"candidate_id"`
	SupportOppose          string                     `json:"support_oppose"`
	SignedAmountMinorUnits string                     `json:"signed_amount_minor_units"`
	ExpenditureCount       uint64                     `json:"expenditure_count"`
	PositiveCount          uint64                     `json:"positive_count"`
	NegativeCount          uint64                     `json:"negative_count"`
	ZeroCount              uint64                     `json:"zero_count"`
	ResolutionCounts       AggregateResolutionCounts  `json:"resolution_counts"`
	ResolutionAmounts      AggregateResolutionAmounts `json:"resolution_amounts"`
}

type AggregateException struct {
	SchemaVersion        string `json:"schema_version"`
	ExceptionID          string `json:"exception_id"`
	CalculationSetID     string `json:"calculation_set_id"`
	ResolutionDecisionID string `json:"resolution_decision_id"`
	FactID               string `json:"fact_id"`
	NaturalKey           string `json:"natural_key"`
	Cycle                string `json:"cycle"`
	SpenderCommitteeID   string `json:"spender_committee_id"`
	ReportedCandidateID  string `json:"reported_candidate_id"`
	SupportOppose        string `json:"support_oppose"`
	State                string `json:"state"`
	Method               string `json:"method"`
	AmountMinorUnits     string `json:"amount_minor_units"`
}
