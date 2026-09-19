// Package candidateresolution publishes per-fact candidate-reference
// decisions for effective Schedule E independent expenditures.
package candidateresolution

import (
	"time"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ContractID            = "fec/independent-expenditure-candidate-resolution"
	ContractVersion       = "1.0.0"
	ManifestSchemaVersion = "legal-tender.fec.independent-expenditure-candidate-resolution-set.v1"
	DecisionSchemaVersion = "legal-tender.fec.independent-expenditure-candidate-resolution.v1"
	MethodVersion         = "legal-tender.fec.independent-expenditure-candidate-resolution-method.v2"
	LegacyMethodVersion   = "legal-tender.fec.independent-expenditure-candidate-resolution-method.v1"
	PublisherVersion      = "legal-tender.fec.independent-expenditure-candidate-resolution-publisher.v1"

	StateConfirmed  = "confirmed"
	StateResolved   = "resolved"
	StateUnverified = "unverified"
	StateAmbiguous  = "ambiguous"
	StateUnresolved = "unresolved"

	MethodReportedIDExactContext = "reported_id_exact_context"
	MethodUniqueExactContext     = "unique_exact_name_office_context"
	MethodMultipleExactContext   = "multiple_exact_name_office_context"
	MethodReportedIDUnverified   = "reported_id_context_unverified"
	MethodReportedIDInsufficient = "reported_id_insufficient_context"
	MethodNoExactContext         = "no_exact_name_office_context"
	MethodInsufficientContext    = "insufficient_reported_context"

	legacyStateConflicting   = "conflicting"
	legacyMethodIDConflict   = "reported_id_context_conflict"
	legacyMethodNoExact      = "no_exact_name_office_context"
	legacyMethodInsufficient = "insufficient_reported_context"
)

type PublishInput struct {
	EffectiveManifestPath string
	CandidateManifestPath string
}

type PublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

type Manifest struct {
	Schema                string                     `json:"$schema"`
	SchemaVersion         string                     `json:"schema_version"`
	CalculationSetID      string                     `json:"calculation_set_id"`
	Calculation           string                     `json:"calculation"`
	CalculationVersion    string                     `json:"calculation_version"`
	PublisherVersion      string                     `json:"publisher_version"`
	DecisionSchemaVersion string                     `json:"decision_schema_version"`
	Cycle                 string                     `json:"cycle"`
	SourceReleaseID       string                     `json:"source_release_id"`
	InputCalculation      CalculationReference       `json:"input_calculation"`
	InputCandidateFactSet CandidateFactSetReference  `json:"input_candidate_fact_set"`
	RunID                 string                     `json:"run_id"`
	State                 string                     `json:"state"`
	PublishedAt           time.Time                  `json:"published_at"`
	Method                ResolutionMethod           `json:"method"`
	Counts                Counts                     `json:"counts"`
	Amounts               Amounts                    `json:"amounts"`
	Decisions             storageartifact.Descriptor `json:"decisions"`
	Checks                []Check                    `json:"checks"`
}

type CalculationReference struct {
	Role                    string `json:"role"`
	Calculation             string `json:"calculation"`
	CalculationVersion      string `json:"calculation_version"`
	CalculationSetID        string `json:"calculation_set_id"`
	ManifestSHA256          string `json:"manifest_sha256"`
	ScheduleEFactSetID      string `json:"schedule_e_fact_set_id"`
	ScheduleEManifestSHA256 string `json:"schedule_e_manifest_sha256"`
}

type CandidateFactSetReference struct {
	Role           string `json:"role"`
	Dataset        string `json:"dataset"`
	FactType       string `json:"fact_type"`
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type ResolutionMethod struct {
	Version           string           `json:"version"`
	NameNormalization string           `json:"name_normalization"`
	ContextRules      []string         `json:"context_rules"`
	DecisionOrder     []ResolutionRule `json:"decision_order"`
}

type ResolutionRule struct {
	State  string `json:"state"`
	Method string `json:"method"`
	When   string `json:"when"`
}

type Counts struct {
	SourceEffectiveFacts uint64 `json:"source_effective_facts"`
	CandidateFacts       uint64 `json:"candidate_facts"`
	UsableCandidateFacts uint64 `json:"usable_candidate_facts"`
	Confirmed            uint64 `json:"confirmed"`
	Resolved             uint64 `json:"resolved"`
	Unverified           uint64 `json:"unverified"`
	Ambiguous            uint64 `json:"ambiguous"`
	LegacyConflicting    uint64 `json:"conflicting,omitempty"`
	Unresolved           uint64 `json:"unresolved"`
}

type Amounts struct {
	SourceEffectiveMinorUnits   string `json:"source_effective_minor_units"`
	ConfirmedMinorUnits         string `json:"confirmed_minor_units"`
	ResolvedMinorUnits          string `json:"resolved_minor_units"`
	UnverifiedMinorUnits        string `json:"unverified_minor_units"`
	AmbiguousMinorUnits         string `json:"ambiguous_minor_units"`
	LegacyConflictingMinorUnits string `json:"conflicting_minor_units,omitempty"`
	UnresolvedMinorUnits        string `json:"unresolved_minor_units"`
}

type ReportedCandidate struct {
	CandidateID    string  `json:"candidate_id"`
	Name           *string `json:"name"`
	Office         *string `json:"office"`
	OfficeState    *string `json:"office_state"`
	OfficeDistrict *string `json:"office_district"`
}

type Decision struct {
	SchemaVersion       string            `json:"schema_version"`
	DecisionID          string            `json:"decision_id"`
	CalculationSetID    string            `json:"calculation_set_id"`
	FactID              string            `json:"fact_id"`
	NaturalKey          string            `json:"natural_key"`
	Cycle               string            `json:"cycle"`
	SpenderCommitteeID  string            `json:"spender_committee_id"`
	SupportOppose       string            `json:"support_oppose"`
	AmountMinorUnits    string            `json:"amount_minor_units"`
	ReportedCandidate   ReportedCandidate `json:"reported_candidate"`
	State               string            `json:"state"`
	Method              string            `json:"method"`
	ResolvedCandidateID *string           `json:"resolved_candidate_id"`
	CandidateFactIDs    []string          `json:"candidate_fact_ids"`
	EvidenceCodes       []string          `json:"evidence_codes"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}
