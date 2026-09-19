// Package committeeidentity publishes exact committee-registration coverage
// decisions for receiver-reported committee-flow graph endpoints.
package committeeidentity

import (
	"time"

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ContractID            = "fec/receiver-flow-committee-identity-coverage"
	ContractVersion       = "1.0.0"
	ManifestSchemaVersion = "legal-tender.fec.receiver-flow-committee-identity-coverage-set.v1"
	DecisionSchemaVersion = "legal-tender.fec.receiver-flow-committee-identity-coverage.v1"
	MethodVersion         = "legal-tender.fec.receiver-flow-committee-identity-coverage-method.v1"
	PublisherVersion      = "legal-tender.fec.receiver-flow-committee-identity-coverage-publisher.v1"

	StateHistoricalRegistration       = "historical_registration"
	StateAlternateReleaseRegistration = "alternate_release_registration"
	StateUnresolvedReportedID         = "unresolved_reported_id"
)

type PublishInput struct {
	ReadinessBundlePath              string
	CommitteeComparisonManifestPaths []string
	CommitteeHistoryManifestPaths    []string
	RawHistoryArchives               []fecflowmastergaps.RawHistoryArchiveInput
	LinkageManifestPath              string
	SummaryManifestPaths             []string
}

type PublishOptions struct {
	StorageRoot         string
	Cycle               string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

// EvidenceInputs contains only evidence that can change an identity decision.
// Linkage and candidate-summary inputs used by the broader audit are excluded.
type EvidenceInputs struct {
	ReadinessBundle        fecflowmastergaps.BundleReference      `json:"readiness_bundle"`
	Calculation            fecflowmastergaps.CalculationReference `json:"calculation"`
	CurrentCommitteeMaster fecflowmastergaps.FactSetReference     `json:"current_committee_master"`
	SameCycleComparisons   []fecflowmastergaps.FactSetReference   `json:"same_cycle_comparison_masters"`
	NormalizedHistory      []fecflowmastergaps.FactSetReference   `json:"normalized_history_masters"`
	RawHistory             []fecflowmastergaps.RawHistoryArchive  `json:"raw_history_archives"`
}

type Manifest struct {
	Schema                string                     `json:"$schema"`
	SchemaVersion         string                     `json:"schema_version"`
	CalculationSetID      string                     `json:"calculation_set_id"`
	Calculation           string                     `json:"calculation"`
	CalculationVersion    string                     `json:"calculation_version"`
	PublisherVersion      string                     `json:"publisher_version"`
	DecisionSchemaVersion string                     `json:"decision_schema_version"`
	MethodVersion         string                     `json:"method_version"`
	Cycle                 string                     `json:"cycle"`
	SourceReleaseID       string                     `json:"source_release_id"`
	Inputs                EvidenceInputs             `json:"inputs"`
	RunID                 string                     `json:"run_id"`
	State                 string                     `json:"state"`
	PublishedAt           time.Time                  `json:"published_at"`
	Counts                Counts                     `json:"counts"`
	Decisions             storageartifact.Descriptor `json:"decisions"`
	Checks                []Check                    `json:"checks"`
}

type Counts struct {
	ReferencedCommittees             uint64 `json:"referenced_committees"`
	CurrentCycleMasters              uint64 `json:"current_cycle_masters"`
	IdentityCoverageDecisions        uint64 `json:"identity_coverage_decisions"`
	HistoricalRegistrations          uint64 `json:"historical_registrations"`
	AlternateReleaseRegistrations    uint64 `json:"alternate_release_registrations"`
	UnresolvedReportedIDs            uint64 `json:"unresolved_reported_ids"`
	TerminalIdentityEligible         uint64 `json:"terminal_identity_eligible"`
	TerminalIdentityIneligible       uint64 `json:"terminal_identity_ineligible"`
	SourceEndpointDecisions          uint64 `json:"source_endpoint_decisions"`
	RecipientEndpointDecisions       uint64 `json:"recipient_endpoint_decisions"`
	BothEndpointRoleDecisions        uint64 `json:"both_endpoint_role_decisions"`
	HistoricalRegistrationAssertions uint64 `json:"historical_registration_assertions"`
	AlternateRegistrationAssertions  uint64 `json:"alternate_registration_assertions"`
}

type Decision struct {
	SchemaVersion            string                                           `json:"schema_version"`
	DecisionID               string                                           `json:"decision_id"`
	CalculationSetID         string                                           `json:"calculation_set_id"`
	Cycle                    string                                           `json:"cycle"`
	CommitteeID              string                                           `json:"committee_id"`
	State                    string                                           `json:"state"`
	EndpointRoles            []string                                         `json:"endpoint_roles"`
	TerminalIdentityEligible bool                                             `json:"terminal_identity_eligible"`
	EvidenceCodes            []string                                         `json:"evidence_codes"`
	SameCycleAssertions      []fecflowmastergaps.CommitteeHistoricalAssertion `json:"same_cycle_comparison_assertions"`
	HistoricalAssertions     []fecflowmastergaps.CommitteeHistoricalAssertion `json:"historical_assertions"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}
