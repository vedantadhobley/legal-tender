package receipts

import (
	"time"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	CompactManifestSchemaVersion  = "legal-tender.fec.candidate-itemized-individual-receipts-compact-set.v1"
	CompactMembershipPredicateV1  = "legal-tender.fec.itemized-individual-receipt-membership-predicate.v1"
	CompactMembershipExceptionV1  = "legal-tender.fec.itemized-individual-receipt-membership-exception.v1"
	CompactCalculationPublisherV1 = "legal-tender.fec.candidate-itemized-individual-receipts-compact-publisher.v1"
)

type CompactPublishInput struct {
	ColumnarFactManifestPath      string
	LinkageFactManifestPath       string
	AllCandidatesFactManifestPath string
	CurrentCampaignsManifestPath  string
}

type CompactPublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

// CompactManifest binds deterministic calculation membership to one exact
// columnar fact index. Only exceptional memberships and candidate results are
// materialized; ordinary row decisions are recovered from the predicate.
type CompactManifest struct {
	Schema              string                       `json:"$schema"`
	SchemaVersion       string                       `json:"schema_version"`
	CalculationSetID    string                       `json:"calculation_set_id"`
	Calculation         string                       `json:"calculation"`
	CalculationVersion  string                       `json:"calculation_version"`
	PublisherVersion    string                       `json:"publisher_version"`
	ResultSchemaVersion string                       `json:"result_schema_version"`
	Cycle               string                       `json:"cycle"`
	SourceReleaseID     string                       `json:"source_release_id"`
	InputFactSets       []FactSetReference           `json:"input_fact_sets"`
	RunID               string                       `json:"run_id"`
	State               string                       `json:"state"`
	PublishedAt         time.Time                    `json:"published_at"`
	Predicate           CompactMembershipPredicate   `json:"predicate"`
	DecisionCounts      DirectProbeDecisionCounts    `json:"decision_counts"`
	ResultCounts        DirectProbeCounts            `json:"result_counts"`
	Reconciliations     DirectProbeReconciliationSet `json:"reconciliations"`
	Exceptions          storageartifact.Descriptor   `json:"exceptions"`
	Results             storageartifact.Descriptor   `json:"results"`
	Checks              []Check                      `json:"checks"`
}

type CompactMembershipPredicate struct {
	Version                    string                 `json:"version"`
	InputPhysicalSchemaVersion string                 `json:"input_physical_schema_version"`
	MembershipIdentity         string                 `json:"membership_identity"`
	RequiredColumns            []string               `json:"required_columns"`
	DecisionOrder              []CompactPredicateRule `json:"decision_order"`
	ExceptionalStates          []string               `json:"exceptional_states"`
}

type CompactPredicateRule struct {
	State string   `json:"state"`
	All   []string `json:"all"`
}

type CompactMembershipException struct {
	SchemaVersion        string  `json:"schema_version"`
	ExceptionID          string  `json:"exception_id"`
	SourceRowOrdinal     uint64  `json:"source_row_ordinal"`
	State                string  `json:"state"`
	RecipientCommitteeID *string `json:"recipient_committee_id,omitempty"`
	AmountState          string  `json:"amount_state,omitempty"`
	Reason               string  `json:"reason"`
}
