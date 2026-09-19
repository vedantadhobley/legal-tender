package committeeflows

import (
	"time"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ManifestSchemaVersion  = "legal-tender.fec.receiver-reported-committee-flow-set.v1"
	ResultSchemaVersion    = "legal-tender.fec.receiver-reported-committee-flow.v1"
	ExceptionSchemaVersion = "legal-tender.fec.receiver-reported-committee-flow-exception.v1"
	PublisherVersion       = "legal-tender.fec.receiver-reported-committee-flow-publisher.v1"
)

type PublishInput struct {
	ColumnarFactManifestPath string
}

type PublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Workers             int
	Clock               func() time.Time
	Progress            func(string)
}

type Manifest struct {
	Schema                 string                     `json:"$schema"`
	SchemaVersion          string                     `json:"schema_version"`
	CalculationSetID       string                     `json:"calculation_set_id"`
	Calculation            string                     `json:"calculation"`
	CalculationVersion     string                     `json:"calculation_version"`
	PolicyVersion          string                     `json:"policy_version"`
	PublisherVersion       string                     `json:"publisher_version"`
	ResultSchemaVersion    string                     `json:"result_schema_version"`
	ExceptionSchemaVersion string                     `json:"exception_schema_version"`
	Cycle                  string                     `json:"cycle"`
	SourceReleaseID        string                     `json:"source_release_id"`
	InputFactSet           FactSetReference           `json:"input_fact_set"`
	RunID                  string                     `json:"run_id"`
	State                  string                     `json:"state"`
	PublishedAt            time.Time                  `json:"published_at"`
	Predicate              MembershipPredicate        `json:"predicate"`
	DecisionCounts         DecisionCounts             `json:"decision_counts"`
	ResultCounts           ResultCounts               `json:"result_counts"`
	Amounts                AmountTotals               `json:"amounts"`
	Exceptions             storageartifact.Descriptor `json:"exceptions"`
	Results                storageartifact.Descriptor `json:"results"`
	Checks                 []Check                    `json:"checks"`
}

type FactSetReference struct {
	Role                  string `json:"role"`
	Dataset               string `json:"dataset"`
	FactType              string `json:"fact_type"`
	FactSetID             string `json:"fact_set_id"`
	ManifestSHA256        string `json:"manifest_sha256"`
	PhysicalSchemaVersion string `json:"physical_schema_version"`
}

type MembershipPredicate struct {
	Version                    string            `json:"version"`
	InputPhysicalSchemaVersion string            `json:"input_physical_schema_version"`
	MembershipIdentity         string            `json:"membership_identity"`
	RequiredColumns            []string          `json:"required_columns"`
	DecisionOrder              []PredicateRule   `json:"decision_order"`
	ReceiptTypeRules           []ReceiptTypeRule `json:"receipt_type_rules"`
	ExceptionalStates          []string          `json:"exceptional_states"`
	ExcludedStates             []string          `json:"excluded_states"`
}

type PredicateRule struct {
	State string   `json:"state"`
	All   []string `json:"all"`
}

type DecisionCounts struct {
	SourceFacts                             uint64 `json:"source_facts"`
	InvalidNormalization                    uint64 `json:"invalid_normalization"`
	UnresolvedRecipientCommitteeID          uint64 `json:"unresolved_recipient_committee_id"`
	ExcludedNoSourceCommitteeID             uint64 `json:"excluded_no_source_committee_id"`
	UnresolvedOneSidedSourceCommitteeID     uint64 `json:"unresolved_one_sided_source_committee_id"`
	UnresolvedConflictingSourceCommitteeIDs uint64 `json:"unresolved_conflicting_source_committee_ids"`
	ExcludedMemoSubtotal                    uint64 `json:"excluded_memo_subtotal"`
	UnresolvedAmount                        uint64 `json:"unresolved_amount"`
	ExcludedOutboundReceiptRole             uint64 `json:"excluded_outbound_receipt_role"`
	ExcludedSemanticMemoReceiptRole         uint64 `json:"excluded_semantic_memo_receipt_role"`
	ExcludedEarmarkedReceiptRole            uint64 `json:"excluded_earmarked_receipt_role"`
	ExcludedNoncommitteeReceiptRole         uint64 `json:"excluded_noncommittee_receipt_role"`
	UnresolvedReceiptRole                   uint64 `json:"unresolved_receipt_role"`
	IncludedReceiverReportedCommitteeFlow   uint64 `json:"included_receiver_reported_committee_flow"`
}

type ResultCounts struct {
	KnownAmountRows      uint64 `json:"known_amount_rows"`
	UnknownAmountRows    uint64 `json:"unknown_amount_rows"`
	IncludedRows         uint64 `json:"included_rows"`
	IncludedPositiveRows uint64 `json:"included_positive_rows"`
	IncludedNegativeRows uint64 `json:"included_negative_rows"`
	IncludedZeroRows     uint64 `json:"included_zero_rows"`
	ResultGroups         uint64 `json:"result_groups"`
	SourceCommittees     uint64 `json:"source_committees"`
	RecipientCommittees  uint64 `json:"recipient_committees"`
	SelfEdgeRows         uint64 `json:"self_edge_rows"`
	SelfEdgeGroups       uint64 `json:"self_edge_groups"`
}

type AmountTotals struct {
	KnownSourceMinorUnits string `json:"known_source_minor_units"`
	IncludedMinorUnits    string `json:"included_minor_units"`
	ExcludedMinorUnits    string `json:"excluded_minor_units"`
	UnresolvedMinorUnits  string `json:"unresolved_minor_units"`
}

type Result struct {
	SchemaVersion          string `json:"schema_version"`
	ResultID               string `json:"result_id"`
	CalculationSetID       string `json:"calculation_set_id"`
	Cycle                  string `json:"cycle"`
	SourceCommitteeID      string `json:"source_committee_id"`
	RecipientCommitteeID   string `json:"recipient_committee_id"`
	ReceiptRole            string `json:"receipt_role"`
	SignedAmountMinorUnits string `json:"signed_amount_minor_units"`
	ReceiptCount           uint64 `json:"receipt_count"`
	PositiveCount          uint64 `json:"positive_count"`
	NegativeCount          uint64 `json:"negative_count"`
	ZeroCount              uint64 `json:"zero_count"`
}

type Exception struct {
	SchemaVersion        string   `json:"schema_version"`
	ExceptionID          string   `json:"exception_id"`
	CalculationSetID     string   `json:"calculation_set_id"`
	NaturalKey           string   `json:"natural_key"`
	SourceRowOrdinal     uint64   `json:"source_row_ordinal"`
	State                string   `json:"state"`
	AmountMinorUnits     *string  `json:"amount_minor_units"`
	RecipientCommitteeID *string  `json:"recipient_committee_id"`
	ContributorID        *string  `json:"contributor_id"`
	CleanContributorID   *string  `json:"clean_contributor_id"`
	ReceiptTypeCode      *string  `json:"receipt_type_code"`
	ReasonCodes          []string `json:"reason_codes"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}
