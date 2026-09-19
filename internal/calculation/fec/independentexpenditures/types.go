// Package independentexpenditures publishes the compact effective Schedule E
// calculation. Source facts remain immutable and outside this package.
package independentexpenditures

import (
	"time"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ContractID             = "fec/effective-independent-expenditures"
	ContractVersion        = "1.0.0"
	ManifestSchemaVersion  = "legal-tender.fec.effective-independent-expenditure-set.v1"
	ResultSchemaVersion    = "legal-tender.fec.effective-independent-expenditure-spender-candidate.v1"
	ExceptionSchemaVersion = "legal-tender.fec.effective-independent-expenditure-exception.v1"
	PredicateVersion       = "legal-tender.fec.effective-independent-expenditure-membership-predicate.v1"
	PublisherVersion       = "legal-tender.fec.effective-independent-expenditure-publisher.v1"
)

type PublishInput struct {
	ScheduleEFactManifestPath string
}

type PublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

type Manifest struct {
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
	InputFactSet           FactSetReference           `json:"input_fact_set"`
	RunID                  string                     `json:"run_id"`
	State                  string                     `json:"state"`
	PublishedAt            time.Time                  `json:"published_at"`
	Predicate              MembershipPredicate        `json:"predicate"`
	DecisionCounts         DecisionCounts             `json:"decision_counts"`
	RouteCounts            RouteCounts                `json:"route_counts"`
	SourceShapeCounts      SourceShapeCounts          `json:"source_shape_counts"`
	Amounts                AmountTotals               `json:"amounts"`
	Exceptions             storageartifact.Descriptor `json:"exceptions"`
	Results                storageartifact.Descriptor `json:"results"`
	Checks                 []Check                    `json:"checks"`
}

type FactSetReference struct {
	Role           string `json:"role"`
	Dataset        string `json:"dataset"`
	FactType       string `json:"fact_type"`
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type MembershipPredicate struct {
	Version                string          `json:"version"`
	InputFactSchemaVersion string          `json:"input_fact_schema_version"`
	MembershipIdentity     string          `json:"membership_identity"`
	RequiredFields         []string        `json:"required_fields"`
	DecisionOrder          []PredicateRule `json:"decision_order"`
	RouteRequirements      []string        `json:"route_requirements"`
	ExceptionalStates      []string        `json:"exceptional_states"`
}

type PredicateRule struct {
	State string   `json:"state"`
	All   []string `json:"all"`
}

type DecisionCounts struct {
	SourceFacts                  uint64 `json:"source_facts"`
	Included                     uint64 `json:"included"`
	ExcludedMemo                 uint64 `json:"excluded_memo"`
	ExcludedMemoAmountUnresolved uint64 `json:"excluded_memo_amount_unresolved"`
	UnresolvedAmount             uint64 `json:"unresolved_amount"`
}

type RouteCounts struct {
	Attributed           uint64 `json:"attributed"`
	Unattributed         uint64 `json:"unattributed"`
	MissingSpender       uint64 `json:"missing_spender"`
	MissingCandidate     uint64 `json:"missing_candidate"`
	InvalidSupportOppose uint64 `json:"invalid_support_oppose"`
	ResultGroups         uint64 `json:"result_groups"`
}

type SourceShapeCounts struct {
	ValidFacts                     uint64 `json:"valid_facts"`
	InvalidFacts                   uint64 `json:"invalid_facts"`
	ActionAdd                      uint64 `json:"action_add"`
	ActionChange                   uint64 `json:"action_change"`
	ActionNoChange                 uint64 `json:"action_no_change"`
	ActionTerminate                uint64 `json:"action_terminate"`
	ActionNull                     uint64 `json:"action_null"`
	ActionOther                    uint64 `json:"action_other"`
	NoticeLikeFacts                uint64 `json:"notice_like_facts"`
	MissingExpenditureType         uint64 `json:"missing_expenditure_type"`
	TransactionIDMissing           uint64 `json:"transaction_id_missing"`
	TransactionKeyedFacts          uint64 `json:"transaction_keyed_facts"`
	DistinctTransactionKeys        uint64 `json:"distinct_transaction_keys"`
	RepeatedTransactionKeys        uint64 `json:"repeated_transaction_keys"`
	RepeatedTransactionOccurrences uint64 `json:"repeated_transaction_occurrences"`
}

type AmountTotals struct {
	IncludedMinorUnits     string `json:"included_minor_units"`
	ExcludedMemoMinorUnits string `json:"excluded_memo_minor_units"`
	AttributedMinorUnits   string `json:"attributed_minor_units"`
	UnattributedMinorUnits string `json:"unattributed_minor_units"`
}

type Result struct {
	SchemaVersion               string `json:"schema_version"`
	ResultID                    string `json:"result_id"`
	CalculationSetID            string `json:"calculation_set_id"`
	Cycle                       string `json:"cycle"`
	SpenderCommitteeID          string `json:"spender_committee_id"`
	CandidateID                 string `json:"candidate_id"`
	SupportOppose               string `json:"support_oppose"`
	SignedAmountMinorUnits      string `json:"signed_amount_minor_units"`
	ExpenditureCount            uint64 `json:"expenditure_count"`
	PositiveCount               uint64 `json:"positive_count"`
	NegativeCount               uint64 `json:"negative_count"`
	ZeroCount                   uint64 `json:"zero_count"`
	MissingExpenditureTypeCount uint64 `json:"missing_expenditure_type_count"`
}

type Exception struct {
	SchemaVersion    string   `json:"schema_version"`
	ExceptionID      string   `json:"exception_id"`
	CalculationSetID string   `json:"calculation_set_id"`
	FactID           string   `json:"fact_id"`
	NaturalKey       string   `json:"natural_key"`
	State            string   `json:"state"`
	AmountMinorUnits *string  `json:"amount_minor_units"`
	ReasonCodes      []string `json:"reason_codes"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}
