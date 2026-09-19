// Package feccommitteeflow measures receiver-reported committee-flow shapes
// before Legal Tender freezes a counting or graph-projection contract.
package feccommitteeflow

import "time"

const (
	SchemaVersion = "legal-tender.audit.fec.receiver-committee-flow-cohort.v1"
	ProbeVersion  = "legal-tender.audit.fec.receiver-committee-flow-cohort.v1"
)

type Options struct {
	StorageRoot  string
	ManifestPath string
	Cycle        string
	Workers      int
	TopEdges     int
	Clock        func() time.Time
	Progress     func(string)
}

type Result struct {
	SchemaVersion              string           `json:"schema_version"`
	ProbeVersion               string           `json:"probe_version"`
	Status                     string           `json:"status"`
	Cycle                      string           `json:"cycle"`
	StartedAt                  time.Time        `json:"started_at"`
	CompletedAt                time.Time        `json:"completed_at"`
	ElapsedSeconds             float64          `json:"elapsed_seconds"`
	Input                      InputReference   `json:"input"`
	Configuration              Configuration    `json:"configuration"`
	Counts                     Counts           `json:"counts"`
	Decisions                  []NamedMoneyStat `json:"decisions"`
	SourceIdentity             []NamedMoneyStat `json:"source_identity"`
	EntityTypes                []NamedMoneyStat `json:"included_entity_types"`
	IndividualClasses          []NamedMoneyStat `json:"included_individual_classes"`
	ReceiptTypes               []NamedMoneyStat `json:"included_receipt_types"`
	ReceiptRoles               []NamedMoneyStat `json:"included_receipt_roles"`
	ReceiptEntityShapes        []NamedMoneyStat `json:"included_receipt_entity_shapes"`
	ExactIdentityReceiptShapes []NamedMoneyStat `json:"exact_identity_receipt_shapes"`
	ActionCodes                []NamedMoneyStat `json:"included_action_codes"`
	TopEdges                   []EdgeGroup      `json:"top_included_edges_by_absolute_amount"`
	Samples                    []IdentitySample `json:"source_identity_samples"`
	Checks                     []Check          `json:"checks"`
}

type InputReference struct {
	FactSetID       string `json:"fact_set_id"`
	SourceReleaseID string `json:"source_release_id"`
	ManifestSHA256  string `json:"manifest_sha256"`
	PhysicalSchema  string `json:"physical_schema_version"`
	Rows            uint64 `json:"rows"`
	Shards          uint64 `json:"shards"`
}

type Configuration struct {
	Workers                 int      `json:"workers"`
	SourceIdentityFields    []string `json:"source_identity_fields"`
	DiagnosticDecisionOrder []string `json:"diagnostic_decision_order"`
}

type Counts struct {
	Rows                      uint64 `json:"rows"`
	ObservedAmountRows        uint64 `json:"observed_amount_rows"`
	UnknownAmountRows         uint64 `json:"unknown_amount_rows"`
	IncludedRows              uint64 `json:"included_rows"`
	IncludedEdges             uint64 `json:"included_edges"`
	IncludedSourceCommittees  uint64 `json:"included_source_committees"`
	IncludedRecipients        uint64 `json:"included_recipient_committees"`
	IncludedSelfEdgeRows      uint64 `json:"included_self_edge_rows"`
	IncludedSelfEdges         uint64 `json:"included_self_edges"`
	IncludedConduitRows       uint64 `json:"included_rows_with_conduit_id"`
	IncludedBackReferenceRows uint64 `json:"included_rows_with_back_reference"`
}

type NamedMoneyStat struct {
	Name              string `json:"name"`
	Rows              uint64 `json:"rows"`
	PositiveRows      uint64 `json:"positive_rows"`
	NegativeRows      uint64 `json:"negative_rows"`
	ZeroRows          uint64 `json:"zero_rows"`
	UnknownAmountRows uint64 `json:"unknown_amount_rows"`
	AmountMinorUnits  string `json:"amount_minor_units"`
}

type EdgeGroup struct {
	SourceCommitteeID    string `json:"source_committee_id"`
	RecipientCommitteeID string `json:"recipient_committee_id"`
	ReceiptRole          string `json:"receipt_role"`
	Rows                 uint64 `json:"rows"`
	PositiveRows         uint64 `json:"positive_rows"`
	NegativeRows         uint64 `json:"negative_rows"`
	ZeroRows             uint64 `json:"zero_rows"`
	AmountMinorUnits     string `json:"amount_minor_units"`
}

type IdentitySample struct {
	State                string  `json:"state"`
	SourceRowOrdinal     int64   `json:"source_row_ordinal"`
	RecipientCommitteeID *string `json:"recipient_committee_id,omitempty"`
	ContributorID        *string `json:"contributor_id,omitempty"`
	CleanContributorID   *string `json:"clean_contributor_id,omitempty"`
	EntityTypeCode       *string `json:"entity_type_code,omitempty"`
	Individual           *bool   `json:"is_individual,omitempty"`
	MemoCode             *string `json:"memo_code,omitempty"`
	ReceiptTypeCode      *string `json:"receipt_type_code,omitempty"`
	AmountMinorUnits     *int64  `json:"amount_minor_units,omitempty"`
}

type Check struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details"`
}
