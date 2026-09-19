// Package fecschedulebsemantics measures reporting and record-selection evidence
// in the published Schedule B facts before accepting an outgoing-flow policy.
package fecschedulebsemantics

import "time"

const Version = "legal-tender.audit.fec.schedule-b-semantics.v1"

type Options struct {
	StorageRoot      string
	FactManifestPath string
	Cycle            string
	Workers          int
	Progress         func(string)
}

// Cell keeps source NULL distinct from source empty text in grouping keys.
type Cell struct {
	Present bool   `json:"present"`
	Value   string `json:"value"`
}

type Shape struct {
	Form              string `json:"form"`
	Line              Cell   `json:"line"`
	Schedule          Cell   `json:"schedule"`
	Action            Cell   `json:"action"`
	Memo              Cell   `json:"memo"`
	DisbursementType  Cell   `json:"disbursement_type"`
	EntityType        Cell   `json:"entity_type"`
	ReportingRole     string `json:"reporting_role"`
	RecipientIdentity string `json:"recipient_identity"`
	ValidSender       bool   `json:"valid_sender"`
	SelfRecipient     bool   `json:"self_recipient"`
}

type Measures struct {
	Rows                         uint64 `json:"rows"`
	AmountRows                   uint64 `json:"amount_rows"`
	MissingAmountRows            uint64 `json:"missing_amount_rows"`
	AmountMinorUnits             int64  `json:"amount_minor_units,string"`
	NegativeRows                 uint64 `json:"negative_rows"`
	ZeroRows                     uint64 `json:"zero_rows"`
	NonMemoRows                  uint64 `json:"non_memo_rows"`
	NonMemoAmountMinorUnits      int64  `json:"non_memo_amount_minor_units,string"`
	TransactionRows              uint64 `json:"transaction_rows"`
	BackReferenceTransactionRows uint64 `json:"back_reference_transaction_rows"`
	BackReferenceScheduleRows    uint64 `json:"back_reference_schedule_rows"`
	OriginalSubmissionRows       uint64 `json:"original_submission_rows"`
	OriginalEqualsSubmissionRows uint64 `json:"original_equals_submission_rows"`
	FileRows                     uint64 `json:"file_rows"`
	LinkRows                     uint64 `json:"link_rows"`
	CandidateRows                uint64 `json:"candidate_rows"`
	ConduitNameRows              uint64 `json:"conduit_name_rows"`
	BeneficiaryNameRows          uint64 `json:"beneficiary_name_rows"`
}

type Example struct {
	Ordinal                  int64   `json:"ordinal"`
	SubID                    string  `json:"sub_id"`
	Sender                   *string `json:"sender"`
	RawRecipient             *string `json:"raw_recipient"`
	CleanRecipient           *string `json:"clean_recipient"`
	Candidate                *string `json:"candidate"`
	File                     *string `json:"file"`
	Transaction              *string `json:"transaction"`
	OriginalSubmission       *string `json:"original_submission"`
	BackReferenceTransaction *string `json:"back_reference_transaction"`
	BackReferenceSchedule    *string `json:"back_reference_schedule"`
	LineLabel                *string `json:"line_label"`
}

type Group struct {
	Shape    Shape    `json:"shape"`
	Measures Measures `json:"measures"`
	Example  Example  `json:"example"`
}

type Input struct {
	FactSetID            string `json:"fact_set_id"`
	ManifestSHA256       string `json:"manifest_sha256"`
	SourceReleaseID      string `json:"source_release_id"`
	SourceArtifactSHA256 string `json:"source_artifact_sha256"`
	Facts                uint64 `json:"facts"`
	Shards               uint64 `json:"shards"`
}

type Result struct {
	SchemaVersion  string    `json:"schema_version"`
	Status         string    `json:"status"`
	Cycle          string    `json:"cycle"`
	StartedAt      time.Time `json:"started_at"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	Workers        int       `json:"workers"`
	Input          Input     `json:"input"`
	Measures       Measures  `json:"measures"`
	Groups         []Group   `json:"groups"`
	ProfileSHA256  string    `json:"profile_sha256"`
	Caveats        []string  `json:"caveats"`
}
