package flowreconciliation

import artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"

const Version = "legal-tender.fec.committee-flow-reconciliation.v1"

type Options struct {
	StorageRoot, OutputRoot, ScheduleA, ScheduleB, Release, Cycle string
	Workers                                                       int
	Progress                                                      func(string)
}

type FactReference struct {
	FactSetID            string `json:"fact_set_id"`
	ManifestSHA256       string `json:"manifest_sha256"`
	SourceReleaseID      string `json:"source_release_id"`
	SourceArtifactSHA256 string `json:"source_artifact_sha256"`
	Facts                uint64 `json:"facts"`
}
type Inputs struct {
	ReleaseID     string        `json:"release_id"`
	ReleaseSHA256 string        `json:"release_sha256"`
	A             FactReference `json:"schedule_a"`
	B             FactReference `json:"schedule_b"`
}
type Policy struct {
	Sender      string       `json:"sender"`
	Receiver    string       `json:"receiver"`
	Reporting   string       `json:"reporting"`
	Matcher     string       `json:"matcher"`
	SenderRules []SenderRule `json:"sender_rules"`
}

type Measures struct {
	Rows   uint64 `json:"rows"`
	Known  uint64 `json:"known_amount_rows"`
	Amount int64  `json:"signed_amount_minor_units,string"`
}

// The result binds each artifact to the exact source fact-set ID.
// A date is a reported calendar date, not a shared transaction timestamp.
type Observation struct {
	Ordinal       uint64 `json:"source_row_ordinal"`
	SubID         string `json:"sub_id"`
	Sender        string `json:"sender_committee_id"`
	Recipient     string `json:"reported_recipient_committee_id"`
	Role          string `json:"flow_role"`
	Type          string `json:"transaction_type"`
	ReportingRole string `json:"reporting_role"`
	Date          *int32 `json:"date_days"`
	Amount        int64  `json:"signed_amount_minor_units,string"`
}
type Bucket struct {
	Key      DecisionKey `json:"key"`
	Measures Measures    `json:"measures"`
}
type Side struct {
	Total        Measures            `json:"total"`
	Decisions    []Bucket            `json:"decisions"`
	Selected     Measures            `json:"selected"`
	Observations artifact.Descriptor `json:"observations"`
}

type Assertion struct {
	ID      string   `json:"assertion_id"`
	State   string   `json:"state"`
	A       []uint64 `json:"schedule_a_ordinals"`
	B       []uint64 `json:"schedule_b_ordinals"`
	AAmount int64    `json:"schedule_a_amount_minor_units,string"`
	BAmount int64    `json:"schedule_b_amount_minor_units,string"`
}
type Summary struct {
	State      string   `json:"state"`
	Components uint64   `json:"components"`
	A          Measures `json:"schedule_a"`
	B          Measures `json:"schedule_b"`
}
type Result struct {
	SchemaVersion    string              `json:"schema_version"`
	CalculationSetID string              `json:"calculation_set_id"`
	Cycle            string              `json:"cycle"`
	State            string              `json:"state"`
	Input            Inputs              `json:"input"`
	Policy           Policy              `json:"policy"`
	A                Side                `json:"schedule_a"`
	B                Side                `json:"schedule_b"`
	Summary          []Summary           `json:"summary"`
	Assertions       artifact.Descriptor `json:"assertions"`
	GraphEligible    bool                `json:"graph_eligible"`
}
