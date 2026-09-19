// Package summaryassertion groups exactly equivalent committee-summary evidence.
// It does not select amendments, repair financial values, or establish cash scope.
package summaryassertion

import (
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/summarypublication"
)

const (
	SchemaVersion = "legal-tender.fec.committee-summary-assertions.v1"
	Policy        = "fec/exact-committee-summary-assertions@1.0.0"
)

type Input struct {
	FactSetID            string `json:"fact_set_id"`
	ManifestSHA256       string `json:"manifest_sha256"`
	SourceReleaseID      string `json:"source_release_id"`
	SourceReleaseSHA256  string `json:"source_release_manifest_sha256"`
	SourceArtifactSHA256 string `json:"source_artifact_sha256"`
}

// Members retain each occurrence, including exact duplicates and invalid candidate IDs.
type Member struct {
	FactID       string                 `json:"fact_id"`
	OccurrenceID string                 `json:"occurrence_id"`
	Ordinal      uint64                 `json:"ordinal"`
	Offset       int64                  `json:"offset"`
	Length       int64                  `json:"length"`
	RawSHA256    string                 `json:"raw_sha256"`
	CandidateRaw string                 `json:"candidate_raw"`
	Candidate    committeesummary.Value `json:"candidate"`
}

type Operand struct {
	Field       string                      `json:"field"`
	Coefficient int                         `json:"coefficient"`
	Raw         string                      `json:"raw"`
	Value       committeesummary.MoneyValue `json:"value"`
}

// Delta is left minus right in signed cents, not a missing receipt or a correction.
type Equation struct {
	State    string    `json:"state"`
	Operands []Operand `json:"operands"`
	Delta    *string   `json:"delta_minor_units"`
}

type Assertion struct {
	ID                   string                   `json:"assertion_id"`
	EvidenceSHA256       string                   `json:"non_candidate_fields_sha256"`
	RepresentativeFactID string                   `json:"representative_fact_id"`
	CommitteeType        string                   `json:"committee_type_raw"`
	Designation          string                   `json:"designation_raw"`
	CoverageStart        committeesummary.Value   `json:"coverage_start"`
	CoverageEnd          committeesummary.Value   `json:"coverage_end"`
	Issues               []committeesummary.Issue `json:"representative_issues"`
	Members              []Member                 `json:"members"`
	Equations            map[string]Equation      `json:"diagnostic_equations"`
}

type Committee struct {
	CommitteeID    string      `json:"committee_id"`
	State          string      `json:"state"`
	ConflictFields []string    `json:"conflict_fields"`
	Assertions     []Assertion `json:"assertions"`
}

type Unindexed struct {
	CommitteeRaw string `json:"committee_raw"`
	Reason       string `json:"reason"`
	Member       Member `json:"member"`
}

type Counts struct {
	SourceRows            uint64 `json:"source_rows"`
	IndexedRows           uint64 `json:"indexed_rows"`
	UnindexedRows         uint64 `json:"unindexed_rows"`
	Committees            uint64 `json:"committees"`
	Assertions            uint64 `json:"assertions"`
	RepeatedEvidenceRows  uint64 `json:"repeated_evidence_rows"`
	ConflictingCommittees uint64 `json:"conflicting_committees"`
}

type Result struct {
	SchemaVersion               string      `json:"schema_version"`
	CalculationID               string      `json:"calculation_id"`
	Policy                      string      `json:"policy"`
	State                       string      `json:"state"`
	Cycle                       string      `json:"cycle"`
	Input                       Input       `json:"input"`
	ExcludedGroupingFields      []string    `json:"excluded_grouping_fields"`
	Counts                      Counts      `json:"counts"`
	Committees                  []Committee `json:"committees"`
	Unindexed                   []Unindexed `json:"unindexed"`
	FinancialUseEligible        bool        `json:"financial_use_eligible"`
	TerminalAttributionEligible bool        `json:"terminal_attribution_eligible"`
}

func member(f summarypublication.Fact) Member {
	r := f.Record
	return Member{f.FactID, f.OccurrenceID, r.Ordinal, r.Offset, r.Length, r.RawSHA256, r.SourceFields["CAND_ID"], r.Identifiers["CAND_ID"]}
}
