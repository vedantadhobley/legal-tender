// Package fecflowmastergaps audits committee IDs referenced by the receiver-
// reported committee-flow calculation but absent from its exact committee
// master.
package fecflowmastergaps

import "time"

const (
	SchemaVersion = "legal-tender.audit.fec.receiver-flow-master-gaps.v1"
	AuditVersion  = "legal-tender.audit.fec.receiver-flow-master-gaps.v1"
)

type Options struct {
	StorageRoot                      string
	Cycle                            string
	ReadinessBundlePath              string
	CommitteeComparisonManifestPaths []string
	CommitteeHistoryManifestPaths    []string
	LinkageManifestPath              string
	SummaryManifestPaths             []string
	RawHistoryArchives               []RawHistoryArchiveInput
	TraceSourceReceipts              bool
	Clock                            func() time.Time
	Progress                         func(string)
}

type RawHistoryArchiveInput struct {
	Cycle string
	Path  string
}

type Report struct {
	SchemaVersion   string         `json:"schema_version"`
	AuditVersion    string         `json:"audit_version"`
	Cycle           string         `json:"cycle"`
	SourceReleaseID string         `json:"source_release_id"`
	AuditedAt       time.Time      `json:"audited_at"`
	Inputs          Inputs         `json:"inputs"`
	Counts          Counts         `json:"counts"`
	Exposure        ExposureReport `json:"exposure"`
	Committees      []CommitteeGap `json:"committees"`
	Checks          []Check        `json:"checks"`
}

type Inputs struct {
	ReadinessBundle        BundleReference         `json:"readiness_bundle"`
	Calculation            CalculationReference    `json:"calculation"`
	ScheduleAFacts         *ScheduleAFactReference `json:"schedule_a_facts,omitempty"`
	CurrentCommitteeMaster FactSetReference        `json:"current_committee_master"`
	SameCycleComparisons   []FactSetReference      `json:"same_cycle_comparison_masters"`
	NormalizedHistory      []FactSetReference      `json:"normalized_history_masters"`
	RawHistory             []RawHistoryArchive     `json:"raw_history_archives"`
	LinkageFacts           FactSetReference        `json:"candidate_committee_linkage"`
	SummaryFacts           []FactSetReference      `json:"candidate_summary_fact_sets"`
}

type ScheduleAFactReference struct {
	FactSetID             string `json:"fact_set_id"`
	ManifestSHA256        string `json:"manifest_sha256"`
	PhysicalSchemaVersion string `json:"physical_schema_version"`
	Facts                 uint64 `json:"facts"`
	Shards                int    `json:"shards"`
}

type BundleReference struct {
	BundleID       string `json:"bundle_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type CalculationReference struct {
	CalculationSetID string `json:"calculation_set_id"`
	ManifestSHA256   string `json:"manifest_sha256"`
	ResultsSHA256    string `json:"results_sha256"`
}

type FactSetReference struct {
	Dataset         string `json:"dataset"`
	Cycle           string `json:"cycle"`
	SourceReleaseID string `json:"source_release_id"`
	FactSetID       string `json:"fact_set_id"`
	ManifestSHA256  string `json:"manifest_sha256"`
	FactsSHA256     string `json:"facts_sha256"`
}

type RawHistoryArchive struct {
	Cycle            string             `json:"cycle"`
	SourceURL        string             `json:"source_url"`
	StorageKey       string             `json:"storage_key"`
	ArchiveSHA256    string             `json:"archive_sha256"`
	ArchiveBytes     uint64             `json:"archive_bytes"`
	Member           string             `json:"member"`
	MemberBytes      uint64             `json:"member_bytes"`
	CommitteeRecords uint64             `json:"committee_records"`
	CleanRecords     uint64             `json:"clean_records"`
	IssueRecords     uint64             `json:"issue_records"`
	IssueCounts      []SourceIssueCount `json:"issue_counts"`
}

type SourceIssueCount struct {
	Code string `json:"code"`
	Rows uint64 `json:"rows"`
}

type Counts struct {
	CalculationResults                        uint64 `json:"calculation_results"`
	ReferencedCommittees                      uint64 `json:"referenced_committees"`
	CommitteesMissingCurrentMaster            uint64 `json:"committees_missing_current_master"`
	FoundInSameCycleComparison                uint64 `json:"found_in_same_cycle_comparison"`
	FoundOnlyInHistory                        uint64 `json:"found_only_in_history"`
	FoundInSameCycleComparisonAndHistory      uint64 `json:"found_in_same_cycle_comparison_and_history"`
	AbsentFromAllAuditedMasters               uint64 `json:"absent_from_all_audited_masters"`
	MissingSourceCommittees                   uint64 `json:"missing_source_committees"`
	MissingRecipientCommittees                uint64 `json:"missing_recipient_committees"`
	MissingCommitteesInBothEndpointRoles      uint64 `json:"missing_committees_in_both_endpoint_roles"`
	MissingCommitteesWithLinkage              uint64 `json:"missing_committees_with_linkage"`
	MissingCommitteesWithoutLinkage           uint64 `json:"missing_committees_without_linkage"`
	LinkedCandidateReferences                 uint64 `json:"linked_candidate_references"`
	LinkedCandidatesInAllCandidatesSummary    uint64 `json:"linked_candidates_in_all_candidates_summary"`
	LinkedCandidatesInCurrentCampaignsSummary uint64 `json:"linked_candidates_in_current_campaigns_summary"`
	LinkedCandidatesAbsentFromSummaries       uint64 `json:"linked_candidates_absent_from_summaries"`
	AbsentMasterSourceReceiptEvidence         uint64 `json:"absent_master_source_receipt_evidence"`
}

type ExposureReport struct {
	AllCalculationResults EdgeExposure `json:"all_calculation_results"`
	AnyMissingEndpoint    EdgeExposure `json:"any_missing_endpoint"`
	OnlyMissingSource     EdgeExposure `json:"only_missing_source"`
	OnlyMissingRecipient  EdgeExposure `json:"only_missing_recipient"`
	BothMissingEndpoints  EdgeExposure `json:"both_missing_endpoints"`
}

type EdgeExposure struct {
	ResultGroups           uint64         `json:"result_groups"`
	ReceiptRows            uint64         `json:"receipt_rows"`
	PositiveRows           uint64         `json:"positive_rows"`
	NegativeRows           uint64         `json:"negative_rows"`
	ZeroRows               uint64         `json:"zero_rows"`
	SignedAmountMinorUnits string         `json:"signed_amount_minor_units"`
	Roles                  []RoleExposure `json:"roles"`
}

type RoleExposure struct {
	ReceiptRole            string `json:"receipt_role"`
	ResultGroups           uint64 `json:"result_groups"`
	ReceiptRows            uint64 `json:"receipt_rows"`
	SignedAmountMinorUnits string `json:"signed_amount_minor_units"`
}

type CommitteeGap struct {
	CommitteeID                string                         `json:"committee_id"`
	State                      string                         `json:"state"`
	EndpointRoles              []string                       `json:"endpoint_roles"`
	Outgoing                   EdgeExposure                   `json:"outgoing"`
	Incoming                   EdgeExposure                   `json:"incoming"`
	CandidateIDs               []string                       `json:"candidate_ids"`
	LinkageFacts               []LinkageAssertion             `json:"linkage_facts"`
	CandidateSummaries         []CandidateSummaryAssertion    `json:"candidate_summaries"`
	SourceReceipts             []SourceReceiptAssertion       `json:"source_receipts"`
	SameCycleComparisonMasters []CommitteeHistoricalAssertion `json:"same_cycle_comparison_masters"`
	HistoricalMasters          []CommitteeHistoricalAssertion `json:"historical_masters"`
}

// SourceReceiptAssertion identifies one exact Schedule A row that contributed
// to a missing source committee's receiver-reported flow. It is evidence for
// the source ID, not a committee-master assertion.
type SourceReceiptAssertion struct {
	SourceRowOrdinal     uint64  `json:"source_row_ordinal"`
	SourceRawByteOffset  uint64  `json:"source_raw_byte_offset"`
	SourceRawByteLength  uint64  `json:"source_raw_byte_length"`
	RecipientCommitteeID string  `json:"recipient_committee_id"`
	ContributorID        *string `json:"contributor_id,omitempty"`
	CleanContributorID   *string `json:"clean_contributor_id,omitempty"`
	ContributorName      *string `json:"contributor_name,omitempty"`
	EntityTypeCode       *string `json:"entity_type_code,omitempty"`
	ReceiptTypeCode      *string `json:"receipt_type_code,omitempty"`
	ReceiptRole          string  `json:"receipt_role"`
	AmountMinorUnits     string  `json:"amount_minor_units"`
	TransactionID        *string `json:"transaction_id,omitempty"`
	FilingForm           string  `json:"filing_form"`
	FileNumber           *string `json:"file_number,omitempty"`
	SubID                string  `json:"sub_id"`
}

type CommitteeHistoricalAssertion struct {
	Cycle                 string   `json:"cycle"`
	SourceKind            string   `json:"source_kind"`
	SourceReleaseID       string   `json:"source_release_id,omitempty"`
	FactSetID             string   `json:"fact_set_id,omitempty"`
	FactID                string   `json:"fact_id,omitempty"`
	ArchiveSHA256         string   `json:"archive_sha256,omitempty"`
	SourceRow             uint64   `json:"source_row,omitempty"`
	SourceRowSHA256       string   `json:"source_row_sha256,omitempty"`
	IssueCodes            []string `json:"issue_codes"`
	Name                  string   `json:"name"`
	PartyAffiliation      string   `json:"party_affiliation"`
	DesignationCode       string   `json:"designation_code"`
	CommitteeTypeCode     string   `json:"committee_type_code"`
	OrganizationTypeCode  string   `json:"organization_type_code"`
	ConnectedOrganization string   `json:"connected_organization"`
	CandidateID           *string  `json:"candidate_id,omitempty"`
}

type LinkageAssertion struct {
	FactID                string `json:"fact_id"`
	FactSetID             string `json:"fact_set_id"`
	CandidateID           string `json:"candidate_id"`
	CandidateElectionYear int    `json:"candidate_election_year"`
	FECElectionYear       int    `json:"fec_election_year"`
	SourceCycle           int    `json:"source_cycle"`
	CommitteeID           string `json:"committee_id"`
	CommitteeTypeCode     string `json:"committee_type_code"`
	DesignationCode       string `json:"designation_code"`
	LinkageID             string `json:"linkage_id"`
}

type CandidateSummaryAssertion struct {
	FactID           string  `json:"fact_id"`
	FactSetID        string  `json:"fact_set_id"`
	Dataset          string  `json:"dataset"`
	CandidateID      string  `json:"candidate_id"`
	Name             string  `json:"name"`
	PartyAffiliation string  `json:"party_affiliation"`
	OfficeState      string  `json:"office_state"`
	OfficeDistrict   string  `json:"office_district"`
	SourceCycle      int     `json:"source_cycle"`
	CoverageThrough  *string `json:"coverage_through,omitempty"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}
