// Package fecmastergaps audits candidate and committee references that are
// absent from the cycle-scoped FEC master facts used by a receipt calculation.
package fecmastergaps

import (
	"time"

	fecreceipts "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

const (
	SchemaVersion = "legal-tender.audit.fec.receipt-master-gaps.v1"
	AuditVersion  = "legal-tender.audit.fec.receipt-master-gaps.v1"
)

type Options struct {
	StorageRoot                      string
	Cycle                            string
	CalculationManifestPath          string
	CandidateManifestPath            string
	CommitteeManifestPath            string
	CandidateComparisonManifestPaths []string
	CommitteeComparisonManifestPaths []string
	CandidateHistoryManifestPaths    []string
	CommitteeHistoryManifestPaths    []string
	Clock                            func() time.Time
	Progress                         func(string)
}

type Report struct {
	SchemaVersion   string         `json:"schema_version"`
	AuditVersion    string         `json:"audit_version"`
	Cycle           string         `json:"cycle"`
	SourceReleaseID string         `json:"source_release_id"`
	AuditedAt       time.Time      `json:"audited_at"`
	Inputs          Inputs         `json:"inputs"`
	Counts          Counts         `json:"counts"`
	Candidates      []CandidateGap `json:"candidates"`
	Committees      []CommitteeGap `json:"committees"`
	Checks          []Check        `json:"checks"`
}

type Inputs struct {
	Calculation                CalculationReference `json:"calculation"`
	CurrentCandidateMaster     FactSetReference     `json:"current_candidate_master"`
	CurrentCommitteeMaster     FactSetReference     `json:"current_committee_master"`
	CandidateComparisonMasters []FactSetReference   `json:"candidate_same_cycle_comparison_masters"`
	CommitteeComparisonMasters []FactSetReference   `json:"committee_same_cycle_comparison_masters"`
	CandidateHistoryMasters    []FactSetReference   `json:"candidate_history_masters"`
	CommitteeHistoryMasters    []FactSetReference   `json:"committee_history_masters"`
	CalculationClassicFactSets []FactSetReference   `json:"calculation_classic_fact_sets"`
}

type CalculationReference struct {
	CalculationSetID string `json:"calculation_set_id"`
	Cycle            string `json:"cycle"`
	SourceReleaseID  string `json:"source_release_id"`
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

type Counts struct {
	CalculationResults                   uint64 `json:"calculation_results"`
	CandidateReferences                  uint64 `json:"candidate_references"`
	CommitteeReferences                  uint64 `json:"committee_references"`
	CandidatesMissingCurrentMaster       uint64 `json:"candidates_missing_current_master"`
	CandidatesFoundInSameCycleComparison uint64 `json:"candidates_found_in_same_cycle_comparison"`
	CandidatesFoundOnlyInHistory         uint64 `json:"candidates_found_only_in_history"`
	CandidatesFoundInBothComparisons     uint64 `json:"candidates_found_in_both_comparisons"`
	CandidatesAbsentFromAllComparisons   uint64 `json:"candidates_absent_from_all_comparisons"`
	CommitteesMissingCurrentMaster       uint64 `json:"committees_missing_current_master"`
	CommitteesFoundInSameCycleComparison uint64 `json:"committees_found_in_same_cycle_comparison"`
	CommitteesFoundOnlyInHistory         uint64 `json:"committees_found_only_in_history"`
	CommitteesFoundInBothComparisons     uint64 `json:"committees_found_in_both_comparisons"`
	CommitteesAbsentFromAllComparisons   uint64 `json:"committees_absent_from_all_comparisons"`
}

type CandidateGap struct {
	CandidateID                string                              `json:"candidate_id"`
	State                      string                              `json:"state"`
	Origins                    []string                            `json:"origins"`
	MoneyAmount                fecreceipts.MoneyAmount             `json:"money_amount"`
	IncludedRecords            fecreceipts.IncludedCounts          `json:"included_records"`
	CommitteeRelationships     []fecreceipts.CommitteeRelationship `json:"committee_relationships"`
	SourceSummaries            []fecreceipts.SourceSummary         `json:"source_summaries"`
	LinkageFacts               []LinkageFactAssertion              `json:"linkage_facts"`
	SummaryFacts               []CandidateSummaryAssertion         `json:"summary_facts"`
	SameCycleComparisonMasters []CandidateHistoricalAssertion      `json:"same_cycle_comparison_masters"`
	HistoricalMasters          []CandidateHistoricalAssertion      `json:"historical_masters"`
}

type CommitteeGap struct {
	CommitteeID                string                         `json:"committee_id"`
	State                      string                         `json:"state"`
	Origins                    []string                       `json:"origins"`
	CandidateIDs               []string                       `json:"candidate_ids"`
	AttributedAmountMinorUnits string                         `json:"attributed_amount_minor_units"`
	IncludedRecords            fecreceipts.IncludedCounts     `json:"included_records"`
	LinkageAssertions          []CommitteeLinkageAssertion    `json:"linkage_assertions"`
	LinkageFacts               []LinkageFactAssertion         `json:"linkage_facts"`
	SameCycleComparisonMasters []CommitteeHistoricalAssertion `json:"same_cycle_comparison_masters"`
	HistoricalMasters          []CommitteeHistoricalAssertion `json:"historical_masters"`
}

type CandidateHistoricalAssertion struct {
	Cycle                        string  `json:"cycle"`
	SourceReleaseID              string  `json:"source_release_id"`
	FactSetID                    string  `json:"fact_set_id"`
	FactID                       string  `json:"fact_id"`
	Name                         string  `json:"name"`
	PartyAffiliation             string  `json:"party_affiliation"`
	Office                       string  `json:"office"`
	OfficeState                  string  `json:"office_state"`
	OfficeDistrict               string  `json:"office_district"`
	CandidateStatus              string  `json:"candidate_status"`
	PrincipalCampaignCommitteeID *string `json:"principal_campaign_committee_id,omitempty"`
}

type CommitteeHistoricalAssertion struct {
	Cycle                 string  `json:"cycle"`
	SourceReleaseID       string  `json:"source_release_id"`
	FactSetID             string  `json:"fact_set_id"`
	FactID                string  `json:"fact_id"`
	Name                  string  `json:"name"`
	PartyAffiliation      string  `json:"party_affiliation"`
	DesignationCode       string  `json:"designation_code"`
	CommitteeTypeCode     string  `json:"committee_type_code"`
	OrganizationTypeCode  string  `json:"organization_type_code"`
	ConnectedOrganization string  `json:"connected_organization"`
	CandidateID           *string `json:"candidate_id,omitempty"`
}

type CommitteeLinkageAssertion struct {
	CandidateID       string   `json:"candidate_id"`
	State             string   `json:"state"`
	DesignationCodes  []string `json:"designation_codes"`
	SupportingFactIDs []string `json:"supporting_fact_ids"`
}

type LinkageFactAssertion struct {
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
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details"`
}
