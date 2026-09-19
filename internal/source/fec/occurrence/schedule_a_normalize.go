package occurrence

import (
	"fmt"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

type ScheduleAMoneyObservation struct {
	SemanticRole       string  `json:"semantic_role"`
	Currency           string  `json:"currency"`
	RawValue           *string `json:"raw_value"`
	ReportedMinorUnits *string `json:"reported_minor_units"`
	SourceScale        *int    `json:"source_scale"`
	ObservationState   string  `json:"observation_state"`
	MeasurementKind    string  `json:"measurement_kind"`
	SourceRuleVersion  string  `json:"source_rule_version"`
}

type ScheduleARecipientFields struct {
	CommitteeID          *string `json:"committee_id"`
	Name                 *string `json:"name"`
	CommitteeTypeCode    *string `json:"committee_type_code"`
	OrganizationTypeCode *string `json:"organization_type_code"`
	DesignationCode      *string `json:"designation_code"`
}

type ScheduleAContributorFields struct {
	ContributorID              *string `json:"contributor_id"`
	CleanContributorID         *string `json:"clean_contributor_id"`
	Name                       *string `json:"name"`
	FirstName                  *string `json:"first_name"`
	MiddleName                 *string `json:"middle_name"`
	LastName                   *string `json:"last_name"`
	Prefix                     *string `json:"prefix"`
	Suffix                     *string `json:"suffix"`
	Street1                    *string `json:"street_1"`
	Street2                    *string `json:"street_2"`
	City                       *string `json:"city"`
	State                      *string `json:"state"`
	ZIP                        *string `json:"zip"`
	EntityTypeCode             *string `json:"entity_type_code"`
	EntityTypeDescription      *string `json:"entity_type_description"`
	Employer                   *string `json:"employer"`
	Occupation                 *string `json:"occupation"`
	NormalizedNameText         *string `json:"normalized_name_text"`
	NormalizedEmployerText     *string `json:"normalized_employer_text"`
	NormalizedOccupationText   *string `json:"normalized_occupation_text"`
	DonorCommitteeName         *string `json:"donor_committee_name"`
	NationalCommitteeNonfed    *string `json:"national_committee_nonfed_account"`
	PublisherClassedIndividual *bool   `json:"publisher_classed_individual"`
}

type ScheduleACandidateFields struct {
	CandidateID     *string `json:"candidate_id"`
	Name            *string `json:"name"`
	FirstName       *string `json:"first_name"`
	MiddleName      *string `json:"middle_name"`
	LastName        *string `json:"last_name"`
	Prefix          *string `json:"prefix"`
	Suffix          *string `json:"suffix"`
	OfficeCode      *string `json:"office_code"`
	Office          *string `json:"office"`
	OfficeState     *string `json:"office_state"`
	OfficeStateName *string `json:"office_state_name"`
	OfficeDistrict  *string `json:"office_district"`
}

type ScheduleAConduitFields struct {
	CommitteeID *string `json:"committee_id"`
	Name        *string `json:"name"`
	Street1     *string `json:"street_1"`
	Street2     *string `json:"street_2"`
	City        *string `json:"city"`
	State       *string `json:"state"`
	ZIP         *string `json:"zip"`
}

type ScheduleAElectionFields struct {
	ElectionTypeCode        *string `json:"election_type_code"`
	FECElectionType         *string `json:"fec_election_type"`
	ElectionTypeDescription *string `json:"election_type_description"`
	FECElectionYear         *int64  `json:"fec_election_year"`
	TransactionPeriod       int64   `json:"two_year_transaction_period"`
	IncreasedLimitCode      *string `json:"increased_limit_code"`
}

type ScheduleAReceiptFields struct {
	ReceivedAtLocal      *string                   `json:"received_at_local"`
	ReceivedOn           *string                   `json:"received_on"`
	Amount               ScheduleAMoneyObservation `json:"amount"`
	ContributorAggregate ScheduleAMoneyObservation `json:"contributor_aggregate_ytd"`
	ReceiptTypeCode      *string                   `json:"receipt_type_code"`
	ReceiptType          *string                   `json:"receipt_type"`
	Description          *string                   `json:"description"`
	MemoCode             *string                   `json:"memo_code"`
	MemoCodeDescription  *string                   `json:"memo_code_description"`
	MemoText             *string                   `json:"memo_text"`
	MemoedSubtotal       bool                      `json:"memoed_subtotal"`
	ScheduleTypeCode     *string                   `json:"schedule_type_code"`
	ScheduleType         *string                   `json:"schedule_type"`
}

type ScheduleAFilingFields struct {
	ActionCode                 *string `json:"action_code"`
	Action                     *string `json:"action"`
	TransactionID              *string `json:"transaction_id"`
	BackReferenceTransactionID *string `json:"back_reference_transaction_id"`
	BackReferenceScheduleName  *string `json:"back_reference_schedule_name"`
	LineNumber                 *string `json:"line_number"`
	LineNumberLabel            *string `json:"line_number_label"`
	ImageNumber                *string `json:"image_number"`
	FileNumber                 *string `json:"file_number"`
	LinkID                     *string `json:"link_id"`
	OriginalSubmissionID       *string `json:"original_submission_id"`
	SubmissionID               string  `json:"submission_id"`
	FilingForm                 string  `json:"filing_form"`
	ReportTypeCode             *string `json:"report_type_code"`
	ReportYear                 *int64  `json:"report_year"`
	PDFURL                     *string `json:"pdf_url"`
	PublisherLoadedAtLocal     *string `json:"publisher_loaded_at_local"`
}

type ScheduleAReceiptTypedFields struct {
	Recipient   ScheduleARecipientFields   `json:"recipient"`
	Contributor ScheduleAContributorFields `json:"contributor"`
	Candidate   ScheduleACandidateFields   `json:"candidate"`
	Conduit     ScheduleAConduitFields     `json:"conduit"`
	Election    ScheduleAElectionFields    `json:"election"`
	Receipt     ScheduleAReceiptFields     `json:"receipt"`
	Filing      ScheduleAFilingFields      `json:"filing"`
}

func normalizeScheduleAReceipt(record schedulea.Record) (ScheduleAReceiptTypedFields, []string, error) {
	text := func(name string) *string {
		value, ok := record.ValueByName(name)
		if !ok || value.Null {
			return nil
		}
		copy := value.Lexeme
		return &copy
	}
	boolean := func(name string) *bool {
		value, ok := record.ValueByName(name)
		if !ok || value.Null {
			return nil
		}
		parsed := value.Lexeme == "t"
		return &parsed
	}
	issues := make([]string, 0, 4)
	integer := func(name string) *int64 {
		value, ok := record.ValueByName(name)
		if !ok || value.Null {
			return nil
		}
		parsed, err := strconv.ParseInt(value.Lexeme, 10, 64)
		if err != nil {
			issues = append(issues, name+"_integer_overflow")
			return nil
		}
		return &parsed
	}
	localTimestamp := func(name string) (*string, *string) {
		value := text(name)
		if value == nil {
			return nil, nil
		}
		parsed, err := time.Parse("2006-01-02 15:04:05.999999999", *value)
		if err != nil {
			issues = append(issues, name+"_invalid_calendar_time")
			return value, nil
		}
		date := parsed.Format("2006-01-02")
		return value, &date
	}

	receivedAt, receivedOn := localTimestamp("contb_receipt_dt")
	loadedAt, _ := localTimestamp("pg_date")
	receiptAmount, receiptIssue := normalizeScheduleAMoney(record, "contb_receipt_amt", "receipt")
	if receiptIssue != "" {
		issues = append(issues, "contb_receipt_amt_"+receiptIssue)
	}
	aggregate, aggregateIssue := normalizeScheduleAMoney(record, "contb_aggregate_ytd", "contributor_aggregate_ytd")
	if aggregateIssue != "" {
		issues = append(issues, "contb_aggregate_ytd_"+aggregateIssue)
	}
	period := integer("two_year_transaction_period")
	if period == nil {
		return ScheduleAReceiptTypedFields{}, nil, fmt.Errorf("validated Schedule A record has no typed transaction period")
	}
	submissionID := text("sub_id")
	filingForm := text("filing_form")
	if submissionID == nil || filingForm == nil {
		return ScheduleAReceiptTypedFields{}, nil, fmt.Errorf("validated Schedule A record has no required source identity")
	}
	memo := text("memo_cd")

	return ScheduleAReceiptTypedFields{
		Recipient: ScheduleARecipientFields{
			CommitteeID: text("cmte_id"), Name: text("cmte_nm"), CommitteeTypeCode: text("cmte_tp"),
			OrganizationTypeCode: text("org_tp"), DesignationCode: text("cmte_dsgn"),
		},
		Contributor: ScheduleAContributorFields{
			ContributorID: text("contbr_id"), CleanContributorID: text("clean_contbr_id"), Name: text("contbr_nm"),
			FirstName: text("contbr_nm_first"), MiddleName: text("contbr_m_nm"), LastName: text("contbr_nm_last"),
			Prefix: text("contbr_prefix"), Suffix: text("contbr_suffix"), Street1: text("contbr_st1"), Street2: text("contbr_st2"),
			City: text("contbr_city"), State: text("contbr_st"), ZIP: text("contbr_zip"), EntityTypeCode: text("entity_tp"),
			EntityTypeDescription: text("entity_tp_desc"), Employer: text("contbr_employer"), Occupation: text("contbr_occupation"),
			NormalizedNameText: text("contributor_name_text"), NormalizedEmployerText: text("contributor_employer_text"),
			NormalizedOccupationText: text("contributor_occupation_text"), DonorCommitteeName: text("donor_cmte_nm"),
			NationalCommitteeNonfed: text("national_cmte_nonfed_acct"), PublisherClassedIndividual: boolean("is_individual"),
		},
		Candidate: ScheduleACandidateFields{
			CandidateID: text("cand_id"), Name: text("cand_nm"), FirstName: text("cand_nm_first"), MiddleName: text("cand_m_nm"),
			LastName: text("cand_nm_last"), Prefix: text("cand_prefix"), Suffix: text("cand_suffix"), OfficeCode: text("cand_office"),
			Office: text("cand_office_desc"), OfficeState: text("cand_office_st"), OfficeStateName: text("cand_office_st_desc"),
			OfficeDistrict: text("cand_office_district"),
		},
		Conduit: ScheduleAConduitFields{
			CommitteeID: text("conduit_cmte_id"), Name: text("conduit_cmte_nm"), Street1: text("conduit_cmte_st1"),
			Street2: text("conduit_cmte_st2"), City: text("conduit_cmte_city"), State: text("conduit_cmte_st"), ZIP: text("conduit_cmte_zip"),
		},
		Election: ScheduleAElectionFields{
			ElectionTypeCode: text("election_tp"), FECElectionType: text("fec_election_tp_desc"),
			ElectionTypeDescription: text("election_tp_desc"), FECElectionYear: integer("fec_election_yr"),
			TransactionPeriod: *period, IncreasedLimitCode: text("increased_limit"),
		},
		Receipt: ScheduleAReceiptFields{
			ReceivedAtLocal: receivedAt, ReceivedOn: receivedOn, Amount: receiptAmount, ContributorAggregate: aggregate,
			ReceiptTypeCode: text("receipt_tp"), ReceiptType: text("receipt_tp_desc"), Description: text("receipt_desc"),
			MemoCode: memo, MemoCodeDescription: text("memo_cd_desc"), MemoText: text("memo_text"),
			MemoedSubtotal: memo != nil && *memo == "X", ScheduleTypeCode: text("schedule_type"), ScheduleType: text("schedule_type_desc"),
		},
		Filing: ScheduleAFilingFields{
			ActionCode: text("action_cd"), Action: text("action_cd_desc"), TransactionID: text("tran_id"),
			BackReferenceTransactionID: text("back_ref_tran_id"), BackReferenceScheduleName: text("back_ref_sched_nm"),
			LineNumber: text("line_num"), LineNumberLabel: text("line_number_label"), ImageNumber: text("image_num"),
			FileNumber: text("file_num"), LinkID: text("link_id"), OriginalSubmissionID: text("orig_sub_id"),
			SubmissionID: *submissionID, FilingForm: *filingForm, ReportTypeCode: text("rpt_tp"), ReportYear: integer("rpt_yr"),
			PDFURL: text("pdf_url"), PublisherLoadedAtLocal: loadedAt,
		},
	}, issues, nil
}

func normalizeScheduleAMoney(record schedulea.Record, field, semanticRole string) (ScheduleAMoneyObservation, string) {
	observation := ScheduleAMoneyObservation{
		SemanticRole: semanticRole, Currency: "USD", MeasurementKind: "reported_point",
		SourceRuleVersion: "fec/schedule-a@1.0.0",
	}
	value, ok := record.ValueByName(field)
	if !ok {
		observation.ObservationState = "invalid"
		return observation, "missing_field"
	}
	if value.Null {
		observation.ObservationState = "source_null"
		return observation, ""
	}
	observation.RawValue = &value.Lexeme
	minor, scale, issue := parseUSDMinorUnits(value.Lexeme)
	observation.SourceScale = &scale
	if issue != "" {
		observation.ObservationState = "invalid"
		return observation, issue
	}
	observation.ObservationState = "reported_value"
	observation.ReportedMinorUnits = &minor
	return observation, ""
}
