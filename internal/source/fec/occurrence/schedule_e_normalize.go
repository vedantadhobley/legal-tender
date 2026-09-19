package occurrence

import (
	"fmt"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulee"
)

type ScheduleEMoneyObservation struct {
	SemanticRole       string  `json:"semantic_role"`
	Currency           string  `json:"currency"`
	RawValue           *string `json:"raw_value"`
	ReportedMinorUnits *string `json:"reported_minor_units"`
	SourceScale        *int    `json:"source_scale"`
	ObservationState   string  `json:"observation_state"`
	MeasurementKind    string  `json:"measurement_kind"`
	SourceRuleVersion  string  `json:"source_rule_version"`
}

type ScheduleESpenderFields struct {
	CommitteeID *string `json:"committee_id"`
	Name        *string `json:"name"`
}

type ScheduleEPayeeFields struct {
	Name                  *string `json:"name"`
	FirstName             *string `json:"first_name"`
	MiddleName            *string `json:"middle_name"`
	LastName              *string `json:"last_name"`
	Prefix                *string `json:"prefix"`
	Suffix                *string `json:"suffix"`
	Street1               *string `json:"street_1"`
	Street2               *string `json:"street_2"`
	City                  *string `json:"city"`
	State                 *string `json:"state"`
	ZIP                   *string `json:"zip"`
	EntityTypeCode        *string `json:"entity_type_code"`
	EntityTypeDescription *string `json:"entity_type_description"`
	NormalizedNameText    *string `json:"normalized_name_text"`
}

type ScheduleECandidateFields struct {
	CandidateID              *string `json:"candidate_id"`
	Name                     *string `json:"name"`
	FirstName                *string `json:"first_name"`
	MiddleName               *string `json:"middle_name"`
	LastName                 *string `json:"last_name"`
	Prefix                   *string `json:"prefix"`
	Suffix                   *string `json:"suffix"`
	OfficeCode               *string `json:"office_code"`
	Office                   *string `json:"office"`
	OfficeState              *string `json:"office_state"`
	OfficeStateName          *string `json:"office_state_name"`
	OfficeDistrict           *string `json:"office_district"`
	SupportOpposeCode        *string `json:"support_oppose_code"`
	SupportOpposeDescription *string `json:"support_oppose_description"`
}

type ScheduleEElectionFields struct {
	ElectionTypeCode           *string                   `json:"election_type_code"`
	FECElectionTypeDescription *string                   `json:"fec_election_type_description"`
	Cycle                      int64                     `json:"cycle"`
	CalendarYTDOfficeSought    ScheduleEMoneyObservation `json:"calendar_ytd_office_sought"`
}

type ScheduleEExpenditureFields struct {
	Description         *string                   `json:"description"`
	CategoryCode        *string                   `json:"category_code"`
	Category            *string                   `json:"category"`
	Amount              ScheduleEMoneyObservation `json:"amount"`
	ExpenditureAtLocal  *string                   `json:"expenditure_at_local"`
	ExpenditureOn       *string                   `json:"expenditure_on"`
	DisseminatedAtLocal *string                   `json:"disseminated_at_local"`
	DisseminatedOn      *string                   `json:"disseminated_on"`
	ExpenditureTypeCode *string                   `json:"expenditure_type_code"`
	ExpenditureType     *string                   `json:"expenditure_type"`
	MemoCode            *string                   `json:"memo_code"`
	MemoCodeDescription *string                   `json:"memo_code_description"`
	MemoText            *string                   `json:"memo_text"`
}

type ScheduleEConduitFields struct {
	CommitteeID *string `json:"committee_id"`
	Name        *string `json:"name"`
	Street1     *string `json:"street_1"`
	Street2     *string `json:"street_2"`
	City        *string `json:"city"`
	State       *string `json:"state"`
	ZIP         *string `json:"zip"`
}

type ScheduleECertificationFields struct {
	IndependentExpenditureSigner   *string `json:"independent_expenditure_signer"`
	IndependentExpenditureSignedAt *string `json:"independent_expenditure_signed_at_local"`
	NotarySigner                   *string `json:"notary_signer"`
	NotarySignedAt                 *string `json:"notary_signed_at_local"`
	NotaryCommissionExpiresAt      *string `json:"notary_commission_expires_at_local"`
}

type ScheduleEFilerFields struct {
	FirstName  *string `json:"first_name"`
	MiddleName *string `json:"middle_name"`
	LastName   *string `json:"last_name"`
	Prefix     *string `json:"prefix"`
	Suffix     *string `json:"suffix"`
}

type ScheduleEFilingFields struct {
	ActionCode                 *string `json:"action_code"`
	Action                     *string `json:"action"`
	TransactionID              *string `json:"transaction_id"`
	BackReferenceTransactionID *string `json:"back_reference_transaction_id"`
	BackReferenceScheduleName  *string `json:"back_reference_schedule_name"`
	ScheduleTypeCode           *string `json:"schedule_type_code"`
	ScheduleType               *string `json:"schedule_type"`
	LineNumber                 *string `json:"line_number"`
	ImageNumber                *string `json:"image_number"`
	FileNumber                 *int64  `json:"file_number"`
	LinkID                     *string `json:"link_id"`
	OriginalSubmissionID       *string `json:"original_submission_id"`
	SubmissionID               string  `json:"submission_id"`
	FilingForm                 string  `json:"filing_form"`
	ReportTypeCode             *string `json:"report_type_code"`
	ReportYear                 *int64  `json:"report_year"`
	PDFURL                     *string `json:"pdf_url"`
	PublisherLoadedAtLocal     *string `json:"publisher_loaded_at_local"`
}

type ScheduleEIndependentExpenditureTypedFields struct {
	Spender       ScheduleESpenderFields       `json:"spender"`
	Payee         ScheduleEPayeeFields         `json:"payee"`
	Candidate     ScheduleECandidateFields     `json:"candidate"`
	Election      ScheduleEElectionFields      `json:"election"`
	Expenditure   ScheduleEExpenditureFields   `json:"expenditure"`
	Conduit       ScheduleEConduitFields       `json:"conduit"`
	Certification ScheduleECertificationFields `json:"certification"`
	Filer         ScheduleEFilerFields         `json:"filer"`
	Filing        ScheduleEFilingFields        `json:"filing"`
}

func normalizeScheduleEIndependentExpenditure(record schedulee.Record) (ScheduleEIndependentExpenditureTypedFields, []string, error) {
	text := func(name string) *string {
		value, ok := record.ValueByName(name)
		if !ok || value.Null {
			return nil
		}
		copy := value.Lexeme
		return &copy
	}
	issues := make([]string, 0, 4)
	integer := func(name string) *int64 {
		value := text(name)
		if value == nil {
			return nil
		}
		parsed, err := strconv.ParseInt(*value, 10, 64)
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

	committeeID := text("cmte_id")
	submissionID := text("sub_id")
	filingForm := text("filing_form")
	cycle := integer("election_cycle")
	if submissionID == nil || filingForm == nil || cycle == nil {
		return ScheduleEIndependentExpenditureTypedFields{}, nil, fmt.Errorf("validated Schedule E record lacks selected-cycle source identity")
	}
	expenditureAt, expenditureOn := localTimestamp("exp_dt")
	disseminatedAt, disseminatedOn := localTimestamp("dissem_dt")
	loadedAt, _ := localTimestamp("pg_date")
	independentSignedAt, _ := localTimestamp("indt_sign_dt")
	notarySignedAt, _ := localTimestamp("notary_sign_dt")
	notaryExpiresAt, _ := localTimestamp("notary_commission_exprtn_dt")
	amount, amountIssue := normalizeScheduleEMoney(record, "exp_amt", "independent_expenditure")
	if amountIssue != "" {
		issues = append(issues, "exp_amt_"+amountIssue)
	}
	ytd, ytdIssue := normalizeScheduleEMoney(record, "cal_ytd_ofc_sought", "calendar_ytd_office_sought")
	if ytdIssue != "" {
		issues = append(issues, "cal_ytd_ofc_sought_"+ytdIssue)
	}

	return ScheduleEIndependentExpenditureTypedFields{
		Spender: ScheduleESpenderFields{CommitteeID: committeeID, Name: text("cmte_nm")},
		Payee: ScheduleEPayeeFields{
			Name: text("pye_nm"), FirstName: text("payee_f_nm"), MiddleName: text("payee_m_nm"), LastName: text("payee_l_nm"),
			Prefix: text("payee_prefix"), Suffix: text("payee_suffix"), Street1: text("pye_st1"), Street2: text("pye_st2"),
			City: text("pye_city"), State: text("pye_st"), ZIP: text("pye_zip"), EntityTypeCode: text("entity_tp"),
			EntityTypeDescription: text("entity_tp_desc"), NormalizedNameText: text("payee_name_text"),
		},
		Candidate: ScheduleECandidateFields{
			CandidateID: text("s_o_cand_id"), Name: text("s_o_cand_nm"), FirstName: text("s_o_cand_nm_first"), MiddleName: text("s_o_cand_m_nm"),
			LastName: text("s_o_cand_nm_last"), Prefix: text("s_o_cand_prefix"), Suffix: text("s_o_cand_suffix"), OfficeCode: text("s_o_cand_office"),
			Office: text("s_o_cand_office_desc"), OfficeState: text("s_o_cand_office_st"), OfficeStateName: text("s_o_cand_office_st_desc"),
			OfficeDistrict: text("s_o_cand_office_district"), SupportOpposeCode: text("s_o_ind"), SupportOpposeDescription: text("s_o_ind_desc"),
		},
		Election: ScheduleEElectionFields{
			ElectionTypeCode: text("election_tp"), FECElectionTypeDescription: text("fec_election_tp_desc"),
			Cycle: *cycle, CalendarYTDOfficeSought: ytd,
		},
		Expenditure: ScheduleEExpenditureFields{
			Description: text("exp_desc"), CategoryCode: text("catg_cd"), Category: text("catg_cd_desc"), Amount: amount,
			ExpenditureAtLocal: expenditureAt, ExpenditureOn: expenditureOn, DisseminatedAtLocal: disseminatedAt, DisseminatedOn: disseminatedOn,
			ExpenditureTypeCode: text("exp_tp"), ExpenditureType: text("exp_tp_desc"), MemoCode: text("memo_cd"),
			MemoCodeDescription: text("memo_cd_desc"), MemoText: text("memo_text"),
		},
		Conduit: ScheduleEConduitFields{
			CommitteeID: text("conduit_cmte_id"), Name: text("conduit_cmte_nm"), Street1: text("conduit_cmte_st1"), Street2: text("conduit_cmte_st2"),
			City: text("conduit_cmte_city"), State: text("conduit_cmte_st"), ZIP: text("conduit_cmte_zip"),
		},
		Certification: ScheduleECertificationFields{
			IndependentExpenditureSigner: text("indt_sign_nm"), IndependentExpenditureSignedAt: independentSignedAt,
			NotarySigner: text("notary_sign_nm"), NotarySignedAt: notarySignedAt, NotaryCommissionExpiresAt: notaryExpiresAt,
		},
		Filer: ScheduleEFilerFields{
			FirstName: text("filer_f_nm"), MiddleName: text("filer_m_nm"), LastName: text("filer_l_nm"),
			Prefix: text("filer_prefix"), Suffix: text("filer_suffix"),
		},
		Filing: ScheduleEFilingFields{
			ActionCode: text("action_cd"), Action: text("action_cd_desc"), TransactionID: text("tran_id"),
			BackReferenceTransactionID: text("back_ref_tran_id"), BackReferenceScheduleName: text("back_ref_sched_nm"),
			ScheduleTypeCode: text("schedule_type"), ScheduleType: text("schedule_type_desc"), LineNumber: text("line_num"), ImageNumber: text("image_num"),
			FileNumber: integer("file_num"), LinkID: text("link_id"), OriginalSubmissionID: text("orig_sub_id"), SubmissionID: *submissionID,
			FilingForm: *filingForm, ReportTypeCode: text("rpt_tp"), ReportYear: integer("rpt_yr"), PDFURL: text("pdf_url"), PublisherLoadedAtLocal: loadedAt,
		},
	}, issues, nil
}

func normalizeScheduleEMoney(record schedulee.Record, fieldName, role string) (ScheduleEMoneyObservation, string) {
	result := ScheduleEMoneyObservation{
		SemanticRole: role, Currency: "USD", MeasurementKind: "reported_point",
		SourceRuleVersion: ScheduleESourceContract,
	}
	value, ok := record.ValueByName(fieldName)
	if !ok {
		result.ObservationState = "invalid"
		return result, "missing_field"
	}
	if value.Null {
		result.ObservationState = "source_null"
		return result, ""
	}
	raw := value.Lexeme
	result.RawValue = &raw
	minor, scale, issue := parseUSDMinorUnits(raw)
	result.SourceScale = &scale
	if issue != "" {
		result.ObservationState = "invalid"
		return result, issue
	}
	result.ReportedMinorUnits = &minor
	result.ObservationState = "reported_value"
	return result, ""
}
