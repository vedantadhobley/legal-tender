// Package scheduleb owns the processed FEC Schedule B source boundary.
package scheduleb

// FieldCount is the exact width of the processed Schedule B relation.
const FieldCount = 81

const (
	disbursementDateIndex    = 36
	disbursementAmountIndex  = 37
	memoCodeIndex            = 38
	disbursementTypeIndex    = 41
	communicationDateIndex   = 51
	bundledRefundAmountIndex = 53
	actionCodeIndex          = 54
	subIDIndex               = 66
	filingFormIndex          = 67
	transactionPeriodIndex   = 70
	cleanRecipientIDIndex    = 75
)

// Kind identifies the source lexeme rules for one relation column.
type Kind uint8

const (
	KindText Kind = iota
	KindDecimal
	KindInteger
	KindTimestamp
)

// Column describes one ordered field in the processed Schedule B relation.
type Column struct {
	Name      string
	Kind      Kind
	Nullable  bool
	Precision int
	Scale     int
}

var columns = [FieldCount]Column{
	{Name: "cmte_id", Kind: KindText, Nullable: true},
	{Name: "recipient_cmte_id", Kind: KindText, Nullable: true},
	{Name: "recipient_nm", Kind: KindText, Nullable: true},
	{Name: "payee_l_nm", Kind: KindText, Nullable: true},
	{Name: "payee_f_nm", Kind: KindText, Nullable: true},
	{Name: "payee_m_nm", Kind: KindText, Nullable: true},
	{Name: "payee_prefix", Kind: KindText, Nullable: true},
	{Name: "payee_suffix", Kind: KindText, Nullable: true},
	{Name: "payee_employer", Kind: KindText, Nullable: true},
	{Name: "payee_occupation", Kind: KindText, Nullable: true},
	{Name: "recipient_st1", Kind: KindText, Nullable: true},
	{Name: "recipient_st2", Kind: KindText, Nullable: true},
	{Name: "recipient_city", Kind: KindText, Nullable: true},
	{Name: "recipient_st", Kind: KindText, Nullable: true},
	{Name: "recipient_zip", Kind: KindText, Nullable: true},
	{Name: "disb_desc", Kind: KindText, Nullable: true},
	{Name: "catg_cd", Kind: KindText, Nullable: true},
	{Name: "catg_cd_desc", Kind: KindText, Nullable: true},
	{Name: "entity_tp", Kind: KindText, Nullable: true},
	{Name: "entity_tp_desc", Kind: KindText, Nullable: true},
	{Name: "election_tp", Kind: KindText, Nullable: true},
	{Name: "fec_election_tp_desc", Kind: KindText, Nullable: true},
	{Name: "fec_election_tp_year", Kind: KindText, Nullable: true},
	{Name: "election_tp_desc", Kind: KindText, Nullable: true},
	{Name: "cand_id", Kind: KindText, Nullable: true},
	{Name: "cand_nm", Kind: KindText, Nullable: true},
	{Name: "cand_nm_first", Kind: KindText, Nullable: true},
	{Name: "cand_nm_last", Kind: KindText, Nullable: true},
	{Name: "cand_m_nm", Kind: KindText, Nullable: true},
	{Name: "cand_prefix", Kind: KindText, Nullable: true},
	{Name: "cand_suffix", Kind: KindText, Nullable: true},
	{Name: "cand_office", Kind: KindText, Nullable: true},
	{Name: "cand_office_desc", Kind: KindText, Nullable: true},
	{Name: "cand_office_st", Kind: KindText, Nullable: true},
	{Name: "cand_office_st_desc", Kind: KindText, Nullable: true},
	{Name: "cand_office_district", Kind: KindText, Nullable: true},
	{Name: "disb_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "disb_amt", Kind: KindDecimal, Nullable: true, Precision: 14, Scale: 2},
	{Name: "memo_cd", Kind: KindText, Nullable: true},
	{Name: "memo_cd_desc", Kind: KindText, Nullable: true},
	{Name: "memo_text", Kind: KindText, Nullable: true},
	{Name: "disb_tp", Kind: KindText, Nullable: true},
	{Name: "disb_tp_desc", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_nm", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st1", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st2", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_city", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_zip", Kind: KindText, Nullable: true},
	{Name: "national_cmte_nonfed_acct", Kind: KindText, Nullable: true},
	{Name: "ref_disp_excess_flg", Kind: KindText, Nullable: true},
	{Name: "comm_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "benef_cmte_nm", Kind: KindText, Nullable: true},
	{Name: "semi_an_bundled_refund", Kind: KindDecimal, Nullable: true, Precision: 14, Scale: 2},
	{Name: "action_cd", Kind: KindText, Nullable: true},
	{Name: "action_cd_desc", Kind: KindText, Nullable: true},
	{Name: "tran_id", Kind: KindText, Nullable: true},
	{Name: "back_ref_tran_id", Kind: KindText, Nullable: true},
	{Name: "back_ref_sched_id", Kind: KindText, Nullable: true},
	{Name: "schedule_type", Kind: KindText, Nullable: true},
	{Name: "schedule_type_desc", Kind: KindText, Nullable: true},
	{Name: "line_num", Kind: KindText, Nullable: true},
	{Name: "image_num", Kind: KindText, Nullable: true},
	{Name: "file_num", Kind: KindInteger, Nullable: true, Precision: 7},
	{Name: "link_id", Kind: KindInteger, Nullable: true, Precision: 19},
	{Name: "orig_sub_id", Kind: KindInteger, Nullable: true, Precision: 19},
	{Name: "sub_id", Kind: KindInteger, Nullable: false, Precision: 19},
	{Name: "filing_form", Kind: KindText, Nullable: false},
	{Name: "rpt_tp", Kind: KindText, Nullable: true},
	{Name: "rpt_yr", Kind: KindInteger, Nullable: true, Precision: 4},
	{Name: "two_year_transaction_period", Kind: KindInteger, Nullable: true, Precision: 4},
	{Name: "pdf_url", Kind: KindText, Nullable: true},
	{Name: "recipient_name_text", Kind: KindText, Nullable: true},
	{Name: "disbursement_description_text", Kind: KindText, Nullable: true},
	{Name: "disbursement_purpose_category", Kind: KindText, Nullable: true},
	{Name: "clean_recipient_cmte_id", Kind: KindText, Nullable: true},
	{Name: "pg_date", Kind: KindTimestamp, Nullable: true},
	{Name: "line_number_label", Kind: KindText, Nullable: true},
	{Name: "cmte_tp", Kind: KindText, Nullable: true},
	{Name: "org_tp", Kind: KindText, Nullable: true},
	{Name: "cmte_dsgn", Kind: KindText, Nullable: true},
}

var columnIndexes = func() map[string]int {
	indexes := make(map[string]int, len(columns))
	for index, column := range columns {
		indexes[column.Name] = index
	}
	return indexes
}()

// Columns returns a copy of the ordered relation schema.
func Columns() [FieldCount]Column { return columns }

// ColumnIndex resolves a source column name to its zero-based relation index.
func ColumnIndex(name string) (int, bool) {
	index, ok := columnIndexes[name]
	return index, ok
}
