// Package schedulee owns the processed FEC Schedule E source boundary.
package schedulee

// FieldCount is the exact width of the accepted processed Schedule E relation.
const FieldCount = 80

const (
	expenditureAmountIndex = 36
	expenditureTypeIndex   = 38
	memoCodeIndex          = 40
	actionCodeIndex        = 60
	subIDIndex             = 72
	filingFormIndex        = 73
	electionCycleIndex     = 76
)

// Kind identifies the source lexeme rules for one relation column.
type Kind uint8

const (
	KindText Kind = iota
	KindDecimal
	KindInteger
	KindTimestamp
)

// Column describes one ordered field in the processed Schedule E relation.
type Column struct {
	Name      string
	Kind      Kind
	Nullable  bool
	Precision int
	Scale     int
}

var columns = [FieldCount]Column{
	{Name: "cmte_id", Kind: KindText, Nullable: true},
	{Name: "cmte_nm", Kind: KindText, Nullable: true},
	{Name: "pye_nm", Kind: KindText, Nullable: true},
	{Name: "payee_l_nm", Kind: KindText, Nullable: true},
	{Name: "payee_f_nm", Kind: KindText, Nullable: true},
	{Name: "payee_m_nm", Kind: KindText, Nullable: true},
	{Name: "payee_prefix", Kind: KindText, Nullable: true},
	{Name: "payee_suffix", Kind: KindText, Nullable: true},
	{Name: "pye_st1", Kind: KindText, Nullable: true},
	{Name: "pye_st2", Kind: KindText, Nullable: true},
	{Name: "pye_city", Kind: KindText, Nullable: true},
	{Name: "pye_st", Kind: KindText, Nullable: true},
	{Name: "pye_zip", Kind: KindText, Nullable: true},
	{Name: "entity_tp", Kind: KindText, Nullable: true},
	{Name: "entity_tp_desc", Kind: KindText, Nullable: true},
	{Name: "exp_desc", Kind: KindText, Nullable: true},
	{Name: "catg_cd", Kind: KindText, Nullable: true},
	{Name: "catg_cd_desc", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_id", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_nm", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_nm_first", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_nm_last", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_m_nm", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_prefix", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_suffix", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_office", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_office_desc", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_office_st", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_office_st_desc", Kind: KindText, Nullable: true},
	{Name: "s_o_cand_office_district", Kind: KindText, Nullable: true},
	{Name: "s_o_ind", Kind: KindText, Nullable: true},
	{Name: "s_o_ind_desc", Kind: KindText, Nullable: true},
	{Name: "election_tp", Kind: KindText, Nullable: true},
	{Name: "fec_election_tp_desc", Kind: KindText, Nullable: true},
	{Name: "cal_ytd_ofc_sought", Kind: KindDecimal, Nullable: true, Precision: 14, Scale: 2},
	{Name: "dissem_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "exp_amt", Kind: KindDecimal, Nullable: true, Precision: 14, Scale: 2},
	{Name: "exp_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "exp_tp", Kind: KindText, Nullable: true},
	{Name: "exp_tp_desc", Kind: KindText, Nullable: true},
	{Name: "memo_cd", Kind: KindText, Nullable: true},
	{Name: "memo_cd_desc", Kind: KindText, Nullable: true},
	{Name: "memo_text", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_id", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_nm", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st1", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st2", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_city", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_zip", Kind: KindText, Nullable: true},
	{Name: "indt_sign_nm", Kind: KindText, Nullable: true},
	{Name: "indt_sign_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "notary_sign_nm", Kind: KindText, Nullable: true},
	{Name: "notary_sign_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "notary_commission_exprtn_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "filer_l_nm", Kind: KindText, Nullable: true},
	{Name: "filer_f_nm", Kind: KindText, Nullable: true},
	{Name: "filer_m_nm", Kind: KindText, Nullable: true},
	{Name: "filer_prefix", Kind: KindText, Nullable: true},
	{Name: "filer_suffix", Kind: KindText, Nullable: true},
	{Name: "action_cd", Kind: KindText, Nullable: true},
	{Name: "action_cd_desc", Kind: KindText, Nullable: true},
	{Name: "tran_id", Kind: KindText, Nullable: true},
	{Name: "back_ref_tran_id", Kind: KindText, Nullable: true},
	{Name: "back_ref_sched_nm", Kind: KindText, Nullable: true},
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
	{Name: "election_cycle", Kind: KindInteger, Nullable: true, Precision: 4},
	{Name: "pdf_url", Kind: KindText, Nullable: true},
	{Name: "payee_name_text", Kind: KindText, Nullable: true},
	{Name: "pg_date", Kind: KindTimestamp, Nullable: true},
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
