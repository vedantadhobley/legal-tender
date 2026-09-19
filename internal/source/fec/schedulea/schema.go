// Package schedulea owns the processed FEC Schedule A source boundary.
package schedulea

// FieldCount is the exact width of the accepted processed Schedule A relation.
const FieldCount = 81

const (
	subIDIndex                    = 65
	filingFormIndex               = 66
	twoYearTransactionPeriodIndex = 69
)

// Kind identifies the source lexeme rules for one relation column.
type Kind uint8

const (
	KindText Kind = iota
	KindDecimal
	KindInteger
	KindTimestamp
	KindBoolean
)

// Column describes one ordered field in the processed Schedule A relation.
type Column struct {
	Name     string
	Kind     Kind
	Nullable bool
}

var columns = [FieldCount]Column{
	{Name: "cmte_id", Kind: KindText, Nullable: true},
	{Name: "cmte_nm", Kind: KindText, Nullable: true},
	{Name: "contbr_id", Kind: KindText, Nullable: true},
	{Name: "contbr_nm", Kind: KindText, Nullable: true},
	{Name: "contbr_nm_first", Kind: KindText, Nullable: true},
	{Name: "contbr_m_nm", Kind: KindText, Nullable: true},
	{Name: "contbr_nm_last", Kind: KindText, Nullable: true},
	{Name: "contbr_prefix", Kind: KindText, Nullable: true},
	{Name: "contbr_suffix", Kind: KindText, Nullable: true},
	{Name: "contbr_st1", Kind: KindText, Nullable: true},
	{Name: "contbr_st2", Kind: KindText, Nullable: true},
	{Name: "contbr_city", Kind: KindText, Nullable: true},
	{Name: "contbr_st", Kind: KindText, Nullable: true},
	{Name: "contbr_zip", Kind: KindText, Nullable: true},
	{Name: "entity_tp", Kind: KindText, Nullable: true},
	{Name: "entity_tp_desc", Kind: KindText, Nullable: true},
	{Name: "contbr_employer", Kind: KindText, Nullable: true},
	{Name: "contbr_occupation", Kind: KindText, Nullable: true},
	{Name: "election_tp", Kind: KindText, Nullable: true},
	{Name: "fec_election_tp_desc", Kind: KindText, Nullable: true},
	{Name: "fec_election_yr", Kind: KindText, Nullable: true},
	{Name: "election_tp_desc", Kind: KindText, Nullable: true},
	{Name: "contb_aggregate_ytd", Kind: KindDecimal, Nullable: true},
	{Name: "contb_receipt_dt", Kind: KindTimestamp, Nullable: true},
	{Name: "contb_receipt_amt", Kind: KindDecimal, Nullable: true},
	{Name: "receipt_tp", Kind: KindText, Nullable: true},
	{Name: "receipt_tp_desc", Kind: KindText, Nullable: true},
	{Name: "receipt_desc", Kind: KindText, Nullable: true},
	{Name: "memo_cd", Kind: KindText, Nullable: true},
	{Name: "memo_cd_desc", Kind: KindText, Nullable: true},
	{Name: "memo_text", Kind: KindText, Nullable: true},
	{Name: "cand_id", Kind: KindText, Nullable: true},
	{Name: "cand_nm", Kind: KindText, Nullable: true},
	{Name: "cand_nm_first", Kind: KindText, Nullable: true},
	{Name: "cand_m_nm", Kind: KindText, Nullable: true},
	{Name: "cand_nm_last", Kind: KindText, Nullable: true},
	{Name: "cand_prefix", Kind: KindText, Nullable: true},
	{Name: "cand_suffix", Kind: KindText, Nullable: true},
	{Name: "cand_office", Kind: KindText, Nullable: true},
	{Name: "cand_office_desc", Kind: KindText, Nullable: true},
	{Name: "cand_office_st", Kind: KindText, Nullable: true},
	{Name: "cand_office_st_desc", Kind: KindText, Nullable: true},
	{Name: "cand_office_district", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_id", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_nm", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st1", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st2", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_city", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_st", Kind: KindText, Nullable: true},
	{Name: "conduit_cmte_zip", Kind: KindText, Nullable: true},
	{Name: "donor_cmte_nm", Kind: KindText, Nullable: true},
	{Name: "national_cmte_nonfed_acct", Kind: KindText, Nullable: true},
	{Name: "increased_limit", Kind: KindText, Nullable: true},
	{Name: "action_cd", Kind: KindText, Nullable: true},
	{Name: "action_cd_desc", Kind: KindText, Nullable: true},
	{Name: "tran_id", Kind: KindText, Nullable: true},
	{Name: "back_ref_tran_id", Kind: KindText, Nullable: true},
	{Name: "back_ref_sched_nm", Kind: KindText, Nullable: true},
	{Name: "schedule_type", Kind: KindText, Nullable: true},
	{Name: "schedule_type_desc", Kind: KindText, Nullable: true},
	{Name: "line_num", Kind: KindText, Nullable: true},
	{Name: "image_num", Kind: KindText, Nullable: true},
	{Name: "file_num", Kind: KindInteger, Nullable: true},
	{Name: "link_id", Kind: KindInteger, Nullable: true},
	{Name: "orig_sub_id", Kind: KindInteger, Nullable: true},
	{Name: "sub_id", Kind: KindInteger, Nullable: false},
	{Name: "filing_form", Kind: KindText, Nullable: false},
	{Name: "rpt_tp", Kind: KindText, Nullable: true},
	{Name: "rpt_yr", Kind: KindInteger, Nullable: true},
	{Name: "two_year_transaction_period", Kind: KindInteger, Nullable: true},
	{Name: "pdf_url", Kind: KindText, Nullable: true},
	{Name: "contributor_name_text", Kind: KindText, Nullable: true},
	{Name: "contributor_employer_text", Kind: KindText, Nullable: true},
	{Name: "contributor_occupation_text", Kind: KindText, Nullable: true},
	{Name: "is_individual", Kind: KindBoolean, Nullable: true},
	{Name: "clean_contbr_id", Kind: KindText, Nullable: true},
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
func Columns() [FieldCount]Column {
	return columns
}

// ColumnIndex resolves a source column name to its zero-based relation index.
func ColumnIndex(name string) (int, bool) {
	index, ok := columnIndexes[name]
	return index, ok
}
