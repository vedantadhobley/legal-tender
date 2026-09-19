// Package committeesummary reads lossless, cycle-scoped FEC summary CSVs.
// It does not publish facts or select committee financial assertions.
package committeesummary

import "slices"

const (
	SourceContract          = "fec/committee-summary@1.0.0"
	ParserVersion           = "legal-tender.fec.committee-summary-reader.v1"
	HeaderSHA256            = "f8e539448b8271b6fc975f0982b5426cf2d1dc08425d0685fdf8f8f7b31de0b9"
	MaxArtifactBytes int64  = 16 << 20
	MaxRecordBytes   int64  = 1 << 20
	MaxRows          uint64 = 100000
)

type kind uint8

const (
	kindText kind = iota
	kindMoney
	kindDate
	kindIdentity
)

type column struct {
	name string
	kind kind
}

var columns = [...]column{
	{"Link_Image", kindText},
	{"CMTE_ID", kindIdentity},
	{"CMTE_NM", kindText},
	{"CMTE_TP", kindText},
	{"CMTE_DSGN", kindText},
	{"CMTE_FILING_FREQ", kindText},
	{"CMTE_ST1", kindText},
	{"CMTE_ST2", kindText},
	{"CMTE_CITY", kindText},
	{"CMTE_ST", kindText},
	{"CMTE_ZIP", kindText},
	{"TRES_NM", kindText},
	{"CAND_ID", kindIdentity},
	{"FEC_ELECTION_YR", kindIdentity},
	{"INDV_CONTB", kindMoney},
	{"PTY_CMTE_CONTB", kindMoney},
	{"OTH_CMTE_CONTB", kindMoney},
	{"TTL_CONTB", kindMoney},
	{"TRANF_FROM_OTHER_AUTH_CMTE", kindMoney},
	{"OFFSETS_TO_OP_EXP", kindMoney},
	{"OTHER_RECEIPTS", kindMoney},
	{"TTL_RECEIPTS", kindMoney},
	{"TRANF_TO_OTHER_AUTH_CMTE", kindMoney},
	{"OTH_LOAN_REPYMTS", kindMoney},
	{"INDV_REF", kindMoney},
	{"POL_PTY_CMTE_REF", kindMoney},
	{"TTL_CONTB_REF", kindMoney},
	{"OTHER_DISB", kindMoney},
	{"TTL_DISB", kindMoney},
	{"NET_CONTB", kindMoney},
	{"NET_OP_EXP", kindMoney},
	{"COH_BOP", kindMoney},
	{"CVG_START_DT", kindDate},
	{"COH_COP", kindMoney},
	{"CVG_END_DT", kindDate},
	{"DEBTS_OWED_BY_CMTE", kindMoney},
	{"DEBTS_OWED_TO_CMTE", kindMoney},
	{"INDV_ITEM_CONTB", kindMoney},
	{"INDV_UNITEM_CONTB", kindMoney},
	{"OTH_LOANS", kindMoney},
	{"TRANF_FROM_NONFED_ACCT", kindMoney},
	{"TRANF_FROM_NONFED_LEVIN", kindMoney},
	{"TTL_NONFED_TRANF", kindMoney},
	{"LOAN_REPYMTS_RECEIVED", kindMoney},
	{"OFFSETS_TO_FNDRSG", kindMoney},
	{"OFFSETS_TO_LEGAL_ACCTG", kindMoney},
	{"FED_CAND_CONTB_REF", kindMoney},
	{"TTL_FED_RECEIPTS", kindMoney},
	{"SHARED_FED_OP_EXP", kindMoney},
	{"SHARED_NONFED_OP_EXP", kindMoney},
	{"OTHER_FED_OP_EXP", kindMoney},
	{"TTL_OP_EXP", kindMoney},
	{"FED_CAND_CMTE_CONTB", kindMoney},
	{"INDT_EXP", kindMoney},
	{"COORD_EXP_BY_PTY_CMTE", kindMoney},
	{"LOANS_MADE", kindMoney},
	{"SHARED_FED_ACTVY_FED_SHR", kindMoney},
	{"SHARED_FED_ACTVY_NONFED", kindMoney},
	{"NON_ALLOC_FED_ELECT_ACTVY", kindMoney},
	{"TTL_FED_ELECT_ACTVY", kindMoney},
	{"TTL_FED_DISB", kindMoney},
	{"CAND_CNTB", kindMoney},
	{"CAND_LOAN", kindMoney},
	{"TTL_LOANS", kindMoney},
	{"OP_EXP", kindMoney},
	{"CAND_LOAN_REPYMNT", kindMoney},
	{"TTL_LOAN_REPYMTS", kindMoney},
	{"OTH_CMTE_REF", kindMoney},
	{"TTL_OFFSETS_TO_OP_EXP", kindMoney},
	{"EXEMPT_LEGAL_ACCTG_DISB", kindMoney},
	{"FNDRSG_DISB", kindMoney},
	{"ITEM_REF_REB_RET", kindMoney},
	{"SUBTTL_REF_REB_RET", kindMoney},
	{"UNITEM_REF_REB_RET", kindMoney},
	{"ITEM_OTHER_REF_REB_RET", kindMoney},
	{"UNITEM_OTHER_REF_REB_RET", kindMoney},
	{"SUBTTL_OTHER_REF_REB_RET", kindMoney},
	{"ITEM_OTHER_INCOME", kindMoney},
	{"UNITEM_OTHER_INCOME", kindMoney},
	{"EXP_PRIOR_YRS_SUBJECT_LIM", kindMoney},
	{"EXP_SUBJECT_LIMITS", kindMoney},
	{"FED_FUNDS", kindMoney},
	{"ITEM_CONVN_EXP_DISB", kindMoney},
	{"ITEM_OTHER_DISB", kindMoney},
	{"SUBTTL_CONVN_EXP_DISB", kindMoney},
	{"TTL_EXP_SUBJECT_LIMITS", kindMoney},
	{"UNITEM_CONVN_EXP_DISB", kindMoney},
	{"UNITEM_OTHER_DISB", kindMoney},
	{"TTL_COMMUNICATION_COST", kindMoney},
	{"COH_BOY", kindMoney},
	{"COH_COY", kindMoney},
	{"ORG_TP", kindText},
}

// Fields returns an independent copy of the pinned source order.
func Fields() []string {
	out := make([]string, len(columns))
	for i, c := range columns {
		out[i] = c.name
	}
	return out
}

func namesOfKind(k kind) []string {
	out := []string{}
	for _, c := range columns {
		if c.kind == k {
			out = append(out, c.name)
		}
	}
	return out
}

var moneyFields = namesOfKind(kindMoney)
var dateFields = namesOfKind(kindDate)
var identityFields = namesOfKind(kindIdentity)

func MoneyFields() []string { return slices.Clone(moneyFields) }
