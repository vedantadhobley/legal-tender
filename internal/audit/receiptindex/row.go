// Package receiptindex measures bounded report-reference sort runs. It does not
// resolve identities, select financial records, or publish a production index.
package receiptindex

import (
	"cmp"
	"fmt"
	"strings"
)

// Row is an access projection, not a replacement fact. The enclosing evidence
// pins the fact set; Ordinal locates every omitted field in that exact source.
// Names and employers are deliberately not identity keys.
type Row struct {
	Ordinal          int64   `parquet:"lt_source_row_ordinal"`
	Cycle            int64   `parquet:"lt_two_year_transaction_period"`
	Normalization    string  `parquet:"lt_normalization_state"`
	Recipient        *string `parquet:"cmte_id,dict"`
	File             *string `parquet:"file_num,dict"`
	Transaction      *string `parquet:"tran_id"`
	BackReference    *string `parquet:"back_ref_tran_id"`
	BackSchedule     *string `parquet:"back_ref_sched_nm,dict"`
	Schedule         *string `parquet:"schedule_type,dict"`
	Line             *string `parquet:"line_num,dict"`
	Entity           *string `parquet:"entity_tp,dict"`
	Contributor      *string `parquet:"contbr_id,dict"`
	CleanContributor *string `parquet:"clean_contbr_id,dict"`
	Conduit          *string `parquet:"conduit_cmte_id,dict"`
	ReceiptType      *string `parquet:"receipt_tp,dict"`
	Individual       *bool   `parquet:"is_individual"`
	Memo             bool    `parquet:"lt_memoed_subtotal"`
	Amount           *int64  `parquet:"lt_receipt_amount_minor_units"`
	AmountState      string  `parquet:"lt_receipt_amount_state,dict"`
}

func validateRow(r Row, ordinal uint64, cycle int64) error {
	if r.Ordinal <= 0 || uint64(r.Ordinal) != ordinal || r.Cycle != cycle || r.Normalization != "valid" {
		return fmt.Errorf("source identity/normalization mismatch at ordinal %d", ordinal)
	}
	if !(r.AmountState == "reported_value" && r.Amount != nil || r.AmountState == "source_null" && r.Amount == nil) {
		return fmt.Errorf("source amount-state mismatch at ordinal %d", ordinal)
	}
	return nil
}

// Null and empty remain distinct. Ordinal is the tie-breaker, never a dedup key.
func compare(a, b Row) int {
	for _, pair := range [][2]*string{{a.Recipient, b.Recipient}, {a.File, b.File}, {a.Transaction, b.Transaction}} {
		if pair[0] == nil && pair[1] != nil {
			return -1
		}
		if pair[0] != nil && pair[1] == nil {
			return 1
		}
		if pair[0] != nil && pair[1] != nil {
			if n := strings.Compare(*pair[0], *pair[1]); n != 0 {
				return n
			}
		}
	}
	return cmp.Compare(a.Ordinal, b.Ordinal)
}

func cloneRow(r Row) Row {
	for _, p := range []**string{&r.Recipient, &r.File, &r.Transaction, &r.BackReference, &r.BackSchedule,
		&r.Schedule, &r.Line, &r.Entity, &r.Contributor, &r.CleanContributor, &r.Conduit, &r.ReceiptType} {
		if *p != nil {
			s := strings.Clone(**p)
			*p = &s
		}
	}
	r.Normalization, r.AmountState = strings.Clone(r.Normalization), strings.Clone(r.AmountState)
	if r.Individual != nil {
		b := *r.Individual
		r.Individual = &b
	}
	if r.Amount != nil {
		n := *r.Amount
		r.Amount = &n
	}
	return r
}
