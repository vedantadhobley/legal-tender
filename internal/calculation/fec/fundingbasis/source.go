package fundingbasis

import (
	"context"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// SourceEvidenceRow is the typed source-field boundary for streaming consumers
// of the existing inventory and source-role policies. It is not a new policy or
// a substitute for the full retained source fact.
type SourceEvidenceRow struct {
	Ordinal          int64   `parquet:"lt_source_row_ordinal"`
	Cycle            int64   `parquet:"lt_two_year_transaction_period"`
	Normalization    string  `parquet:"lt_normalization_state"`
	Recipient        *string `parquet:"cmte_id"`
	Contributor      *string `parquet:"contbr_id"`
	CleanContributor *string `parquet:"clean_contbr_id"`
	Individual       *bool   `parquet:"is_individual"`
	Memo             bool    `parquet:"lt_memoed_subtotal"`
	ReceiptType      *string `parquet:"receipt_tp"`
	Amount           *int64  `parquet:"lt_receipt_amount_minor_units"`
	AmountState      string  `parquet:"lt_receipt_amount_state"`
	ConduitID        *string `parquet:"conduit_cmte_id"`
	Entity           *string `parquet:"entity_tp"`
	ConduitName      *string `parquet:"conduit_cmte_nm"`
	MemoText         *string `parquet:"memo_text"`
	File             *string `parquet:"file_num"`
	Transaction      *string `parquet:"tran_id"`
	BackReference    *string `parquet:"back_ref_tran_id"`
	BackSchedule     *string `parquet:"back_ref_sched_nm"`
}

func (s SourceEvidenceRow) input() evidenceInput {
	return evidenceInput{row: receiptRow{Ordinal: s.Ordinal, Cycle: s.Cycle, Normalization: s.Normalization, Recipient: s.Recipient,
		Contributor: s.Contributor, CleanContributor: s.CleanContributor, Individual: s.Individual, Memo: s.Memo,
		ReceiptType: s.ReceiptType, Amount: s.Amount, AmountState: s.AmountState, ConduitID: s.ConduitID},
		entity: s.Entity, conduitName: s.ConduitName, memo: s.MemoText, file: s.File, transaction: s.Transaction, backReference: s.BackReference, backSchedule: s.BackSchedule}
}

func (s SourceEvidenceRow) Validate(ordinal uint64, cycle int64) error {
	return validRow(s.input().row, ordinal, cycle)
}

func (s SourceEvidenceRow) Classify() (Key, EvidenceDecision) {
	in := s.input()
	key := classify(in.row)
	return key, assessClassifiedEvidence(in, key)
}

func (s SourceEvidenceRow) AssociationEvidence() earmarkassociation.Evidence {
	return associationEvidence(s.input())
}

func SourceEvidenceFromReceipt(r Receipt) (SourceEvidenceRow, error) {
	in, err := decodeEvidence(r)
	if err != nil {
		return SourceEvidenceRow{}, err
	}
	v := in.row
	return SourceEvidenceRow{Ordinal: v.Ordinal, Cycle: v.Cycle, Normalization: v.Normalization, Recipient: v.Recipient, Contributor: v.Contributor, CleanContributor: v.CleanContributor,
		Individual: v.Individual, Memo: v.Memo, ReceiptType: v.ReceiptType, Amount: v.Amount, AmountState: v.AmountState, ConduitID: v.ConduitID,
		Entity: in.entity, ConduitName: in.conduitName, MemoText: in.memo, File: in.file, Transaction: in.transaction, BackReference: in.backReference, BackSchedule: in.backSchedule}, nil
}

// ReadSourceOccurrence reuses the strict full-row reader without selecting by
// committee identity or monetary membership. The caller must load/verify the
// immutable manifest first. The opened shard's complete bytes are checked here.
func ReadSourceOccurrence(ctx context.Context, root string, m occ.ScheduleAColumnarManifest, ordinal uint64) (Receipt, error) {
	if ordinal == 0 || ordinal > m.Counts.SourceOccurrences {
		return Receipt{}, fmt.Errorf("source occurrence out of range")
	}
	for i, s := range m.Shards {
		if ordinal >= s.FirstSourceRowOrdinal && ordinal <= s.LastSourceRowOrdinal {
			r := Reader{root: root, manifest: m, result: Result{Cycle: m.Cycle}}
			rows, err := r.queryShard(ctx, i, Query{After: ordinal - 1}, 1)
			if err != nil {
				return Receipt{}, err
			}
			if len(rows) != 1 || rows[0].Ordinal != ordinal {
				return Receipt{}, fmt.Errorf("exact source occurrence not found")
			}
			return rows[0], nil
		}
	}
	return Receipt{}, fmt.Errorf("source occurrence has no backing shard")
}
