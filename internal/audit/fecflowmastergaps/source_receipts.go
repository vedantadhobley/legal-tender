package fecflowmastergaps

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"
	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type sourceReceiptRow struct {
	SourceRowOrdinal    int64   `parquet:"lt_source_row_ordinal"`
	SourceRawByteOffset int64   `parquet:"lt_source_raw_byte_offset"`
	SourceRawByteLength int64   `parquet:"lt_source_raw_byte_length"`
	Normalization       string  `parquet:"lt_normalization_state"`
	RecipientID         *string `parquet:"cmte_id,optional"`
	ContributorID       *string `parquet:"contbr_id,optional"`
	CleanContributorID  *string `parquet:"clean_contbr_id,optional"`
	ContributorName     *string `parquet:"contbr_nm,optional"`
	EntityTypeCode      *string `parquet:"entity_tp,optional"`
	ReceiptTypeCode     *string `parquet:"receipt_tp,optional"`
	MemoedSubtotal      bool    `parquet:"lt_memoed_subtotal"`
	AmountMinorUnits    *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState         string  `parquet:"lt_receipt_amount_state"`
	TransactionID       *string `parquet:"tran_id,optional"`
	FilingForm          string  `parquet:"filing_form"`
	FileNumber          *string `parquet:"file_num,optional"`
	SubID               string  `parquet:"sub_id"`
}

func scanAbsentMasterSourceReceipts(
	ctx context.Context,
	storageRoot string,
	calculation fecflows.Manifest,
	accumulator *auditAccumulator,
	comparisons map[string][]CommitteeHistoricalAssertion,
	history map[string][]CommitteeHistoricalAssertion,
	progress func(string),
) (ScheduleAFactReference, map[string][]SourceReceiptAssertion, error) {
	manifestPath := filepath.Join(
		storageRoot, "facts", "fec", "schedule-a", "columnar", "manifests",
		calculation.InputFactSet.FactSetID+".json",
	)
	manifest, manifestDigest, err := fecoccurrence.LoadPublishedScheduleAColumnarManifest(ctx, storageRoot, manifestPath)
	if err != nil {
		return ScheduleAFactReference{}, nil, fmt.Errorf("load exact Schedule A facts for source evidence: %w", err)
	}
	if manifest.FactSetID != calculation.InputFactSet.FactSetID ||
		manifestDigest != calculation.InputFactSet.ManifestSHA256 ||
		manifest.PhysicalSchemaVersion != calculation.InputFactSet.PhysicalSchemaVersion ||
		manifest.Cycle != calculation.Cycle || manifest.SourceReleaseID != calculation.SourceReleaseID {
		return ScheduleAFactReference{}, nil, fmt.Errorf("Schedule A source evidence does not match receiver-flow calculation lineage")
	}
	reference := ScheduleAFactReference{
		FactSetID: manifest.FactSetID, ManifestSHA256: manifestDigest,
		PhysicalSchemaVersion: manifest.PhysicalSchemaVersion,
		Facts:                 manifest.Counts.Facts, Shards: len(manifest.Shards),
	}

	targets := make(map[string]*gapAccumulator)
	for committeeID, gap := range accumulator.gaps {
		_, isSource := gap.endpointRoles["source"]
		if len(comparisons[committeeID]) == 0 && len(history[committeeID]) == 0 && isSource {
			targets[committeeID] = gap
		}
	}
	result := make(map[string][]SourceReceiptAssertion, len(targets))
	if len(targets) == 0 {
		return reference, result, nil
	}

	for index, shard := range manifest.Shards {
		if err := ctx.Err(); err != nil {
			return ScheduleAFactReference{}, nil, err
		}
		if err := scanSourceReceiptShard(ctx, storageRoot, shard, targets, result); err != nil {
			return ScheduleAFactReference{}, nil, fmt.Errorf("scan Schedule A shard %d for source evidence: %w", shard.Index, err)
		}
		if progress != nil && ((index+1)%16 == 0 || index+1 == len(manifest.Shards)) {
			progress(fmt.Sprintf("traced absent-master source IDs through %d of %d Schedule A shards", index+1, len(manifest.Shards)))
		}
	}
	for committeeID := range result {
		sort.Slice(result[committeeID], func(left, right int) bool {
			return result[committeeID][left].SourceRowOrdinal < result[committeeID][right].SourceRowOrdinal
		})
	}
	if err := validateSourceReceiptEvidence(targets, result); err != nil {
		return ScheduleAFactReference{}, nil, err
	}
	return reference, result, nil
}

func scanSourceReceiptShard(
	ctx context.Context,
	storageRoot string,
	shard fecoccurrence.ScheduleAColumnarShard,
	targets map[string]*gapAccumulator,
	result map[string][]SourceReceiptAssertion,
) error {
	path, err := storageartifact.Resolve(storageRoot, shard.StorageKey)
	if err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	reader := parquet.NewGenericReader[sourceReceiptRow](file)
	buffer := make([]sourceReceiptRow, 8192)
	for {
		count, readErr := reader.Read(buffer)
		for rowIndex := range count {
			row := buffer[rowIndex]
			if row.SourceRowOrdinal <= 0 || row.SourceRawByteOffset < 0 || row.SourceRawByteLength <= 0 ||
				uint64(row.SourceRowOrdinal) < shard.FirstSourceRowOrdinal || uint64(row.SourceRowOrdinal) > shard.LastSourceRowOrdinal {
				_ = reader.Close()
				_ = file.Close()
				return fmt.Errorf("invalid source locator at row %d", row.SourceRowOrdinal)
			}
			evaluation := fecflows.Evaluate(fecflows.EvaluationInput{
				NormalizationState: row.Normalization, RecipientCommitteeID: row.RecipientID,
				ContributorID: row.ContributorID, CleanContributorID: row.CleanContributorID,
				MemoedSubtotal: row.MemoedSubtotal, AmountObservationState: row.AmountState,
				AmountMinorUnits: row.AmountMinorUnits, ReceiptTypeCode: row.ReceiptTypeCode,
			})
			if evaluation.Decision != fecflows.DecisionIncluded {
				continue
			}
			if _, exists := targets[evaluation.SourceCommitteeID]; !exists {
				continue
			}
			if row.RecipientID == nil || row.AmountMinorUnits == nil {
				_ = reader.Close()
				_ = file.Close()
				return fmt.Errorf("included source row %d lacks recipient or amount", row.SourceRowOrdinal)
			}
			result[evaluation.SourceCommitteeID] = append(result[evaluation.SourceCommitteeID], SourceReceiptAssertion{
				SourceRowOrdinal: uint64(row.SourceRowOrdinal), SourceRawByteOffset: uint64(row.SourceRawByteOffset),
				SourceRawByteLength: uint64(row.SourceRawByteLength), RecipientCommitteeID: *row.RecipientID,
				ContributorID: row.ContributorID, CleanContributorID: row.CleanContributorID,
				ContributorName: row.ContributorName, EntityTypeCode: row.EntityTypeCode,
				ReceiptTypeCode: row.ReceiptTypeCode, ReceiptRole: evaluation.ReceiptRole,
				AmountMinorUnits: big.NewInt(*row.AmountMinorUnits).String(), TransactionID: row.TransactionID,
				FilingForm: row.FilingForm, FileNumber: row.FileNumber, SubID: row.SubID,
			})
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			_ = reader.Close()
			_ = file.Close()
			return readErr
		}
		if err := ctx.Err(); err != nil {
			_ = reader.Close()
			_ = file.Close()
			return err
		}
	}
	if err := reader.Close(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func validateSourceReceiptEvidence(targets map[string]*gapAccumulator, evidence map[string][]SourceReceiptAssertion) error {
	for committeeID, gap := range targets {
		rows := evidence[committeeID]
		var amount big.Int
		for _, row := range rows {
			value, ok := new(big.Int).SetString(row.AmountMinorUnits, 10)
			if !ok {
				return fmt.Errorf("source evidence for %s has invalid amount %q", committeeID, row.AmountMinorUnits)
			}
			amount.Add(&amount, value)
		}
		if uint64(len(rows)) != gap.outgoing.receiptRows || amount.String() != gap.outgoing.amount.String() {
			return fmt.Errorf(
				"source evidence for %s does not conserve outgoing exposure: rows=%d/%d amount=%s/%s",
				committeeID, len(rows), gap.outgoing.receiptRows, amount.String(), gap.outgoing.amount.String(),
			)
		}
	}
	return nil
}

func absentMasterSourceEvidenceConserves(report Report) bool {
	var rows uint64
	for _, committee := range report.Committees {
		if committee.State != stateAbsentAll {
			if len(committee.SourceReceipts) != 0 {
				return false
			}
			continue
		}
		if contains(committee.EndpointRoles, "source") {
			if uint64(len(committee.SourceReceipts)) != committee.Outgoing.ReceiptRows {
				return false
			}
			var amount big.Int
			for _, receipt := range committee.SourceReceipts {
				value, ok := new(big.Int).SetString(receipt.AmountMinorUnits, 10)
				if !ok {
					return false
				}
				amount.Add(&amount, value)
			}
			if amount.String() != committee.Outgoing.SignedAmountMinorUnits {
				return false
			}
		} else if len(committee.SourceReceipts) != 0 {
			return false
		}
		rows += uint64(len(committee.SourceReceipts))
	}
	return rows == report.Counts.AbsentMasterSourceReceiptEvidence
}
