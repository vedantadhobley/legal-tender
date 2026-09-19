package receipts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type compactParquetReceipt struct {
	SourceRowOrdinal int64   `parquet:"lt_source_row_ordinal"`
	CommitteeID      *string `parquet:"cmte_id,optional"`
	SourceReceiptAt  *string `parquet:"contb_receipt_dt,optional"`
	ReceiptDate      *int32  `parquet:"lt_receipt_date,date,optional"`
	Individual       *bool   `parquet:"is_individual,optional"`
	MemoedSubtotal   bool    `parquet:"lt_memoed_subtotal"`
	AmountMinorUnits *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState      string  `parquet:"lt_receipt_amount_state"`
	Normalization    string  `parquet:"lt_normalization_state"`
}

type compactMembershipScan struct {
	directProbeScan
	ExceptionRecords uint64
}

func compactMembershipPredicate() CompactMembershipPredicate {
	return CompactMembershipPredicate{
		Version:                    CompactMembershipPredicateV1,
		InputPhysicalSchemaVersion: scheduleaparquet.PhysicalSchemaVersion,
		MembershipIdentity:         "columnar fact-set ID plus one-based source row ordinal",
		RequiredColumns: []string{
			scheduleaparquet.ColumnSourceRowOrdinal,
			"cmte_id",
			"contb_receipt_dt",
			scheduleaparquet.ColumnReceiptDate,
			"is_individual",
			scheduleaparquet.ColumnMemoedSubtotal,
			scheduleaparquet.ColumnReceiptAmountMinorUnits,
			scheduleaparquet.ColumnReceiptAmountState,
			scheduleaparquet.ColumnNormalizationState,
		},
		DecisionOrder: []CompactPredicateRule{
			{State: "unresolved_individual_class", All: []string{"is_individual is null"}},
			{State: "excluded_non_individual", All: []string{"is_individual is false"}},
			{State: "excluded_memo_subtotal", All: []string{"is_individual is true", "lt_memoed_subtotal is true"}},
			{State: "unresolved_amount", All: []string{"is_individual is true", "lt_memoed_subtotal is false", "lt_receipt_amount_state is not reported_value or lt_receipt_amount_minor_units is null"}},
			{State: "included", All: []string{"is_individual is true", "lt_memoed_subtotal is false", "lt_receipt_amount_state is reported_value", "lt_receipt_amount_minor_units is not null"}},
		},
		ExceptionalStates: []string{"unresolved_individual_class", "unresolved_amount", "invalid_receipt_date"},
	}
}

func scanCompactMembership(
	ctx context.Context,
	storageRoot string,
	manifest fecoccurrence.ScheduleAColumnarManifest,
	calculationSetID string,
	calculator *CycleCalculator,
	exceptions *storageartifact.Writer,
	progress func(string),
) (compactMembershipScan, error) {
	var scan compactMembershipScan
	physical, err := scheduleaparquet.NewSchema()
	if err != nil {
		return scan, err
	}
	for _, shard := range manifest.Shards {
		path, err := storageartifact.Resolve(storageRoot, shard.StorageKey)
		if err != nil {
			return scan, err
		}
		file, err := os.Open(path)
		if err != nil {
			return scan, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return scan, err
		}
		opened, err := parquet.OpenFile(file, info.Size())
		if err != nil {
			_ = file.Close()
			return scan, err
		}
		if opened.Schema().String() != physical.Parquet().String() {
			_ = file.Close()
			return scan, fmt.Errorf("Schedule A columnar shard %d schema is incompatible", shard.Index)
		}
		reader, err := openCompactParquetReader(file)
		if err != nil {
			_ = file.Close()
			return scan, err
		}
		buffer := make([]compactParquetReceipt, 8192)
		var shardRows uint64
		for {
			count, readErr := reader.Read(buffer)
			for index := range count {
				row := buffer[index]
				if row.SourceRowOrdinal <= 0 || uint64(row.SourceRowOrdinal) != scan.SourceRows+1 ||
					uint64(row.SourceRowOrdinal) < shard.FirstSourceRowOrdinal || uint64(row.SourceRowOrdinal) > shard.LastSourceRowOrdinal {
					_ = reader.Close()
					_ = file.Close()
					return scan, fmt.Errorf("Schedule A columnar membership row ordinal is not contiguous at shard %d", shard.Index)
				}
				if row.Normalization != "valid" {
					_ = reader.Close()
					_ = file.Close()
					return scan, fmt.Errorf("Schedule A columnar row %d has unsupported normalization state %q", row.SourceRowOrdinal, row.Normalization)
				}
				scan.SourceRows++
				scan.ValidatedRows++
				shardRows++
				committeeID := ""
				if row.CommitteeID != nil {
					committeeID = *row.CommitteeID
				}
				routed := committeeID != "" && calculator.hasCommitteeRouteBytes([]byte(committeeID))
				input := receiptInput{
					Cycle: manifest.Cycle, PublisherClassedIndividual: row.Individual,
					MemoedSubtotal: row.MemoedSubtotal, AmountObservationState: row.AmountState,
				}
				if routed {
					input.CommitteeID = committeeID
					scan.RoutedRows++
				}
				if row.AmountMinorUnits != nil {
					amount := strconv.FormatInt(*row.AmountMinorUnits, 10)
					input.AmountMinorUnits = &amount
				}
				decision := decideReceipt(input)
				if err := addDirectProbeDecision(&scan.directProbeScan, decision); err != nil {
					_ = reader.Close()
					_ = file.Close()
					return scan, err
				}
				if decision.State == "unresolved_individual_class" || decision.State == "unresolved_amount" {
					if err := writeCompactMembershipException(exceptions, calculationSetID, uint64(row.SourceRowOrdinal), decision.State, row.CommitteeID, row.AmountState, "receipt decision requires unresolved source evidence"); err != nil {
						_ = reader.Close()
						_ = file.Close()
						return scan, err
					}
					scan.ExceptionRecords++
				}
				if routed {
					if decision.State == "included" {
						switch {
						case row.ReceiptDate != nil:
							date := time.Unix(0, 0).UTC().AddDate(0, 0, int(*row.ReceiptDate)).Format("2006-01-02")
							input.ReceivedOn = &date
						case row.SourceReceiptAt != nil:
							scan.InvalidReceiptDates++
							if err := writeCompactMembershipException(exceptions, calculationSetID, uint64(row.SourceRowOrdinal), "invalid_receipt_date", row.CommitteeID, row.AmountState, "included routed receipt has non-null source date but no valid typed date"); err != nil {
								_ = reader.Close()
								_ = file.Close()
								return scan, err
							}
							scan.ExceptionRecords++
						}
					}
					if _, err := calculator.addReceiptInput(input); err != nil {
						_ = reader.Close()
						_ = file.Close()
						return scan, err
					}
				}
				if scan.SourceRows&0x3fff == 0 {
					if err := ctx.Err(); err != nil {
						_ = reader.Close()
						_ = file.Close()
						return scan, err
					}
				}
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				_ = reader.Close()
				_ = file.Close()
				return scan, readErr
			}
		}
		if err := reader.Close(); err != nil {
			_ = file.Close()
			return scan, err
		}
		if err := file.Close(); err != nil {
			return scan, err
		}
		if shardRows != shard.Facts {
			return scan, fmt.Errorf("Schedule A columnar shard %d yielded %d membership rows; want %d", shard.Index, shardRows, shard.Facts)
		}
		if progress != nil && (shard.Index+1)%16 == 0 {
			progress(fmt.Sprintf("evaluated %d of %d Schedule A columnar shards for compact membership", shard.Index+1, len(manifest.Shards)))
		}
	}
	scan.Decisions.IncludedAmountMinorUnits = strconv.FormatInt(scan.includedAmountMinorUnits, 10)
	return scan, nil
}

func openCompactParquetReader(file *os.File) (reader *parquet.GenericReader[compactParquetReceipt], err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			reader = nil
			err = fmt.Errorf("open compact Schedule A Parquet projection: %v", recovered)
		}
	}()
	return parquet.NewGenericReader[compactParquetReceipt](file), nil
}

func writeCompactMembershipException(
	writer *storageartifact.Writer,
	calculationSetID string,
	rowOrdinal uint64,
	state string,
	committeeID *string,
	amountState, reason string,
) error {
	return writer.WriteJSON(CompactMembershipException{
		SchemaVersion:    CompactMembershipExceptionV1,
		ExceptionID:      digestParts("fec.itemized-individual-receipt-membership-exception.v1", calculationSetID, strconv.FormatUint(rowOrdinal, 10), state),
		SourceRowOrdinal: rowOrdinal, State: state, RecipientCommitteeID: committeeID,
		AmountState: amountState, Reason: reason,
	})
}

func verifyColumnarShardSHA256(ctx context.Context, path, expected string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	buffer := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, readErr := file.Read(buffer)
		if count > 0 {
			_, _ = hash.Write(buffer[:count])
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	if hex.EncodeToString(hash.Sum(nil)) != expected {
		return fmt.Errorf("Parquet shard SHA-256 mismatch")
	}
	return nil
}
