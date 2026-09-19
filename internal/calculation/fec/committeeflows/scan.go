package committeeflows

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/parquet-go/parquet-go"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type flowParquetRow struct {
	SourceRowOrdinal   int64   `parquet:"lt_source_row_ordinal"`
	Normalization      string  `parquet:"lt_normalization_state"`
	RecipientID        *string `parquet:"cmte_id,optional"`
	ContributorID      *string `parquet:"contbr_id,optional"`
	CleanContributorID *string `parquet:"clean_contbr_id,optional"`
	MemoedSubtotal     bool    `parquet:"lt_memoed_subtotal"`
	ReceiptTypeCode    *string `parquet:"receipt_tp,optional"`
	AmountMinorUnits   *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState        string  `parquet:"lt_receipt_amount_state"`
}

type groupKey struct {
	source, recipient, role string
}

type groupAccumulator struct {
	amount                          big.Int
	count, positive, negative, zero uint64
}

type scanAmounts struct {
	known, included, excluded, unresolved big.Int
}

type shardScan struct {
	index      uint64
	rows       uint64
	decisions  DecisionCounts
	counts     ResultCounts
	amounts    scanAmounts
	exceptions []Exception
	groups     map[groupKey]*groupAccumulator
	sources    map[string]struct{}
	recipients map[string]struct{}
	selfGroups map[groupKey]struct{}
	err        error
}

type completeScan struct {
	decisions  DecisionCounts
	counts     ResultCounts
	amounts    scanAmounts
	exceptions []Exception
	groups     map[groupKey]*groupAccumulator
	sources    map[string]struct{}
	recipients map[string]struct{}
	selfGroups map[groupKey]struct{}
}

func scanColumnarFacts(
	ctx context.Context,
	storageRoot string,
	manifest fecoccurrence.ScheduleAColumnarManifest,
	calculationSetID string,
	workers int,
	progress func(string),
) (completeScan, error) {
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan fecoccurrence.ScheduleAColumnarShard)
	results := make(chan shardScan, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for shard := range jobs {
				result := scanColumnarShard(workerContext, storageRoot, manifest.Cycle, calculationSetID, shard)
				select {
				case results <- result:
				case <-workerContext.Done():
					return
				}
				if result.err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, shard := range manifest.Shards {
			select {
			case jobs <- shard:
			case <-workerContext.Done():
				return
			}
		}
	}()
	go func() {
		group.Wait()
		close(results)
	}()

	shards := make([]shardScan, len(manifest.Shards))
	seen := make([]bool, len(manifest.Shards))
	completed := 0
	var firstError error
	for result := range results {
		if result.err != nil {
			if firstError == nil {
				firstError = result.err
			}
			continue
		}
		if result.index >= uint64(len(shards)) || seen[result.index] {
			if firstError == nil {
				firstError = fmt.Errorf("duplicate or invalid Schedule A shard result %d", result.index)
				cancel()
			}
			continue
		}
		shards[result.index] = result
		seen[result.index] = true
		completed++
		if progress != nil && (completed%16 == 0 || completed == len(manifest.Shards)) {
			progress(fmt.Sprintf("classified %d of %d Schedule A shards", completed, len(manifest.Shards)))
		}
	}
	if firstError != nil {
		return completeScan{}, firstError
	}
	if completed != len(manifest.Shards) {
		return completeScan{}, fmt.Errorf("classified %d of %d Schedule A shards", completed, len(manifest.Shards))
	}

	combined := completeScan{
		groups: make(map[groupKey]*groupAccumulator), sources: make(map[string]struct{}),
		recipients: make(map[string]struct{}), selfGroups: make(map[groupKey]struct{}),
	}
	for _, shard := range shards {
		if err := mergeShardScan(&combined, shard); err != nil {
			return completeScan{}, err
		}
	}
	combined.counts.ResultGroups = uint64(len(combined.groups))
	combined.counts.SourceCommittees = uint64(len(combined.sources))
	combined.counts.RecipientCommittees = uint64(len(combined.recipients))
	combined.counts.SelfEdgeGroups = uint64(len(combined.selfGroups))
	return combined, nil
}

func scanColumnarShard(ctx context.Context, storageRoot, cycle, calculationSetID string, shard fecoccurrence.ScheduleAColumnarShard) shardScan {
	result := shardScan{
		index: shard.Index, groups: make(map[groupKey]*groupAccumulator),
		sources: make(map[string]struct{}), recipients: make(map[string]struct{}), selfGroups: make(map[groupKey]struct{}),
	}
	path, err := storageartifact.Resolve(storageRoot, shard.StorageKey)
	if err != nil {
		result.err = err
		return result
	}
	file, err := os.Open(path)
	if err != nil {
		result.err = err
		return result
	}
	reader := parquet.NewGenericReader[flowParquetRow](file)
	buffer := make([]flowParquetRow, 8192)
	for {
		count, readErr := reader.Read(buffer)
		for index := range count {
			row := buffer[index]
			ordinal := uint64(0)
			if row.SourceRowOrdinal > 0 {
				ordinal = uint64(row.SourceRowOrdinal)
			}
			expected := shard.FirstSourceRowOrdinal + result.rows
			if ordinal != expected || ordinal > shard.LastSourceRowOrdinal {
				result.err = fmt.Errorf("shard %d row ordinal %d; want %d", shard.Index, ordinal, expected)
				_ = reader.Close()
				_ = file.Close()
				return result
			}
			result.rows++
			result.decisions.SourceFacts++
			evaluation := Evaluate(EvaluationInput{
				NormalizationState: row.Normalization, RecipientCommitteeID: row.RecipientID,
				ContributorID: row.ContributorID, CleanContributorID: row.CleanContributorID,
				MemoedSubtotal: row.MemoedSubtotal, AmountObservationState: row.AmountState,
				AmountMinorUnits: row.AmountMinorUnits, ReceiptTypeCode: row.ReceiptTypeCode,
			})
			if err := addDecision(&result.decisions, evaluation.Decision); err != nil {
				result.err = err
				_ = reader.Close()
				_ = file.Close()
				return result
			}
			disposition := DecisionDisposition(evaluation.Decision)
			if disposition == "unknown" {
				result.err = fmt.Errorf("row %d has unknown decision %q", ordinal, evaluation.Decision)
				_ = reader.Close()
				_ = file.Close()
				return result
			}
			if row.AmountMinorUnits == nil {
				result.counts.UnknownAmountRows++
			} else {
				result.counts.KnownAmountRows++
				amount := big.NewInt(*row.AmountMinorUnits)
				result.amounts.known.Add(&result.amounts.known, amount)
				switch disposition {
				case "included":
					result.amounts.included.Add(&result.amounts.included, amount)
				case "excluded":
					result.amounts.excluded.Add(&result.amounts.excluded, amount)
				case "unresolved":
					result.amounts.unresolved.Add(&result.amounts.unresolved, amount)
				}
			}
			if disposition == "unresolved" {
				exception, err := newException(calculationSetID, cycle, ordinal, row, evaluation)
				if err != nil {
					result.err = fmt.Errorf("row %d: %w", ordinal, err)
					_ = reader.Close()
					_ = file.Close()
					return result
				}
				result.exceptions = append(result.exceptions, exception)
			}
			if evaluation.Decision == DecisionIncluded {
				if row.AmountMinorUnits == nil {
					result.err = fmt.Errorf("included row %d has no amount", ordinal)
					_ = reader.Close()
					_ = file.Close()
					return result
				}
				result.counts.IncludedRows++
				switch {
				case *row.AmountMinorUnits > 0:
					result.counts.IncludedPositiveRows++
				case *row.AmountMinorUnits < 0:
					result.counts.IncludedNegativeRows++
				default:
					result.counts.IncludedZeroRows++
				}
				key := groupKey{source: evaluation.SourceCommitteeID, recipient: *row.RecipientID, role: evaluation.ReceiptRole}
				value := result.groups[key]
				if value == nil {
					value = &groupAccumulator{}
					result.groups[key] = value
				}
				value.amount.Add(&value.amount, big.NewInt(*row.AmountMinorUnits))
				value.count++
				switch {
				case *row.AmountMinorUnits > 0:
					value.positive++
				case *row.AmountMinorUnits < 0:
					value.negative++
				default:
					value.zero++
				}
				result.sources[evaluation.SourceCommitteeID] = struct{}{}
				result.recipients[*row.RecipientID] = struct{}{}
				if evaluation.SourceCommitteeID == *row.RecipientID {
					result.counts.SelfEdgeRows++
					result.selfGroups[key] = struct{}{}
				}
			}
			if result.rows&0x3fff == 0 {
				if err := ctx.Err(); err != nil {
					result.err = err
					_ = reader.Close()
					_ = file.Close()
					return result
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			result.err = readErr
			_ = reader.Close()
			_ = file.Close()
			return result
		}
	}
	if err := reader.Close(); err != nil {
		result.err = err
		_ = file.Close()
		return result
	}
	if err := file.Close(); err != nil {
		result.err = err
		return result
	}
	if result.rows != shard.Facts {
		result.err = fmt.Errorf("shard %d yielded %d facts; want %d", shard.Index, result.rows, shard.Facts)
	}
	return result
}

func addDecision(counts *DecisionCounts, decision string) error {
	switch decision {
	case DecisionInvalidNormalization:
		counts.InvalidNormalization++
	case DecisionUnresolvedRecipient:
		counts.UnresolvedRecipientCommitteeID++
	case DecisionExcludedNoSource:
		counts.ExcludedNoSourceCommitteeID++
	case DecisionUnresolvedOneSided:
		counts.UnresolvedOneSidedSourceCommitteeID++
	case DecisionUnresolvedConflict:
		counts.UnresolvedConflictingSourceCommitteeIDs++
	case DecisionExcludedMemo:
		counts.ExcludedMemoSubtotal++
	case DecisionUnresolvedAmount:
		counts.UnresolvedAmount++
	case DecisionExcludedOutbound:
		counts.ExcludedOutboundReceiptRole++
	case DecisionExcludedSemanticMemo:
		counts.ExcludedSemanticMemoReceiptRole++
	case DecisionExcludedEarmarked:
		counts.ExcludedEarmarkedReceiptRole++
	case DecisionExcludedNoncommittee:
		counts.ExcludedNoncommitteeReceiptRole++
	case DecisionUnresolvedRole:
		counts.UnresolvedReceiptRole++
	case DecisionIncluded:
		counts.IncludedReceiverReportedCommitteeFlow++
	default:
		return fmt.Errorf("unsupported committee-flow decision %q", decision)
	}
	return nil
}

func mergeShardScan(target *completeScan, source shardScan) error {
	mergeDecisionCounts(&target.decisions, source.decisions)
	mergeResultCounts(&target.counts, source.counts)
	target.amounts.known.Add(&target.amounts.known, &source.amounts.known)
	target.amounts.included.Add(&target.amounts.included, &source.amounts.included)
	target.amounts.excluded.Add(&target.amounts.excluded, &source.amounts.excluded)
	target.amounts.unresolved.Add(&target.amounts.unresolved, &source.amounts.unresolved)
	target.exceptions = append(target.exceptions, source.exceptions...)
	for key, value := range source.groups {
		accumulator := target.groups[key]
		if accumulator == nil {
			accumulator = &groupAccumulator{}
			target.groups[key] = accumulator
		}
		accumulator.amount.Add(&accumulator.amount, &value.amount)
		accumulator.count += value.count
		accumulator.positive += value.positive
		accumulator.negative += value.negative
		accumulator.zero += value.zero
	}
	for value := range source.sources {
		target.sources[value] = struct{}{}
	}
	for value := range source.recipients {
		target.recipients[value] = struct{}{}
	}
	for value := range source.selfGroups {
		target.selfGroups[value] = struct{}{}
	}
	return nil
}

func mergeDecisionCounts(target *DecisionCounts, source DecisionCounts) {
	target.SourceFacts += source.SourceFacts
	target.InvalidNormalization += source.InvalidNormalization
	target.UnresolvedRecipientCommitteeID += source.UnresolvedRecipientCommitteeID
	target.ExcludedNoSourceCommitteeID += source.ExcludedNoSourceCommitteeID
	target.UnresolvedOneSidedSourceCommitteeID += source.UnresolvedOneSidedSourceCommitteeID
	target.UnresolvedConflictingSourceCommitteeIDs += source.UnresolvedConflictingSourceCommitteeIDs
	target.ExcludedMemoSubtotal += source.ExcludedMemoSubtotal
	target.UnresolvedAmount += source.UnresolvedAmount
	target.ExcludedOutboundReceiptRole += source.ExcludedOutboundReceiptRole
	target.ExcludedSemanticMemoReceiptRole += source.ExcludedSemanticMemoReceiptRole
	target.ExcludedEarmarkedReceiptRole += source.ExcludedEarmarkedReceiptRole
	target.ExcludedNoncommitteeReceiptRole += source.ExcludedNoncommitteeReceiptRole
	target.UnresolvedReceiptRole += source.UnresolvedReceiptRole
	target.IncludedReceiverReportedCommitteeFlow += source.IncludedReceiverReportedCommitteeFlow
}

func mergeResultCounts(target *ResultCounts, source ResultCounts) {
	target.KnownAmountRows += source.KnownAmountRows
	target.UnknownAmountRows += source.UnknownAmountRows
	target.IncludedRows += source.IncludedRows
	target.IncludedPositiveRows += source.IncludedPositiveRows
	target.IncludedNegativeRows += source.IncludedNegativeRows
	target.IncludedZeroRows += source.IncludedZeroRows
	target.SelfEdgeRows += source.SelfEdgeRows
}

func decisionTotal(counts DecisionCounts) uint64 {
	return counts.InvalidNormalization + counts.UnresolvedRecipientCommitteeID + counts.ExcludedNoSourceCommitteeID +
		counts.UnresolvedOneSidedSourceCommitteeID + counts.UnresolvedConflictingSourceCommitteeIDs + counts.ExcludedMemoSubtotal +
		counts.UnresolvedAmount + counts.ExcludedOutboundReceiptRole + counts.ExcludedSemanticMemoReceiptRole +
		counts.ExcludedEarmarkedReceiptRole + counts.ExcludedNoncommitteeReceiptRole + counts.UnresolvedReceiptRole +
		counts.IncludedReceiverReportedCommitteeFlow
}

func unresolvedTotal(counts DecisionCounts) uint64 {
	return counts.InvalidNormalization + counts.UnresolvedRecipientCommitteeID + counts.UnresolvedOneSidedSourceCommitteeID +
		counts.UnresolvedConflictingSourceCommitteeIDs + counts.UnresolvedAmount + counts.UnresolvedReceiptRole
}

func sortedGroupKeys(groups map[groupKey]*groupAccumulator) []groupKey {
	keys := make([]groupKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].source != keys[right].source {
			return keys[left].source < keys[right].source
		}
		if keys[left].recipient != keys[right].recipient {
			return keys[left].recipient < keys[right].recipient
		}
		return keys[left].role < keys[right].role
	})
	return keys
}

func newException(calculationSetID, cycle string, ordinal uint64, row flowParquetRow, evaluation Evaluation) (Exception, error) {
	amount := (*string)(nil)
	if row.AmountMinorUnits != nil {
		value := strconv.FormatInt(*row.AmountMinorUnits, 10)
		amount = &value
	}
	reasons, err := exceptionReasons(row, evaluation.Decision)
	if err != nil {
		return Exception{}, err
	}
	return Exception{
		SchemaVersion:    ExceptionSchemaVersion,
		ExceptionID:      digestParts("fec.receiver-reported-committee-flow.exception.v1", calculationSetID, strconv.FormatUint(ordinal, 10), evaluation.Decision),
		CalculationSetID: calculationSetID, NaturalKey: "fec:schedule-a:" + cycle + ":" + strconv.FormatUint(ordinal, 10),
		SourceRowOrdinal: ordinal, State: evaluation.Decision, AmountMinorUnits: amount,
		RecipientCommitteeID: cloneString(row.RecipientID), ContributorID: cloneString(row.ContributorID),
		CleanContributorID: cloneString(row.CleanContributorID), ReceiptTypeCode: cloneString(row.ReceiptTypeCode),
		ReasonCodes: reasons,
	}, nil
}

func exceptionReasons(row flowParquetRow, decision string) ([]string, error) {
	switch decision {
	case DecisionInvalidNormalization:
		return []string{"normalization_invalid"}, nil
	case DecisionUnresolvedRecipient:
		return []string{"recipient_committee_id_invalid"}, nil
	case DecisionUnresolvedOneSided:
		identity, _ := ClassifySourceIdentity(row.ContributorID, row.CleanContributorID)
		if identity == IdentityRawOnly {
			return []string{"source_committee_id_raw_only"}, nil
		}
		return []string{"source_committee_id_clean_only"}, nil
	case DecisionUnresolvedConflict:
		return []string{"source_committee_ids_conflict"}, nil
	case DecisionUnresolvedAmount:
		if row.AmountState == "source_null" {
			return []string{"amount_source_null"}, nil
		}
		return []string{"amount_invalid"}, nil
	case DecisionUnresolvedRole:
		if row.ReceiptTypeCode == nil || *row.ReceiptTypeCode == "" {
			return []string{"receipt_type_missing"}, nil
		}
		return []string{"receipt_type_unmapped"}, nil
	default:
		return nil, fmt.Errorf("decision %q has no exception contract", decision)
	}
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
