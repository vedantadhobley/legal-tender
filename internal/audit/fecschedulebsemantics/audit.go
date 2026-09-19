package fecschedulebsemantics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const maxGroups = 100_000

// Only the columns used by this audit are decoded. The complete backing bytes
// are digest-verified by the manifest loader before this projection is read.
type probeRow struct {
	Ordinal                  int64   `parquet:"lt_source_row_ordinal"`
	Period                   int64   `parquet:"lt_two_year_transaction_period"`
	Amount                   *int64  `parquet:"lt_disbursement_amount_minor_units,optional"`
	AmountState              string  `parquet:"lt_disbursement_amount_state"`
	Memoed                   bool    `parquet:"lt_memoed_subtotal"`
	SubID                    string  `parquet:"sub_id"`
	Form                     string  `parquet:"filing_form"`
	Line                     *string `parquet:"line_num,optional"`
	LineLabel                *string `parquet:"line_number_label,optional"`
	Schedule                 *string `parquet:"schedule_type,optional"`
	Action                   *string `parquet:"action_cd,optional"`
	Memo                     *string `parquet:"memo_cd,optional"`
	DisbursementType         *string `parquet:"disb_tp,optional"`
	EntityType               *string `parquet:"entity_tp,optional"`
	Sender                   *string `parquet:"cmte_id,optional"`
	RawRecipient             *string `parquet:"recipient_cmte_id,optional"`
	CleanRecipient           *string `parquet:"clean_recipient_cmte_id,optional"`
	Candidate                *string `parquet:"cand_id,optional"`
	File                     *string `parquet:"file_num,optional"`
	Link                     *string `parquet:"link_id,optional"`
	Transaction              *string `parquet:"tran_id,optional"`
	OriginalSubmission       *string `parquet:"orig_sub_id,optional"`
	BackReferenceTransaction *string `parquet:"back_ref_tran_id,optional"`
	BackReferenceSchedule    *string `parquet:"back_ref_sched_id,optional"`
	ConduitName              *string `parquet:"conduit_cmte_nm,optional"`
	BeneficiaryName          *string `parquet:"benef_cmte_nm,optional"`
}

// Audit scans every fact once and proves disjoint group and exact-cent
// conservation. No result from this diagnostic changes fact or graph state.
func Audit(ctx context.Context, options Options) (Result, error) {
	started := time.Now().UTC()
	result := Result{SchemaVersion: Version, Status: "incomplete", Cycle: options.Cycle, StartedAt: started}
	if options.Workers == 0 {
		options.Workers = 4
	}
	if options.Workers < 1 || options.Workers > 16 {
		return result, fmt.Errorf("workers must be between 1 and 16")
	}
	if options.Progress == nil {
		options.Progress = func(string) {}
	}
	options.Progress("verifying Schedule B fact manifest and every backing shard digest")
	manifest, digest, err := fecoccurrence.LoadPublishedScheduleBColumnarManifest(ctx, options.StorageRoot, options.FactManifestPath)
	if err != nil {
		return result, err
	}
	if manifest.Cycle != options.Cycle {
		return result, fmt.Errorf("Schedule B fact cycle %s differs from requested %s", manifest.Cycle, options.Cycle)
	}
	result.Workers = options.Workers
	result.Input = Input{manifest.FactSetID, digest, manifest.SourceReleaseID, manifest.SourceArtifactSHA256, manifest.Counts.Facts, uint64(len(manifest.Shards))}
	result.Caveats = []string{
		"Reporting roles describe exact form lines; they do not establish an economic transfer, beneficial owner, or cash payment.",
		"The non-memo subtotal is a selection hypothesis over the publisher processed snapshot, not an accepted total-spending calculation.",
		"Action codes, original submissions and back references are measured without local amendment selection or transaction-key deduplication.",
		"The example for each shape is the lowest source ordinal, not a representative statistical sample.",
	}
	groups, rows, err := scan(ctx, options, manifest)
	if err != nil {
		return result, err
	}
	result.Groups = make([]Group, 0, len(groups))
	for _, group := range groups {
		if err := mergeMeasures(&result.Measures, group.Measures); err != nil {
			return result, err
		}
		result.Groups = append(result.Groups, *group)
	}
	// Deterministic ordering makes worker completion order irrelevant.
	sort.Slice(result.Groups, func(i, j int) bool { return result.Groups[i].Example.Ordinal < result.Groups[j].Example.Ordinal })
	if rows != manifest.Counts.Facts || result.Measures.Rows != rows || result.Measures.AmountRows+result.Measures.MissingAmountRows != rows {
		return result, fmt.Errorf("Schedule B profile failed row conservation")
	}
	content, err := json.Marshal(result.Groups)
	if err != nil {
		return result, err
	}
	profileDigest := sha256.Sum256(content)
	result.ProfileSHA256 = hex.EncodeToString(profileDigest[:])
	result.Status = "complete_diagnostic"
	result.ElapsedSeconds = time.Since(started).Seconds()
	return result, nil
}

type shardResult struct {
	groups map[Shape]*Group
	rows   uint64
	err    error
}

func scan(ctx context.Context, options Options, manifest fecoccurrence.ScheduleBColumnarManifest) (map[Shape]*Group, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan fecoccurrence.ScheduleBColumnarShard)
	results := make(chan shardResult, options.Workers)
	var workers sync.WaitGroup
	for range options.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for shard := range jobs {
				if ctx.Err() != nil {
					return
				}
				result := scanShard(ctx, options.StorageRoot, options.Cycle, shard)
				results <- result
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
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	groups := make(map[Shape]*Group)
	var rows uint64
	var firstErr error
	completed := 0
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			cancel()
			continue
		}
		if firstErr != nil {
			continue
		}
		rows += result.rows
		for shape, incoming := range result.groups {
			if group, exists := groups[shape]; exists {
				if err := mergeMeasures(&group.Measures, incoming.Measures); err != nil {
					firstErr = err
					cancel()
					break
				}
				if incoming.Example.Ordinal < group.Example.Ordinal {
					group.Example = incoming.Example
				}
			} else {
				if len(groups) >= maxGroups {
					firstErr = fmt.Errorf("profile exceeds %d shapes; review cardinality before expanding memory", maxGroups)
					cancel()
					break
				}
				groups[shape] = incoming
			}
		}
		completed++
		if completed%16 == 0 || completed == len(manifest.Shards) {
			options.Progress(fmt.Sprintf("profiled %d/%d shards, %d rows, %d shapes", completed, len(manifest.Shards), rows, len(groups)))
		}
	}
	if firstErr != nil {
		return nil, rows, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, rows, err
	}
	if completed != len(manifest.Shards) {
		return nil, rows, fmt.Errorf("incomplete shard scan")
	}
	return groups, rows, nil
}

func scanShard(ctx context.Context, storageRoot, cycle string, shard fecoccurrence.ScheduleBColumnarShard) (result shardResult) {
	result.groups = make(map[Shape]*Group)
	period, err := strconv.ParseInt(cycle, 10, 64)
	if err != nil {
		result.err = err
		return
	}
	path, err := storageartifact.Resolve(storageRoot, shard.StorageKey)
	if err != nil {
		result.err = err
		return
	}
	file, err := os.Open(path)
	if err != nil {
		result.err = err
		return
	}
	defer file.Close()
	physical, err := parquet.OpenFile(file, int64(shard.Bytes))
	if err != nil {
		result.err = err
		return
	}
	expected, err := schedulebparquet.NewSchema()
	if err != nil {
		result.err = err
		return
	}
	if physical.Schema().String() != expected.Parquet().String() {
		result.err = fmt.Errorf("shard %d physical schema mismatch", shard.Index)
		return
	}
	reader := parquet.NewGenericReader[probeRow](physical)
	defer reader.Close()
	buffer := make([]probeRow, 4096)
	for {
		if err := ctx.Err(); err != nil {
			result.err = err
			return
		}
		n, readErr := reader.Read(buffer)
		for i := 0; i < n; i++ {
			row := &buffer[i]
			expectedOrdinal := shard.FirstSourceRowOrdinal + result.rows
			if row.Ordinal <= 0 || uint64(row.Ordinal) != expectedOrdinal || row.Period != period {
				result.err = fmt.Errorf("shard %d locator or cycle mismatch at ordinal %d", shard.Index, row.Ordinal)
				return
			}
			if err := observe(result.groups, row); err != nil {
				result.err = err
				return
			}
			result.rows++
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				result.err = readErr
			}
			break
		}
	}
	if result.err == nil && result.rows != shard.Facts {
		result.err = fmt.Errorf("shard %d row count mismatch", shard.Index)
	}
	return
}

func observe(groups map[Shape]*Group, row *probeRow) error {
	if row.Memoed != (row.Memo != nil && *row.Memo == "X") {
		return fmt.Errorf("memo projection mismatch at row %d", row.Ordinal)
	}
	if (row.AmountState == "reported_value") != (row.Amount != nil) || (row.AmountState != "reported_value" && row.AmountState != "source_null") {
		return fmt.Errorf("amount projection mismatch at row %d: %s", row.Ordinal, row.AmountState)
	}
	shape := Shape{Form: row.Form, Line: cell(row.Line), Schedule: cell(row.Schedule), Action: cell(row.Action), Memo: cell(row.Memo), DisbursementType: cell(row.DisbursementType), EntityType: cell(row.EntityType), RecipientIdentity: recipientIdentity(row.RawRecipient, row.CleanRecipient), ValidSender: committeeID(row.Sender)}
	shape.ReportingRole = reportingRole(shape.Form, shape.Line, shape.Schedule)
	shape.SelfRecipient = shape.ValidSender && committeeID(row.RawRecipient) && *row.Sender == *row.RawRecipient
	group, exists := groups[shape]
	if !exists {
		if len(groups) >= maxGroups {
			return fmt.Errorf("shard profile exceeds %d shapes", maxGroups)
		}
		group = &Group{Shape: shape}
		// JSON copy detaches the few retained examples from reused reader buffers.
		example := Example{row.Ordinal, row.SubID, row.Sender, row.RawRecipient, row.CleanRecipient, row.Candidate, row.File, row.Transaction, row.OriginalSubmission, row.BackReferenceTransaction, row.BackReferenceSchedule, row.LineLabel}
		content, err := json.Marshal(example)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(content, &group.Example); err != nil {
			return err
		}
		groups[shape] = group
	}
	m := &group.Measures
	m.Rows++
	if row.Amount != nil {
		m.AmountRows++
		if err := addMoney(&m.AmountMinorUnits, *row.Amount); err != nil {
			return err
		}
		if *row.Amount < 0 {
			m.NegativeRows++
		}
		if *row.Amount == 0 {
			m.ZeroRows++
		}
	} else {
		m.MissingAmountRows++
	}
	if !row.Memoed {
		m.NonMemoRows++
		if row.Amount != nil {
			if err := addMoney(&m.NonMemoAmountMinorUnits, *row.Amount); err != nil {
				return err
			}
		}
	}
	if present(row.Transaction) {
		m.TransactionRows++
	}
	if present(row.BackReferenceTransaction) {
		m.BackReferenceTransactionRows++
	}
	if present(row.BackReferenceSchedule) {
		m.BackReferenceScheduleRows++
	}
	if present(row.OriginalSubmission) {
		m.OriginalSubmissionRows++
		if *row.OriginalSubmission == row.SubID {
			m.OriginalEqualsSubmissionRows++
		}
	}
	if present(row.File) {
		m.FileRows++
	}
	if present(row.Link) {
		m.LinkRows++
	}
	if present(row.Candidate) {
		m.CandidateRows++
	}
	if present(row.ConduitName) {
		m.ConduitNameRows++
	}
	if present(row.BeneficiaryName) {
		m.BeneficiaryNameRows++
	}
	return nil
}

func addMoney(target *int64, amount int64) error {
	if amount > 0 && *target > math.MaxInt64-amount || amount < 0 && *target < math.MinInt64-amount {
		return fmt.Errorf("signed-cent overflow")
	}
	*target += amount
	return nil
}

func mergeMeasures(target *Measures, source Measures) error {
	if err := addMoney(&target.AmountMinorUnits, source.AmountMinorUnits); err != nil {
		return err
	}
	if err := addMoney(&target.NonMemoAmountMinorUnits, source.NonMemoAmountMinorUnits); err != nil {
		return err
	}
	target.Rows += source.Rows
	target.AmountRows += source.AmountRows
	target.MissingAmountRows += source.MissingAmountRows
	target.NegativeRows += source.NegativeRows
	target.ZeroRows += source.ZeroRows
	target.NonMemoRows += source.NonMemoRows
	target.TransactionRows += source.TransactionRows
	target.BackReferenceTransactionRows += source.BackReferenceTransactionRows
	target.BackReferenceScheduleRows += source.BackReferenceScheduleRows
	target.OriginalSubmissionRows += source.OriginalSubmissionRows
	target.OriginalEqualsSubmissionRows += source.OriginalEqualsSubmissionRows
	target.FileRows += source.FileRows
	target.LinkRows += source.LinkRows
	target.CandidateRows += source.CandidateRows
	target.ConduitNameRows += source.ConduitNameRows
	target.BeneficiaryNameRows += source.BeneficiaryNameRows
	return nil
}
