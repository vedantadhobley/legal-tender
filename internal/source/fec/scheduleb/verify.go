package scheduleb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	verificationSchemaVersion = "legal-tender.schedule-b-verification.v1"
	verificationShards        = 256
	maxReportedIssues         = 20
)

type VerifyOptions struct {
	ExpectedPeriod  string
	WorkDir         string
	MaxRows         uint64
	Progress        func(string)
	ObserveRow      func(*Row, error) error
	ObserveValidRow func(*Row) error
}

type ValueCount struct {
	Value string `json:"value"`
	Rows  uint64 `json:"rows"`
}

type FieldCoverage struct {
	FilerCommitteeID          uint64 `json:"filer_committee_id"`
	RawRecipientCommitteeID   uint64 `json:"raw_recipient_committee_id"`
	CleanRecipientCommitteeID uint64 `json:"clean_recipient_committee_id"`
	MatchingRecipientIDs      uint64 `json:"matching_recipient_committee_ids"`
	ConflictingRecipientIDs   uint64 `json:"conflicting_recipient_committee_ids"`
	CandidateID               uint64 `json:"candidate_id"`
	DisbursementDate          uint64 `json:"disbursement_date"`
	CommunicationDate         uint64 `json:"communication_date"`
	Purpose                   uint64 `json:"purpose"`
	Category                  uint64 `json:"category"`
	TransactionID             uint64 `json:"transaction_id"`
	BackReferenceID           uint64 `json:"back_reference_transaction_id"`
	OriginalSubmissionID      uint64 `json:"original_submission_id"`
}

type Verification struct {
	SchemaVersion         string        `json:"schema_version"`
	Complete              bool          `json:"complete"`
	ExpectedPeriod        string        `json:"expected_period,omitempty"`
	Rows                  uint64        `json:"rows"`
	ValidRows             uint64        `json:"valid_rows"`
	InvalidRows           uint64        `json:"invalid_rows"`
	UniqueSubIDs          uint64        `json:"unique_sub_ids"`
	DuplicateSubIDRows    uint64        `json:"duplicate_sub_id_rows"`
	Bytes                 uint64        `json:"bytes,omitempty"`
	SHA256                string        `json:"sha256,omitempty"`
	NullAmounts           uint64        `json:"null_amounts"`
	NegativeAmounts       uint64        `json:"negative_amounts"`
	FractionalAmounts     uint64        `json:"fractional_amounts"`
	NullDisbursementDates uint64        `json:"null_disbursement_dates"`
	Coverage              FieldCoverage `json:"field_coverage"`
	Periods               []ValueCount  `json:"periods"`
	DisbursementTypes     []ValueCount  `json:"disbursement_types"`
	ActionCodes           []ValueCount  `json:"action_codes"`
	MemoCodes             []ValueCount  `json:"memo_codes"`
	FilingForms           []ValueCount  `json:"filing_forms"`
	ElapsedMilliseconds   int64         `json:"elapsed_milliseconds"`
	Issues                []string      `json:"issues,omitempty"`
}

type VerifyError struct {
	InvalidRows        uint64
	DuplicateSubIDRows uint64
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("Schedule B verification failed: %d invalid rows, %d duplicate sub_id rows", e.InvalidRows, e.DuplicateSubIDRows)
}

func IsVerifyError(err error) bool {
	var target *VerifyError
	return errors.As(err, &target)
}

// Verify streams data-row-only PostgreSQL COPY text. It validates every row,
// profiles retained sender-side fields, and checks SUB_ID uniqueness with
// bounded memory and automatically removed temporary shards.
func Verify(ctx context.Context, source io.Reader, options VerifyOptions) (Verification, error) {
	started := time.Now()
	result := Verification{SchemaVersion: verificationSchemaVersion, ExpectedPeriod: options.ExpectedPeriod}
	temporaryRoot, err := os.MkdirTemp(options.WorkDir, "legal-tender-schedule-b-verify-")
	if err != nil {
		return result, fmt.Errorf("create Schedule B verification work directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(temporaryRoot) }()
	shards, err := newIDShards(temporaryRoot)
	if err != nil {
		return result, err
	}
	defer shards.abort()

	periods := make(map[string]uint64)
	types := make(map[string]uint64)
	actions := make(map[string]uint64)
	memos := make(map[string]uint64)
	forms := make(map[string]uint64)
	counter := &countingHashReader{reader: source, hash: sha256.New()}
	decoder := NewDecoder(counter)
	partial := false
	for decoder.Scan() {
		result.Rows++
		row := decoder.Row()
		validationErr := Validate(row, options.ExpectedPeriod)
		if options.ObserveRow != nil {
			if err := options.ObserveRow(row, validationErr); err != nil {
				return finishVerification(result, started), fmt.Errorf("observe Schedule B row %d: %w", row.Number(), err)
			}
		}
		if validationErr != nil {
			result.InvalidRows++
			if len(result.Issues) < maxReportedIssues {
				result.Issues = append(result.Issues, validationErr.Error())
			}
		} else {
			result.ValidRows++
			id, err := rowUint64(row, subIDIndex)
			if err != nil {
				return finishVerification(result, started), err
			}
			if err := shards.add(id); err != nil {
				return finishVerification(result, started), err
			}
			if options.ObserveValidRow != nil {
				if err := options.ObserveValidRow(row); err != nil {
					return finishVerification(result, started), fmt.Errorf("observe valid Schedule B row %d: %w", row.Number(), err)
				}
			}
			profileRow(row, &result, periods, types, actions, memos, forms)
		}
		if result.Rows&0xfffff == 0 {
			if options.Progress != nil {
				options.Progress(fmt.Sprintf("processed_schedule_b: parsed %d rows", result.Rows))
			}
			if err := ctx.Err(); err != nil {
				return finishVerification(result, started), err
			}
		}
		if options.MaxRows > 0 && result.Rows >= options.MaxRows {
			partial = true
			break
		}
	}
	if err := decoder.Err(); err != nil {
		return finishVerification(result, started), fmt.Errorf("read Schedule B COPY stream: %w", err)
	}
	if err := shards.close(); err != nil {
		return finishVerification(result, started), err
	}
	duplicates, unique, err := countShardDuplicates(shards.paths)
	if err != nil {
		return finishVerification(result, started), err
	}
	result.DuplicateSubIDRows = duplicates
	result.UniqueSubIDs = unique
	result.Complete = !partial
	if result.Complete {
		result.Bytes = counter.bytes
		result.SHA256 = hex.EncodeToString(counter.hash.Sum(nil))
	}
	result.Periods = sortedCounts(periods)
	result.DisbursementTypes = sortedCounts(types)
	result.ActionCodes = sortedCounts(actions)
	result.MemoCodes = sortedCounts(memos)
	result.FilingForms = sortedCounts(forms)
	result = finishVerification(result, started)
	if result.InvalidRows > 0 || result.DuplicateSubIDRows > 0 {
		return result, &VerifyError{InvalidRows: result.InvalidRows, DuplicateSubIDRows: result.DuplicateSubIDRows}
	}
	return result, nil
}

func profileRow(row *Row, result *Verification, periods, types, actions, memos, forms map[string]uint64) {
	addFieldCount(row, transactionPeriodIndex, periods)
	addFieldCount(row, disbursementTypeIndex, types)
	addFieldCount(row, actionCodeIndex, actions)
	addFieldCount(row, memoCodeIndex, memos)
	addFieldCount(row, filingFormIndex, forms)
	amount, _ := row.Field(disbursementAmountIndex)
	if amount.IsNull() {
		result.NullAmounts++
	} else {
		lexeme := amount.String()
		if strings.HasPrefix(lexeme, "-") {
			result.NegativeAmounts++
		}
		if dot := strings.IndexByte(lexeme, '.'); dot >= 0 && strings.Trim(lexeme[dot+1:], "0") != "" {
			result.FractionalAmounts++
		}
	}
	if fieldNull(row, disbursementDateIndex) {
		result.NullDisbursementDates++
	}
	coverage := &result.Coverage
	addPresent(row, 0, &coverage.FilerCommitteeID)
	addPresent(row, 1, &coverage.RawRecipientCommitteeID)
	addPresent(row, cleanRecipientIDIndex, &coverage.CleanRecipientCommitteeID)
	raw, _ := row.Field(1)
	clean, _ := row.Field(cleanRecipientIDIndex)
	if !raw.IsNull() && !clean.IsNull() {
		if raw.String() == clean.String() {
			coverage.MatchingRecipientIDs++
		} else {
			coverage.ConflictingRecipientIDs++
		}
	}
	addPresent(row, 24, &coverage.CandidateID)
	addPresent(row, disbursementDateIndex, &coverage.DisbursementDate)
	addPresent(row, communicationDateIndex, &coverage.CommunicationDate)
	addPresent(row, 15, &coverage.Purpose)
	addPresent(row, 16, &coverage.Category)
	addPresent(row, 56, &coverage.TransactionID)
	addPresent(row, 57, &coverage.BackReferenceID)
	addPresent(row, 65, &coverage.OriginalSubmissionID)
}

func addPresent(row *Row, index int, destination *uint64) {
	field, _ := row.Field(index)
	if !field.IsNull() && len(field.Bytes()) > 0 {
		(*destination)++
	}
}

func fieldNull(row *Row, index int) bool {
	field, _ := row.Field(index)
	return field.IsNull()
}

func addFieldCount(row *Row, index int, counts map[string]uint64) {
	field, _ := row.Field(index)
	value := "<null>"
	if !field.IsNull() {
		value = field.String()
	}
	counts[value]++
}

func sortedCounts(counts map[string]uint64) []ValueCount {
	values := make([]string, 0, len(counts))
	for value := range counts {
		values = append(values, value)
	}
	sort.Strings(values)
	result := make([]ValueCount, 0, len(values))
	for _, value := range values {
		result = append(result, ValueCount{Value: value, Rows: counts[value]})
	}
	return result
}

func finishVerification(result Verification, started time.Time) Verification {
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	return result
}

func rowUint64(row *Row, index int) (uint64, error) {
	field, _ := row.Field(index)
	value, err := strconv.ParseUint(field.String(), 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("row %d has unencodable sub_id %q", row.Number(), field.String())
	}
	return value, nil
}

type countingHashReader struct {
	reader io.Reader
	hash   hash.Hash
	bytes  uint64
}

func (reader *countingHashReader) Read(destination []byte) (int, error) {
	count, err := reader.reader.Read(destination)
	if count > 0 {
		reader.bytes += uint64(count)
		_, _ = reader.hash.Write(destination[:count])
	}
	return count, err
}

type idShard struct {
	path   string
	file   *os.File
	buffer *bufio.Writer
}

type idShards struct {
	paths  []string
	shards []idShard
	closed bool
}

func newIDShards(root string) (*idShards, error) {
	result := &idShards{paths: make([]string, verificationShards), shards: make([]idShard, verificationShards)}
	for index := range result.shards {
		path := filepath.Join(root, fmt.Sprintf("%03d.ids", index))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			result.abort()
			return nil, err
		}
		result.paths[index] = path
		result.shards[index] = idShard{path: path, file: file, buffer: bufio.NewWriterSize(file, 256<<10)}
	}
	return result, nil
}

func (shards *idShards) add(id uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], id)
	_, err := shards.shards[byte(id)].buffer.Write(encoded[:])
	return err
}

func (shards *idShards) close() error {
	if shards.closed {
		return nil
	}
	shards.closed = true
	var first error
	for index := range shards.shards {
		if err := shards.shards[index].buffer.Flush(); err != nil && first == nil {
			first = err
		}
		if err := shards.shards[index].file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (shards *idShards) abort() {
	_ = shards.close()
	for _, path := range shards.paths {
		_ = os.Remove(path)
	}
}

func countShardDuplicates(paths []string) (duplicates, unique uint64, err error) {
	for _, path := range paths {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return 0, 0, readErr
		}
		if len(content)%8 != 0 {
			return 0, 0, fmt.Errorf("SUB_ID shard has a partial record")
		}
		ids := make([]uint64, len(content)/8)
		for index := range ids {
			ids[index] = binary.BigEndian.Uint64(content[index*8 : index*8+8])
		}
		sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
		for index, id := range ids {
			if id == 0 {
				return 0, 0, fmt.Errorf("SUB_ID shard contains zero")
			}
			if index > 0 && id == ids[index-1] {
				duplicates++
			} else {
				unique++
			}
		}
	}
	return duplicates, unique, nil
}
