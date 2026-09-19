package schedulee

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	verificationSchemaVersion = "legal-tender.schedule-e-verification.v1"
	maxReportedIssues         = 20
)

// VerifyOptions defines an optional cycle selection and whole-stream
// conservation expectations. MaxRows creates a partial smoke pass.
type VerifyOptions struct {
	ExpectedCycle  string
	ExpectedRows   uint64
	ExpectedBytes  uint64
	ExpectedSHA256 string
	MaxRows        uint64
}

// Check is one stable machine-readable verification result.
type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Expected string `json:"expected,omitempty"`
	Actual   string `json:"actual,omitempty"`
}

// ValueCount preserves one source-code profile entry.
type ValueCount struct {
	Value string `json:"value"`
	Rows  uint64 `json:"rows"`
}

// Verification is the bounded-memory result for one COPY-row stream.
type Verification struct {
	SchemaVersion          string       `json:"schema_version"`
	Complete               bool         `json:"complete"`
	ExpectedCycle          string       `json:"expected_cycle,omitempty"`
	Rows                   uint64       `json:"rows"`
	ValidRows              uint64       `json:"valid_rows"`
	InvalidRows            uint64       `json:"invalid_rows"`
	DuplicateSubIDs        uint64       `json:"duplicate_sub_ids"`
	Bytes                  uint64       `json:"bytes,omitempty"`
	SHA256                 string       `json:"sha256,omitempty"`
	NegativeAmounts        uint64       `json:"negative_amounts"`
	FractionalAmounts      uint64       `json:"fractional_amounts"`
	NullAmounts            uint64       `json:"null_amounts"`
	NullExpenseDates       uint64       `json:"null_expense_dates"`
	NullDisseminationDates uint64       `json:"null_dissemination_dates"`
	Cycles                 []ValueCount `json:"cycles"`
	ActionCodes            []ValueCount `json:"action_codes"`
	ExpenditureTypes       []ValueCount `json:"expenditure_types"`
	MemoCodes              []ValueCount `json:"memo_codes"`
	ElapsedMilliseconds    int64        `json:"elapsed_milliseconds"`
	Checks                 []Check      `json:"checks"`
	Issues                 []string     `json:"issues,omitempty"`
}

// VerifyError reports completed source-contract checks that failed.
type VerifyError struct {
	FailedChecks int
	InvalidRows  uint64
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("Schedule E verification failed: %d failed checks, %d invalid rows", e.FailedChecks, e.InvalidRows)
}

// Verify streams data-row-only PostgreSQL COPY text, validates every consumed
// row, and computes exact physical identity on a complete pass.
func Verify(ctx context.Context, source io.Reader, options VerifyOptions) (Verification, error) {
	started := time.Now()
	result := Verification{
		SchemaVersion: verificationSchemaVersion,
		ExpectedCycle: options.ExpectedCycle,
		Checks:        make([]Check, 0, 4),
	}
	cycleCounts := make(map[string]uint64)
	actionCounts := make(map[string]uint64)
	typeCounts := make(map[string]uint64)
	memoCounts := make(map[string]uint64)
	seenSubIDs := make(map[uint64]struct{})

	counter := &countingHashReader{reader: source, hash: sha256.New()}
	decoder := NewDecoder(counter)
	partial := false
	for decoder.Scan() {
		result.Rows++
		row := decoder.Row()
		if err := Validate(row, options.ExpectedCycle); err != nil {
			result.InvalidRows++
			if len(result.Issues) < maxReportedIssues {
				result.Issues = append(result.Issues, err.Error())
			}
		} else {
			result.ValidRows++
			subID := rowSubID(row)
			if _, duplicate := seenSubIDs[subID]; duplicate {
				result.DuplicateSubIDs++
				if len(result.Issues) < maxReportedIssues {
					result.Issues = append(result.Issues, fmt.Sprintf("row %d duplicates sub_id %d", row.Number(), subID))
				}
			} else {
				seenSubIDs[subID] = struct{}{}
			}
			profileRow(row, &result, cycleCounts, actionCounts, typeCounts, memoCounts)
		}

		if result.Rows&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return finishVerification(result, started), ctx.Err()
			default:
			}
		}
		if options.MaxRows > 0 && result.Rows >= options.MaxRows {
			partial = true
			break
		}
	}
	if err := decoder.Err(); err != nil {
		return finishVerification(result, started), fmt.Errorf("read COPY stream: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return finishVerification(result, started), err
	}

	result.Complete = !partial
	if result.Complete {
		result.Bytes = counter.bytes
		result.SHA256 = hex.EncodeToString(counter.hash.Sum(nil))
	}
	result.Cycles = sortedCounts(cycleCounts)
	result.ActionCodes = sortedCounts(actionCounts)
	result.ExpenditureTypes = sortedCounts(typeCounts)
	result.MemoCodes = sortedCounts(memoCounts)
	result.Checks = append(result.Checks, Check{
		ID: "row_validity", Passed: result.InvalidRows == 0,
		Expected: "0 invalid rows", Actual: fmt.Sprintf("%d invalid rows", result.InvalidRows),
	})
	result.Checks = append(result.Checks, Check{
		ID: "sub_id_uniqueness", Passed: result.DuplicateSubIDs == 0,
		Expected: "0 duplicate sub_id values", Actual: fmt.Sprintf("%d duplicate sub_id values", result.DuplicateSubIDs),
	})
	if options.ExpectedRows > 0 && result.Complete {
		result.Checks = append(result.Checks, equalityCheck("row_count", options.ExpectedRows, result.Rows))
	}
	if options.ExpectedBytes > 0 && result.Complete {
		result.Checks = append(result.Checks, equalityCheck("byte_count", options.ExpectedBytes, result.Bytes))
	}
	if options.ExpectedSHA256 != "" && result.Complete {
		result.Checks = append(result.Checks, stringCheck("sha256", options.ExpectedSHA256, result.SHA256))
	}

	result = finishVerification(result, started)
	failed := 0
	for _, check := range result.Checks {
		if !check.Passed {
			failed++
		}
	}
	if failed > 0 {
		return result, &VerifyError{FailedChecks: failed, InvalidRows: result.InvalidRows}
	}
	return result, nil
}

func rowSubID(row *Row) uint64 {
	field, _ := row.Field(subIDIndex)
	value, _ := strconv.ParseUint(field.String(), 10, 64)
	return value
}

func profileRow(row *Row, result *Verification, cycles, actions, types, memos map[string]uint64) {
	addFieldCount(row, electionCycleIndex, cycles)
	addFieldCount(row, actionCodeIndex, actions)
	addFieldCount(row, expenditureTypeIndex, types)
	addFieldCount(row, memoCodeIndex, memos)

	amount, _ := row.Field(expenditureAmountIndex)
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
	expenseDate, _ := row.Field(37)
	if expenseDate.IsNull() {
		result.NullExpenseDates++
	}
	disseminationDate, _ := row.Field(35)
	if disseminationDate.IsNull() {
		result.NullDisseminationDates++
	}
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

func equalityCheck(id string, expected, actual uint64) Check {
	return Check{ID: id, Passed: expected == actual, Expected: fmt.Sprintf("%d", expected), Actual: fmt.Sprintf("%d", actual)}
}

func stringCheck(id, expected, actual string) Check {
	return Check{ID: id, Passed: expected == actual, Expected: expected, Actual: actual}
}

type countingHashReader struct {
	reader io.Reader
	hash   hash.Hash
	bytes  uint64
}

func (r *countingHashReader) Read(destination []byte) (int, error) {
	read, err := r.reader.Read(destination)
	if read > 0 {
		r.bytes += uint64(read)
		_, _ = r.hash.Write(destination[:read])
	}
	return read, err
}

// IsVerifyError reports a completed contract-check failure.
func IsVerifyError(err error) bool {
	var verificationError *VerifyError
	return errors.As(err, &verificationError)
}
