// Package fecclassicflow profiles the classic FEC pas2 and oth products used
// by the legacy graph before a processed Schedule B or E source is selected.
package fecclassicflow

import (
	"archive/zip"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	schemaVersion   = "legal-tender.fec.classic-flow-audit.v1"
	pas2FieldCount  = 22
	othFieldCount   = 21
	maxScannerToken = 1 << 20
	maxIssues       = 20
)

var (
	errAccumulatorOverflow = errors.New("classic flow accumulator overflow")

	legacyTransferEntityTypes = map[string]struct{}{
		"CCM": {}, "COM": {}, "ORG": {}, "PAC": {}, "PTY": {},
	}
	legacyOthEntityTypes = map[string]struct{}{
		"COM": {}, "ORG": {}, "PAC": {}, "PTY": {},
	}
	legacyPas2TransferTypes = map[string]struct{}{
		"24K": {}, "24P": {}, "24Z": {},
	}
	legacyOthTransferTypes = map[string]struct{}{
		"11": {}, "11A": {}, "11B": {}, "11C": {},
		"15": {}, "15B": {}, "15C": {}, "15E": {}, "15Z": {},
		"18G": {}, "18H": {}, "18K": {}, "18L": {}, "22Z": {},
	}
	legacyIndependentExpenditureTypes = map[string]struct{}{
		"24A": {}, "24E": {},
	}
)

// Options names exact local classic artifacts for one two-year period.
type Options struct {
	Pas2Path string
	OthPath  string
	Period   string
	Progress func(string)
}

// InputEvidence identifies the exact physical member that was scanned.
type InputEvidence struct {
	Kind               string `json:"kind"`
	Path               string `json:"path"`
	ArchiveBytes       int64  `json:"archive_bytes"`
	ModifiedAt         string `json:"modified_at"`
	Member             string `json:"member"`
	MemberBytes        uint64 `json:"member_bytes"`
	MemberCRC32        string `json:"member_crc32"`
	MemberSHA256       string `json:"member_sha256"`
	Rows               uint64 `json:"rows"`
	ValidRows          uint64 `json:"valid_rows"`
	InvalidRows        uint64 `json:"invalid_rows"`
	DuplicateSubIDRows uint64 `json:"duplicate_sub_id_rows,omitempty"`
}

// MoneyRows conserves exact row counts and signed cents.
type MoneyRows struct {
	Rows              uint64 `json:"rows"`
	PositiveRows      uint64 `json:"positive_rows"`
	NegativeRows      uint64 `json:"negative_rows"`
	ZeroRows          uint64 `json:"zero_rows"`
	NullAmountRows    uint64 `json:"null_amount_rows"`
	InvalidAmountRows uint64 `json:"invalid_amount_rows"`
	SignedAmountCents int64  `json:"signed_amount_cents"`
}

// RowShape reports whether source rows retain the endpoints and lineage keys
// needed by the target flow facts.
type RowShape struct {
	MoneyRows
	WithFilerCommitteeID        uint64 `json:"with_filer_committee_id"`
	WithCounterpartyCommitteeID uint64 `json:"with_counterparty_committee_id"`
	WithCandidateID             uint64 `json:"with_candidate_id"`
	WithTransactionDate         uint64 `json:"with_transaction_date"`
	WithTransactionID           uint64 `json:"with_transaction_id"`
	WithFileNumber              uint64 `json:"with_file_number"`
	MemoXRows                   uint64 `json:"memo_x_rows"`
}

// ProductProfile describes one complete classic member without interpreting
// its code values beyond grouping them.
type ProductProfile struct {
	Total             RowShape            `json:"total"`
	ByTransactionType map[string]RowShape `json:"by_transaction_type"`
	ByAmendment       map[string]RowShape `json:"by_amendment"`
	ByReportType      map[string]RowShape `json:"by_report_type"`
	ByEntityType      map[string]RowShape `json:"by_entity_type"`
	ByMemoCode        map[string]RowShape `json:"by_memo_code"`
}

// Cohorts reproduces only named legacy graph predicates. These are measured
// parity evidence, not accepted target calculations.
type Cohorts struct {
	Pas2LegacyTransferProjection RowShape `json:"pas2_legacy_transfer_projection"`
	OthLegacyTransferProjection  RowShape `json:"oth_legacy_transfer_projection"`
	Pas2IndependentExpenditures  RowShape `json:"pas2_independent_expenditures"`
}

// SubIDOverlap measures the FEC's claim that pas2 is a subset of oth for the
// exact local snapshots.
type SubIDOverlap struct {
	Pas2UniqueSubIDs           uint64            `json:"pas2_unique_sub_ids"`
	Pas2DuplicateSubIDRows     uint64            `json:"pas2_duplicate_sub_id_rows"`
	Pas2SubIDsFoundInOth       uint64            `json:"pas2_sub_ids_found_in_oth"`
	Pas2SubIDsAbsentFromOth    uint64            `json:"pas2_sub_ids_absent_from_oth"`
	OthRowsMatchingPas2SubID   uint64            `json:"oth_rows_matching_pas2_sub_id"`
	FoundByPas2TransactionType map[string]uint64 `json:"found_by_pas2_transaction_type"`
}

// TransferSignatureOverlap identifies possible two-sided reports using only
// source committee, destination committee, date, and signed amount. It does
// not assert that a matched signature is one economic transfer.
type TransferSignatureOverlap struct {
	Pas2Signatures             uint64 `json:"pas2_signatures"`
	OthSignatures              uint64 `json:"oth_signatures"`
	SharedSignatures           uint64 `json:"shared_signatures"`
	Pas2RowsAtSharedSignatures uint64 `json:"pas2_rows_at_shared_signatures"`
	OthRowsAtSharedSignatures  uint64 `json:"oth_rows_at_shared_signatures"`
	PossibleMatchedRowPairs    uint64 `json:"possible_matched_row_pairs"`
	PossibleMatchedAmountCents int64  `json:"possible_matched_amount_cents"`
}

// LogicalKeyProfile measures repeated filer-plus-transaction-ID keys. These
// are amendment or repeated-report candidates, not automatic duplicates.
type LogicalKeyProfile struct {
	RowsWithKey        uint64 `json:"rows_with_key"`
	UniqueKeys         uint64 `json:"unique_keys"`
	RepeatedKeys       uint64 `json:"repeated_keys"`
	RowsInRepeatedKeys uint64 `json:"rows_in_repeated_keys"`
}

// Result is the stable machine-readable classic-flow audit.
type Result struct {
	SchemaVersion            string                   `json:"schema_version"`
	Complete                 bool                     `json:"complete"`
	Period                   string                   `json:"period"`
	Inputs                   []InputEvidence          `json:"inputs"`
	Pas2                     ProductProfile           `json:"pas2"`
	Oth                      ProductProfile           `json:"oth"`
	Cohorts                  Cohorts                  `json:"cohorts"`
	SubIDOverlap             SubIDOverlap             `json:"sub_id_overlap"`
	TransferSignatureOverlap TransferSignatureOverlap `json:"transfer_signature_overlap"`
	IELogicalKeys            LogicalKeyProfile        `json:"ie_logical_keys"`
	Issues                   []string                 `json:"issues,omitempty"`
	Caveats                  []string                 `json:"caveats"`
	ElapsedMilliseconds      int64                    `json:"elapsed_milliseconds"`
}

// AuditError marks physical evidence that failed strict validation while
// retaining the completed result for inspection.
type AuditError struct {
	InvalidRows         uint64
	DuplicatePas2SubIDs uint64
}

func (e *AuditError) Error() string {
	return fmt.Sprintf("classic flow audit failed: %d invalid rows, %d duplicate pas2 SUB_ID rows", e.InvalidRows, e.DuplicatePas2SubIDs)
}

type amountState uint8

const (
	amountNull amountState = iota
	amountValid
	amountInvalid
)

type classicRow struct {
	committeeID     []byte
	amendment       []byte
	reportType      []byte
	transactionType []byte
	entityType      []byte
	transactionDate []byte
	amount          []byte
	otherID         []byte
	candidateID     []byte
	transactionID   []byte
	fileNumber      []byte
	memoCode        []byte
	subIDBytes      []byte
	subID           uint64
	cents           int64
	amountState     amountState
}

type transferCounts struct {
	pas2  uint64
	oth   uint64
	cents int64
}

// Audit scans the complete exact ZIP members in bounded memory. The largest
// resident structures scale with pas2, not with the much larger oth product.
func Audit(ctx context.Context, options Options) (Result, error) {
	started := time.Now()
	result := newResult(options.Period)
	if options.Pas2Path == "" || options.OthPath == "" {
		return result, errors.New("pas2 and oth paths are required")
	}

	pas2IDs := make(map[uint64]string, 1_000_000)
	foundPas2IDs := make(map[uint64]struct{}, 1_000_000)
	transferSignatures := make(map[string]*transferCounts, 500_000)
	othTransferSignatures := make(map[string]uint64, 500_000)
	ieLogicalCounts := make(map[string]uint32, 250_000)

	pas2Input, err := scanZIP(ctx, options.Pas2Path, "classic_pas2", "itpas2.txt", func(line []byte) (bool, error) {
		row, parseErr := parseRow(line, true)
		if parseErr != nil {
			return false, parseErr
		}
		duplicate := false
		transactionType := fieldKey(row.transactionType)
		if existing, ok := pas2IDs[row.subID]; ok {
			duplicate = true
			if existing != transactionType {
				addIssue(&result, fmt.Sprintf("pas2 SUB_ID %d appears under transaction types %q and %q", row.subID, existing, transactionType))
			}
		} else {
			pas2IDs[row.subID] = transactionType
		}
		if err := addProductRow(&result.Pas2, row); err != nil {
			return duplicate, err
		}

		if legacyPas2TransferRow(row) {
			if err := addShape(&result.Cohorts.Pas2LegacyTransferProjection, row); err != nil {
				return duplicate, err
			}
			key := transferSignature(row.committeeID, row.otherID, row.transactionDate, row.cents)
			counts := transferSignatures[key]
			if counts == nil {
				counts = &transferCounts{cents: row.cents}
				transferSignatures[key] = counts
			}
			if !incrementUint64(&counts.pas2) {
				return duplicate, errAccumulatorOverflow
			}
		}
		if legacyIERow(row) {
			if err := addShape(&result.Cohorts.Pas2IndependentExpenditures, row); err != nil {
				return duplicate, err
			}
			if len(row.committeeID) > 0 && len(row.transactionID) > 0 {
				key := string(row.committeeID) + "\x1f" + string(row.transactionID)
				count := ieLogicalCounts[key]
				if count == math.MaxUint32 {
					return duplicate, errAccumulatorOverflow
				}
				ieLogicalCounts[key] = count + 1
			}
		}
		return duplicate, nil
	}, options.Progress, &result)
	if err != nil {
		return result, err
	}
	result.Inputs = append(result.Inputs, pas2Input)

	othInput, err := scanZIP(ctx, options.OthPath, "classic_oth", "itoth.txt", func(line []byte) (bool, error) {
		row, parseErr := parseRow(line, false)
		if parseErr != nil {
			return false, parseErr
		}
		if err := addProductRow(&result.Oth, row); err != nil {
			return false, err
		}
		if transactionType, ok := pas2IDs[row.subID]; ok {
			if !incrementUint64(&result.SubIDOverlap.OthRowsMatchingPas2SubID) {
				return false, errAccumulatorOverflow
			}
			if _, seen := foundPas2IDs[row.subID]; !seen {
				foundPas2IDs[row.subID] = struct{}{}
				count := result.SubIDOverlap.FoundByPas2TransactionType[transactionType]
				if count == math.MaxUint64 {
					return false, errAccumulatorOverflow
				}
				result.SubIDOverlap.FoundByPas2TransactionType[transactionType] = count + 1
			}
		}
		if legacyOthTransferRow(row) {
			if err := addShape(&result.Cohorts.OthLegacyTransferProjection, row); err != nil {
				return false, err
			}
			key := transferSignature(row.otherID, row.committeeID, row.transactionDate, row.cents)
			othCount := othTransferSignatures[key]
			if othCount == math.MaxUint64 {
				return false, errAccumulatorOverflow
			}
			othTransferSignatures[key] = othCount + 1
			if counts := transferSignatures[key]; counts != nil {
				if !incrementUint64(&counts.oth) {
					return false, errAccumulatorOverflow
				}
			}
		}
		return false, nil
	}, options.Progress, &result)
	if err != nil {
		return result, err
	}
	result.Inputs = append(result.Inputs, othInput)

	result.SubIDOverlap.Pas2UniqueSubIDs = uint64(len(pas2IDs))
	result.SubIDOverlap.Pas2DuplicateSubIDRows = pas2Input.DuplicateSubIDRows
	result.SubIDOverlap.Pas2SubIDsFoundInOth = uint64(len(foundPas2IDs))
	result.SubIDOverlap.Pas2SubIDsAbsentFromOth = uint64(len(pas2IDs) - len(foundPas2IDs))
	if err := finalizeTransferOverlap(transferSignatures, othTransferSignatures, &result.TransferSignatureOverlap); err != nil {
		return result, err
	}
	finalizeLogicalKeys(ieLogicalCounts, &result.IELogicalKeys)

	result.Complete = true
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	invalidRows := pas2Input.InvalidRows + othInput.InvalidRows
	if invalidRows > 0 || pas2Input.DuplicateSubIDRows > 0 {
		return result, &AuditError{InvalidRows: invalidRows, DuplicatePas2SubIDs: pas2Input.DuplicateSubIDRows}
	}
	return result, nil
}

func newResult(period string) Result {
	return Result{
		SchemaVersion: schemaVersion,
		Period:        period,
		Pas2:          newProductProfile(),
		Oth:           newProductProfile(),
		SubIDOverlap: SubIDOverlap{
			FoundByPas2TransactionType: make(map[string]uint64),
		},
		Caveats: []string{
			"The named cohorts reproduce legacy predicates and do not establish accepted target calculations.",
			"Exact SUB_ID overlap proves publisher-record overlap between these snapshots, not identity of an underlying economic event.",
			"An endpoint/date/amount signature match is only a possible two-sided report; it is never automatic deduplication evidence.",
			"Repeated filer-plus-transaction-ID keys are amendment or repeated-report candidates and require filing-chain evidence before effective-record selection.",
		},
	}
}

func newProductProfile() ProductProfile {
	return ProductProfile{
		ByTransactionType: make(map[string]RowShape),
		ByAmendment:       make(map[string]RowShape),
		ByReportType:      make(map[string]RowShape),
		ByEntityType:      make(map[string]RowShape),
		ByMemoCode:        make(map[string]RowShape),
	}
}

func scanZIP(ctx context.Context, path, kind, memberName string, consume func([]byte) (bool, error), progress func(string), result *Result) (InputEvidence, error) {
	input := InputEvidence{Kind: kind, Path: path}
	stat, err := os.Stat(path)
	if err != nil {
		return input, fmt.Errorf("stat %s: %w", kind, err)
	}
	input.ArchiveBytes = stat.Size()
	input.ModifiedAt = stat.ModTime().UTC().Format(time.RFC3339Nano)

	archive, err := zip.OpenReader(path)
	if err != nil {
		return input, fmt.Errorf("open %s ZIP: %w", kind, err)
	}
	defer archive.Close()
	var member *zip.File
	for _, candidate := range archive.File {
		if candidate.Name == memberName {
			member = candidate
			break
		}
	}
	if member == nil {
		return input, fmt.Errorf("%s does not contain exact member %q", path, memberName)
	}
	input.Member = member.Name
	input.MemberBytes = member.UncompressedSize64
	input.MemberCRC32 = fmt.Sprintf("%08x", member.CRC32)

	reader, err := member.Open()
	if err != nil {
		return input, fmt.Errorf("open %s member: %w", kind, err)
	}
	defer reader.Close()
	digest := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(reader, digest))
	scanner.Buffer(make([]byte, 64<<10), maxScannerToken)
	for scanner.Scan() {
		input.Rows++
		duplicate, consumeErr := consume(scanner.Bytes())
		if consumeErr != nil {
			if errors.Is(consumeErr, errAccumulatorOverflow) {
				return input, fmt.Errorf("%s row %d: %w", kind, input.Rows, consumeErr)
			}
			input.InvalidRows++
			addIssue(result, fmt.Sprintf("%s row %d: %v", kind, input.Rows, consumeErr))
		} else {
			input.ValidRows++
			if duplicate {
				input.DuplicateSubIDRows++
			}
		}
		if input.Rows%1_000_000 == 0 {
			report(progress, fmt.Sprintf("%s: parsed %d rows", kind, input.Rows))
			if err := ctx.Err(); err != nil {
				return input, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return input, fmt.Errorf("scan %s: %w", kind, err)
	}
	if err := ctx.Err(); err != nil {
		return input, err
	}
	input.MemberSHA256 = hex.EncodeToString(digest.Sum(nil))
	report(progress, fmt.Sprintf("%s: complete at %d rows", kind, input.Rows))
	return input, nil
}

func parseRow(line []byte, pas2 bool) (classicRow, error) {
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if !utf8.Valid(line) {
		return classicRow{}, errors.New("row is not valid UTF-8")
	}
	expected := othFieldCount
	if pas2 {
		expected = pas2FieldCount
	}
	var selected [22][]byte
	fieldStart := 0
	fieldIndex := 0
	for index := 0; index <= len(line); index++ {
		if index < len(line) && line[index] != '|' {
			continue
		}
		if fieldIndex < len(selected) {
			selected[fieldIndex] = line[fieldStart:index]
		}
		fieldIndex++
		fieldStart = index + 1
	}
	if fieldIndex != expected {
		return classicRow{}, fmt.Errorf("row has %d fields; want %d", fieldIndex, expected)
	}
	row := classicRow{
		committeeID: selected[0], amendment: selected[1], reportType: selected[2],
		transactionType: selected[5], entityType: selected[6], transactionDate: selected[13],
		amount: selected[14], otherID: selected[15],
	}
	if pas2 {
		row.candidateID = selected[16]
		row.transactionID = selected[17]
		row.fileNumber = selected[18]
		row.memoCode = selected[19]
		row.subIDBytes = selected[21]
	} else {
		row.transactionID = selected[16]
		row.fileNumber = selected[17]
		row.memoCode = selected[18]
		row.subIDBytes = selected[20]
	}
	var ok bool
	row.subID, ok = parseUint(row.subIDBytes)
	if !ok || row.subID == 0 {
		return classicRow{}, errors.New("SUB_ID is empty or invalid")
	}
	if len(row.amount) == 0 {
		row.amountState = amountNull
	} else if cents, valid := parseCents(row.amount); valid {
		row.amountState = amountValid
		row.cents = cents
	} else {
		row.amountState = amountInvalid
	}
	return row, nil
}

func addProductRow(profile *ProductProfile, row classicRow) error {
	if err := addShape(&profile.Total, row); err != nil {
		return err
	}
	for _, group := range []struct {
		values map[string]RowShape
		key    []byte
	}{
		{profile.ByTransactionType, row.transactionType},
		{profile.ByAmendment, row.amendment},
		{profile.ByReportType, row.reportType},
		{profile.ByEntityType, row.entityType},
		{profile.ByMemoCode, row.memoCode},
	} {
		key := fieldKey(group.key)
		shape := group.values[key]
		if err := addShape(&shape, row); err != nil {
			return err
		}
		group.values[key] = shape
	}
	return nil
}

func addShape(shape *RowShape, row classicRow) error {
	shape.Rows++
	switch row.amountState {
	case amountNull:
		shape.NullAmountRows++
	case amountInvalid:
		shape.InvalidAmountRows++
	case amountValid:
		switch {
		case row.cents > 0:
			shape.PositiveRows++
		case row.cents < 0:
			shape.NegativeRows++
		default:
			shape.ZeroRows++
		}
		if (row.cents > 0 && shape.SignedAmountCents > math.MaxInt64-row.cents) ||
			(row.cents < 0 && shape.SignedAmountCents < math.MinInt64-row.cents) {
			return errAccumulatorOverflow
		}
		shape.SignedAmountCents += row.cents
	}
	if len(row.committeeID) > 0 {
		shape.WithFilerCommitteeID++
	}
	if len(row.otherID) > 0 {
		shape.WithCounterpartyCommitteeID++
	}
	if len(row.candidateID) > 0 {
		shape.WithCandidateID++
	}
	if len(row.transactionDate) > 0 {
		shape.WithTransactionDate++
	}
	if len(row.transactionID) > 0 {
		shape.WithTransactionID++
	}
	if len(row.fileNumber) > 0 {
		shape.WithFileNumber++
	}
	if string(row.memoCode) == "X" {
		shape.MemoXRows++
	}
	return nil
}

func legacyPas2TransferRow(row classicRow) bool {
	_, transactionType := legacyPas2TransferTypes[string(row.transactionType)]
	_, entityType := legacyTransferEntityTypes[string(row.entityType)]
	return transactionType && entityType && string(row.memoCode) != "X" &&
		len(row.committeeID) > 0 && len(row.otherID) > 0 && row.amountState == amountValid
}

func legacyOthTransferRow(row classicRow) bool {
	_, transactionType := legacyOthTransferTypes[string(row.transactionType)]
	_, entityType := legacyOthEntityTypes[string(row.entityType)]
	return transactionType && entityType && string(row.memoCode) != "X" &&
		len(row.committeeID) > 0 && len(row.otherID) > 0 && row.amountState == amountValid
}

func legacyIERow(row classicRow) bool {
	_, transactionType := legacyIndependentExpenditureTypes[string(row.transactionType)]
	return transactionType && len(row.committeeID) > 0 && len(row.candidateID) > 0 && row.amountState == amountValid
}

func transferSignature(source, destination, date []byte, cents int64) string {
	return string(source) + "\x1f" + string(destination) + "\x1f" + string(date) + "\x1f" + strconv.FormatInt(cents, 10)
}

func finalizeTransferOverlap(pas2Signatures map[string]*transferCounts, othSignatures map[string]uint64, target *TransferSignatureOverlap) error {
	target.Pas2Signatures = uint64(len(pas2Signatures))
	target.OthSignatures = uint64(len(othSignatures))
	for _, counts := range pas2Signatures {
		if counts.oth == 0 {
			continue
		}
		if !incrementUint64(&target.SharedSignatures) ||
			!addUint64(&target.Pas2RowsAtSharedSignatures, counts.pas2) ||
			!addUint64(&target.OthRowsAtSharedSignatures, counts.oth) {
			return errAccumulatorOverflow
		}
		pairs := counts.pas2
		if counts.oth < pairs {
			pairs = counts.oth
		}
		if !addUint64(&target.PossibleMatchedRowPairs, pairs) {
			return errAccumulatorOverflow
		}
		amount, ok := multiplyCents(pairs, counts.cents)
		if !ok || !addInt64(&target.PossibleMatchedAmountCents, amount) {
			return errAccumulatorOverflow
		}
	}
	return nil
}

func finalizeLogicalKeys(counts map[string]uint32, target *LogicalKeyProfile) {
	target.UniqueKeys = uint64(len(counts))
	for _, count := range counts {
		target.RowsWithKey += uint64(count)
		if count > 1 {
			target.RepeatedKeys++
			target.RowsInRepeatedKeys += uint64(count)
		}
	}
}

func fieldKey(value []byte) string {
	if len(value) == 0 {
		return "<blank>"
	}
	return string(value)
}

func parseUint(value []byte) (uint64, bool) {
	if len(value) == 0 {
		return 0, false
	}
	var result uint64
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		digitValue := uint64(digit - '0')
		if result > (math.MaxUint64-digitValue)/10 {
			return 0, false
		}
		result = result*10 + digitValue
	}
	return result, true
}

func parseCents(value []byte) (int64, bool) {
	if len(value) == 0 {
		return 0, false
	}
	negative := false
	if value[0] == '-' {
		negative = true
		value = value[1:]
	}
	if len(value) == 0 {
		return 0, false
	}
	decimal := -1
	for index, digit := range value {
		if digit == '.' {
			if decimal >= 0 {
				return 0, false
			}
			decimal = index
			continue
		}
		if digit < '0' || digit > '9' {
			return 0, false
		}
	}
	whole := value
	fraction := []byte(nil)
	if decimal >= 0 {
		whole = value[:decimal]
		fraction = value[decimal+1:]
		if len(whole) == 0 || len(fraction) == 0 || len(fraction) > 2 {
			return 0, false
		}
	}
	wholeValue, ok := parseUint(whole)
	if !ok {
		return 0, false
	}
	var fractionValue uint64
	if len(fraction) > 0 {
		fractionValue, ok = parseUint(fraction)
		if !ok {
			return 0, false
		}
		if len(fraction) == 1 {
			fractionValue *= 10
		}
	}
	if wholeValue > (uint64(math.MaxInt64)-fractionValue)/100 {
		return 0, false
	}
	cents := int64(wholeValue*100 + fractionValue)
	if negative {
		cents = -cents
	}
	return cents, true
}

func addIssue(result *Result, message string) {
	if len(result.Issues) < maxIssues {
		result.Issues = append(result.Issues, message)
	}
}

func incrementUint64(target *uint64) bool {
	return addUint64(target, 1)
}

func addUint64(target *uint64, value uint64) bool {
	if *target > math.MaxUint64-value {
		return false
	}
	*target += value
	return true
}

func addInt64(target *int64, value int64) bool {
	if (value > 0 && *target > math.MaxInt64-value) ||
		(value < 0 && *target < math.MinInt64-value) {
		return false
	}
	*target += value
	return true
}

func multiplyCents(count uint64, cents int64) (int64, bool) {
	if count == 0 || cents == 0 {
		return 0, true
	}
	if count > math.MaxInt64 {
		return 0, false
	}
	countInt64 := int64(count)
	if cents > 0 && countInt64 > math.MaxInt64/cents {
		return 0, false
	}
	if cents < 0 && cents < math.MinInt64/countInt64 {
		return 0, false
	}
	return countInt64 * cents, true
}

func report(progress func(string), message string) {
	if progress != nil {
		progress(message)
	}
}
