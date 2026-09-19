// Package fecschedulea compares a processed Schedule A partition with the
// classic FEC indiv and oth products used by the legacy pipeline.
package fecschedulea

import (
	"archive/zip"
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

	"github.com/klauspost/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

const (
	schemaVersion     = "legal-tender.schedule-a-classic-overlap.v1"
	classicFieldCount = 21
	duplicateShards   = 256
	zstdMemoryLimit   = 64 << 20
	maxIssues         = 20

	memberIndiv       uint8 = 1 << 0
	memberOth         uint8 = 1 << 1
	memberOthRetained uint8 = 1 << 2
	memberScheduleA   uint8 = 1 << 3
)

var legacyOthEntityTypes = map[string]struct{}{
	"COM": {},
	"ORG": {},
	"PAC": {},
	"PTY": {},
}

// Options selects the three local source artifacts and the expected FEC
// transaction period. WorkDir receives only automatically removed temporary
// duplicate-check shards.
type Options struct {
	ScheduleAPath   string
	IndivPath       string
	OthPath         string
	Period          string
	WorkDir         string
	MaxScheduleRows uint64
	Progress        func(string)
}

// InputEvidence identifies one physical input and the exact archive member
// consumed from classic ZIP products.
type InputEvidence struct {
	Kind               string `json:"kind"`
	Path               string `json:"path"`
	ArchiveBytes       int64  `json:"archive_bytes"`
	ModifiedAt         string `json:"modified_at"`
	Member             string `json:"member,omitempty"`
	MemberBytes        uint64 `json:"member_bytes,omitempty"`
	MemberCRC32        string `json:"member_crc32,omitempty"`
	MemberSHA256       string `json:"member_sha256,omitempty"`
	CompressedSHA256   string `json:"compressed_sha256,omitempty"`
	UncompressedBytes  uint64 `json:"uncompressed_bytes,omitempty"`
	UncompressedSHA256 string `json:"uncompressed_sha256,omitempty"`
	Rows               uint64 `json:"rows"`
	ValidRows          uint64 `json:"valid_rows"`
	InvalidRows        uint64 `json:"invalid_rows"`
	UniqueSubIDs       uint64 `json:"unique_sub_ids"`
	DuplicateSubIDRows uint64 `json:"duplicate_sub_id_rows"`
}

// MoneyRows conserves row counts and exact signed cents for source amounts
// whose physical scale can be represented as cents.
type MoneyRows struct {
	Rows              uint64 `json:"rows"`
	PositiveRows      uint64 `json:"positive_rows"`
	NegativeRows      uint64 `json:"negative_rows"`
	ZeroRows          uint64 `json:"zero_rows"`
	NullAmountRows    uint64 `json:"null_amount_rows"`
	InvalidAmountRows uint64 `json:"invalid_amount_rows"`
	SignedAmountCents int64  `json:"signed_amount_cents"`
}

// ScheduleBreakdown describes source rows without interpreting code values.
type ScheduleBreakdown struct {
	Total         MoneyRows            `json:"total"`
	ByEntityType  map[string]MoneyRows `json:"by_entity_type"`
	ByReceiptType map[string]MoneyRows `json:"by_receipt_type"`
	ByActionCode  map[string]MoneyRows `json:"by_action_code"`
	ByMemoCode    map[string]MoneyRows `json:"by_memo_code"`
	ByFilingForm  map[string]MoneyRows `json:"by_filing_form"`
	ByIndividual  map[string]MoneyRows `json:"by_is_individual"`
}

// Overlap contains occurrence counts. They are also unique-SUB_ID counts when
// ScheduleDuplicateSubIDRows is zero.
type Overlap struct {
	ScheduleRowsByClassicMembership map[string]MoneyRows `json:"schedule_rows_by_classic_membership"`
	ScheduleRowsMatchingLegacyOth   MoneyRows            `json:"schedule_rows_matching_legacy_oth_retained"`
	ClassicUniqueSubIDsByMembership map[string]uint64    `json:"classic_unique_sub_ids_by_membership"`
	ClassicOnlySubIDsByMembership   map[string]uint64    `json:"classic_only_sub_ids_by_membership"`
	ClassicOthRetainedUniqueSubIDs  uint64               `json:"classic_oth_retained_unique_sub_ids"`
	ClassicOthRetainedOnlySubIDs    uint64               `json:"classic_oth_retained_only_sub_ids"`
}

// ProductCohorts applies only rules already named in target or legacy design.
// The legacy committee-flow shape is evidence, not an accepted target total.
type ProductCohorts struct {
	ItemizedIndividualIncluded              map[string]MoneyRows `json:"itemized_individual_included_by_classic_membership"`
	ItemizedIndividualNonIndividualExcluded map[string]MoneyRows `json:"itemized_individual_non_individual_excluded_by_classic_membership"`
	ItemizedIndividualMemoExcluded          map[string]MoneyRows `json:"itemized_individual_memo_excluded_by_classic_membership"`
	ItemizedIndividualClassUnresolved       map[string]MoneyRows `json:"itemized_individual_class_unresolved_by_classic_membership"`
	ItemizedIndividualAmountUnresolved      map[string]MoneyRows `json:"itemized_individual_amount_unresolved_by_classic_membership"`
	LegacyCommitteeFlowEntityShape          map[string]MoneyRows `json:"legacy_committee_flow_entity_shape_by_classic_membership"`
	NonIndividualNonMemoValidAmount         map[string]MoneyRows `json:"non_individual_non_memo_valid_amount_by_classic_membership"`
}

// Result is the stable machine-readable overlap audit.
type Result struct {
	SchemaVersion              string            `json:"schema_version"`
	Complete                   bool              `json:"complete"`
	Period                     string            `json:"period"`
	Inputs                     []InputEvidence   `json:"inputs"`
	ScheduleUniqueSubIDs       uint64            `json:"schedule_unique_sub_ids"`
	ScheduleDuplicateSubIDRows uint64            `json:"schedule_duplicate_sub_id_rows"`
	ScheduleAll                ScheduleBreakdown `json:"schedule_a_all"`
	ScheduleAbsentFromClassic  ScheduleBreakdown `json:"schedule_a_absent_from_classic"`
	Overlap                    Overlap           `json:"overlap"`
	ProductCohorts             ProductCohorts    `json:"product_cohorts"`
	Issues                     []string          `json:"issues,omitempty"`
	Caveats                    []string          `json:"caveats"`
	ElapsedMilliseconds        int64             `json:"elapsed_milliseconds"`
}

// AuditError marks a completed audit whose evidence gates failed.
type AuditError struct {
	InvalidRows             uint64
	ScheduleDuplicateSubIDs uint64
}

func (e *AuditError) Error() string {
	return fmt.Sprintf("Schedule A overlap audit failed: %d invalid rows, %d duplicate Schedule A SUB_ID rows", e.InvalidRows, e.ScheduleDuplicateSubIDs)
}

// Audit compares all three inputs in bounded working storage. Classic SUB_ID
// membership remains resident; Schedule A duplicate detection uses temporary
// hash shards and does not retain the 81-column relation.
func Audit(ctx context.Context, options Options) (Result, error) {
	started := time.Now()
	result := newResult(options.Period)

	if options.ScheduleAPath == "" || options.IndivPath == "" || options.OthPath == "" {
		return result, errors.New("schedule-a, indiv, and oth paths are required")
	}

	classicCapacity, err := estimatedClassicRows(options.IndivPath, options.OthPath)
	if err != nil {
		return result, err
	}
	membership := make(map[uint64]uint8, classicCapacity)

	indivInput, err := loadClassic(ctx, options.IndivPath, "classic_indiv", "itcont.txt", memberIndiv, membership, options.Progress)
	if err != nil {
		return result, err
	}
	result.Inputs = append(result.Inputs, indivInput)

	othInput, err := loadClassic(ctx, options.OthPath, "classic_oth", "itoth.txt", memberOth, membership, options.Progress)
	if err != nil {
		return result, err
	}
	result.Inputs = append(result.Inputs, othInput)

	for _, bits := range membership {
		result.Overlap.ClassicUniqueSubIDsByMembership[classicMembership(bits)]++
		if bits&memberOthRetained != 0 {
			result.Overlap.ClassicOthRetainedUniqueSubIDs++
		}
	}

	temporaryRoot, err := os.MkdirTemp(options.WorkDir, "legal-tender-schedule-a-overlap-")
	if err != nil {
		return result, fmt.Errorf("create duplicate-check work directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(temporaryRoot) }()

	scheduleInput, duplicateRows, err := scanScheduleA(ctx, options, membership, temporaryRoot, &result)
	if err != nil {
		return result, err
	}
	result.Inputs = append([]InputEvidence{scheduleInput}, result.Inputs...)
	result.ScheduleDuplicateSubIDRows = duplicateRows
	result.ScheduleUniqueSubIDs = scheduleInput.ValidRows - duplicateRows

	for _, bits := range membership {
		if bits&memberScheduleA == 0 {
			result.Overlap.ClassicOnlySubIDsByMembership[classicMembership(bits)]++
			if bits&memberOthRetained != 0 {
				result.Overlap.ClassicOthRetainedOnlySubIDs++
			}
		}
	}

	result.Complete = options.MaxScheduleRows == 0
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	invalidRows := indivInput.InvalidRows + othInput.InvalidRows + scheduleInput.InvalidRows
	if invalidRows > 0 || duplicateRows > 0 {
		return result, &AuditError{InvalidRows: invalidRows, ScheduleDuplicateSubIDs: duplicateRows}
	}
	return result, nil
}

func newResult(period string) Result {
	return Result{
		SchemaVersion:             schemaVersion,
		Period:                    period,
		ScheduleAll:               newBreakdown(),
		ScheduleAbsentFromClassic: newBreakdown(),
		Overlap: Overlap{
			ScheduleRowsByClassicMembership: make(map[string]MoneyRows),
			ClassicUniqueSubIDsByMembership: make(map[string]uint64),
			ClassicOnlySubIDsByMembership:   make(map[string]uint64),
		},
		ProductCohorts: ProductCohorts{
			ItemizedIndividualIncluded:              make(map[string]MoneyRows),
			ItemizedIndividualNonIndividualExcluded: make(map[string]MoneyRows),
			ItemizedIndividualMemoExcluded:          make(map[string]MoneyRows),
			ItemizedIndividualClassUnresolved:       make(map[string]MoneyRows),
			ItemizedIndividualAmountUnresolved:      make(map[string]MoneyRows),
			LegacyCommitteeFlowEntityShape:          make(map[string]MoneyRows),
			NonIndividualNonMemoValidAmount:         make(map[string]MoneyRows),
		},
		Caveats: []string{
			"SUB_ID establishes processed-record overlap, not identity of an underlying economic transfer.",
			"Classic-row validity in this audit covers the exact 21-field shape and decimal SUB_ID needed for comparison; it is not a full semantic validation of every classic field.",
			"The classic ZIPs and processed Schedule A dump may have different publisher observation times; classic-only and Schedule-A-only rows can therefore include revision lag.",
			"itemized_individual_included reproduces the accepted is_individual=true, memo_cd!=X, valid-amount rule; legacy_committee_flow_entity_shape only reproduces the old oth entity filter and is not an accepted target total.",
		},
	}
}

func newBreakdown() ScheduleBreakdown {
	return ScheduleBreakdown{
		ByEntityType:  make(map[string]MoneyRows),
		ByReceiptType: make(map[string]MoneyRows),
		ByActionCode:  make(map[string]MoneyRows),
		ByMemoCode:    make(map[string]MoneyRows),
		ByFilingForm:  make(map[string]MoneyRows),
		ByIndividual:  make(map[string]MoneyRows),
	}
}

func estimatedClassicRows(paths ...string) (int, error) {
	var bytes uint64
	for _, path := range paths {
		archive, err := zip.OpenReader(path)
		if err != nil {
			return 0, fmt.Errorf("open classic ZIP %s: %w", path, err)
		}
		for _, file := range archive.File {
			if file.Name == "itcont.txt" || file.Name == "itoth.txt" {
				bytes += file.UncompressedSize64
			}
		}
		if err := archive.Close(); err != nil {
			return 0, fmt.Errorf("close classic ZIP %s: %w", path, err)
		}
	}
	estimate := bytes / 180
	if estimate < 16 {
		estimate = 16
	}
	maxInt := uint64(^uint(0) >> 1)
	if estimate > maxInt {
		return 0, errors.New("classic row estimate exceeds platform int")
	}
	return int(estimate), nil
}

func loadClassic(ctx context.Context, path, kind, memberName string, sourceBit uint8, membership map[uint64]uint8, progress func(string)) (InputEvidence, error) {
	input, err := inputEvidence(path, kind)
	if err != nil {
		return input, err
	}
	archive, err := zip.OpenReader(path)
	if err != nil {
		return input, fmt.Errorf("open %s: %w", kind, err)
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
		return input, fmt.Errorf("open %s member %s: %w", kind, member.Name, err)
	}
	defer reader.Close()
	digest := sha256.New()
	lines := newLineReader(io.TeeReader(reader, digest))
	for lines.Scan() {
		input.Rows++
		fields, parseErr := parseClassicRow(lines.Bytes())
		if parseErr != nil {
			input.InvalidRows++
			continue
		}
		id, parseErr := strconv.ParseUint(string(fields.subID), 10, 64)
		if parseErr != nil || id == 0 {
			input.InvalidRows++
			continue
		}
		input.ValidRows++
		bits := membership[id]
		if bits&sourceBit != 0 {
			input.DuplicateSubIDRows++
		} else {
			input.UniqueSubIDs++
		}
		bits |= sourceBit
		if sourceBit == memberOth {
			if _, retained := legacyOthEntityTypes[string(fields.entityType)]; retained {
				bits |= memberOthRetained
			}
		}
		membership[id] = bits

		if input.Rows%5_000_000 == 0 {
			report(progress, fmt.Sprintf("%s: parsed %d rows", kind, input.Rows))
			if err := ctx.Err(); err != nil {
				return input, err
			}
		}
	}
	if err := lines.Err(); err != nil {
		return input, fmt.Errorf("read %s member %s: %w", kind, member.Name, err)
	}
	input.MemberSHA256 = hex.EncodeToString(digest.Sum(nil))
	report(progress, fmt.Sprintf("%s: complete at %d rows", kind, input.Rows))
	return input, nil
}

type classicFields struct {
	entityType []byte
	subID      []byte
}

func parseClassicRow(line []byte) (classicFields, error) {
	line = trimLineEnding(line)
	var fields classicFields
	fieldStart := 0
	fieldIndex := 0
	for index := 0; index <= len(line); index++ {
		if index < len(line) && line[index] != '|' {
			continue
		}
		value := line[fieldStart:index]
		switch fieldIndex {
		case 6:
			fields.entityType = value
		case 20:
			fields.subID = value
		}
		fieldIndex++
		fieldStart = index + 1
	}
	if fieldIndex != classicFieldCount {
		return classicFields{}, fmt.Errorf("classic row has %d fields; want %d", fieldIndex, classicFieldCount)
	}
	if len(fields.subID) == 0 {
		return classicFields{}, errors.New("classic row has empty SUB_ID")
	}
	return fields, nil
}

func scanScheduleA(ctx context.Context, options Options, membership map[uint64]uint8, temporaryRoot string, result *Result) (InputEvidence, uint64, error) {
	input, err := inputEvidence(options.ScheduleAPath, "processed_schedule_a")
	if err != nil {
		return input, 0, err
	}
	file, err := os.Open(options.ScheduleAPath)
	if err != nil {
		return input, 0, fmt.Errorf("open processed Schedule A: %w", err)
	}
	defer file.Close()

	shards, err := newIDShards(temporaryRoot)
	if err != nil {
		return input, 0, err
	}
	defer shards.abort()

	compressedDigest := sha256.New()
	compressed := io.TeeReader(file, compressedDigest)
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(zstdMemoryLimit))
	if err != nil {
		return input, 0, fmt.Errorf("open Schedule A zstd stream: %w", err)
	}
	defer decoder.Close()
	uncompressedDigest := sha256.New()
	uncompressed := &countingHashReader{reader: decoder, hash: uncompressedDigest}
	copyDecoder := schedulea.NewDecoder(uncompressed)
	indexes, err := scheduleIndexes()
	if err != nil {
		return input, 0, err
	}

	for copyDecoder.Scan() {
		input.Rows++
		row := copyDecoder.Row()
		if err := schedulea.Validate(row, options.Period); err != nil {
			input.InvalidRows++
			if len(result.Issues) < maxIssues {
				result.Issues = append(result.Issues, err.Error())
			}
		} else {
			input.ValidRows++
			id, err := rowUint64(row, indexes.subID)
			if err != nil {
				return input, 0, err
			}
			if err := shards.add(id); err != nil {
				return input, 0, err
			}
			bits := membership[id]
			group := classicMembership(bits)
			if group != "neither" {
				bits |= memberScheduleA
				membership[id] = bits
			}

			amount := rowValue(row, indexes.amount)
			measure := measureFor(amount)
			addMeasureMap(result.Overlap.ScheduleRowsByClassicMembership, group, measure)
			addMeasure(&result.ScheduleAll.Total, measure)
			addBreakdown(&result.ScheduleAll, row, indexes, measure)
			if group == "neither" {
				addMeasure(&result.ScheduleAbsentFromClassic.Total, measure)
				addBreakdown(&result.ScheduleAbsentFromClassic, row, indexes, measure)
			}
			if bits&memberOthRetained != 0 {
				addMeasure(&result.Overlap.ScheduleRowsMatchingLegacyOth, measure)
			}
			addProductCohorts(&result.ProductCohorts, row, indexes, group, measure)
		}

		if input.Rows%10_000_000 == 0 {
			report(options.Progress, fmt.Sprintf("processed_schedule_a: parsed %d rows", input.Rows))
			if err := ctx.Err(); err != nil {
				return input, 0, err
			}
		}
		if options.MaxScheduleRows > 0 && input.Rows >= options.MaxScheduleRows {
			break
		}
	}
	if err := copyDecoder.Err(); err != nil {
		return input, 0, fmt.Errorf("read processed Schedule A: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return input, 0, err
	}
	if err := shards.close(); err != nil {
		return input, 0, err
	}

	duplicateRows, uniqueIDs, err := countShardDuplicates(shards.paths)
	if err != nil {
		return input, 0, err
	}
	input.UniqueSubIDs = uniqueIDs
	input.DuplicateSubIDRows = duplicateRows
	if options.MaxScheduleRows == 0 {
		input.UncompressedBytes = uncompressed.bytes
		input.CompressedSHA256 = hex.EncodeToString(compressedDigest.Sum(nil))
		input.UncompressedSHA256 = hex.EncodeToString(uncompressedDigest.Sum(nil))
	}
	report(options.Progress, fmt.Sprintf("processed_schedule_a: complete at %d rows", input.Rows))
	return input, duplicateRows, nil
}

type scheduleFieldIndexes struct {
	subID        int
	amount       int
	entityType   int
	receiptType  int
	actionCode   int
	memoCode     int
	filingForm   int
	isIndividual int
}

func scheduleIndexes() (scheduleFieldIndexes, error) {
	resolve := func(name string) (int, error) {
		index, ok := schedulea.ColumnIndex(name)
		if !ok {
			return 0, fmt.Errorf("Schedule A schema does not contain %s", name)
		}
		return index, nil
	}
	var result scheduleFieldIndexes
	var err error
	for name, destination := range map[string]*int{
		"sub_id": &result.subID, "contb_receipt_amt": &result.amount,
		"entity_tp": &result.entityType, "receipt_tp": &result.receiptType,
		"action_cd": &result.actionCode, "memo_cd": &result.memoCode,
		"filing_form": &result.filingForm, "is_individual": &result.isIndividual,
	} {
		*destination, err = resolve(name)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func addBreakdown(breakdown *ScheduleBreakdown, row *schedulea.Row, indexes scheduleFieldIndexes, measure MoneyRows) {
	addMeasureMap(breakdown.ByEntityType, displayValue(rowValue(row, indexes.entityType)), measure)
	addMeasureMap(breakdown.ByReceiptType, displayValue(rowValue(row, indexes.receiptType)), measure)
	addMeasureMap(breakdown.ByActionCode, displayValue(rowValue(row, indexes.actionCode)), measure)
	addMeasureMap(breakdown.ByMemoCode, displayValue(rowValue(row, indexes.memoCode)), measure)
	addMeasureMap(breakdown.ByFilingForm, displayValue(rowValue(row, indexes.filingForm)), measure)
	addMeasureMap(breakdown.ByIndividual, displayValue(rowValue(row, indexes.isIndividual)), measure)
}

func addProductCohorts(cohorts *ProductCohorts, row *schedulea.Row, indexes scheduleFieldIndexes, group string, measure MoneyRows) {
	individual := rowValue(row, indexes.isIndividual)
	memo := rowValue(row, indexes.memoCode)
	entity := rowValue(row, indexes.entityType)

	switch {
	case individual.null || (individual.value != "t" && individual.value != "f"):
		addMeasureMap(cohorts.ItemizedIndividualClassUnresolved, group, measure)
	case individual.value == "f":
		addMeasureMap(cohorts.ItemizedIndividualNonIndividualExcluded, group, measure)
	case !memo.null && memo.value == "X":
		addMeasureMap(cohorts.ItemizedIndividualMemoExcluded, group, measure)
	case measure.NullAmountRows != 0 || measure.InvalidAmountRows != 0:
		addMeasureMap(cohorts.ItemizedIndividualAmountUnresolved, group, measure)
	default:
		addMeasureMap(cohorts.ItemizedIndividualIncluded, group, measure)
	}

	if !entity.null {
		if _, ok := legacyOthEntityTypes[entity.value]; ok {
			addMeasureMap(cohorts.LegacyCommitteeFlowEntityShape, group, measure)
		}
	}
	if individual.value == "f" && !(memo.value == "X" && !memo.null) && measure.NullAmountRows == 0 && measure.InvalidAmountRows == 0 {
		addMeasureMap(cohorts.NonIndividualNonMemoValidAmount, group, measure)
	}
}

type rowLexeme struct {
	value string
	null  bool
}

func rowValue(row *schedulea.Row, index int) rowLexeme {
	field, ok := row.Field(index)
	if !ok || field.IsNull() {
		return rowLexeme{null: true}
	}
	return rowLexeme{value: field.String()}
}

func rowUint64(row *schedulea.Row, index int) (uint64, error) {
	value := rowValue(row, index)
	if value.null {
		return 0, errors.New("validated Schedule A row has null SUB_ID")
	}
	parsed, err := strconv.ParseUint(value.value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse Schedule A SUB_ID %q: %w", value.value, err)
	}
	return parsed, nil
}

func displayValue(value rowLexeme) string {
	if value.null {
		return "<null>"
	}
	if value.value == "" {
		return "<empty>"
	}
	return value.value
}

func measureFor(amount rowLexeme) MoneyRows {
	measure := MoneyRows{Rows: 1}
	if amount.null {
		measure.NullAmountRows = 1
		return measure
	}
	cents, ok := parseCents(amount.value)
	if !ok {
		measure.InvalidAmountRows = 1
		return measure
	}
	measure.SignedAmountCents = cents
	switch {
	case cents > 0:
		measure.PositiveRows = 1
	case cents < 0:
		measure.NegativeRows = 1
	default:
		measure.ZeroRows = 1
	}
	return measure
}

func parseCents(value string) (int64, bool) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = strings.TrimPrefix(value, "-")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || len(parts[0]) == 0 {
		return 0, false
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if len(fraction) == 0 || len(fraction) > 2 {
			return 0, false
		}
	}
	for len(fraction) < 2 {
		fraction += "0"
	}
	dollars, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || dollars > (1<<63-1)/100 {
		return 0, false
	}
	centsPart, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return 0, false
	}
	cents := dollars*100 + centsPart
	if negative {
		cents = -cents
	}
	return cents, true
}

func addMeasure(destination *MoneyRows, value MoneyRows) {
	destination.Rows += value.Rows
	destination.PositiveRows += value.PositiveRows
	destination.NegativeRows += value.NegativeRows
	destination.ZeroRows += value.ZeroRows
	destination.NullAmountRows += value.NullAmountRows
	destination.InvalidAmountRows += value.InvalidAmountRows
	destination.SignedAmountCents += value.SignedAmountCents
}

func addMeasureMap(destination map[string]MoneyRows, key string, value MoneyRows) {
	current := destination[key]
	addMeasure(&current, value)
	destination[key] = current
}

func classicMembership(bits uint8) string {
	switch bits & (memberIndiv | memberOth) {
	case memberIndiv:
		return "indiv_only"
	case memberOth:
		return "oth_only"
	case memberIndiv | memberOth:
		return "indiv_and_oth"
	default:
		return "neither"
	}
}

type lineReader struct {
	reader *bufio.Reader
	line   []byte
	err    error
}

func newLineReader(reader io.Reader) *lineReader {
	return &lineReader{reader: bufio.NewReaderSize(reader, 256*1024)}
}

func (r *lineReader) Scan() bool {
	if r.err != nil {
		return false
	}
	r.line = r.line[:0]
	for {
		fragment, err := r.reader.ReadSlice('\n')
		r.line = append(r.line, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(r.line) > 0 {
			return true
		}
		if err != nil {
			r.err = err
			return false
		}
		return true
	}
}

func (r *lineReader) Bytes() []byte { return r.line }

func (r *lineReader) Err() error {
	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

func trimLineEnding(line []byte) []byte {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}

type idShards struct {
	paths   []string
	files   []*os.File
	writers []*bufio.Writer
	closed  bool
}

func newIDShards(root string) (*idShards, error) {
	shards := &idShards{
		paths:   make([]string, duplicateShards),
		files:   make([]*os.File, duplicateShards),
		writers: make([]*bufio.Writer, duplicateShards),
	}
	for index := 0; index < duplicateShards; index++ {
		path := filepath.Join(root, fmt.Sprintf("sub-id-%03d.bin", index))
		file, err := os.Create(path)
		if err != nil {
			shards.abort()
			return nil, fmt.Errorf("create duplicate-check shard: %w", err)
		}
		shards.paths[index] = path
		shards.files[index] = file
		shards.writers[index] = bufio.NewWriterSize(file, 64*1024)
	}
	return shards, nil
}

func (s *idShards) add(id uint64) error {
	shard := byte(hashID(id))
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], id)
	if _, err := s.writers[shard].Write(encoded[:]); err != nil {
		return fmt.Errorf("write duplicate-check shard: %w", err)
	}
	return nil
}

func (s *idShards) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	for index := range s.files {
		if s.writers[index] != nil {
			if err := s.writers[index].Flush(); err != nil {
				return fmt.Errorf("flush duplicate-check shard: %w", err)
			}
		}
		if s.files[index] != nil {
			if err := s.files[index].Close(); err != nil {
				return fmt.Errorf("close duplicate-check shard: %w", err)
			}
		}
	}
	return nil
}

func (s *idShards) abort() {
	if s == nil || s.closed {
		return
	}
	for _, file := range s.files {
		if file != nil {
			_ = file.Close()
		}
	}
	s.closed = true
}

func hashID(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func countShardDuplicates(paths []string) (duplicateRows, uniqueIDs uint64, err error) {
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return 0, 0, fmt.Errorf("read duplicate-check shard: %w", readErr)
		}
		if len(data)%8 != 0 {
			return 0, 0, fmt.Errorf("duplicate-check shard %s has invalid byte count %d", path, len(data))
		}
		ids := make([]uint64, len(data)/8)
		for index := range ids {
			ids[index] = binary.LittleEndian.Uint64(data[index*8 : index*8+8])
		}
		sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
		for index, id := range ids {
			if index > 0 && id == ids[index-1] {
				duplicateRows++
			} else {
				uniqueIDs++
			}
		}
	}
	return duplicateRows, uniqueIDs, nil
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

func inputEvidence(path, kind string) (InputEvidence, error) {
	info, err := os.Stat(path)
	if err != nil {
		return InputEvidence{}, fmt.Errorf("stat %s: %w", kind, err)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return InputEvidence{}, fmt.Errorf("resolve %s path: %w", kind, err)
	}
	return InputEvidence{
		Kind:         kind,
		Path:         absolute,
		ArchiveBytes: info.Size(),
		ModifiedAt:   info.ModTime().UTC().Format(time.RFC3339Nano),
	}, nil
}

func report(progress func(string), message string) {
	if progress != nil {
		progress(message)
	}
}
