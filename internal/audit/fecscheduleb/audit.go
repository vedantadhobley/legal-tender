// Package fecscheduleb compares processed Schedule B with classic oth and
// pas2 evidence without declaring any source authoritative.
package fecscheduleb

import (
	"archive/zip"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
)

const (
	schemaVersion = "legal-tender.schedule-b-classic-overlap.v1"
	memberPas2    = uint8(1 << 0)
	memberOth     = uint8(1 << 1)
	memberB       = uint8(1 << 2)
	maxIssues     = 20
)

type Options struct {
	Pas2Path string
	OthPath  string
	Period   string
	WorkDir  string
	MaxRows  uint64
	Progress func(string)
}

type InputEvidence struct {
	Kind               string `json:"kind"`
	Path               string `json:"path"`
	ArchiveBytes       int64  `json:"archive_bytes"`
	ArchiveSHA256      string `json:"archive_sha256"`
	ModifiedAt         string `json:"modified_at"`
	Member             string `json:"member"`
	MemberBytes        uint64 `json:"member_bytes"`
	MemberCRC32        string `json:"member_crc32"`
	MemberSHA256       string `json:"member_sha256"`
	Rows               uint64 `json:"rows"`
	ValidRows          uint64 `json:"valid_rows"`
	InvalidRows        uint64 `json:"invalid_rows"`
	UniqueSubIDs       uint64 `json:"unique_sub_ids"`
	DuplicateSubIDRows uint64 `json:"duplicate_sub_id_rows"`
}

type MoneyRows struct {
	Rows              uint64 `json:"rows"`
	PositiveRows      uint64 `json:"positive_rows"`
	NegativeRows      uint64 `json:"negative_rows"`
	ZeroRows          uint64 `json:"zero_rows"`
	NullAmountRows    uint64 `json:"null_amount_rows"`
	SignedAmountCents int64  `json:"signed_amount_cents"`
}

type Overlap struct {
	ScheduleBByClassicMembership map[string]MoneyRows `json:"schedule_b_by_classic_membership"`
	ClassicUniqueByMembership    map[string]uint64    `json:"classic_unique_sub_ids_by_membership"`
	ClassicAbsentFromScheduleB   map[string]uint64    `json:"classic_sub_ids_absent_from_schedule_b"`
	Pas2FoundInScheduleB         uint64               `json:"pas2_sub_ids_found_in_schedule_b"`
	Pas2AbsentFromScheduleB      uint64               `json:"pas2_sub_ids_absent_from_schedule_b"`
	OthFoundInScheduleB          uint64               `json:"oth_sub_ids_found_in_schedule_b"`
	OthAbsentFromScheduleB       uint64               `json:"oth_sub_ids_absent_from_schedule_b"`
	Pas2Agreement                ClassicAgreement     `json:"pas2_agreement"`
	OthAgreement                 ClassicAgreement     `json:"oth_agreement"`
}

// ClassicAgreement compares exact publisher records by SUB_ID. It reports
// both endpoint orientations and both raw and cleaned Schedule B recipient
// assertions instead of selecting a direction during source ingestion.
type ClassicAgreement struct {
	SharedSubIDs                      uint64 `json:"shared_sub_ids"`
	BSourceCommitteeIDs               uint64 `json:"b_source_committee_ids"`
	BRawRecipientCommitteeIDs         uint64 `json:"b_raw_recipient_committee_ids"`
	BCleanRecipientCommitteeIDs       uint64 `json:"b_clean_recipient_committee_ids"`
	ClassicFilerCommitteeIDs          uint64 `json:"classic_filer_committee_ids"`
	ClassicCounterpartyCommitteeIDs   uint64 `json:"classic_counterparty_committee_ids"`
	SourceMatchesClassicFiler         uint64 `json:"source_matches_classic_filer"`
	SourceMatchesClassicCounterparty  uint64 `json:"source_matches_classic_counterparty"`
	RawRecipientMatchesCounterparty   uint64 `json:"raw_recipient_matches_classic_counterparty"`
	CleanRecipientMatchesCounterparty uint64 `json:"clean_recipient_matches_classic_counterparty"`
	RawRecipientMatchesFiler          uint64 `json:"raw_recipient_matches_classic_filer"`
	CleanRecipientMatchesFiler        uint64 `json:"clean_recipient_matches_classic_filer"`
	SameDirectionRawEndpoints         uint64 `json:"same_direction_raw_endpoints"`
	SameDirectionCleanEndpoints       uint64 `json:"same_direction_clean_endpoints"`
	ReverseDirectionRawEndpoints      uint64 `json:"reverse_direction_raw_endpoints"`
	ReverseDirectionCleanEndpoints    uint64 `json:"reverse_direction_clean_endpoints"`
	BNullAmounts                      uint64 `json:"b_null_amounts"`
	ClassicNullAmounts                uint64 `json:"classic_null_amounts"`
	ComparableAmounts                 uint64 `json:"comparable_amounts"`
	ExactAmountMatches                uint64 `json:"exact_amount_matches"`
	WholeDollarTruncationMatches      uint64 `json:"whole_dollar_truncation_matches"`
	AmountConflicts                   uint64 `json:"amount_conflicts"`
	ClassicInvalidAmounts             uint64 `json:"classic_invalid_amounts"`
}

type scheduleBComparable struct {
	sourceID            uint32
	sourceValid         bool
	rawRecipientID      uint32
	rawRecipientValid   bool
	cleanRecipientID    uint32
	cleanRecipientValid bool
	amountCents         int64
	amountValid         bool
}

type Result struct {
	SchemaVersion       string                 `json:"schema_version"`
	Complete            bool                   `json:"complete"`
	Period              string                 `json:"period"`
	ClassicInputs       []InputEvidence        `json:"classic_inputs"`
	ScheduleB           scheduleb.Verification `json:"schedule_b"`
	Overlap             Overlap                `json:"overlap"`
	Issues              []string               `json:"issues,omitempty"`
	Caveats             []string               `json:"caveats"`
	ElapsedMilliseconds int64                  `json:"elapsed_milliseconds"`
}

type AuditError struct {
	InvalidClassicRows   uint64
	DuplicateClassicRows uint64
}

func (e *AuditError) Error() string {
	return fmt.Sprintf("Schedule B classic overlap failed: %d invalid classic rows, %d duplicate classic rows", e.InvalidClassicRows, e.DuplicateClassicRows)
}

func Audit(ctx context.Context, source io.Reader, options Options) (Result, error) {
	started := time.Now()
	result := Result{
		SchemaVersion: schemaVersion, Period: options.Period,
		Overlap: Overlap{
			ScheduleBByClassicMembership: make(map[string]MoneyRows),
			ClassicUniqueByMembership:    make(map[string]uint64),
			ClassicAbsentFromScheduleB:   make(map[string]uint64),
		},
		Caveats: []string{
			"SUB_ID establishes publisher-record overlap, not identity of an underlying economic payment.",
			"Schedule A and Schedule B are independent receiver- and sender-side assertions and must never be added solely because both exist.",
			"Classic oth and pas2 observation times can differ from the processed Schedule B snapshot; one-sided rows can therefore include release lag.",
		},
	}
	if options.Pas2Path == "" || options.OthPath == "" {
		return result, errors.New("pas2 and oth paths are required")
	}
	membership := make(map[uint64]uint8)
	pas2, err := loadClassic(ctx, options.Pas2Path, "classic_pas2", "itpas2.txt", 22, 21, memberPas2, membership, options.Progress)
	if err != nil {
		return result, err
	}
	oth, err := loadClassic(ctx, options.OthPath, "classic_oth", "itoth.txt", 21, 20, memberOth, membership, options.Progress)
	if err != nil {
		return result, err
	}
	result.ClassicInputs = []InputEvidence{pas2, oth}
	for _, bits := range membership {
		result.Overlap.ClassicUniqueByMembership[classicMembership(bits)]++
	}
	bComparables := make(map[uint64]scheduleBComparable)

	verification, verifyErr := scheduleb.Verify(ctx, source, scheduleb.VerifyOptions{
		ExpectedPeriod: options.Period, WorkDir: options.WorkDir, MaxRows: options.MaxRows,
		Progress: options.Progress,
		ObserveValidRow: func(row *scheduleb.Row) error {
			subIDField, _ := row.Field(mustColumn("sub_id"))
			subID, err := strconv.ParseUint(subIDField.String(), 10, 64)
			if err != nil || subID == 0 {
				return fmt.Errorf("invalid sub_id %q", subIDField.String())
			}
			bits := membership[subID]
			group := classicMembership(bits)
			amountField, _ := row.Field(mustColumn("disb_amt"))
			measure, err := measure(amountField)
			if err != nil {
				return err
			}
			current := result.Overlap.ScheduleBByClassicMembership[group]
			if err := addMoney(&current, measure); err != nil {
				return err
			}
			result.Overlap.ScheduleBByClassicMembership[group] = current
			if bits != 0 {
				membership[subID] = bits | memberB
				comparable, err := comparableScheduleBRow(row)
				if err != nil {
					return err
				}
				bComparables[subID] = comparable
			}
			return nil
		},
	})
	result.ScheduleB = verification
	if err := compareClassic(ctx, options.Pas2Path, "itpas2.txt", 22, 21, bComparables, &result.Overlap.Pas2Agreement, options.Progress); err != nil {
		return result, err
	}
	if err := compareClassic(ctx, options.OthPath, "itoth.txt", 21, 20, bComparables, &result.Overlap.OthAgreement, options.Progress); err != nil {
		return result, err
	}
	for _, bits := range membership {
		group := classicMembership(bits)
		if bits&memberPas2 != 0 {
			if bits&memberB != 0 {
				result.Overlap.Pas2FoundInScheduleB++
			} else {
				result.Overlap.Pas2AbsentFromScheduleB++
			}
		}
		if bits&memberOth != 0 {
			if bits&memberB != 0 {
				result.Overlap.OthFoundInScheduleB++
			} else {
				result.Overlap.OthAbsentFromScheduleB++
			}
		}
		if bits&memberB == 0 {
			result.Overlap.ClassicAbsentFromScheduleB[group]++
		}
	}
	result.Complete = verification.Complete
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	if verifyErr != nil {
		return result, verifyErr
	}
	invalid := pas2.InvalidRows + oth.InvalidRows
	duplicates := pas2.DuplicateSubIDRows + oth.DuplicateSubIDRows
	if invalid > 0 || duplicates > 0 {
		return result, &AuditError{InvalidClassicRows: invalid, DuplicateClassicRows: duplicates}
	}
	return result, nil
}

func loadClassic(ctx context.Context, path, kind, memberName string, fieldCount, subIDIndex int, bit uint8, membership map[uint64]uint8, progress func(string)) (InputEvidence, error) {
	info, err := os.Stat(path)
	if err != nil {
		return InputEvidence{}, err
	}
	input := InputEvidence{Kind: kind, Path: path, ArchiveBytes: info.Size(), ModifiedAt: info.ModTime().UTC().Format(time.RFC3339Nano)}
	input.ArchiveSHA256, err = sha256File(path)
	if err != nil {
		return input, err
	}
	archive, err := zip.OpenReader(path)
	if err != nil {
		return input, err
	}
	defer archive.Close()
	var member *zip.File
	for _, candidate := range archive.File {
		if candidate.Name == memberName {
			if member != nil {
				return input, fmt.Errorf("classic ZIP contains %q more than once", memberName)
			}
			member = candidate
		}
	}
	if member == nil {
		return input, fmt.Errorf("classic ZIP lacks exact member %q", memberName)
	}
	input.Member = member.Name
	input.MemberBytes = member.UncompressedSize64
	input.MemberCRC32 = fmt.Sprintf("%08x", member.CRC32)
	opened, err := member.Open()
	if err != nil {
		return input, err
	}
	defer opened.Close()
	digest := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(opened, digest))
	scanner.Buffer(make([]byte, 256<<10), 2<<20)
	seen := make(map[uint64]struct{})
	for scanner.Scan() {
		input.Rows++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		fields := strings.Split(line, "|")
		if len(fields) != fieldCount || fields[subIDIndex] == "" {
			input.InvalidRows++
			continue
		}
		id, err := strconv.ParseUint(fields[subIDIndex], 10, 64)
		if err != nil || id == 0 {
			input.InvalidRows++
			continue
		}
		input.ValidRows++
		if _, duplicate := seen[id]; duplicate {
			input.DuplicateSubIDRows++
		} else {
			seen[id] = struct{}{}
			input.UniqueSubIDs++
		}
		membership[id] |= bit
		if input.Rows%5_000_000 == 0 {
			if progress != nil {
				progress(fmt.Sprintf("%s: parsed %d rows", kind, input.Rows))
			}
			if err := ctx.Err(); err != nil {
				return input, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return input, err
	}
	input.MemberSHA256 = hex.EncodeToString(digest.Sum(nil))
	if progress != nil {
		progress(fmt.Sprintf("%s: complete at %d rows", kind, input.Rows))
	}
	return input, nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func comparableScheduleBRow(row *scheduleb.Row) (scheduleBComparable, error) {
	result := scheduleBComparable{}
	for _, source := range []struct {
		name  string
		value *uint32
		valid *bool
	}{
		{"cmte_id", &result.sourceID, &result.sourceValid},
		{"recipient_cmte_id", &result.rawRecipientID, &result.rawRecipientValid},
		{"clean_recipient_cmte_id", &result.cleanRecipientID, &result.cleanRecipientValid},
	} {
		field, _ := row.Field(mustColumn(source.name))
		if field.IsNull() {
			continue
		}
		*source.value, *source.valid = committeeID(field.String())
	}
	amount, _ := row.Field(mustColumn("disb_amt"))
	if !amount.IsNull() {
		cents, err := parseCents(amount.String())
		if err != nil {
			return result, err
		}
		result.amountCents = cents
		result.amountValid = true
	}
	return result, nil
}

func compareClassic(ctx context.Context, path, memberName string, fieldCount, subIDIndex int, bRows map[uint64]scheduleBComparable, result *ClassicAgreement, progress func(string)) error {
	archive, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer archive.Close()
	var member *zip.File
	for _, candidate := range archive.File {
		if candidate.Name == memberName {
			if member != nil {
				return fmt.Errorf("classic ZIP contains %q more than once", memberName)
			}
			member = candidate
		}
	}
	if member == nil {
		return fmt.Errorf("classic ZIP lacks exact member %q", memberName)
	}
	opened, err := member.Open()
	if err != nil {
		return err
	}
	defer opened.Close()
	scanner := bufio.NewScanner(opened)
	scanner.Buffer(make([]byte, 256<<10), 2<<20)
	var rows uint64
	for scanner.Scan() {
		rows++
		fields := strings.Split(strings.TrimSuffix(scanner.Text(), "\r"), "|")
		if len(fields) != fieldCount {
			return fmt.Errorf("%s comparison row %d has %d fields; want %d", memberName, rows, len(fields), fieldCount)
		}
		subID, err := strconv.ParseUint(fields[subIDIndex], 10, 64)
		if err != nil || subID == 0 {
			return fmt.Errorf("%s comparison row %d has invalid SUB_ID", memberName, rows)
		}
		bRow, shared := bRows[subID]
		if !shared {
			continue
		}
		observeAgreement(result, bRow, fields[0], fields[15], fields[14])
		if result.SharedSubIDs%1_000_000 == 0 && result.SharedSubIDs != 0 && progress != nil {
			progress(fmt.Sprintf("%s: compared %d shared rows", memberName, result.SharedSubIDs))
		}
		if rows&0xfffff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if progress != nil {
		progress(fmt.Sprintf("%s: complete at %d shared rows", memberName, result.SharedSubIDs))
	}
	return nil
}

func observeAgreement(result *ClassicAgreement, bRow scheduleBComparable, classicFiler, classicCounterparty, classicAmount string) {
	result.SharedSubIDs++
	filerID, filerValid := committeeID(classicFiler)
	counterpartyID, counterpartyValid := committeeID(classicCounterparty)
	if bRow.sourceValid {
		result.BSourceCommitteeIDs++
	}
	if bRow.rawRecipientValid {
		result.BRawRecipientCommitteeIDs++
	}
	if bRow.cleanRecipientValid {
		result.BCleanRecipientCommitteeIDs++
	}
	if filerValid {
		result.ClassicFilerCommitteeIDs++
	}
	if counterpartyValid {
		result.ClassicCounterpartyCommitteeIDs++
	}
	if bRow.sourceValid && filerValid && bRow.sourceID == filerID {
		result.SourceMatchesClassicFiler++
	}
	if bRow.sourceValid && counterpartyValid && bRow.sourceID == counterpartyID {
		result.SourceMatchesClassicCounterparty++
	}
	if bRow.rawRecipientValid && counterpartyValid && bRow.rawRecipientID == counterpartyID {
		result.RawRecipientMatchesCounterparty++
	}
	if bRow.cleanRecipientValid && counterpartyValid && bRow.cleanRecipientID == counterpartyID {
		result.CleanRecipientMatchesCounterparty++
	}
	if bRow.rawRecipientValid && filerValid && bRow.rawRecipientID == filerID {
		result.RawRecipientMatchesFiler++
	}
	if bRow.cleanRecipientValid && filerValid && bRow.cleanRecipientID == filerID {
		result.CleanRecipientMatchesFiler++
	}
	if bRow.sourceValid && filerValid && bRow.sourceID == filerID {
		if bRow.rawRecipientValid && counterpartyValid && bRow.rawRecipientID == counterpartyID {
			result.SameDirectionRawEndpoints++
		}
		if bRow.cleanRecipientValid && counterpartyValid && bRow.cleanRecipientID == counterpartyID {
			result.SameDirectionCleanEndpoints++
		}
	}
	if bRow.sourceValid && counterpartyValid && bRow.sourceID == counterpartyID {
		if bRow.rawRecipientValid && filerValid && bRow.rawRecipientID == filerID {
			result.ReverseDirectionRawEndpoints++
		}
		if bRow.cleanRecipientValid && filerValid && bRow.cleanRecipientID == filerID {
			result.ReverseDirectionCleanEndpoints++
		}
	}
	if !bRow.amountValid {
		result.BNullAmounts++
	}
	if classicAmount == "" {
		result.ClassicNullAmounts++
		return
	}
	classicCents, ok := parseClassicCents(classicAmount)
	if !ok {
		result.ClassicInvalidAmounts++
		return
	}
	if !bRow.amountValid {
		return
	}
	result.ComparableAmounts++
	switch {
	case classicCents == bRow.amountCents:
		result.ExactAmountMatches++
	case classicCents == bRow.amountCents/100*100:
		result.WholeDollarTruncationMatches++
	default:
		result.AmountConflicts++
	}
}

func committeeID(value string) (uint32, bool) {
	if len(value) != 9 || value[0] != 'C' {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value[1:], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(parsed), true
}

func parseClassicCents(value string) (int64, bool) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = strings.TrimPrefix(value, "-")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" || len(parts) == 2 && (len(parts[1]) == 0 || len(parts[1]) > 2) {
		return 0, false
	}
	dollars, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || dollars > (1<<63-1-99)/100 {
		return 0, false
	}
	var fraction int64
	if len(parts) == 2 {
		fraction, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, false
		}
		if len(parts[1]) == 1 {
			fraction *= 10
		}
	}
	cents := dollars*100 + fraction
	if negative {
		cents = -cents
	}
	return cents, true
}

func classicMembership(bits uint8) string {
	switch bits & (memberPas2 | memberOth) {
	case memberPas2 | memberOth:
		return "pas2_and_oth"
	case memberPas2:
		return "pas2_only"
	case memberOth:
		return "oth_only"
	default:
		return "neither"
	}
}

func mustColumn(name string) int {
	index, ok := scheduleb.ColumnIndex(name)
	if !ok {
		panic("compiled Schedule B schema lacks " + name)
	}
	return index
}

func measure(field scheduleb.Field) (MoneyRows, error) {
	result := MoneyRows{Rows: 1}
	if field.IsNull() {
		result.NullAmountRows = 1
		return result, nil
	}
	cents, err := parseCents(field.String())
	if err != nil {
		return result, err
	}
	result.SignedAmountCents = cents
	switch {
	case cents > 0:
		result.PositiveRows = 1
	case cents < 0:
		result.NegativeRows = 1
	default:
		result.ZeroRows = 1
	}
	return result, nil
}

func parseCents(value string) (int64, error) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = strings.TrimPrefix(value, "-")
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 || len(parts[1]) != 2 {
		return 0, fmt.Errorf("invalid exact-cent amount %q", value)
	}
	dollars, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || dollars > (1<<63-1-99)/100 {
		return 0, fmt.Errorf("invalid exact-cent amount %q", value)
	}
	fraction, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid exact-cent amount %q", value)
	}
	cents := dollars*100 + fraction
	if negative {
		cents = -cents
	}
	return cents, nil
}

func addMoney(destination *MoneyRows, value MoneyRows) error {
	if value.SignedAmountCents > 0 && destination.SignedAmountCents > 1<<63-1-value.SignedAmountCents ||
		value.SignedAmountCents < 0 && destination.SignedAmountCents < -1<<63-value.SignedAmountCents {
		return errors.New("Schedule B amount accumulator overflow")
	}
	destination.Rows += value.Rows
	destination.PositiveRows += value.PositiveRows
	destination.NegativeRows += value.NegativeRows
	destination.ZeroRows += value.ZeroRows
	destination.NullAmountRows += value.NullAmountRows
	destination.SignedAmountCents += value.SignedAmountCents
	return nil
}
