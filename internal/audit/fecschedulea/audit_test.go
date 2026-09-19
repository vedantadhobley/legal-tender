package fecschedulea

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func TestAuditClassifiesOverlapAndProductCohorts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	indivPath := filepath.Join(root, "indiv.zip")
	othPath := filepath.Join(root, "oth.zip")
	schedulePath := filepath.Join(root, "schedule-a.copy.zst")

	writeClassicZIP(t, indivPath, map[string][]string{
		"itcont.txt": {
			classicRow("IND", "100"),
			classicRow("IND", "999"),
		},
		"by_date/itcont_part.txt": {classicRow("IND", "777")},
	})
	writeClassicZIP(t, othPath, map[string][]string{
		"itoth.txt": {
			classicRow("IND", "100"),
			classicRow("ORG", "200"),
		},
	})
	writeScheduleZstd(t, schedulePath, []string{
		scheduleRow(t, "100", "10.00", "IND", "15", "N", "", "t"),
		scheduleRow(t, "200", "20.00", "ORG", "10J", "A", "", "f"),
		scheduleRow(t, "300", "30.00", "IND", "15", "N", "X", "t"),
		scheduleRow(t, "400", "40.00", "PAC", "10J", "A", "", "f"),
	})

	result, err := Audit(context.Background(), Options{
		ScheduleAPath: schedulePath,
		IndivPath:     indivPath,
		OthPath:       othPath,
		Period:        "2024",
		WorkDir:       root,
	})
	if err != nil {
		t.Fatalf("Audit() error = %v", err)
	}
	if !result.Complete || result.ScheduleUniqueSubIDs != 4 || result.ScheduleDuplicateSubIDRows != 0 {
		t.Fatalf("unexpected completion result: %+v", result)
	}
	assertMoneyRows(t, result.Overlap.ScheduleRowsByClassicMembership["indiv_and_oth"], 1, 1_000)
	assertMoneyRows(t, result.Overlap.ScheduleRowsByClassicMembership["oth_only"], 1, 2_000)
	assertMoneyRows(t, result.Overlap.ScheduleRowsByClassicMembership["neither"], 2, 7_000)
	assertMoneyRows(t, result.Overlap.ScheduleRowsMatchingLegacyOth, 1, 2_000)

	if got := result.Overlap.ClassicUniqueSubIDsByMembership["indiv_only"]; got != 1 {
		t.Fatalf("classic indiv_only = %d; want 1", got)
	}
	if got := result.Overlap.ClassicOnlySubIDsByMembership["indiv_only"]; got != 1 {
		t.Fatalf("classic-only indiv_only = %d; want 1", got)
	}
	if result.Overlap.ClassicOthRetainedUniqueSubIDs != 1 || result.Overlap.ClassicOthRetainedOnlySubIDs != 0 {
		t.Fatalf("unexpected legacy oth retained counts: %+v", result.Overlap)
	}
	assertMoneyRows(t, result.ProductCohorts.ItemizedIndividualIncluded["indiv_and_oth"], 1, 1_000)
	assertMoneyRows(t, result.ProductCohorts.ItemizedIndividualMemoExcluded["neither"], 1, 3_000)
	assertMoneyRows(t, result.ProductCohorts.ItemizedIndividualNonIndividualExcluded["oth_only"], 1, 2_000)
	assertMoneyRows(t, result.ProductCohorts.LegacyCommitteeFlowEntityShape["oth_only"], 1, 2_000)
	assertMoneyRows(t, result.ProductCohorts.LegacyCommitteeFlowEntityShape["neither"], 1, 4_000)
	assertMoneyRows(t, result.ProductCohorts.NonIndividualNonMemoValidAmount["neither"], 1, 4_000)
	assertMoneyRows(t, result.ScheduleAbsentFromClassic.Total, 2, 7_000)
	assertMoneyRows(t, result.ScheduleAbsentFromClassic.ByEntityType["PAC"], 1, 4_000)

	for _, input := range result.Inputs {
		if input.Kind == "classic_indiv" && input.Rows != 2 {
			t.Fatalf("classic indiv rows = %d; exact itcont member was not selected", input.Rows)
		}
		if input.Kind == "processed_schedule_a" && (input.CompressedSHA256 == "" || input.UncompressedSHA256 == "") {
			t.Fatal("complete Schedule A input is missing physical digests")
		}
	}
}

func TestAuditRejectsDuplicateScheduleSubID(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	indivPath := filepath.Join(root, "indiv.zip")
	othPath := filepath.Join(root, "oth.zip")
	schedulePath := filepath.Join(root, "schedule-a.copy.zst")
	writeClassicZIP(t, indivPath, map[string][]string{"itcont.txt": {classicRow("IND", "100")}})
	writeClassicZIP(t, othPath, map[string][]string{"itoth.txt": {classicRow("ORG", "200")}})
	row := scheduleRow(t, "500", "1.00", "IND", "15", "N", "", "t")
	writeScheduleZstd(t, schedulePath, []string{row, row})

	result, err := Audit(context.Background(), Options{
		ScheduleAPath: schedulePath,
		IndivPath:     indivPath,
		OthPath:       othPath,
		Period:        "2024",
		WorkDir:       root,
	})
	if err == nil {
		t.Fatal("Audit() error = nil; want duplicate-SUB_ID failure")
	}
	if result.ScheduleDuplicateSubIDRows != 1 || result.ScheduleUniqueSubIDs != 1 {
		t.Fatalf("unexpected duplicate accounting: %+v", result)
	}
}

func TestParseCents(t *testing.T) {
	t.Parallel()
	for input, expected := range map[string]int64{
		"0":     0,
		"1":     100,
		"1.2":   120,
		"1.23":  123,
		"-0.50": -50,
	} {
		actual, ok := parseCents(input)
		if !ok || actual != expected {
			t.Errorf("parseCents(%q) = %d, %t; want %d, true", input, actual, ok, expected)
		}
	}
	for _, input := range []string{"", ".1", "1.", "1.234", "abc"} {
		if actual, ok := parseCents(input); ok {
			t.Errorf("parseCents(%q) = %d, true; want invalid", input, actual)
		}
	}
}

func assertMoneyRows(t *testing.T, actual MoneyRows, rows uint64, cents int64) {
	t.Helper()
	if actual.Rows != rows || actual.SignedAmountCents != cents {
		t.Fatalf("money rows = %+v; want rows=%d cents=%d", actual, rows, cents)
	}
}

func classicRow(entityType, subID string) string {
	fields := make([]string, classicFieldCount)
	fields[0] = "C00000001"
	fields[1] = "N"
	fields[2] = "Q1"
	fields[5] = "15"
	fields[6] = entityType
	fields[14] = "1.00"
	fields[20] = subID
	return strings.Join(fields, "|")
}

func writeClassicZIP(t *testing.T, path string, members map[string][]string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create ZIP: %v", err)
	}
	archive := zip.NewWriter(file)
	for name, rows := range members {
		member, err := archive.Create(name)
		if err != nil {
			t.Fatalf("create ZIP member: %v", err)
		}
		if _, err := member.Write([]byte(strings.Join(rows, "\n") + "\n")); err != nil {
			t.Fatalf("write ZIP member: %v", err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close ZIP: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close ZIP file: %v", err)
	}
}

func scheduleRow(t *testing.T, subID, amount, entityType, receiptType, actionCode, memoCode, individual string) string {
	t.Helper()
	columns := schedulea.Columns()
	values := make([]string, len(columns))
	for index, column := range columns {
		values[index] = `\N`
		switch column.Name {
		case "sub_id":
			values[index] = subID
		case "filing_form":
			values[index] = "F3X"
		case "two_year_transaction_period":
			values[index] = "2024"
		case "contb_receipt_amt":
			values[index] = amount
		case "entity_tp":
			values[index] = entityType
		case "receipt_tp":
			values[index] = receiptType
		case "action_cd":
			values[index] = actionCode
		case "memo_cd":
			if memoCode != "" {
				values[index] = memoCode
			}
		case "is_individual":
			values[index] = individual
		}
	}
	return strings.Join(values, "\t")
}

func writeScheduleZstd(t *testing.T, path string, rows []string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create Schedule A fixture: %v", err)
	}
	encoder, err := zstd.NewWriter(file, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatalf("create zstd encoder: %v", err)
	}
	if _, err := bytes.NewBufferString(strings.Join(rows, "\n") + "\n").WriteTo(encoder); err != nil {
		t.Fatalf("write Schedule A fixture: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close zstd encoder: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close Schedule A fixture: %v", err)
	}
}
