package fecclassicflow

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuditProfilesClassicFlowEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pas2Path := filepath.Join(root, "pas2.zip")
	othPath := filepath.Join(root, "oth.zip")

	pas2Rows := []string{
		pas2Row("C00000001", "N", "Q1", "24K", "PAC", "01012024", "10.00", "C00000002", "H0AA00001", "TRANSFER1", "100"),
		pas2Row("C00000001", "N", "Q1", "24E", "ORG", "02012024", "20.00", "", "H0AA00001", "IE1", "200"),
		pas2Row("C00000001", "A", "Q1", "24E", "ORG", "02012024", "25.00", "", "H0AA00001", "IE1", "201"),
	}
	othRows := []string{
		othRow("C00000001", "N", "Q1", "24K", "PAC", "01012024", "10.00", "C00000002", "TRANSFER1", "100"),
		othRow("C00000001", "N", "Q1", "24E", "ORG", "02012024", "20.00", "", "IE1", "200"),
		othRow("C00000001", "A", "Q1", "24E", "ORG", "02012024", "25.00", "", "IE1", "201"),
		othRow("C00000002", "N", "Q1", "18K", "PAC", "01012024", "10.00", "C00000001", "RECEIPT1", "300"),
	}
	writeZIP(t, pas2Path, "itpas2.txt", pas2Rows)
	writeZIP(t, othPath, "itoth.txt", othRows)

	result, err := Audit(context.Background(), Options{Pas2Path: pas2Path, OthPath: othPath, Period: "2024"})
	if err != nil {
		t.Fatalf("Audit() error = %v", err)
	}
	if !result.Complete || result.Pas2.Total.Rows != 3 || result.Oth.Total.Rows != 4 {
		t.Fatalf("unexpected result totals: %+v", result)
	}
	if result.Cohorts.Pas2LegacyTransferProjection.Rows != 1 || result.Cohorts.Pas2LegacyTransferProjection.SignedAmountCents != 1_000 {
		t.Fatalf("unexpected pas2 transfer cohort: %+v", result.Cohorts.Pas2LegacyTransferProjection)
	}
	if result.Cohorts.OthLegacyTransferProjection.Rows != 1 || result.Cohorts.OthLegacyTransferProjection.SignedAmountCents != 1_000 {
		t.Fatalf("unexpected oth transfer cohort: %+v", result.Cohorts.OthLegacyTransferProjection)
	}
	if result.Cohorts.Pas2IndependentExpenditures.Rows != 2 || result.Cohorts.Pas2IndependentExpenditures.SignedAmountCents != 4_500 {
		t.Fatalf("unexpected IE cohort: %+v", result.Cohorts.Pas2IndependentExpenditures)
	}
	if result.SubIDOverlap.Pas2UniqueSubIDs != 3 || result.SubIDOverlap.Pas2SubIDsFoundInOth != 3 || result.SubIDOverlap.Pas2SubIDsAbsentFromOth != 0 {
		t.Fatalf("unexpected SUB_ID overlap: %+v", result.SubIDOverlap)
	}
	if result.TransferSignatureOverlap.SharedSignatures != 1 || result.TransferSignatureOverlap.PossibleMatchedRowPairs != 1 || result.TransferSignatureOverlap.PossibleMatchedAmountCents != 1_000 {
		t.Fatalf("unexpected signature overlap: %+v", result.TransferSignatureOverlap)
	}
	if result.IELogicalKeys.UniqueKeys != 1 || result.IELogicalKeys.RepeatedKeys != 1 || result.IELogicalKeys.RowsInRepeatedKeys != 2 {
		t.Fatalf("unexpected IE logical keys: %+v", result.IELogicalKeys)
	}
}

func TestAuditRejectsMalformedRowsAndDuplicatePas2SubIDs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pas2Path := filepath.Join(root, "pas2.zip")
	othPath := filepath.Join(root, "oth.zip")
	row := pas2Row("C00000001", "N", "Q1", "24K", "PAC", "01012024", "10.00", "C00000002", "H0AA00001", "T1", "100")
	writeZIP(t, pas2Path, "itpas2.txt", []string{row, row})
	writeZIP(t, othPath, "itoth.txt", []string{"too|short"})

	result, err := Audit(context.Background(), Options{Pas2Path: pas2Path, OthPath: othPath, Period: "2024"})
	if err == nil {
		t.Fatal("Audit() error = nil; want validation error")
	}
	if result.SubIDOverlap.Pas2DuplicateSubIDRows != 1 || result.Inputs[1].InvalidRows != 1 {
		t.Fatalf("unexpected validation evidence: %+v", result)
	}
}

func TestParseCents(t *testing.T) {
	t.Parallel()
	for input, expected := range map[string]int64{
		"0": 0, "1": 100, "1.2": 120, "1.23": 123, "-0.50": -50,
	} {
		actual, ok := parseCents([]byte(input))
		if !ok || actual != expected {
			t.Errorf("parseCents(%q) = %d, %t; want %d, true", input, actual, ok, expected)
		}
	}
	for _, input := range []string{"", ".1", "1.", "1.234", "abc", "92233720368547758.99"} {
		if actual, ok := parseCents([]byte(input)); ok {
			t.Errorf("parseCents(%q) = %d, true; want invalid", input, actual)
		}
	}
}

func pas2Row(committeeID, amendment, reportType, transactionType, entityType, date, amount, otherID, candidateID, transactionID, subID string) string {
	fields := make([]string, pas2FieldCount)
	fields[0] = committeeID
	fields[1] = amendment
	fields[2] = reportType
	fields[5] = transactionType
	fields[6] = entityType
	fields[13] = date
	fields[14] = amount
	fields[15] = otherID
	fields[16] = candidateID
	fields[17] = transactionID
	fields[18] = "123"
	fields[21] = subID
	return strings.Join(fields, "|")
}

func othRow(committeeID, amendment, reportType, transactionType, entityType, date, amount, otherID, transactionID, subID string) string {
	fields := make([]string, othFieldCount)
	fields[0] = committeeID
	fields[1] = amendment
	fields[2] = reportType
	fields[5] = transactionType
	fields[6] = entityType
	fields[13] = date
	fields[14] = amount
	fields[15] = otherID
	fields[16] = transactionID
	fields[17] = "123"
	fields[20] = subID
	return strings.Join(fields, "|")
}

func writeZIP(t *testing.T, path, memberName string, rows []string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create ZIP: %v", err)
	}
	archive := zip.NewWriter(file)
	member, err := archive.Create(memberName)
	if err != nil {
		t.Fatalf("create ZIP member: %v", err)
	}
	if _, err := member.Write([]byte(strings.Join(rows, "\n") + "\n")); err != nil {
		t.Fatalf("write ZIP member: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close ZIP: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close ZIP file: %v", err)
	}
}
