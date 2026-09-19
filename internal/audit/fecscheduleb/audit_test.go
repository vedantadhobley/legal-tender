package fecscheduleb

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
)

func TestAuditMeasuresClassicMembershipWithoutCreatingAnotherLedger(t *testing.T) {
	root := t.TempDir()
	pas2 := filepath.Join(root, "pas2.zip")
	oth := filepath.Join(root, "oth.zip")
	writeZIP(t, pas2, "itpas2.txt", []string{
		classicComparableRow(22, 21, "100", "C00000001", "C00000002", "10"),
		classicComparableRow(22, 21, "101", "C00000003", "C00000004", "20"),
	})
	writeZIP(t, oth, "itoth.txt", []string{
		classicComparableRow(21, 20, "100", "C00000001", "C00000002", "10"),
		classicComparableRow(21, 20, "101", "C00000003", "C00000004", "20"),
		classicComparableRow(21, 20, "102", "C00000005", "C00000006", "30"),
	})

	stream := scheduleBRow(t, "100", "10.25", "C00000001", "C00000002") + scheduleBRow(t, "103", "-1.00", "C00000007", "C00000008")
	result, err := Audit(context.Background(), strings.NewReader(stream), Options{
		Pas2Path: pas2, OthPath: oth, Period: "2024", WorkDir: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.ScheduleB.Rows != 2 || result.Overlap.Pas2FoundInScheduleB != 1 || result.Overlap.Pas2AbsentFromScheduleB != 1 {
		t.Fatalf("unexpected overlap result: %+v", result)
	}
	for _, input := range result.ClassicInputs {
		if len(input.ArchiveSHA256) != 64 || len(input.MemberSHA256) != 64 {
			t.Fatalf("classic input lacks exact archive/member identity: %+v", input)
		}
	}
	if result.Overlap.OthFoundInScheduleB != 1 || result.Overlap.OthAbsentFromScheduleB != 2 {
		t.Fatalf("unexpected oth overlap: %+v", result.Overlap)
	}
	if shared := result.Overlap.ScheduleBByClassicMembership["pas2_and_oth"]; shared.Rows != 1 || shared.SignedAmountCents != 1025 {
		t.Fatalf("unexpected shared measure: %+v", shared)
	}
	if neither := result.Overlap.ScheduleBByClassicMembership["neither"]; neither.Rows != 1 || neither.SignedAmountCents != -100 {
		t.Fatalf("unexpected Schedule-B-only measure: %+v", neither)
	}
	for name, agreement := range map[string]ClassicAgreement{"pas2": result.Overlap.Pas2Agreement, "oth": result.Overlap.OthAgreement} {
		if agreement.SharedSubIDs != 1 || agreement.SameDirectionRawEndpoints != 1 || agreement.SameDirectionCleanEndpoints != 1 || agreement.ReverseDirectionRawEndpoints != 0 || agreement.WholeDollarTruncationMatches != 1 {
			t.Fatalf("unexpected %s direction/amount agreement: %+v", name, agreement)
		}
	}
}

func scheduleBRow(t *testing.T, subID, amount, sourceID, recipientID string) string {
	t.Helper()
	values := make([]string, scheduleb.FieldCount)
	for index := range values {
		values[index] = `\N`
	}
	set := func(name, value string) {
		index, ok := scheduleb.ColumnIndex(name)
		if !ok {
			t.Fatalf("missing Schedule B column %s", name)
		}
		values[index] = value
	}
	set("sub_id", subID)
	set("filing_form", "F3X")
	set("two_year_transaction_period", "2024")
	set("disb_amt", amount)
	set("cmte_id", sourceID)
	set("recipient_cmte_id", recipientID)
	set("clean_recipient_cmte_id", recipientID)
	return strings.Join(values, "\t") + "\n"
}

func classicRow(width, subIDIndex int, subID string) string {
	fields := make([]string, width)
	fields[subIDIndex] = subID
	return strings.Join(fields, "|")
}

func classicComparableRow(width, subIDIndex int, subID, filerID, counterpartyID, amount string) string {
	fields := make([]string, width)
	fields[0] = filerID
	fields[14] = amount
	fields[15] = counterpartyID
	fields[subIDIndex] = subID
	return strings.Join(fields, "|")
}

func writeZIP(t *testing.T, path, member string, rows []string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(file)
	writer, err := archive.Create(member)
	if err != nil {
		t.Fatal(err)
	}
	var content bytes.Buffer
	for _, row := range rows {
		content.WriteString(row)
		content.WriteByte('\n')
	}
	if _, err := writer.Write(content.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
