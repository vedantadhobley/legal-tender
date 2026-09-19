package fecflowmastergaps

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
)

func TestReadRawHistoryArchivePreservesOfficialAssertions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cm18.zip")
	rows := []string{
		committeeHistoryRow("C00000001", "FIRST COMMITTEE", "A", "N", "DEM", "H0AA00003"),
		committeeHistoryRow("C00000002", "SECOND COMMITTEE", "U", "Q", "NON", ""),
	}
	writeHistoryZIP(t, path, strings.Join(rows, "\n")+"\n")

	reference, assertions, err := readRawHistoryArchive(context.Background(), filepath.Dir(path), RawHistoryArchiveInput{Cycle: "2018", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if reference.Cycle != "2018" || reference.CommitteeRecords != 2 || reference.Member != "cm.txt" || len(reference.ArchiveSHA256) != 64 {
		t.Fatalf("unexpected archive reference: %+v", reference)
	}
	first := assertions["C00000001"][0]
	if first.SourceKind != "official_bulk_archive" || first.Name != "FIRST COMMITTEE" || first.SourceRow != 1 || first.CandidateID == nil || *first.CandidateID != "H0AA00003" {
		t.Fatalf("unexpected historical assertion: %+v", first)
	}
}

func TestAuditAccumulatorPartitionsMissingEndpointsAndClassifiesGaps(t *testing.T) {
	t.Parallel()
	current := map[string]struct{}{"C00000001": {}, "C00000002": {}}
	accumulator := &auditAccumulator{references: make(map[string]struct{}), gaps: make(map[string]*gapAccumulator)}
	results := []fecflows.Result{
		flowResult("C00000003", "C00000001", "100", 2),
		flowResult("C00000001", "C00000004", "-20", 1),
		flowResult("C00000003", "C00000004", "30", 1),
		flowResult("C00000001", "C00000002", "40", 1),
	}
	for _, result := range results {
		if err := accumulator.addResult(result, current); err != nil {
			t.Fatal(err)
		}
	}
	history := map[string][]CommitteeHistoricalAssertion{
		"C00000003": {{Cycle: "2018", SourceKind: "official_bulk_archive", Name: "OLD COMMITTEE"}},
	}
	linkages := map[string][]LinkageAssertion{
		"C00000004": {{FactID: "link", FactSetID: strings.Repeat("1", 64), CandidateID: "H0AA00001", CommitteeID: "C00000004"}},
	}
	summaries := map[string][]CandidateSummaryAssertion{
		"H0AA00001": {{FactID: "summary", FactSetID: strings.Repeat("2", 64), Dataset: "all-candidates-summary", CandidateID: "H0AA00001"}},
	}
	committees, counts := buildGaps(accumulator, nil, history, linkages, summaries, nil)
	if len(committees) != 2 || counts.CommitteesMissingCurrentMaster != 2 || counts.FoundOnlyInHistory != 1 || counts.AbsentFromAllAuditedMasters != 1 {
		t.Fatalf("unexpected gap classification: counts=%+v committees=%+v", counts, committees)
	}
	if counts.MissingSourceCommittees != 1 || counts.MissingRecipientCommittees != 1 || counts.MissingCommitteesInBothEndpointRoles != 0 {
		t.Fatalf("unexpected endpoint populations: %+v", counts)
	}
	if accumulator.anyMissing.resultGroups != 3 || accumulator.onlySource.resultGroups != 1 || accumulator.onlyTarget.resultGroups != 1 || accumulator.both.resultGroups != 1 || accumulator.anyMissing.amount.String() != "110" {
		t.Fatalf("unexpected edge exposure partition: any=%+v", accumulator.anyMissing.snapshot())
	}
}

func TestReadRawHistoryArchivePreservesIssuesWithoutInventingAnID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cm18.zip")
	writeHistoryZIP(t, path, "NOT_A_COMMITTEE|bad\n")
	reference, assertions, err := readRawHistoryArchive(context.Background(), filepath.Dir(path), RawHistoryArchiveInput{Cycle: "2018", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if reference.IssueRecords != 1 || len(reference.IssueCounts) != 1 || reference.IssueCounts[0].Code != "field_count" || len(assertions) != 0 {
		t.Fatalf("unexpected issue preservation: reference=%+v assertions=%+v", reference, assertions)
	}
}

func TestIndexRawHistoryRejectsInvalidCycleBeforeOpeningArchive(t *testing.T) {
	t.Parallel()
	_, _, err := indexRawHistory(context.Background(), t.TempDir(), []RawHistoryArchiveInput{{Cycle: "94", Path: "missing.zip"}})
	if err == nil || !strings.Contains(err.Error(), "four-digit even year") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSourceReceiptEvidenceConservesRowsAndAmounts(t *testing.T) {
	t.Parallel()
	gap := &gapAccumulator{}
	gap.outgoing.receiptRows = 2
	gap.outgoing.amount.SetInt64(125)
	targets := map[string]*gapAccumulator{"C00000003": gap}
	evidence := map[string][]SourceReceiptAssertion{
		"C00000003": {{AmountMinorUnits: "100"}, {AmountMinorUnits: "25"}},
	}
	if err := validateSourceReceiptEvidence(targets, evidence); err != nil {
		t.Fatal(err)
	}
	evidence["C00000003"] = evidence["C00000003"][:1]
	if err := validateSourceReceiptEvidence(targets, evidence); err == nil {
		t.Fatal("expected incomplete evidence to fail conservation")
	}
}

func flowResult(source, recipient, amount string, receipts uint64) fecflows.Result {
	return fecflows.Result{
		SourceCommitteeID: source, RecipientCommitteeID: recipient,
		ReceiptRole: fecflows.RoleAffiliatedTransferIn, SignedAmountMinorUnits: amount,
		ReceiptCount: receipts, PositiveCount: receipts,
	}
}

func committeeHistoryRow(id, name, designation, committeeType, party, candidateID string) string {
	return strings.Join([]string{
		id, name, "TREASURER", "1 MAIN ST", "", "CITY", "VA", "22000",
		designation, committeeType, party, "Q", "C", "CONNECTED ORG", candidateID,
	}, "|")
}

func writeHistoryZIP(t *testing.T, path, content string) {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create("cm.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buffer.Bytes(), 0o640); err != nil {
		t.Fatal(err)
	}
}
