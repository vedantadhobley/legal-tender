package receipts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func TestScanDirectScheduleAReusesAcceptedReceiptCalculation(t *testing.T) {
	t.Parallel()
	fixturePath := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", "schedule-a", "v1", "fixtures", "dump-2026-08-23", "targeted-individual.copy")
	rows, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	storageRoot := t.TempDir()
	stagedPath := filepath.Join(storageRoot, "schedule-a.copy.zst")
	compressed, err := compressProbeFixture(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stagedPath, compressed, 0o640); err != nil {
		t.Fatal(err)
	}
	compressedDigest := sha256.Sum256(compressed)
	uncompressedDigest := sha256.Sum256(rows)
	rowCount := uint64(1)
	output := fecrelease.StagedOutput{
		RowCount: &rowCount, CompressedByteCount: uint64(len(compressed)),
		CompressedSHA256:      hex.EncodeToString(compressedDigest[:]),
		UncompressedByteCount: uint64(len(rows)), UncompressedSHA256: hex.EncodeToString(uncompressedDigest[:]),
	}
	coverage := "2026-12-31"
	summaryAmount := "5000"
	calculator, err := NewCycleCalculator("2026", nil, []LinkageFact{{
		FactID: "link", State: "valid", CandidateID: "H6AA00001", CommitteeID: "C00392928", DesignationCode: "A",
	}}, []SummaryFact{{
		FactID: "summary", FactType: "fec.candidate_summary_all.v1", Dataset: "all-candidates-summary",
		CandidateID: "H6AA00001", CoverageThrough: &coverage,
		TotalIndividualContributions: SummaryAmount{RawValue: "50.00", ReportedMinorUnits: &summaryAmount, ObservationState: "reported_value"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	scan, err := scanDirectScheduleA(context.Background(), stagedPath, output, "2026", calculator, nil)
	if err != nil {
		t.Fatal(err)
	}
	if scan.SourceRows != 1 || scan.ValidatedRows != 1 || scan.RoutedRows != 1 || scan.Decisions.Included != 1 || scan.Decisions.IncludedAmountMinorUnits != "5000" {
		t.Fatalf("unexpected direct scan: %+v", scan)
	}
	results, err := calculator.Results()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].CandidateID != "H6AA00001" || pointerValue(results[0].MoneyMeasure.Amount.LowerMinorUnits) != "5000" {
		t.Fatalf("unexpected candidate results: %+v", results)
	}
	if len(results[0].Reconciliations) != 1 || pointerValue(results[0].Reconciliations[0].DifferenceMinorUnits) != "0" {
		t.Fatalf("unexpected source reconciliation: %+v", results[0].Reconciliations)
	}
}

func TestDirectProbeReconciliationTreatsExactZeroAsWithinEveryBand(t *testing.T) {
	t.Parallel()
	zero := "0"
	counts := DirectProbeReconciliationCounts{}
	addDirectProbeReconciliation(&counts, SummaryReconciliation{
		SummaryMinorUnits: &zero, ResolvedDetailMinorUnits: &zero, DifferenceMinorUnits: &zero,
	})
	if counts.Comparable != 1 || counts.Exact != 1 || counts.ZeroSummary != 1 || counts.NonzeroSummary != 0 || counts.WithinFivePercent != 1 || counts.WithinTenPercent != 1 || counts.WithinTwentyFivePercent != 1 {
		t.Fatalf("unexpected exact-zero reconciliation counts: %+v", counts)
	}
}

func compressProbeFixture(content []byte) ([]byte, error) {
	var destination bytes.Buffer
	encoder, err := zstd.NewWriter(&destination, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	if _, err := encoder.Write(content); err != nil {
		encoder.Close()
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return destination.Bytes(), nil
}
