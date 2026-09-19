package committeeflows

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

type scanFixtureRow struct {
	recipient, raw, clean, receiptType *string
	amount                             *int64
	amountState                        string
	memo                               bool
}

func TestScanColumnarFactsUsesAcceptedPolicyAndConserves(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	one := "C00000001"
	two := "C00000002"
	three := "C00000003"
	invalid := "P00000001"
	inboundTransfer := "18G"
	inboundContribution := "15K"
	inKind := "15Z"
	outbound := "24K"
	unknownType := "15"
	amount := func(value int64) *int64 { return &value }
	rows := []scanFixtureRow{
		{&two, &one, &one, &inboundTransfer, amount(10000), "reported_value", false},
		{&two, &one, &one, &inboundContribution, amount(-500), "reported_value", false},
		{&three, &one, &one, &inKind, amount(250), "reported_value", false},
		{&two, nil, nil, &inboundContribution, amount(1800), "reported_value", false},
		{&two, &one, nil, &inboundContribution, amount(1400), "reported_value", false},
		{&two, &one, &one, &inboundContribution, amount(700), "reported_value", true},
		{&two, &one, &one, &unknownType, amount(1200), "reported_value", false},
		{&two, &one, &one, &inboundContribution, nil, "source_null", false},
		{&two, &one, &one, &outbound, amount(800), "reported_value", false},
		{&invalid, &one, &one, &inboundContribution, amount(1600), "reported_value", false},
	}
	manifest := writeFlowScanFixture(t, storageRoot, rows)
	scan, err := scanColumnarFacts(context.Background(), storageRoot, manifest, strings.Repeat("a", 64), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantDecisions := DecisionCounts{
		SourceFacts: 10, UnresolvedRecipientCommitteeID: 1, ExcludedNoSourceCommitteeID: 1,
		UnresolvedOneSidedSourceCommitteeID: 1, ExcludedMemoSubtotal: 1, UnresolvedAmount: 1,
		ExcludedOutboundReceiptRole: 1, UnresolvedReceiptRole: 1,
		IncludedReceiverReportedCommitteeFlow: 3,
	}
	if !reflect.DeepEqual(scan.decisions, wantDecisions) {
		t.Fatalf("decisions = %+v; want %+v", scan.decisions, wantDecisions)
	}
	if scan.amounts.known.String() != "17250" || scan.amounts.included.String() != "9750" ||
		scan.amounts.excluded.String() != "3300" || scan.amounts.unresolved.String() != "4200" {
		t.Fatalf("unexpected amount conservation: known=%s included=%s excluded=%s unresolved=%s", &scan.amounts.known, &scan.amounts.included, &scan.amounts.excluded, &scan.amounts.unresolved)
	}
	if scan.counts.KnownAmountRows != 9 || scan.counts.UnknownAmountRows != 1 || scan.counts.ResultGroups != 3 ||
		scan.counts.IncludedRows != 3 || scan.counts.IncludedPositiveRows != 2 || scan.counts.IncludedNegativeRows != 1 || scan.counts.IncludedZeroRows != 0 {
		t.Fatalf("unexpected result counts: %+v", scan.counts)
	}
	if len(scan.exceptions) != 4 || scan.exceptions[0].State != DecisionUnresolvedOneSided ||
		scan.exceptions[1].State != DecisionUnresolvedRole || scan.exceptions[2].State != DecisionUnresolvedAmount ||
		scan.exceptions[3].State != DecisionUnresolvedRecipient {
		t.Fatalf("unexpected exceptions: %+v", scan.exceptions)
	}
	if !amountConserves(&scan.amounts.known, &scan.amounts.included, &scan.amounts.excluded, &scan.amounts.unresolved) {
		t.Fatal("signed amounts do not conserve")
	}
}

func TestReceiptTypeRulesAndClassifierStayConnected(t *testing.T) {
	seen := make(map[string]struct{})
	for _, rule := range ReceiptTypeRules() {
		for _, code := range rule.Codes {
			if _, exists := seen[code]; exists {
				t.Fatalf("receipt type %s appears in more than one rule", code)
			}
			seen[code] = struct{}{}
			gotRole, gotDecision := ClassifyReceiptRole(&code)
			if gotRole != rule.Role || gotDecision != rule.Decision {
				t.Fatalf("receipt type %s classified as (%s, %s); want (%s, %s)", code, gotRole, gotDecision, rule.Role, rule.Decision)
			}
		}
	}
}

func TestValidateChecksRequiresExactContract(t *testing.T) {
	t.Parallel()
	definitions := expectedCheckDefinitions()
	checks := make([]Check, len(definitions))
	for index, definition := range definitions {
		checks[index] = Check{ID: definition.id, Passed: true, Severity: definition.severity, Detail: "fixture"}
	}
	if err := validateChecks(checks); err != nil {
		t.Fatalf("valid checks rejected: %v", err)
	}
	if err := validateChecks(checks[:len(checks)-1]); err == nil {
		t.Fatal("missing check accepted")
	}
	wrongID := append([]Check(nil), checks...)
	wrongID[0].ID = "unexpected"
	if err := validateChecks(wrongID); err == nil {
		t.Fatal("unexpected check ID accepted")
	}
	failedBlock := append([]Check(nil), checks...)
	failedBlock[0].Passed = false
	if err := validateChecks(failedBlock); err == nil {
		t.Fatal("failed blocking check accepted")
	}
}

func writeFlowScanFixture(t *testing.T, storageRoot string, fixtureRows []scanFixtureRow) fecoccurrence.ScheduleAColumnarManifest {
	t.Helper()
	fixturePath := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", "schedule-a", "v1", "fixtures", "dump-2026-08-23", "targeted-individual.copy")
	content, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Split(strings.TrimSuffix(string(content), "\n"), "\t")
	set := func(values []string, name, value string) {
		index, ok := schedulea.ColumnIndex(name)
		if !ok {
			t.Fatalf("missing Schedule A column %s", name)
		}
		values[index] = value
	}
	var copyText strings.Builder
	for _, candidate := range fixtureRows {
		values := append([]string(nil), base...)
		set(values, "cmte_id", fixtureString(candidate.recipient))
		set(values, "contbr_id", fixtureString(candidate.raw))
		set(values, "clean_contbr_id", fixtureString(candidate.clean))
		set(values, "receipt_tp", fixtureString(candidate.receiptType))
		set(values, "contb_receipt_amt", fixtureAmountText(candidate.amount))
		set(values, "two_year_transaction_period", "2024")
		copyText.WriteString(strings.Join(values, "\t"))
		copyText.WriteByte('\n')
	}
	decoder := schedulea.NewDecoder(bytes.NewBufferString(copyText.String()))
	physical, err := scheduleaparquet.NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(storageRoot, "fixture.parquet")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewWriter(file, physical.Parquet(), parquet.Compression(&parquetzstd.Codec{Level: parquetzstd.SpeedDefault, Concurrency: 1}))
	var encoded parquet.Row
	var rawOffset uint64
	index := 0
	for decoder.Scan() {
		row := decoder.Row()
		candidate := fixtureRows[index]
		scale := int32(2)
		encoded, err = physical.Encode(encoded, row, scheduleaparquet.Metadata{
			SourceRowOrdinal: row.Number(), SourceRawByteOffset: rawOffset, SourceRawByteLength: uint64(len(row.Raw())),
		}, scheduleaparquet.Derived{
			NormalizationState: "valid", ReceiptAmountMinorUnits: candidate.amount, ReceiptAmountSourceScale: &scale,
			ReceiptAmountState: candidate.amountState, AggregateYTDState: "reported_value",
			TwoYearTransactionPeriod: 2024, MemoedSubtotal: candidate.memo,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.WriteRows([]parquet.Row{encoded}); err != nil {
			t.Fatal(err)
		}
		rawOffset += uint64(len(row.Raw()))
		index++
	}
	if err := decoder.Err(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	parquetBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(parquetBytes)
	sha := hex.EncodeToString(digest[:])
	return fecoccurrence.ScheduleAColumnarManifest{
		Cycle: "2024", Counts: fecoccurrence.ScheduleAFactCounts{Facts: uint64(len(fixtureRows))},
		Shards: []fecoccurrence.ScheduleAColumnarShard{{
			Index: 0, FirstSourceRowOrdinal: 1, LastSourceRowOrdinal: uint64(len(fixtureRows)),
			Facts: uint64(len(fixtureRows)), StorageKey: "fixture.parquet", Bytes: uint64(len(parquetBytes)), SHA256: sha,
		}},
	}
}

func fixtureString(value *string) string {
	if value == nil {
		return `\N`
	}
	return *value
}

func fixtureAmountText(value *int64) string {
	if value == nil {
		return `\N`
	}
	return "1.00"
}
