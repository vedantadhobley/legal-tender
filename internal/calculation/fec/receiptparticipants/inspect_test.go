package receiptparticipants

import (
	"context"
	"encoding/json"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestEveryAppearanceOpensFullSourceIncludingUnresolvedRecipient(t *testing.T) {
	rows := fixtureRows()
	root, m := sourceFixture(t, rows, 5)
	dir := t.TempDir()
	r, err := publish(context.Background(), Options{StorageRoot: root, Workers: 3, MaxOutputBytes: 64 << 20}, m, []int{0, 1, 2}, dir, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	r.SourceRows = uint64(len(rows))
	r.FactSetID = m.FactSetID
	r.CalculationID = strings.Repeat("b", 64)
	for _, input := range rows {
		got, err := inspectVerified(context.Background(), root, m, r, dir, uint64(input.Ordinal))
		if err != nil {
			t.Fatal(input.Ordinal, err)
		}
		if len(got.Source.Fields) != 99 || got.IdentityResolved || got.FinancialEligibility || got.Role != ContributorRole || !reflect.DeepEqual(got.Participant, project(input)) {
			t.Fatal("incomplete occurrence inspection", input.Ordinal)
		}
	}
	for _, ordinal := range []uint64{0, 13} {
		if _, err := inspectVerified(context.Background(), root, m, r, dir, ordinal); err == nil {
			t.Fatal("out-of-scope inspection accepted")
		}
	}
}

func TestManifestIdentityAndScopeFailClosed(t *testing.T) {
	root, m := sourceFixture(t, fixtureRows(), 12)
	dir := t.TempDir()
	r, err := publish(context.Background(), Options{StorageRoot: root, Workers: 1, MaxOutputBytes: 64 << 20}, m, []int{0}, dir, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	r.SchemaVersion = Version
	r.Policy = Policy
	r.InventoryPolicy = fundingbasis.Policy
	r.SourceRolePolicy = fundingbasis.EvidencePolicy
	r.IndividualPolicy = receipts.ContractID + "@" + receipts.ContractVersion
	r.CommitteePolicy = committeeflows.ContractID + "@" + committeeflows.ContractVersion
	r.Workers = 1
	r.AppearanceRole = ContributorRole
	r.State = "complete_cycle_participant_index"
	r.Scope = "complete_published_schedule_a_cycle"
	r.SourceRows = 12
	r.FactSetID = m.FactSetID
	r.ManifestSHA256 = strings.Repeat("b", 64)
	r.BuildSHA256 = strings.Repeat("c", 64)
	r.AdditionalConduitAmount = "0"
	r.CalculationID = logicalID(r)
	manifestDir := t.TempDir()
	if err = save(manifestDir, r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(manifestDir, "manifest.json")
	if _, err = Load(path, r.CalculationID); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(path, strings.Repeat("d", 64)); err == nil {
		t.Fatal("wrong identity accepted")
	}
	r.FinancialEligibility = true
	r.CalculationID = logicalID(r)
	b, _ := json.Marshal(r)
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(path, r.CalculationID); err == nil {
		t.Fatal("financial promotion accepted")
	}
}

func TestManifestReserveIsEnforcedBeforePublication(t *testing.T) {
	dir := t.TempDir()
	if err := save(dir, Result{State: strings.Repeat("x", 1<<20)}); err == nil {
		t.Fatal("oversized manifest accepted")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("oversized manifest wrote output", entries, err)
	}
}
