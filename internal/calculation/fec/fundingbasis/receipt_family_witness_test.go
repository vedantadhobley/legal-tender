package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
)

// Opt-in real source gate, not named runtime exceptions or fixture-only scope.
func TestReceiptFamilyCompleteF3XWitness(t *testing.T) {
	audit, root := os.Getenv("LT_FAMILY_WITNESS_AUDIT"), os.Getenv("LT_FAMILY_WITNESS_STORAGE")
	if audit == "" || root == "" {
		t.Skip("requires retained complete F3X witness")
	}
	request := ReceiptWindowRequest{
		Summary: summaryassertion.WindowComparisonRequest{
			StorageRoot:     root,
			SummaryManifest: filepath.Join(root, "facts/fec/committee-summary/v1/manifests/603d086eb26baa5a9a99d7a717ec5b7469098c173d2d3eabaa119c00d9b7f637.json"),
			Window: reportperiod.WindowRequest{
				Membership:    reportperiod.Request{CapturePath: filepath.Join(root, "dumps/audits/fec/report-metadata-reader/2026-09-10/attempt-01/nrcc-reports-capture.json"), Start: "2023-01-01", End: "2023-01-31"},
				DocumentsPath: filepath.Join(audit, "nrcc-january-documents.json"),
			},
		},
		ProfilePath:   filepath.Join(root, "dumps/audits/fec/receipt-report-profile-v2/2026-09-10/attempt-01/profile.json"),
		ProfileSHA256: "acce0a13f2d87abfe78beb66c6bd2ae60f82a53bf656b3745343966ae57f3647",
	}
	r, err := CompareReceiptFamilies(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Reports) != 1 || len(r.FamilyReports.Bindings) != 1 || !r.FamilyReports.Bindings[0].ScopeBound || r.Reports[0].Total.Rows != 2595 || len(r.Reports[0].Groups) != 10 {
		t.Fatal("changed report scope or population")
	}
	if r.SourceBodyRescanned || r.FamilyWindowComparisonReady || r.UniqueTransactionMembershipProven || r.FinancialUseEligible || r.TerminalEligible {
		t.Fatal("promoted financial or window eligibility")
	}
	expected := map[string]string{"other_committee_contributions": "173100000", "affiliated_or_party_transfers": "54848772"}
	counts := map[string]uint64{"other_committee_contributions": 67, "affiliated_or_party_transfers": 18}
	matched := 0
	for _, f := range r.Reports[0].Families {
		if amount, ok := expected[f.Field.ID]; ok {
			if f.State != "equal" || f.DetailMinorUnits == nil || *f.DetailMinorUnits != amount || f.ReportedMinorUnits == nil || *f.ReportedMinorUnits != amount || f.Nonmemo.Rows != counts[f.Field.ID] {
				t.Fatal("changed qualified observation", f)
			}
			matched++
		} else if f.State != "blocked" || f.DetailMinorUnits != nil || f.DeltaMinorUnits != nil {
			t.Fatal("invented absent detail", f)
		}
	}
	if matched != 2 {
		t.Fatal("missing accepted family")
	}
	// This exact-byte replay uses the original /audit and /storage mount aliases.
	want, err := os.ReadFile(filepath.Join(audit, "nrcc-january.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(r, "", "  ")
	if err != nil || !bytes.Equal(append(got, '\n'), want) {
		t.Fatal("changed retained output", err)
	}
}
