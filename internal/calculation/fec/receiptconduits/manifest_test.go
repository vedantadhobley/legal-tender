package receiptconduits

import (
	"encoding/json"
	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestIdentityScopeAndPhysicalDigestBinding(t *testing.T) {
	h := strings.Repeat("a", 64)
	base := Result{SchemaVersion: Version, State: "complete_cycle_conduit_evidence", BuildSHA256: h, Policy: Policy, AssociationPolicy: policy.Policy, ParticipantID: h, ParticipantSHA256: h, TopologyID: h, TopologySHA256: h, FactSetID: h, FactManifestSHA256: h, SourceRows: 3, EligibleRoleRows: 1, OtherRows: 2, OtherDisposition: "not_a_non_memo_reviewed_earmark", AdditionalAmount: "0", Workers: 1, States: map[string]uint64{Unassessed: 1}, Amounts: map[string]uint64{"not_assessed": 1}, Decisions: xsort.File{Name: "run.zst", Rows: 1, SHA256: h, ValuesSHA256: h}}
	for _, kind := range []string{"valid", "wrong_expected", "promoted", "unknown_state", "wrong_counts", "wrong_policy"} {
		t.Run(kind, func(t *testing.T) {
			r := base
			switch kind {
			case "promoted":
				r.FinancialEligibility = true
			case "unknown_state":
				r.States = map[string]uint64{"invented": 1}
			case "wrong_counts":
				r.OtherRows++
			case "wrong_policy":
				r.AssociationPolicy = "unknown"
			}
			r.CalculationID = identity(r)
			dir := t.TempDir()
			if err := save(dir, r); err != nil {
				t.Fatal(err)
			}
			expected := r.CalculationID
			if kind == "wrong_expected" {
				expected = h
			}
			_, err := Load(filepath.Join(dir, "manifest.json"), expected)
			if (kind == "valid") != (err == nil) {
				t.Fatal(kind, err)
			}
		})
	}
	r := base
	r.CalculationID = identity(r)
	dir := t.TempDir()
	if err := save(dir, r); err != nil {
		t.Fatal(err)
	}
	r.States = map[string]uint64{"ambiguous_or_incomplete_reference_evidence": 1}
	b, _ := json.Marshal(r)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(dir, "manifest.json"), r.CalculationID); err == nil {
		t.Fatal("changed logical values accepted")
	}
	if err := save(t.TempDir(), Result{State: strings.Repeat("x", 1<<20)}); err == nil {
		t.Fatal("oversized manifest accepted")
	}
}
