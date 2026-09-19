package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func summaryReviewFixture(t *testing.T) (*Reader, summaryassertion.Result) {
	t.Helper()
	id := strings.Repeat("a", 64)
	r := &Reader{manifest: occ.ScheduleAColumnarManifest{SourceReleaseID: "fec-" + id, SourceReleaseManifestSHA256: id}}
	r.result = Result{CalculationID: id, Cycle: "2024", Input: Input{id, id, 4, 1}}
	for i, component := range []string{"itemized_individual_only", "overlapping_individual_and_committee", "memo_subtotal", "unknown_amount"} {
		row := baseRow()
		amount := int64((i + 1) * 100)
		row.Amount = &amount
		if i == 1 {
			amount = -200
		}
		if i == 3 {
			row.Amount = nil
		}
		var m Measures
		if err := m.observe(row); err != nil {
			t.Fatal(err)
		}
		decision := "excluded_memo_subtotal"
		if i < 2 {
			decision = "included"
		}
		r.result.Buckets = append(r.result.Buckets, Bucket{Key: Key{Recipient: Cell{true, "C00000001"}, Component: component, ReceiptRole: "synthetic_role", IndividualDecision: decision}, Measures: m})
	}
	zero := "0"
	valid := func(s string) committeesummary.Value { return committeesummary.Value{State: "valid", Value: &s} }
	a := summaryassertion.Assertion{
		ID: id, RepresentativeFactID: id, CommitteeType: "H", Designation: "P",
		CoverageStart: valid("2023-01-01"), CoverageEnd: valid("2024-12-31"),
		Members:   []summaryassertion.Member{{FactID: id, OccurrenceID: id, Ordinal: 1, Offset: 1, Length: 1, RawSHA256: id, Candidate: committeesummary.Value{State: "source_blank"}}},
		Equations: map[string]summaryassertion.Equation{},
	}
	for _, name := range []string{"cash", "individual", "cash_federal_columns"} {
		a.Equations[name] = summaryassertion.Equation{State: "equal", Delta: &zero, Operands: []summaryassertion.Operand{}}
	}
	for _, rule := range summaryReviewFields {
		eq := a.Equations[rule.Equation]
		eq.Operands = append(eq.Operands, summaryassertion.Operand{Field: rule.Field, Raw: "0", Value: committeesummary.MoneyValue{State: "valid", MinorUnits: &zero}})
		a.Equations[rule.Equation] = eq
	}
	s := summaryassertion.Result{Cycle: "2024", CalculationID: id, Input: summaryassertion.Input{FactSetID: id, ManifestSHA256: id, SourceReleaseID: "fec-" + id, SourceReleaseSHA256: id, SourceArtifactSHA256: id}, Committees: []summaryassertion.Committee{{CommitteeID: "C00000001", State: "single_assertion", ConflictFields: []string{}, Assertions: []summaryassertion.Assertion{a}}}}
	return r, s
}

func TestSummaryReviewNoArithmeticPromotionAndCohortConservation(t *testing.T) {
	r, summary := summaryReviewFixture(t)
	a, err := r.reviewSummary(context.Background(), summary, "C00000001")
	if err != nil {
		t.Fatal(err)
	}
	if a.ComparisonReady || a.FundingEligible || a.TerminalEligible || a.SourceAlignment != "same_source_release" || len(a.ComparisonBlocks) != 2 {
		t.Fatal("matching source and equal arithmetic promoted readiness")
	}
	if a.Receipts.Total.Rows != 4 || a.Receipts.Total.Unknown != 1 || a.Receipts.Total.Signed != 200 || a.Receipts.Individual.Signed != -100 || a.Receipts.Individual.Rows != 2 || a.Receipts.Overlap.Signed != -200 {
		t.Fatal("receipt cohorts lost signs, unknowns, or counted overlap twice")
	}
	for _, field := range a.Assertions[0].Fields {
		if field.Delta != nil || len(field.Blockers) == 0 || field.Value.MinorUnits == nil || *field.Value.MinorUnits != "0" {
			t.Fatal("field was repaired or compared")
		}
	}
	b, err := r.reviewSummary(context.Background(), summary, "C00000001")
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("nonidentical review replay", err)
	}
	if path := os.Getenv("LT_SUMMARY_REVIEW_FIXTURE"); path != "" {
		body, _ := json.MarshalIndent(a, "", "  ")
		if err := os.WriteFile(path, body, 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSummaryReviewSourceAndMissingPopulationStates(t *testing.T) {
	r, summary := summaryReviewFixture(t)
	summary.Input.SourceReleaseID = "fec-" + strings.Repeat("b", 64)
	got, err := r.reviewSummary(context.Background(), summary, "C00000001")
	if err != nil || got.SourceAlignment != "different_source_release" || !slices.Contains(got.ComparisonBlocks, "source_release_mismatch") {
		t.Fatal(got, err)
	}
	summary.Input.SourceReleaseID = r.manifest.SourceReleaseID
	summary.Input.SourceReleaseSHA256 = strings.Repeat("b", 64)
	if _, err := r.reviewSummary(context.Background(), summary, "C00000001"); err == nil {
		t.Fatal("accepted conflicting digest for same release")
	}
	summary.Input.SourceReleaseSHA256 = r.manifest.SourceReleaseManifestSHA256
	got, err = r.reviewSummary(context.Background(), summary, "C99999999")
	if err != nil || got.Receipts.State != "no_rows_in_snapshot" || got.SummaryState != "no_indexed_summary_in_snapshot" || len(got.Assertions) != 0 || len(got.ComparisonBlocks) != 4 {
		t.Fatal("absence was not explicit", err)
	}
	summary.Cycle = "2022"
	if _, err := r.reviewSummary(context.Background(), summary, "C00000001"); err == nil {
		t.Fatal("accepted mixed cycles")
	}
	if _, err := r.ReviewSummary(context.Background(), "unused", "2024", "BAD"); err == nil {
		t.Fatal("accepted invalid identity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.reviewSummary(ctx, summary, "C00000001"); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestSummaryReviewFieldScopedConflictsAndSensitivity(t *testing.T) {
	r, summary := summaryReviewFixture(t)
	a := summary.Committees[0].Assertions[0]
	copyA := a
	copyA.ID = strings.Repeat("b", 64)
	summary.Committees[0].State = "conflicting_assertions"
	summary.Committees[0].ConflictFields = []string{"CMTE_NM", "INDV_UNITEM_CONTB"}
	summary.Committees[0].Assertions = append(summary.Committees[0].Assertions, copyA)
	eq := a.Equations["individual"]
	eq.State, eq.Delta = "missing", nil
	for i := range eq.Operands {
		if eq.Operands[i].Field == "INDV_UNITEM_CONTB" {
			eq.Operands[i].Raw = ""
			eq.Operands[i].Value = committeesummary.MoneyValue{State: "source_blank"}
		}
	}
	a.Equations["individual"] = eq
	federal := a.Equations["cash_federal_columns"]
	delta := "12345"
	federal.State, federal.Delta = "different", &delta
	a.Equations["cash_federal_columns"] = federal
	got, err := r.reviewSummary(context.Background(), summary, "C00000001")
	if err != nil || len(got.Assertions) != 2 || len(got.Assertions[0].ScopeBlockers) != 0 {
		t.Fatal("variant lost or irrelevant metadata made a scope conflict", err)
	}
	for _, field := range got.Assertions[0].Fields {
		if field.Field == "INDV_UNITEM_CONTB" && (field.Value.MinorUnits != nil || !slices.Contains(field.Blockers, "conflicting_reported_field") || !slices.Contains(field.Blockers, "summary_value_source_blank")) {
			t.Fatal("blank or field conflict lost")
		}
		if field.Field == "TTL_FED_RECEIPTS" && len(field.Blockers) != 1 {
			t.Fatal("federal sensitivity used as cash acceptance rule")
		}
	}
	for _, tc := range []struct{ start, end, want string }{{"2024-05-01", "2024-04-01", "summary_coverage_reversed"}, {"2022-12-31", "2024-12-31", "summary_coverage_outside_source_cycle"}} {
		a.CoverageStart.Value, a.CoverageEnd.Value = &tc.start, &tc.end
		v, err := reviewVariant("2024", []string{"CVG_START_DT"}, a)
		if err != nil || !slices.Contains(v.ScopeBlockers, tc.want) || !slices.Contains(v.ScopeBlockers, "conflicting_summary_scope") {
			t.Fatal(v, err)
		}
	}
	a.CoverageStart = committeesummary.Value{State: "invalid"}
	v, err := reviewVariant("2024", nil, a)
	if err != nil || !slices.Contains(v.ScopeBlockers, "summary_coverage_unavailable") {
		t.Fatal(v, err)
	}
}

func TestSummaryReviewPinnedPolicy(t *testing.T) {
	var policy struct {
		Policy string             `json:"policy"`
		Fields []summaryFieldRule `json:"fields"`
	}
	body, err := os.ReadFile("../../../../contracts/calculations/fec/summary-receipt-review/v1/policy.json")
	if err != nil || json.Unmarshal(body, &policy) != nil || policy.Policy != SummaryReviewPolicy || !reflect.DeepEqual(policy.Fields, summaryReviewFields) {
		t.Fatal("compiled field policy differs from pinned contract", err)
	}
}

func TestPublishedSummaryReviewCorpus(t *testing.T) {
	root, basis, manifest, output := os.Getenv("LT_SUMMARY_REVIEW_STORAGE"), os.Getenv("LT_SUMMARY_REVIEW_BASIS"), os.Getenv("LT_SUMMARY_REVIEW_MANIFEST"), os.Getenv("LT_SUMMARY_REVIEW_OUTPUT")
	if root == "" || basis == "" || manifest == "" || output == "" {
		t.Skip("requires exact published receipt inventory and summary facts")
	}
	r, err := Open(context.Background(), root, basis)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := summaryassertion.Run(context.Background(), root, manifest, r.result.Cycle)
	if err != nil {
		t.Fatal(err)
	}
	// Audit witnesses only; production accepts any exact committee ID.
	for _, committee := range []string{"C00075820", "C00843367", "C00249581", "C00000935", "C99999999"} {
		a, err := r.reviewSummary(context.Background(), summary, committee)
		if err != nil {
			t.Fatal(err)
		}
		b, err := r.reviewSummary(context.Background(), summary, committee)
		first, _ := json.MarshalIndent(a, "", "  ")
		second, _ := json.MarshalIndent(b, "", "  ")
		if err != nil || !bytes.Equal(first, second) || a.ComparisonReady || a.FundingEligible || a.TerminalEligible {
			t.Fatal("review replay/readiness failure", err)
		}
		if err := os.WriteFile(filepath.Join(output, committee+".json"), first, 0o640); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: source_alignment=%s variants=%d receipt_rows=%d", committee, a.SourceAlignment, len(a.Assertions), a.Receipts.Total.Rows)
	}
}
