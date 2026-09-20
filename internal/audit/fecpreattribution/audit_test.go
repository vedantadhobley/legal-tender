package fecpreattribution

import (
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/audit/fecschedulebsemantics"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

func TestResolvedCohortRequiresOneEvidenceClass(t *testing.T) {
	tests := []struct {
		codes []string
		want  string
		ok    bool
	}{
		{[]string{"unique_exact_name_office_context", "reported_id_absent_from_candidate_master"}, "reported_id_absent_from_candidate_master", true},
		{[]string{"unique_exact_name_office_context", "reported_id_context_conflict"}, "reported_id_context_conflict", true},
		{[]string{"unique_exact_name_office_context"}, "", false},
		{[]string{"reported_id_absent_from_candidate_master", "reported_id_context_conflict"}, "", false},
	}
	for _, test := range tests {
		got, err := resolvedCohort(test.codes)
		if (err == nil) != test.ok || got != test.want {
			t.Fatalf("resolvedCohort(%v) = %q, %v", test.codes, got, err)
		}
	}
}

func TestCandidateExamplesPreserveSeparateEndpoints(t *testing.T) {
	reportedName := "Example, Alex"
	resolvedOne, resolvedTwo := "H4AA00002", "H4AA00003"
	first := candidateresolution.Decision{
		FactID: "a", ReportedCandidate: candidateresolution.ReportedCandidate{CandidateID: "H4AA00001", Name: &reportedName},
		ResolvedCandidateID: &resolvedOne, AmountMinorUnits: "100", EvidenceCodes: []string{"reported_id_context_conflict"},
	}
	largest := candidateresolution.Decision{
		FactID: "b", ReportedCandidate: candidateresolution.ReportedCandidate{CandidateID: "H4AA00004", Name: &reportedName},
		ResolvedCandidateID: &resolvedTwo, AmountMinorUnits: "-500", EvidenceCodes: []string{"reported_id_context_conflict"},
	}
	got := candidateExamples(&candidateAccumulator{first: &first, largest: &largest})
	if len(got) != 2 || got[0].ReportedCandidateID == got[0].ResolvedCandidateID || got[1].AmountMinorUnits != "-500" {
		t.Fatalf("candidate examples = %#v", got)
	}
}

func TestProfileScheduleBSeparatesOneSidedAndConflict(t *testing.T) {
	result := fecschedulebsemantics.Result{
		SchemaVersion: fecschedulebsemantics.Version, Status: "complete_diagnostic",
		Input:    fecschedulebsemantics.Input{Facts: 6},
		Measures: fecschedulebsemantics.Measures{Rows: 6, AmountMinorUnits: 210},
		Groups: []fecschedulebsemantics.Group{
			{Shape: fecschedulebsemantics.Shape{RecipientIdentity: "raw_committee_id_only", SelfRecipient: true}, Measures: fecschedulebsemantics.Measures{Rows: 2, AmountMinorUnits: 30, NonMemoRows: 2, NonMemoAmountMinorUnits: 30}, Example: fecschedulebsemantics.Example{Ordinal: 2}},
			{Shape: fecschedulebsemantics.Shape{RecipientIdentity: "clean_committee_id_only"}, Measures: fecschedulebsemantics.Measures{Rows: 1, AmountMinorUnits: 30, NonMemoRows: 1, NonMemoAmountMinorUnits: 30}, Example: fecschedulebsemantics.Example{Ordinal: 3}},
			{Shape: fecschedulebsemantics.Shape{RecipientIdentity: "conflicting_committee_ids"}, Measures: fecschedulebsemantics.Measures{Rows: 1, AmountMinorUnits: 40, NonMemoRows: 1, NonMemoAmountMinorUnits: 40}, Example: fecschedulebsemantics.Example{Ordinal: 4}},
			{Shape: fecschedulebsemantics.Shape{RecipientIdentity: "no_valid_committee_id"}, Measures: fecschedulebsemantics.Measures{Rows: 2, AmountMinorUnits: 110, NonMemoRows: 2, NonMemoAmountMinorUnits: 110}, Example: fecschedulebsemantics.Example{Ordinal: 1}},
		},
	}
	cohorts, rows, err := profileScheduleB(result)
	if err != nil {
		t.Fatal(err)
	}
	if rows.oneSided != 3 || rows.conflicts != 1 || len(cohorts) != 3 || cohorts[0].SelfRecipientRows != 2 ||
		cohorts[0].NonMemoRows == nil || *cohorts[0].NonMemoRows != 2 {
		t.Fatalf("rows=%+v cohorts=%+v", rows, cohorts)
	}
}

func TestProfileABBuildsExplicitGapBands(t *testing.T) {
	review := flowreconciliation.ReviewResult{
		Shapes: []flowreconciliation.ReviewShape{
			{Key: flowreconciliation.ReviewKey{State: "corroborated_exact_signature"}, Components: 2},
			{Key: flowreconciliation.ReviewKey{State: "candidate_date_disagreement"}, Components: 5},
			{Key: flowreconciliation.ReviewKey{State: "ambiguous_candidates"}, Components: 3},
		},
		Dates: []flowreconciliation.ReviewDate{
			{AMinusBDays: 0, Components: 2}, {AMinusBDays: -2, Components: 1},
			{AMinusBDays: 8, Components: 1}, {AMinusBDays: 31, Components: 1},
			{AMinusBDays: 400, Components: 2},
		},
	}
	got, shapeCount, err := profileAB(review)
	if err != nil {
		t.Fatal(err)
	}
	if got.Components != 10 || got.DatedOneToOneComponents != 7 || got.DateDisagreementComponents != 5 ||
		got.MaximumAbsoluteGapDays != 400 || got.AMinusBNegativeComponents != 1 || got.AMinusBPositiveComponents != 4 || shapeCount != 5 {
		t.Fatalf("profile = %+v, shapeCount=%d", got, shapeCount)
	}
	wantBands := []uint64{2, 1, 1, 0, 1, 0, 2}
	for i, want := range wantBands {
		if got.GapBands[i].Components != want {
			t.Fatalf("band %s = %d; want %d", got.GapBands[i].Name, got.GapBands[i].Components, want)
		}
	}
	if len(got.DateDisagreementShapes) != 1 || got.DateDisagreementShapes[0].Components != 5 {
		t.Fatalf("date disagreement shapes = %+v", got.DateDisagreementShapes)
	}
}
