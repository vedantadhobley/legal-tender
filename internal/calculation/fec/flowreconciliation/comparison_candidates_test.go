package flowreconciliation

import (
	"context"
	"strings"
	"testing"
)

func TestBuildComparisonCandidatesPublishesDirectPairsBeforeUnion(t *testing.T) {
	t.Parallel()
	date := func(value int32) *int32 { return &value }
	observation := func(ordinal uint64, sender, recipient, role, kind string, day *int32, amount int64) Observation {
		return Observation{Ordinal: ordinal, SubID: strings.Repeat(string(rune('a'+ordinal%20)), 12), Sender: sender, Recipient: recipient, Role: role, Type: kind, Date: day, Amount: amount}
	}
	a := []Observation{
		observation(1, "C00000001", "C00000002", "contribution", "15K", date(100), 100),
		observation(2, "C00000003", "C00000004", "contribution", "15K", date(110), 200),
		observation(3, "C00000005", "C00000006", "in_kind", "15E", nil, 300),
		observation(4, "C00000007", "C00000008", "loan", "13", date(120), 400),
		observation(5, "C00000009", "C00000010", "contribution", "15K", date(130), 500),
		observation(6, "C00000011", "C00000012", "contribution", "15K", date(200), 600),
		observation(7, "C00000013", "C00000014", "contribution", "15K", date(140), 700),
	}
	b := []Observation{
		observation(11, "C00000001", "C00000002", "contribution", "24K", date(100), 100),
		observation(12, "C00000003", "C00000004", "contribution", "24K", date(105), 200),
		observation(13, "C00000005", "C00000006", "in_kind", "24Z", nil, 300),
		observation(14, "C00000007", "C00000008", "loan", "22H", date(120), 401),
		observation(15, "C00000009", "C00000010", "in_kind", "24Z", date(130), 500),
		observation(16, "C00000011", "C00000012", "contribution", "24K", date(198), 600),
		observation(17, "C00000011", "C00000012", "contribution", "24K", date(180), 600),
		observation(18, "C00000015", "C00000016", "contribution", "24K", date(150), 800),
	}
	id := strings.Repeat("a", 64)
	candidates, counts, err := buildComparisonCandidates(context.Background(), "2024", id, a, b, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := ComparisonCounts{
		ScheduleAObservations: 7, ScheduleBObservations: 8,
		ScheduleAWithCandidates: 6, ScheduleAWithoutCandidates: 1,
		ScheduleBWithCandidates: 7, ScheduleBWithoutCandidates: 1,
		CandidatePairs: 7, MutualOneToOne: 5, CompetingCandidates: 2,
		ExactSignature: 1, RoleAmountDateDisagreement: 3, RoleAmountMissingDate: 1,
		RoleDateAmountConflict: 1, AmountDateRoleConflict: 1,
	}
	if counts != want {
		t.Fatalf("counts = %#v, want %#v", counts, want)
	}
	if len(candidates) != 7 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	states := map[string]int{}
	for index, candidate := range candidates {
		if err := validateComparisonCandidate(candidate); err != nil {
			t.Fatalf("candidate %d: %v", index, err)
		}
		if candidate.FinancialEffect != ComparisonFinancialEffectNone {
			t.Fatal("comparison candidate acquired financial effect")
		}
		states[candidate.State]++
	}
	if candidates[1].DateGapDays == nil || *candidates[1].DateGapDays != 5 || candidates[1].DateGapBand != "4_to_10_days" {
		t.Fatalf("date-gap evidence = %#v", candidates[1])
	}
	if candidates[5].Ambiguity != ComparisonCompetingCandidates || candidates[5].ScheduleACandidateCount != 2 || candidates[5].ScheduleBCandidateCount != 1 {
		t.Fatalf("direct ambiguity = %#v", candidates[5])
	}
	if states[ComparisonDateDisagreement] != 3 || states[ComparisonAmountConflict] != 1 || states[ComparisonRoleConflict] != 1 {
		t.Fatalf("states = %#v", states)
	}
}

func TestBuildComparisonCandidatesFailsBeforePartialCapacityResult(t *testing.T) {
	t.Parallel()
	day := int32(100)
	a := []Observation{{Ordinal: 1, SubID: "a", Sender: "C00000001", Recipient: "C00000002", Role: "contribution", Type: "15K", Date: &day, Amount: 100}}
	b := []Observation{
		{Ordinal: 2, SubID: "b", Sender: "C00000001", Recipient: "C00000002", Role: "contribution", Type: "24K", Date: &day, Amount: 100},
		{Ordinal: 3, SubID: "c", Sender: "C00000001", Recipient: "C00000002", Role: "contribution", Type: "24K", Date: &day, Amount: 100},
	}
	if values, counts, err := buildComparisonCandidates(context.Background(), "2024", strings.Repeat("a", 64), a, b, 1); err == nil || values != nil || counts != (ComparisonCounts{}) {
		t.Fatalf("capacity failure returned partial output: values=%#v counts=%#v err=%v", values, counts, err)
	}
}
