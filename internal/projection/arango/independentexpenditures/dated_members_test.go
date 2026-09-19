package independentexpenditures

import (
	"context"
	"math/big"
	"testing"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
)

func TestMemberDateIsStrictAndNeverFillsUnknown(t *testing.T) {
	if got, err := memberDate(nil); got != nil || err != nil {
		t.Fatal(got, err)
	}
	for _, day := range []string{"", "2023-02-29", "2024-1-01", "2024-01-01 00:00:00", "2024"} {
		if got, err := memberDate(&day); err == nil || got != nil {
			t.Fatal("accepted non-day", day)
		}
	}
	day := "1970-01-01"
	got, err := memberDate(&day)
	if err != nil || got == nil || *got != 0 {
		t.Fatal(got, err)
	}
	day = "2024-02-29"
	if _, err := memberDate(&day); err != nil {
		t.Fatal(err)
	}
}

func TestMemberAggregateConservationDetectsOffsettingErrors(t *testing.T) {
	var total memberTotal
	for _, row := range []struct {
		state  string
		amount int64
	}{
		{resolution.StateConfirmed, 725}, {resolution.StateResolved, -125}, {resolution.StateUnverified, 0},
	} {
		total.add(resolution.Decision{State: row.state}, big.NewInt(row.amount))
	}
	e := resolvedExpenditureEdge{ExpenditureCount: 3, PositiveCount: 1, NegativeCount: 1, ZeroCount: 1, AmountMinorUnits: "600",
		ResolutionCounts:  EdgeResolutionCounts{Confirmed: 1, Resolved: 1, Unverified: 1},
		ResolutionAmounts: EdgeResolutionAmounts{ConfirmedMinorUnits: "725", ResolvedMinorUnits: "-125", UnverifiedMinorUnits: "0"}}
	if !total.matches(e) {
		t.Fatal("valid members rejected")
	}
	for _, mutate := range []func(*resolvedExpenditureEdge){
		func(e *resolvedExpenditureEdge) { e.ExpenditureCount++ },
		func(e *resolvedExpenditureEdge) { e.AmountMinorUnits = "601" },
		func(e *resolvedExpenditureEdge) { e.PositiveCount++; e.NegativeCount-- },
		func(e *resolvedExpenditureEdge) { e.ZeroCount-- },
		func(e *resolvedExpenditureEdge) { e.ResolutionCounts.Confirmed++; e.ResolutionCounts.Resolved-- },
		func(e *resolvedExpenditureEdge) {
			e.ResolutionAmounts.ConfirmedMinorUnits = "700"
			e.ResolutionAmounts.ResolvedMinorUnits = "-100"
		},
		func(e *resolvedExpenditureEdge) { e.ResolutionAmounts.UnverifiedMinorUnits = "1" },
	} {
		bad := e
		mutate(&bad)
		if total.matches(bad) {
			t.Fatal("accepted inconsistent group", bad)
		}
	}
}

func TestMemberReaderRequiresOpenedSourceAndBoundedSelection(t *testing.T) {
	r := &ResolvedReader{}
	if err := r.VisitDatedMembers(context.Background(), nil); err == nil {
		t.Fatal("accepted nil visitor")
	}
	if err := r.VisitDatedMembers(context.Background(), func(DatedMember) error { return nil }); err == nil {
		t.Fatal("accepted unbound source")
	}
	for _, want := range [][]DatedMember{nil, make([]DatedMember, 11), {{FactID: "unprojectable"}}} {
		if got, err := r.DatedMemberEvidence(context.Background(), want); err == nil || got != nil {
			t.Fatal("accepted invalid selection")
		}
	}
}
