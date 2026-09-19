package fecscheduleab

import "testing"

func TestMatcherClassifiesCandidatesWithoutMergingAmounts(t *testing.T) {
	a := []flowObservation{
		{SourceID: 1, RecipientID: 2, DateDays: 10, Amount: 100},
		{SourceID: 3, RecipientID: 4, DateDays: 20, Amount: 200},
		{SourceID: 5, RecipientID: 6, DateDays: 30, Amount: 300},
		{SourceID: 7, RecipientID: 8, DateDays: 40, Amount: 400},
		{SourceID: 9, RecipientID: 10, DateDays: 50, Amount: 500},
	}
	matcher, err := newMatcher(a)
	if err != nil {
		t.Fatal(err)
	}
	b := []flowObservation{
		{SourceID: 1, RecipientID: 2, DateDays: 10, Amount: 100},
		{SourceID: 3, RecipientID: 4, DateDays: 21, Amount: 200},
		{SourceID: 5, RecipientID: 6, DateDays: 30, Amount: 301},
		{SourceID: 7, RecipientID: 8, DateDays: 40, Amount: 400},
		{SourceID: 7, RecipientID: 8, DateDays: 40, Amount: 400},
		{SourceID: 9, RecipientID: 10, DateDays: 51, Amount: 501},
		{SourceID: 11, RecipientID: 12, DateDays: 60, Amount: 600},
	}
	for _, observation := range b {
		if err := matcher.observeB(observation); err != nil {
			t.Fatal(err)
		}
	}
	matcher.observeIneligibleB()
	states, err := matcher.candidateStates()
	if err != nil {
		t.Fatal(err)
	}
	byState := make(map[string]CandidateState)
	for _, state := range states {
		byState[state.State] = state
	}
	assertState := func(name string, groups, aRows, bRows uint64, aAmount, bAmount string) {
		t.Helper()
		got := byState[name]
		if got.SignatureGroups != groups || got.ScheduleARows != aRows || got.ScheduleBRows != bRows ||
			got.ScheduleAAmountMinorUnits != aAmount || got.ScheduleBAmountMinorUnits != bAmount {
			t.Fatalf("state %s = %+v", name, got)
		}
	}
	assertState(stateExact, 1, 1, 1, "100", "100")
	assertState(stateCompatible, 1, 1, 1, "200", "200")
	assertState(stateConflict, 1, 1, 1, "300", "301")
	assertState(stateAmbiguous, 1, 1, 2, "400", "800")
	assertState(stateAOnly, 1, 1, 0, "500", "0")

	dispositions := make(map[string]uint64)
	for _, value := range matcher.dispositions() {
		dispositions[value.Name] = value.Rows
	}
	if dispositions[bDispositionExact] != 3 || dispositions[bDispositionCompatible] != 1 ||
		dispositions[bDispositionConflict] != 1 || dispositions[bDispositionUnmatched] != 1 ||
		dispositions[bDispositionOutside] != 1 || dispositions[bDispositionIneligible] != 1 {
		t.Fatalf("dispositions = %+v", dispositions)
	}
}

func TestMatcherDoesNotRelaxAmbiguousScheduleAKeys(t *testing.T) {
	matcher, err := newMatcher([]flowObservation{
		{SourceID: 1, RecipientID: 2, DateDays: 10, Amount: 100},
		{SourceID: 1, RecipientID: 2, DateDays: 11, Amount: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := matcher.observeB(flowObservation{SourceID: 1, RecipientID: 2, DateDays: 12, Amount: 100}); err != nil {
		t.Fatal(err)
	}
	dispositions := make(map[string]uint64)
	for _, value := range matcher.dispositions() {
		dispositions[value.Name] = value.Rows
	}
	if dispositions[bDispositionCompatible] != 0 || dispositions[bDispositionUnmatched] != 1 {
		t.Fatalf("ambiguous relaxed key was matched: %+v", dispositions)
	}
}

func TestMatcherConservesCandidatesAcrossMatchClasses(t *testing.T) {
	matcher, err := newMatcher([]flowObservation{
		{SourceID: 1, RecipientID: 2, DateDays: 10, Amount: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range []flowObservation{
		{SourceID: 1, RecipientID: 2, DateDays: 10, Amount: 100},
		{SourceID: 1, RecipientID: 2, DateDays: 11, Amount: 100},
	} {
		if err := matcher.observeB(observation); err != nil {
			t.Fatal(err)
		}
	}
	states, err := matcher.candidateStates()
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.State == stateAmbiguous {
			if state.SignatureGroups != 1 || state.ScheduleARows != 1 || state.ScheduleBRows != 2 ||
				state.ScheduleAAmountMinorUnits != "100" || state.ScheduleBAmountMinorUnits != "200" {
				t.Fatalf("ambiguous state = %+v", state)
			}
			return
		}
	}
	t.Fatal("missing ambiguous state")
}

func TestSourceParsing(t *testing.T) {
	if value, ok := parseCommitteeIDString("C00123456"); !ok || value != 123456 {
		t.Fatalf("committee ID = %d, %t", value, ok)
	}
	for _, value := range []string{"", "P00123456", "C123", "C00123X56"} {
		if _, ok := parseCommitteeIDString(value); ok {
			t.Fatalf("accepted committee ID %q", value)
		}
	}
	if value, ok := parseScheduleBCents([]byte("-42.50")); !ok || value != -4250 {
		t.Fatalf("cents = %d, %t", value, ok)
	}
	if _, ok := parseScheduleBCents([]byte("42")); ok {
		t.Fatal("accepted amount without contracted cents")
	}
	if value, ok := parseDateDays([]byte("1970-01-02 00:00:00")); !ok || value != 1 {
		t.Fatalf("date days = %d, %t", value, ok)
	}
}

func TestSourceAlignmentUsesPublisherDate(t *testing.T) {
	alignment, err := sourceAlignment("Sun, 30 Aug 2026 18:54:29 GMT", "2026-08-30T15:21:54Z")
	if err != nil {
		t.Fatal(err)
	}
	if !alignment.SamePublisherDate || alignment.ScheduleAPublisherDate != "2026-08-30" {
		t.Fatalf("alignment = %+v", alignment)
	}
	notAligned, err := sourceAlignment("Sun, 30 Aug 2026 18:54:29 GMT", "2026-08-31T15:21:54Z")
	if err != nil {
		t.Fatal(err)
	}
	if notAligned.SamePublisherDate {
		t.Fatalf("unexpected alignment = %+v", notAligned)
	}
}
