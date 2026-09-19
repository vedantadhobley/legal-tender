package candidateresolution

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type candidateResolutionPolicyFixture struct {
	SchemaVersion  string `json:"schema_version"`
	Cycle          string `json:"cycle"`
	CandidateFacts []struct {
		FactID      string `json:"fact_id"`
		CandidateID string `json:"candidate_id"`
		Name        string `json:"name"`
		Office      string `json:"office"`
		State       string `json:"state"`
		District    string `json:"district"`
	} `json:"candidate_facts"`
	EffectiveFacts []struct {
		FactID              string  `json:"fact_id"`
		ReportedCandidateID string  `json:"reported_candidate_id"`
		Name                *string `json:"name"`
		Office              string  `json:"office"`
		State               string  `json:"state"`
		District            string  `json:"district"`
		AmountMinorUnits    string  `json:"amount_minor_units"`
	} `json:"effective_facts"`
	Expected struct {
		Counts    Counts  `json:"counts"`
		Amounts   Amounts `json:"amounts"`
		Decisions []struct {
			FactID              string  `json:"fact_id"`
			State               string  `json:"state"`
			Method              string  `json:"method"`
			ResolvedCandidateID *string `json:"resolved_candidate_id"`
		} `json:"decisions"`
	} `json:"expected"`
}

type resolvedAggregatePolicyFixture struct {
	SchemaVersion string `json:"schema_version"`
	Cycle         string `json:"cycle"`
	Decisions     []struct {
		DecisionID          string  `json:"decision_id"`
		SpenderCommitteeID  string  `json:"spender_committee_id"`
		ReportedCandidateID string  `json:"reported_candidate_id"`
		ResolvedCandidateID *string `json:"resolved_candidate_id"`
		SupportOppose       string  `json:"support_oppose"`
		State               string  `json:"state"`
		AmountMinorUnits    string  `json:"amount_minor_units"`
	} `json:"candidate_resolution_decisions"`
	Expected struct {
		Counts  AggregateCounts  `json:"counts"`
		Amounts AggregateAmounts `json:"amounts"`
		Results []struct {
			SpenderCommitteeID     string                     `json:"spender_committee_id"`
			CandidateID            string                     `json:"candidate_id"`
			SupportOppose          string                     `json:"support_oppose"`
			SignedAmountMinorUnits string                     `json:"signed_amount_minor_units"`
			ExpenditureCount       uint64                     `json:"expenditure_count"`
			PositiveCount          uint64                     `json:"positive_count"`
			NegativeCount          uint64                     `json:"negative_count"`
			ZeroCount              uint64                     `json:"zero_count"`
			ResolutionCounts       AggregateResolutionCounts  `json:"resolution_counts"`
			ResolutionAmounts      AggregateResolutionAmounts `json:"resolution_amounts"`
		} `json:"results"`
		ExceptionDecisionIDs []string `json:"exception_decision_ids"`
	} `json:"expected"`
}

func TestCandidateResolutionContractPolicyFixture(t *testing.T) {
	t.Parallel()
	var fixture candidateResolutionPolicyFixture
	readPolicyFixture(t, "independent-expenditure-candidate-resolution", &fixture)
	if fixture.SchemaVersion != "legal-tender.fec.independent-expenditure-candidate-resolution-policy-fixture.v1" {
		t.Fatalf("unexpected fixture schema %q", fixture.SchemaVersion)
	}

	index := newCandidateIndex()
	for _, fact := range fixture.CandidateFacts {
		if !index.add(fecoccurrence.ClassicFact{FactID: fact.FactID}, fecoccurrence.CandidateTypedFields{
			CandidateID: fact.CandidateID, Name: fact.Name, Office: fact.Office,
			OfficeState: fact.State, OfficeDistrict: fact.District,
		}) {
			t.Fatalf("fixture candidate %s has unusable context", fact.CandidateID)
		}
	}

	expected := make(map[string]struct {
		state, method string
		resolved      *string
	}, len(fixture.Expected.Decisions))
	for _, decision := range fixture.Expected.Decisions {
		expected[decision.FactID] = struct {
			state, method string
			resolved      *string
		}{decision.State, decision.Method, decision.ResolvedCandidateID}
	}
	actualCounts := Counts{SourceEffectiveFacts: uint64(len(fixture.EffectiveFacts))}
	amounts := map[string]*big.Int{
		"source": new(big.Int), StateConfirmed: new(big.Int), StateResolved: new(big.Int),
		StateUnverified: new(big.Int), StateAmbiguous: new(big.Int), StateUnresolved: new(big.Int),
	}
	for _, fact := range fixture.EffectiveFacts {
		candidateID := fact.ReportedCandidateID
		office, state, district := fact.Office, fact.State, fact.District
		actual := index.resolve(fecoccurrence.ScheduleECandidateFields{
			CandidateID: &candidateID, Name: fact.Name, OfficeCode: &office,
			OfficeState: &state, OfficeDistrict: &district,
		})
		want, ok := expected[fact.FactID]
		if !ok {
			t.Fatalf("fixture has no expected decision for %s", fact.FactID)
		}
		if actual.state != want.state || actual.method != want.method || !reflect.DeepEqual(actual.resolvedCandidateID, want.resolved) {
			t.Fatalf("%s = %s/%s/%v; want %s/%s/%v", fact.FactID, actual.state, actual.method, actual.resolvedCandidateID, want.state, want.method, want.resolved)
		}
		amount, err := canonicalMinorUnits(fact.AmountMinorUnits)
		if err != nil {
			t.Fatalf("%s amount: %v", fact.FactID, err)
		}
		amounts["source"].Add(amounts["source"], amount)
		amounts[actual.state].Add(amounts[actual.state], amount)
		switch actual.state {
		case StateConfirmed:
			actualCounts.Confirmed++
		case StateResolved:
			actualCounts.Resolved++
		case StateUnverified:
			actualCounts.Unverified++
		case StateAmbiguous:
			actualCounts.Ambiguous++
		case StateUnresolved:
			actualCounts.Unresolved++
		default:
			t.Fatalf("%s produced unknown state %q", fact.FactID, actual.state)
		}
	}
	if actualCounts.SourceEffectiveFacts != fixture.Expected.Counts.SourceEffectiveFacts ||
		actualCounts.Confirmed != fixture.Expected.Counts.Confirmed || actualCounts.Resolved != fixture.Expected.Counts.Resolved ||
		actualCounts.Unverified != fixture.Expected.Counts.Unverified || actualCounts.Ambiguous != fixture.Expected.Counts.Ambiguous ||
		actualCounts.Unresolved != fixture.Expected.Counts.Unresolved {
		t.Fatalf("runtime counts = %+v; contract fixture wants %+v", actualCounts, fixture.Expected.Counts)
	}
	actualAmounts := Amounts{
		SourceEffectiveMinorUnits: amounts["source"].String(), ConfirmedMinorUnits: amounts[StateConfirmed].String(),
		ResolvedMinorUnits: amounts[StateResolved].String(), UnverifiedMinorUnits: amounts[StateUnverified].String(),
		AmbiguousMinorUnits: amounts[StateAmbiguous].String(), UnresolvedMinorUnits: amounts[StateUnresolved].String(),
	}
	if actualAmounts != fixture.Expected.Amounts {
		t.Fatalf("runtime amounts = %+v; contract fixture wants %+v", actualAmounts, fixture.Expected.Amounts)
	}
}

func TestResolvedAggregateContractPolicyFixture(t *testing.T) {
	t.Parallel()
	var fixture resolvedAggregatePolicyFixture
	readPolicyFixture(t, "resolved-independent-expenditures", &fixture)
	if fixture.SchemaVersion != "legal-tender.fec.resolved-independent-expenditure-policy-fixture.v1" {
		t.Fatalf("unexpected fixture schema %q", fixture.SchemaVersion)
	}

	ctx := context.Background()
	storageRoot := t.TempDir()
	writer, err := storageartifact.NewWriter(ctx, storageRoot, filepath.Join(storageRoot, "staging"), aggregateCalculationBase(), "exceptions")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	calculationSetID := digestParts("aggregate-policy-fixture")
	scan := aggregateScan{groups: make(map[aggregateKey]*aggregateAccumulator)}
	labelsByDecisionID := make(map[string]string, len(fixture.Decisions))
	for _, item := range fixture.Decisions {
		decision := aggregateFixtureDecision(calculationSetID, fixture.Cycle, item.DecisionID, item.SpenderCommitteeID,
			item.ReportedCandidateID, item.ResolvedCandidateID, item.SupportOppose, item.State, item.AmountMinorUnits)
		if err := validateDecision(decision); err != nil {
			t.Fatalf("%s decision is invalid: %v", item.DecisionID, err)
		}
		amount, err := canonicalMinorUnits(item.AmountMinorUnits)
		if err != nil {
			t.Fatal(err)
		}
		if err := scan.addDecision(decision, amount, calculationSetID, writer); err != nil {
			t.Fatal(err)
		}
		labelsByDecisionID[decision.DecisionID] = item.DecisionID
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	scan.counts.ResultGroups = uint64(len(scan.groups))
	if scan.counts != fixture.Expected.Counts {
		t.Fatalf("runtime counts = %+v; contract fixture wants %+v", scan.counts, fixture.Expected.Counts)
	}
	actualAmounts := AggregateAmounts{
		SourceMinorUnits: scan.stateAmounts.source.String(), ProjectableMinorUnits: scan.projectable.String(),
		UnprojectableMinorUnits: scan.unprojectable.String(), ConfirmedMinorUnits: scan.stateAmounts.confirmed.String(),
		ResolvedMinorUnits: scan.stateAmounts.resolved.String(), UnverifiedMinorUnits: scan.stateAmounts.unverified.String(),
		AmbiguousMinorUnits: scan.stateAmounts.ambiguous.String(), UnresolvedMinorUnits: scan.stateAmounts.unresolved.String(),
	}
	if actualAmounts != fixture.Expected.Amounts {
		t.Fatalf("runtime amounts = %+v; contract fixture wants %+v", actualAmounts, fixture.Expected.Amounts)
	}
	keys := sortedAggregateKeys(scan.groups)
	if len(keys) != len(fixture.Expected.Results) {
		t.Fatalf("runtime groups = %d; contract fixture wants %d", len(keys), len(fixture.Expected.Results))
	}
	for index, key := range keys {
		actual := scan.groups[key]
		want := fixture.Expected.Results[index]
		if key.spender != want.SpenderCommitteeID || key.candidate != want.CandidateID || key.stance != want.SupportOppose ||
			actual.amount.String() != want.SignedAmountMinorUnits || actual.count != want.ExpenditureCount ||
			actual.positive != want.PositiveCount || actual.negative != want.NegativeCount || actual.zero != want.ZeroCount ||
			actual.counts != want.ResolutionCounts ||
			(AggregateResolutionAmounts{ConfirmedMinorUnits: actual.confirmed.String(), ResolvedMinorUnits: actual.resolved.String(), UnverifiedMinorUnits: actual.unverified.String()}) != want.ResolutionAmounts {
			t.Fatalf("runtime group %d does not match contract fixture: key=%+v value=%+v want=%+v", index, key, actual, want)
		}
	}
	exceptions, err := readAggregateArtifact[AggregateException](ctx, storageRoot, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	actualExceptionLabels := make([]string, 0, len(exceptions))
	for _, exception := range exceptions {
		actualExceptionLabels = append(actualExceptionLabels, labelsByDecisionID[exception.ResolutionDecisionID])
	}
	sort.Strings(actualExceptionLabels)
	wantExceptionLabels := append([]string(nil), fixture.Expected.ExceptionDecisionIDs...)
	sort.Strings(wantExceptionLabels)
	if !reflect.DeepEqual(actualExceptionLabels, wantExceptionLabels) {
		t.Fatalf("runtime exceptions = %v; contract fixture wants %v", actualExceptionLabels, wantExceptionLabels)
	}
}

func aggregateFixtureDecision(calculationSetID, cycle, label, spender, reportedID string, resolvedID *string, stance, state, amount string) Decision {
	methods := map[string]string{
		StateConfirmed: MethodReportedIDExactContext, StateResolved: MethodUniqueExactContext,
		StateUnverified: MethodReportedIDUnverified, StateAmbiguous: MethodMultipleExactContext,
		StateUnresolved: MethodNoExactContext,
	}
	candidateFactIDs := []string{}
	switch state {
	case StateAmbiguous:
		candidateFactIDs = []string{digestParts("candidate-1"), digestParts("candidate-2")}
	case StateConfirmed, StateResolved, StateUnverified:
		candidateFactIDs = []string{digestParts("candidate-1")}
	}
	sort.Strings(candidateFactIDs)
	decision := Decision{
		SchemaVersion: DecisionSchemaVersion, CalculationSetID: calculationSetID,
		FactID: digestParts("policy-fact-" + label), NaturalKey: "fec:schedule-e:" + cycle + ":" + label,
		Cycle: cycle, SpenderCommitteeID: spender, SupportOppose: stance, AmountMinorUnits: amount,
		ReportedCandidate: ReportedCandidate{CandidateID: reportedID}, State: state, Method: methods[state],
		ResolvedCandidateID: resolvedID, CandidateFactIDs: candidateFactIDs, EvidenceCodes: []string{"policy_fixture_" + label},
	}
	decision.DecisionID = digestParts(
		"fec.independent-expenditure-candidate-resolution.v1", decision.CalculationSetID, decision.FactID,
		decision.State, decision.Method, pointerValue(decision.ResolvedCandidateID),
		strings.Join(decision.CandidateFactIDs, ","), strings.Join(decision.EvidenceCodes, ","),
	)
	return decision
}

func readPolicyFixture(t *testing.T, contract string, target any) {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve policy fixture test source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../../.."))
	path := filepath.Join(root, "contracts", "calculations", "fec", contract, "v1", "fixtures", "policy-cases.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open policy fixture %s: %v", path, err)
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode policy fixture %s: %v", path, err)
	}
}
