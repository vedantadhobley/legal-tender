package candidateresolution

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestPublishAggregateConservesResolutionStatesAndAmounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storageRoot := t.TempDir()
	resolutionPath := publishResolutionFixture(t, ctx, storageRoot)
	publishedAt := time.Date(2026, 8, 31, 14, 0, 0, 0, time.UTC)

	manifest, err := PublishAggregate(
		ctx,
		AggregatePublishInput{CandidateResolutionManifestPath: resolutionPath},
		"aggregate-test-run",
		AggregatePublishOptions{StorageRoot: storageRoot, Clock: func() time.Time { return publishedAt }},
	)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Counts.SourceDecisions != 5 || manifest.Counts.ProjectableDecisions != 3 ||
		manifest.Counts.UnprojectableDecisions != 2 || manifest.Counts.ResultGroups != 1 || manifest.Counts.Exceptions != 2 {
		t.Fatalf("unexpected counts: %#v", manifest.Counts)
	}
	if manifest.Amounts.SourceMinorUnits != "125" || manifest.Amounts.ProjectableMinorUnits != "75" ||
		manifest.Amounts.UnprojectableMinorUnits != "50" {
		t.Fatalf("unexpected amounts: %#v", manifest.Amounts)
	}

	results, err := readAggregateArtifact[AggregateResult](ctx, storageRoot, manifest.Results)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("result count = %d, want 1", len(results))
	}
	result := results[0]
	if result.CandidateID != "H4AA00001" || result.SignedAmountMinorUnits != "75" || result.ExpenditureCount != 3 ||
		result.PositiveCount != 1 || result.NegativeCount != 1 || result.ZeroCount != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.ResolutionCounts != (AggregateResolutionCounts{Confirmed: 1, Resolved: 1, Unverified: 1}) ||
		result.ResolutionAmounts != (AggregateResolutionAmounts{ConfirmedMinorUnits: "100", ResolvedMinorUnits: "-25", UnverifiedMinorUnits: "0"}) {
		t.Fatalf("unexpected resolution breakdown: %#v %#v", result.ResolutionCounts, result.ResolutionAmounts)
	}

	exceptions, err := readAggregateArtifact[AggregateException](ctx, storageRoot, manifest.Exceptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(exceptions) != 2 || exceptions[0].State != StateAmbiguous || exceptions[1].State != StateUnresolved {
		t.Fatalf("unexpected exceptions: %#v", exceptions)
	}

	replayed, err := PublishAggregate(
		ctx,
		AggregatePublishInput{CandidateResolutionManifestPath: resolutionPath},
		"aggregate-replay-run",
		AggregatePublishOptions{StorageRoot: storageRoot},
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.CalculationSetID != manifest.CalculationSetID || replayed.Results != manifest.Results || replayed.Exceptions != manifest.Exceptions {
		t.Fatal("aggregate replay did not reuse the immutable publication")
	}
}

func publishResolutionFixture(t *testing.T, ctx context.Context, storageRoot string) string {
	t.Helper()
	calculationReference := CalculationReference{
		Role: "effective_independent_expenditures", Calculation: "fec/effective-independent-expenditures",
		CalculationVersion: "1.0.0", CalculationSetID: digestParts("effective-calculation"),
		ManifestSHA256: digestParts("effective-manifest"), ScheduleEFactSetID: digestParts("schedule-e-facts"),
		ScheduleEManifestSHA256: digestParts("schedule-e-manifest"),
	}
	candidateReference := CandidateFactSetReference{
		Role: "cycle_candidate_master", Dataset: "candidate-master", FactType: "fec.candidate_assertion.v1",
		FactSetID: digestParts("candidate-facts"), ManifestSHA256: digestParts("candidate-manifest"),
	}
	calculationSetID := digestParts(
		ManifestSchemaVersion, ContractVersion, DecisionSchemaVersion, MethodVersion, PublisherVersion,
		calculationReference.CalculationSetID, calculationReference.ManifestSHA256,
		calculationReference.ScheduleEFactSetID, calculationReference.ScheduleEManifestSHA256,
		candidateReference.FactSetID, candidateReference.ManifestSHA256,
	)
	decisionDirectory := filepath.Join(storageRoot, "fixture-staging")
	writer, err := storageartifact.NewWriter(ctx, storageRoot, decisionDirectory, calculationBase(), "decisions")
	if err != nil {
		t.Fatal(err)
	}
	candidateFacts := []string{digestParts("candidate-1"), digestParts("candidate-2")}
	sort.Strings(candidateFacts)
	decisions := []Decision{
		fixtureDecision(calculationSetID, "confirmed", "100", StateConfirmed, MethodReportedIDExactContext, "H4AA00001", "H4AA00001", []string{candidateFacts[0]}),
		fixtureDecision(calculationSetID, "resolved", "-25", StateResolved, MethodUniqueExactContext, "H4AA99999", "H4AA00001", []string{candidateFacts[0]}),
		fixtureDecision(calculationSetID, "unverified", "0", StateUnverified, MethodReportedIDUnverified, "H4AA00001", "H4AA00001", []string{candidateFacts[0]}),
		fixtureDecision(calculationSetID, "ambiguous", "40", StateAmbiguous, MethodMultipleExactContext, "H4AA99998", "", candidateFacts),
		fixtureDecision(calculationSetID, "unresolved", "10", StateUnresolved, MethodNoExactContext, "H4AA99997", "", []string{}),
	}
	for _, decision := range decisions {
		if err := writer.WriteJSON(decision); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{
		Schema: "manifest.schema.json", SchemaVersion: ManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID, CalculationVersion: ContractVersion,
		PublisherVersion: PublisherVersion, DecisionSchemaVersion: DecisionSchemaVersion,
		Cycle: "2024", SourceReleaseID: "fec-" + digestParts("release"),
		InputCalculation: calculationReference, InputCandidateFactSet: candidateReference,
		RunID: "resolution-fixture-run", State: "published",
		PublishedAt: time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC), Method: resolutionMethod(),
		Counts: Counts{
			SourceEffectiveFacts: 5, CandidateFacts: 2, UsableCandidateFacts: 2,
			Confirmed: 1, Resolved: 1, Unverified: 1, Ambiguous: 1, Unresolved: 1,
		},
		Amounts: Amounts{
			SourceEffectiveMinorUnits: "125", ConfirmedMinorUnits: "100", ResolvedMinorUnits: "-25",
			UnverifiedMinorUnits: "0", AmbiguousMinorUnits: "40", UnresolvedMinorUnits: "10",
		},
		Decisions: descriptor,
		Checks: []Check{
			{ID: "input_lineage", Passed: true, Severity: "block", Detail: "fixture"},
			{ID: "cycle_release_coherence", Passed: true, Severity: "block", Detail: "fixture"},
			{ID: "effective_membership_replay", Passed: true, Severity: "block", Detail: "fixture"},
			{ID: "decision_conservation", Passed: true, Severity: "block", Detail: "fixture"},
			{ID: "signed_conservation", Passed: true, Severity: "block", Detail: "fixture"},
			{ID: "no_silent_resolution", Passed: true, Severity: "block", Detail: "fixture"},
			{ID: "unresolved_preservation", Passed: true, Severity: "block", Detail: "fixture"},
		},
	}
	if err := validateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	immutable := filepath.Join(storageRoot, calculationBase(), "manifests", calculationSetID+".json")
	current := filepath.Join(storageRoot, calculationBase(), "current", "2024.json")
	if err := writeAtomicJSON(immutable, manifest); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomicJSON(current, manifest); err != nil {
		t.Fatal(err)
	}
	return current
}

func fixtureDecision(calculationSetID, label, amount, state, method, reportedID, resolvedID string, candidateFactIDs []string) Decision {
	evidenceCodes := []string{"fixture_" + label}
	switch state {
	case StateConfirmed:
		evidenceCodes = append(evidenceCodes, "reported_id_present_in_candidate_master", "exact_name_office_context")
	case StateResolved:
		evidenceCodes = append(evidenceCodes, "unique_exact_name_office_context")
		if strings.HasPrefix(reportedID, "H4AA999") {
			evidenceCodes = append(evidenceCodes, "reported_id_absent_from_candidate_master")
		} else {
			evidenceCodes = append(evidenceCodes, "reported_id_context_conflict")
		}
	case StateUnverified:
		evidenceCodes = append(evidenceCodes, "reported_id_present_in_candidate_master", "no_exact_name_office_candidate")
	case StateAmbiguous:
		evidenceCodes = append(evidenceCodes, "reported_id_absent_from_candidate_master", "multiple_exact_name_office_candidates")
	case StateUnresolved:
		evidenceCodes = append(evidenceCodes, "reported_id_absent_from_candidate_master", "no_exact_name_office_candidate")
	}
	decision := Decision{
		SchemaVersion: DecisionSchemaVersion, CalculationSetID: calculationSetID,
		FactID: digestParts("fact-" + label), NaturalKey: "fec:schedule-e:2024:" + label,
		Cycle: "2024", SpenderCommitteeID: "C00000001", SupportOppose: "S", AmountMinorUnits: amount,
		ReportedCandidate: ReportedCandidate{CandidateID: reportedID}, State: state, Method: method,
		CandidateFactIDs: candidateFactIDs, EvidenceCodes: evidenceCodes,
	}
	if resolvedID != "" {
		decision.ResolvedCandidateID = &resolvedID
	}
	decision.DecisionID = digestParts(
		"fec.independent-expenditure-candidate-resolution.v1", decision.CalculationSetID, decision.FactID,
		decision.State, decision.Method, pointerValue(decision.ResolvedCandidateID),
		strings.Join(decision.CandidateFactIDs, ","), strings.Join(decision.EvidenceCodes, ","),
	)
	return decision
}

func readAggregateArtifact[T any](ctx context.Context, storageRoot string, descriptor storageartifact.Descriptor) ([]T, error) {
	reader, err := storageartifact.Open[T](ctx, storageRoot, descriptor)
	if err != nil {
		return nil, err
	}
	defer reader.Abort()
	var values []T
	for {
		value, ok, err := reader.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		values = append(values, value)
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return values, nil
}
