package candidateresolution

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestInterpretCandidateDecisionSeparatesReportedAndAlternativeEndpoints(t *testing.T) {
	t.Parallel()
	calculationSetID := digestParts("interpretations")
	candidateFacts := []string{digestParts("candidate-1"), digestParts("candidate-2")}
	tests := []struct {
		name                 string
		decision             Decision
		state                string
		reportedEndpoint     string
		alternativeEndpoint  string
		alternativeCandidate string
		safeDefaultCandidate string
	}{
		{
			name:     "confirmed reported endpoint",
			decision: fixtureDecision(calculationSetID, "confirmed-interpretation", "100", StateConfirmed, MethodReportedIDExactContext, "H4AA00001", "H4AA00001", candidateFacts[:1]),
			state:    InterpretationConfirmed, reportedEndpoint: ReportedEndpointConfirmed,
			alternativeEndpoint: AlternativeNone, safeDefaultCandidate: "H4AA00001",
		},
		{
			name:     "absent reported endpoint with unique inference",
			decision: fixtureDecision(calculationSetID, "inferred-interpretation", "200", StateResolved, MethodUniqueExactContext, "H4AA99999", "H4AA00001", candidateFacts[:1]),
			state:    InterpretationInferred, reportedEndpoint: ReportedEndpointAbsent,
			alternativeEndpoint: AlternativeUniqueExactContext, alternativeCandidate: "H4AA00001",
		},
		{
			name:     "present reported endpoint conflicts with unique alternative",
			decision: fixtureDecision(calculationSetID, "conflicting-interpretation", "300", StateResolved, MethodUniqueExactContext, "H4AA00002", "H4AA00001", candidateFacts[:1]),
			state:    InterpretationConflicting, reportedEndpoint: ReportedEndpointContextConflict,
			alternativeEndpoint: AlternativeUniqueExactContext, alternativeCandidate: "H4AA00001",
		},
		{
			name:     "unverified reported endpoint",
			decision: fixtureDecision(calculationSetID, "unverified-interpretation", "400", StateUnverified, MethodReportedIDUnverified, "H4AA00001", "H4AA00001", candidateFacts[:1]),
			state:    InterpretationUnverified, reportedEndpoint: ReportedEndpointUnverified,
			alternativeEndpoint: AlternativeNoExactContext,
		},
		{
			name:     "ambiguous alternatives",
			decision: fixtureDecision(calculationSetID, "ambiguous-interpretation", "500", StateAmbiguous, MethodMultipleExactContext, "H4AA99998", "", candidateFacts),
			state:    InterpretationAmbiguous, reportedEndpoint: ReportedEndpointAbsent,
			alternativeEndpoint: AlternativeMultipleExactContext,
		},
		{
			name:     "unresolved endpoint",
			decision: fixtureDecision(calculationSetID, "unresolved-interpretation", "600", StateUnresolved, MethodNoExactContext, "H4AA99997", "", []string{}),
			state:    InterpretationUnresolved, reportedEndpoint: ReportedEndpointAbsent,
			alternativeEndpoint: AlternativeNoExactContext,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := interpretCandidateDecision(test.decision, calculationSetID)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateCandidateInterpretation(actual); err != nil {
				t.Fatal(err)
			}
			if actual.State != test.state || actual.ReportedEndpointState != test.reportedEndpoint || actual.AlternativeEndpointState != test.alternativeEndpoint ||
				pointerValue(actual.AlternativeCandidateID) != test.alternativeCandidate || pointerValue(actual.SafeDefaultCandidateID) != test.safeDefaultCandidate {
				t.Fatalf("interpretation = %#v", actual)
			}
			if actual.ReportedCandidate != test.decision.ReportedCandidate || actual.AmountMinorUnits != test.decision.AmountMinorUnits || actual.SourceDecisionID != test.decision.DecisionID {
				t.Fatal("interpretation changed source evidence")
			}
		})
	}
}

func TestPublishCandidateInterpretationsConservesMoneyAndReplays(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storageRoot := t.TempDir()
	resolutionPath := publishResolutionFixture(t, ctx, storageRoot)
	publishedAt := time.Date(2026, 9, 20, 16, 0, 0, 0, time.UTC)

	manifest, err := PublishInterpretations(
		ctx,
		InterpretationPublishInput{CandidateResolutionManifestPath: resolutionPath},
		"candidate-interpretation-test-run",
		InterpretationPublishOptions{StorageRoot: storageRoot, Clock: func() time.Time { return publishedAt }},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantCounts := InterpretationCounts{SourceDecisions: 5, Confirmed: 1, Inferred: 1, Unverified: 1, Ambiguous: 1, Unresolved: 1, SafeDefaults: 1}
	if manifest.Counts != wantCounts {
		t.Fatalf("counts = %#v, want %#v", manifest.Counts, wantCounts)
	}
	wantAmounts := InterpretationAmounts{
		SourceMinorUnits: "125", ConfirmedMinorUnits: "100", InferredMinorUnits: "-25",
		ConflictingMinorUnits: "0", UnverifiedMinorUnits: "0", AmbiguousMinorUnits: "40",
		UnresolvedMinorUnits: "10", SafeDefaultMinorUnits: "100",
	}
	if manifest.Amounts != wantAmounts {
		t.Fatalf("amounts = %#v, want %#v", manifest.Amounts, wantAmounts)
	}
	values, err := readAggregateArtifact[CandidateInterpretation](ctx, storageRoot, manifest.Interpretations)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 5 || values[0].State != InterpretationConfirmed || values[1].State != InterpretationInferred {
		t.Fatalf("unexpected interpretations: %#v", values)
	}
	replayed, err := PublishInterpretations(
		ctx,
		InterpretationPublishInput{CandidateResolutionManifestPath: resolutionPath},
		"candidate-interpretation-replay-run",
		InterpretationPublishOptions{StorageRoot: storageRoot},
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.CalculationSetID != manifest.CalculationSetID || replayed.Interpretations != manifest.Interpretations || replayed.PublishedAt != publishedAt {
		t.Fatal("candidate interpretation replay did not reuse the immutable publication")
	}
	var visited uint64
	loaded, digest, err := WalkPublishedInterpretations(
		ctx, storageRoot,
		filepath.Join(storageRoot, interpretationCalculationBase(), "current", "2024.json"),
		func(value CandidateInterpretation) error {
			visited++
			if value.CalculationSetID != manifest.CalculationSetID {
				t.Fatal("walker returned a foreign interpretation")
			}
			return nil
		},
	)
	if err != nil || loaded.CalculationSetID != manifest.CalculationSetID || !validDigest(digest) || visited != manifest.Counts.SourceDecisions {
		t.Fatal(loaded.CalculationSetID, digest, visited, err)
	}
}
