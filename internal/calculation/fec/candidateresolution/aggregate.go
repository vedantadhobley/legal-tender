package candidateresolution

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type aggregateKey struct {
	spender   string
	candidate string
	stance    string
}

type aggregateAccumulator struct {
	amount     big.Int
	count      uint64
	positive   uint64
	negative   uint64
	zero       uint64
	counts     AggregateResolutionCounts
	confirmed  big.Int
	resolved   big.Int
	unverified big.Int
}

type aggregateScan struct {
	counts        AggregateCounts
	stateAmounts  amountState
	projectable   big.Int
	unprojectable big.Int
	groups        map[aggregateKey]*aggregateAccumulator
}

// LoadPublishedAggregateManifest resolves one exact grouped calculation and
// verifies its immutable manifest and both backing artifacts.
func LoadPublishedAggregateManifest(ctx context.Context, storageRoot, path string) (AggregateManifest, string, error) {
	if storageRoot == "" || path == "" {
		return AggregateManifest{}, "", fmt.Errorf("storage root and resolved independent-expenditure manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return AggregateManifest{}, "", err
	}
	pointer, _, err := readStrictJSON[AggregateManifest](path)
	if err != nil {
		return AggregateManifest{}, "", err
	}
	if err := validateAggregateManifest(pointer); err != nil {
		return AggregateManifest{}, "", err
	}
	immutablePath := filepath.Join(storageRoot, aggregateCalculationBase(), "manifests", pointer.CalculationSetID+".json")
	immutable, digest, err := readStrictJSON[AggregateManifest](immutablePath)
	if err != nil {
		return AggregateManifest{}, "", err
	}
	if !reflect.DeepEqual(pointer, immutable) {
		return AggregateManifest{}, "", fmt.Errorf("resolved independent-expenditure pointer differs from immutable manifest")
	}
	if err := validateAggregateManifestBacking(ctx, storageRoot, immutable); err != nil {
		return AggregateManifest{}, "", err
	}
	return immutable, digest, nil
}

// PublishAggregate groups projectable candidate decisions by spender,
// resolved candidate, and stance. Ambiguous and unresolved decisions remain
// exact sparse exceptions and never become candidate edges.
func PublishAggregate(ctx context.Context, input AggregatePublishInput, runID string, options AggregatePublishOptions) (AggregateManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.CandidateResolutionManifestPath == "" {
		return AggregateManifest{}, fmt.Errorf("storage root and candidate-resolution manifest are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return AggregateManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}

	resolution, resolutionDigest, err := LoadPublishedManifest(ctx, options.StorageRoot, input.CandidateResolutionManifestPath)
	if err != nil {
		return AggregateManifest{}, fmt.Errorf("load candidate-resolution calculation: %w", err)
	}
	if resolution.Method.Version != MethodVersion {
		return AggregateManifest{}, fmt.Errorf("candidate-resolution method %s is not projectable", resolution.Method.Version)
	}
	reference := AggregateInputReference{
		Role: "independent_expenditure_candidate_resolution", Calculation: resolution.Calculation,
		CalculationVersion: resolution.CalculationVersion, CalculationSetID: resolution.CalculationSetID,
		ManifestSHA256: resolutionDigest, DecisionSchemaVersion: resolution.DecisionSchemaVersion,
		DecisionsSHA256: resolution.Decisions.CompressedSHA256,
	}
	calculationSetID := digestParts(
		AggregateManifestSchemaVersion, AggregateContractVersion, AggregateResultSchemaVersion,
		AggregateExceptionSchemaVersion, AggregateGroupingPolicyVersion, AggregatePublisherVersion,
		resolution.Cycle, resolution.SourceReleaseID, reference.CalculationSetID,
		reference.ManifestSHA256, reference.DecisionSchemaVersion, reference.DecisionsSHA256,
	)
	basePath := aggregateCalculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", resolution.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return AggregateManifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+resolution.Cycle+".lock"))
	if err != nil {
		return AggregateManifest{}, err
	}
	defer unlock()
	current, err := readAggregateManifestIfPresent(currentPath)
	if err != nil {
		return AggregateManifest{}, err
	}
	if current != nil {
		if err := validateAggregateManifest(*current); err != nil {
			return AggregateManifest{}, fmt.Errorf("invalid current resolved independent-expenditure calculation: %w", err)
		}
		if err := validateAggregateManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return AggregateManifest{}, err
		}
		if current.Cycle != resolution.Cycle {
			return AggregateManifest{}, fmt.Errorf("current resolved independent expenditures belong to cycle %s", current.Cycle)
		}
		if current.CalculationSetID == calculationSetID {
			return *current, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", calculationSetID+".json")
	if existing, readErr := readAggregateManifestIfPresent(manifestPath); readErr != nil {
		return AggregateManifest{}, readErr
	} else if existing != nil {
		if err := validateAggregateManifest(*existing); err != nil {
			return AggregateManifest{}, err
		}
		if existing.InputResolution != reference {
			return AggregateManifest{}, fmt.Errorf("immutable resolved independent-expenditure manifest collision")
		}
		if err := validateAggregateManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return AggregateManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return AggregateManifest{}, err
		}
		return *existing, nil
	}

	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return AggregateManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	exceptionWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "exceptions")
	if err != nil {
		return AggregateManifest{}, err
	}
	defer exceptionWriter.Abort()
	if options.Progress != nil {
		options.Progress("grouping resolved independent expenditures for " + resolution.Cycle)
	}
	scan, err := scanResolutionDecisions(ctx, options.StorageRoot, resolution, calculationSetID, exceptionWriter, options.Progress)
	if err != nil {
		return AggregateManifest{}, err
	}
	exceptionArtifact, err := exceptionWriter.Finalize()
	if err != nil {
		return AggregateManifest{}, err
	}
	if exceptionArtifact.RecordCount != scan.counts.UnprojectableDecisions || exceptionArtifact.RecordCount != scan.counts.Exceptions {
		return AggregateManifest{}, fmt.Errorf("resolved independent-expenditure exceptions are not conserved")
	}

	resultWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "results")
	if err != nil {
		return AggregateManifest{}, err
	}
	defer resultWriter.Abort()
	var resultRows uint64
	var resultAmount big.Int
	for _, key := range sortedAggregateKeys(scan.groups) {
		accumulator := scan.groups[key]
		result := AggregateResult{
			SchemaVersion: AggregateResultSchemaVersion,
			ResultID: digestParts(
				"fec.resolved-independent-expenditure.result.v1", calculationSetID,
				resolution.Cycle, key.spender, key.candidate, key.stance,
			),
			CalculationSetID: calculationSetID, Cycle: resolution.Cycle,
			SpenderCommitteeID: key.spender, CandidateID: key.candidate, SupportOppose: key.stance,
			SignedAmountMinorUnits: accumulator.amount.String(), ExpenditureCount: accumulator.count,
			PositiveCount: accumulator.positive, NegativeCount: accumulator.negative, ZeroCount: accumulator.zero,
			ResolutionCounts: accumulator.counts,
			ResolutionAmounts: AggregateResolutionAmounts{
				ConfirmedMinorUnits: accumulator.confirmed.String(), ResolvedMinorUnits: accumulator.resolved.String(),
				UnverifiedMinorUnits: accumulator.unverified.String(),
			},
		}
		if err := validateAggregateResult(result); err != nil {
			return AggregateManifest{}, err
		}
		if err := resultWriter.WriteJSON(result); err != nil {
			return AggregateManifest{}, err
		}
		resultRows += accumulator.count
		resultAmount.Add(&resultAmount, &accumulator.amount)
	}
	resultArtifact, err := resultWriter.Finalize()
	if err != nil {
		return AggregateManifest{}, err
	}
	scan.counts.ResultGroups = uint64(len(scan.groups))
	if resultArtifact.RecordCount != scan.counts.ResultGroups || resultRows != scan.counts.ProjectableDecisions || resultAmount.Cmp(&scan.projectable) != 0 {
		return AggregateManifest{}, fmt.Errorf("resolved independent-expenditure results are not conserved")
	}

	manifest := AggregateManifest{
		Schema: "manifest.schema.json", SchemaVersion: AggregateManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: AggregateContractID, CalculationVersion: AggregateContractVersion,
		PublisherVersion: AggregatePublisherVersion, ResultSchemaVersion: AggregateResultSchemaVersion,
		ExceptionSchemaVersion: AggregateExceptionSchemaVersion, Cycle: resolution.Cycle,
		SourceReleaseID: resolution.SourceReleaseID, InputResolution: reference,
		RunID: runID, State: "published", PublishedAt: options.Clock().UTC(), GroupingPolicy: aggregateGroupingPolicy(),
		Counts: scan.counts,
		Amounts: AggregateAmounts{
			SourceMinorUnits: scan.stateAmounts.source.String(), ProjectableMinorUnits: scan.projectable.String(),
			UnprojectableMinorUnits: scan.unprojectable.String(), ConfirmedMinorUnits: scan.stateAmounts.confirmed.String(),
			ResolvedMinorUnits: scan.stateAmounts.resolved.String(), UnverifiedMinorUnits: scan.stateAmounts.unverified.String(),
			AmbiguousMinorUnits: scan.stateAmounts.ambiguous.String(), UnresolvedMinorUnits: scan.stateAmounts.unresolved.String(),
		},
		Results: resultArtifact, Exceptions: exceptionArtifact,
	}
	manifest.Checks = []Check{
		{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the exact candidate-resolution manifest and complete decision artifact passed immutable backing verification"},
		{ID: "current_resolution_method", Passed: resolution.Method.Version == MethodVersion, Severity: "block", Detail: "the aggregate accepts only the current candidate-resolution method"},
		{ID: "decision_conservation", Passed: aggregateStateCount(manifest.Counts) == manifest.Counts.SourceDecisions, Severity: "block", Detail: "every candidate-resolution decision was replayed exactly once"},
		{ID: "route_conservation", Passed: manifest.Counts.SourceDecisions == manifest.Counts.ProjectableDecisions+manifest.Counts.UnprojectableDecisions, Severity: "block", Detail: "every decision lands in one candidate group or one sparse exception"},
		{ID: "signed_conservation", Passed: aggregateAmountsConserve(manifest.Amounts), Severity: "block", Detail: "exact signed cents conserve by route and candidate-resolution state"},
		{ID: "result_conservation", Passed: resultRows == manifest.Counts.ProjectableDecisions && resultArtifact.RecordCount == manifest.Counts.ResultGroups, Severity: "block", Detail: "every projectable decision contributes once to one resolved spender-candidate-stance group"},
		{ID: "exception_conservation", Passed: exceptionArtifact.RecordCount == manifest.Counts.UnprojectableDecisions, Severity: "block", Detail: "every ambiguous or unresolved decision remains one exact sparse exception outside candidate edges"},
		{ID: "quality_preservation", Passed: true, Severity: "block", Detail: "each result preserves confirmed, resolved, and unverified count and signed-amount components"},
	}
	if err := validateAggregateManifest(manifest); err != nil {
		return AggregateManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return AggregateManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return AggregateManifest{}, err
	}
	return manifest, nil
}

func scanResolutionDecisions(
	ctx context.Context,
	storageRoot string,
	resolution Manifest,
	calculationSetID string,
	exceptionWriter *storageartifact.Writer,
	progress func(string),
) (aggregateScan, error) {
	reader, err := storageartifact.Open[Decision](ctx, storageRoot, resolution.Decisions)
	if err != nil {
		return aggregateScan{}, err
	}
	defer reader.Abort()
	scan := aggregateScan{groups: make(map[aggregateKey]*aggregateAccumulator)}
	seen := make(map[string]struct{}, resolution.Counts.SourceEffectiveFacts)
	for {
		decision, ok, readErr := reader.Next()
		if readErr != nil {
			return aggregateScan{}, readErr
		}
		if !ok {
			break
		}
		if err := validateDecision(decision); err != nil {
			return aggregateScan{}, fmt.Errorf("candidate-resolution decision %d: %w", scan.counts.SourceDecisions+1, err)
		}
		if decision.CalculationSetID != resolution.CalculationSetID || decision.Cycle != resolution.Cycle {
			return aggregateScan{}, fmt.Errorf("candidate-resolution decision %s does not match its manifest", decision.DecisionID)
		}
		if _, duplicate := seen[decision.DecisionID]; duplicate {
			return aggregateScan{}, fmt.Errorf("duplicate candidate-resolution decision ID %s", decision.DecisionID)
		}
		seen[decision.DecisionID] = struct{}{}
		amount, err := canonicalMinorUnits(decision.AmountMinorUnits)
		if err != nil {
			return aggregateScan{}, fmt.Errorf("candidate-resolution decision %s: %w", decision.DecisionID, err)
		}
		if err := scan.addDecision(decision, amount, calculationSetID, exceptionWriter); err != nil {
			return aggregateScan{}, err
		}
		if progress != nil && scan.counts.SourceDecisions%50_000 == 0 {
			progress(fmt.Sprintf("grouped %d candidate-resolution decisions for %s", scan.counts.SourceDecisions, resolution.Cycle))
		}
	}
	if err := reader.Close(); err != nil {
		return aggregateScan{}, err
	}
	if scan.counts.SourceDecisions != resolution.Counts.SourceEffectiveFacts || uint64(len(seen)) != resolution.Counts.SourceEffectiveFacts {
		return aggregateScan{}, fmt.Errorf("candidate-resolution decision membership is not conserved")
	}
	if err := scan.matchesResolutionManifest(resolution); err != nil {
		return aggregateScan{}, err
	}
	return scan, nil
}

func (scan *aggregateScan) addDecision(decision Decision, amount *big.Int, calculationSetID string, exceptionWriter *storageartifact.Writer) error {
	scan.counts.SourceDecisions++
	scan.stateAmounts.source.Add(&scan.stateAmounts.source, amount)
	switch decision.State {
	case StateConfirmed:
		scan.counts.Confirmed++
	case StateResolved:
		scan.counts.Resolved++
	case StateUnverified:
		scan.counts.Unverified++
	case StateAmbiguous:
		scan.counts.Ambiguous++
	case StateUnresolved:
		scan.counts.Unresolved++
	}
	addResolutionAmount(&scan.stateAmounts, decision.State, amount)
	if decision.State == StateAmbiguous || decision.State == StateUnresolved {
		scan.counts.UnprojectableDecisions++
		scan.counts.Exceptions++
		scan.unprojectable.Add(&scan.unprojectable, amount)
		exception := newAggregateException(calculationSetID, decision)
		if err := validateAggregateException(exception); err != nil {
			return err
		}
		return exceptionWriter.WriteJSON(exception)
	}
	if decision.State != StateConfirmed && decision.State != StateResolved && decision.State != StateUnverified {
		return fmt.Errorf("candidate-resolution decision %s has unsupported state %s", decision.DecisionID, decision.State)
	}
	if decision.ResolvedCandidateID == nil || *decision.ResolvedCandidateID == "" {
		return fmt.Errorf("projectable candidate-resolution decision %s has no resolved candidate", decision.DecisionID)
	}
	scan.counts.ProjectableDecisions++
	scan.projectable.Add(&scan.projectable, amount)
	key := aggregateKey{
		spender: decision.SpenderCommitteeID, candidate: *decision.ResolvedCandidateID, stance: decision.SupportOppose,
	}
	accumulator := scan.groups[key]
	if accumulator == nil {
		accumulator = &aggregateAccumulator{}
		scan.groups[key] = accumulator
	}
	accumulator.amount.Add(&accumulator.amount, amount)
	accumulator.count++
	switch amount.Sign() {
	case -1:
		accumulator.negative++
	case 0:
		accumulator.zero++
	case 1:
		accumulator.positive++
	}
	switch decision.State {
	case StateConfirmed:
		accumulator.counts.Confirmed++
		accumulator.confirmed.Add(&accumulator.confirmed, amount)
	case StateResolved:
		accumulator.counts.Resolved++
		accumulator.resolved.Add(&accumulator.resolved, amount)
	case StateUnverified:
		accumulator.counts.Unverified++
		accumulator.unverified.Add(&accumulator.unverified, amount)
	}
	return nil
}

func (scan aggregateScan) matchesResolutionManifest(resolution Manifest) error {
	counts := resolution.Counts
	if scan.counts.Confirmed != counts.Confirmed || scan.counts.Resolved != counts.Resolved ||
		scan.counts.Unverified != counts.Unverified || scan.counts.Ambiguous != counts.Ambiguous ||
		scan.counts.Unresolved != counts.Unresolved {
		return fmt.Errorf("candidate-resolution state counts differ from their manifest")
	}
	want := resolution.Amounts
	got := AggregateAmounts{
		SourceMinorUnits: scan.stateAmounts.source.String(), ConfirmedMinorUnits: scan.stateAmounts.confirmed.String(),
		ResolvedMinorUnits: scan.stateAmounts.resolved.String(), UnverifiedMinorUnits: scan.stateAmounts.unverified.String(),
		AmbiguousMinorUnits: scan.stateAmounts.ambiguous.String(), UnresolvedMinorUnits: scan.stateAmounts.unresolved.String(),
	}
	if got.SourceMinorUnits != want.SourceEffectiveMinorUnits || got.ConfirmedMinorUnits != want.ConfirmedMinorUnits ||
		got.ResolvedMinorUnits != want.ResolvedMinorUnits || got.UnverifiedMinorUnits != want.UnverifiedMinorUnits ||
		got.AmbiguousMinorUnits != want.AmbiguousMinorUnits || got.UnresolvedMinorUnits != want.UnresolvedMinorUnits {
		return fmt.Errorf("candidate-resolution state amounts differ from their manifest")
	}
	return nil
}

func newAggregateException(calculationSetID string, decision Decision) AggregateException {
	return AggregateException{
		SchemaVersion: AggregateExceptionSchemaVersion,
		ExceptionID: digestParts(
			"fec.resolved-independent-expenditure.exception.v1", calculationSetID,
			decision.DecisionID, decision.State,
		),
		CalculationSetID: calculationSetID, ResolutionDecisionID: decision.DecisionID,
		FactID: decision.FactID, NaturalKey: decision.NaturalKey, Cycle: decision.Cycle,
		SpenderCommitteeID: decision.SpenderCommitteeID, ReportedCandidateID: decision.ReportedCandidate.CandidateID,
		SupportOppose: decision.SupportOppose, State: decision.State, Method: decision.Method,
		AmountMinorUnits: decision.AmountMinorUnits,
	}
}

func aggregateGroupingPolicy() AggregateGroupingPolicy {
	return AggregateGroupingPolicy{
		Version:         AggregateGroupingPolicyVersion,
		GroupBy:         []string{"spender_committee_id", "resolved_candidate_id", "support_oppose"},
		ProjectedStates: []string{StateConfirmed, StateResolved, StateUnverified},
		ExceptionStates: []string{StateAmbiguous, StateUnresolved},
	}
}

func sortedAggregateKeys(groups map[aggregateKey]*aggregateAccumulator) []aggregateKey {
	keys := make([]aggregateKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].spender != keys[right].spender {
			return keys[left].spender < keys[right].spender
		}
		if keys[left].candidate != keys[right].candidate {
			return keys[left].candidate < keys[right].candidate
		}
		return keys[left].stance < keys[right].stance
	})
	return keys
}

func canonicalMinorUnits(text string) (*big.Int, error) {
	value, ok := new(big.Int).SetString(text, 10)
	if !ok || value.String() != text {
		return nil, fmt.Errorf("amount minor units are not a canonical signed integer")
	}
	return value, nil
}

func aggregateStateCount(counts AggregateCounts) uint64 {
	return counts.Confirmed + counts.Resolved + counts.Unverified + counts.Ambiguous + counts.Unresolved
}

func aggregateAmountsConserve(amounts AggregateAmounts) bool {
	source, err := canonicalMinorUnits(amounts.SourceMinorUnits)
	if err != nil {
		return false
	}
	projectable, err := canonicalMinorUnits(amounts.ProjectableMinorUnits)
	if err != nil {
		return false
	}
	unprojectable, err := canonicalMinorUnits(amounts.UnprojectableMinorUnits)
	if err != nil {
		return false
	}
	if new(big.Int).Add(projectable, unprojectable).Cmp(source) != 0 {
		return false
	}
	var states big.Int
	for _, text := range []string{
		amounts.ConfirmedMinorUnits, amounts.ResolvedMinorUnits, amounts.UnverifiedMinorUnits,
		amounts.AmbiguousMinorUnits, amounts.UnresolvedMinorUnits,
	} {
		value, err := canonicalMinorUnits(text)
		if err != nil {
			return false
		}
		states.Add(&states, value)
	}
	return states.Cmp(source) == 0
}

func validateAggregateResult(result AggregateResult) error {
	if result.SchemaVersion != AggregateResultSchemaVersion || !validDigest(result.ResultID) ||
		!validDigest(result.CalculationSetID) || !validCycle(result.Cycle) || result.SpenderCommitteeID == "" ||
		result.CandidateID == "" || (result.SupportOppose != "S" && result.SupportOppose != "O") || result.ExpenditureCount == 0 {
		return fmt.Errorf("resolved independent-expenditure result identity is incomplete")
	}
	amount, err := canonicalMinorUnits(result.SignedAmountMinorUnits)
	if err != nil {
		return err
	}
	if result.ExpenditureCount != result.PositiveCount+result.NegativeCount+result.ZeroCount ||
		result.ExpenditureCount != result.ResolutionCounts.Confirmed+result.ResolutionCounts.Resolved+result.ResolutionCounts.Unverified {
		return fmt.Errorf("resolved independent-expenditure result counts are not conserved")
	}
	var stateAmounts big.Int
	for _, text := range []string{
		result.ResolutionAmounts.ConfirmedMinorUnits, result.ResolutionAmounts.ResolvedMinorUnits,
		result.ResolutionAmounts.UnverifiedMinorUnits,
	} {
		value, err := canonicalMinorUnits(text)
		if err != nil {
			return err
		}
		stateAmounts.Add(&stateAmounts, value)
	}
	if stateAmounts.Cmp(amount) != 0 {
		return fmt.Errorf("resolved independent-expenditure result amounts are not conserved")
	}
	expectedID := digestParts(
		"fec.resolved-independent-expenditure.result.v1", result.CalculationSetID,
		result.Cycle, result.SpenderCommitteeID, result.CandidateID, result.SupportOppose,
	)
	if result.ResultID != expectedID {
		return fmt.Errorf("resolved independent-expenditure result ID does not match canonical identity")
	}
	return nil
}

// ValidateAggregateResult verifies one grouped result against its canonical
// schema and identity invariants for downstream projections.
func ValidateAggregateResult(result AggregateResult) error {
	return validateAggregateResult(result)
}

func validateAggregateException(exception AggregateException) error {
	if exception.SchemaVersion != AggregateExceptionSchemaVersion || !validDigest(exception.ExceptionID) ||
		!validDigest(exception.CalculationSetID) || !validDigest(exception.ResolutionDecisionID) ||
		!validDigest(exception.FactID) || exception.NaturalKey == "" || !validCycle(exception.Cycle) ||
		exception.SpenderCommitteeID == "" || exception.ReportedCandidateID == "" ||
		(exception.SupportOppose != "S" && exception.SupportOppose != "O") ||
		(exception.State != StateAmbiguous && exception.State != StateUnresolved) || exception.Method == "" {
		return fmt.Errorf("resolved independent-expenditure exception identity is incomplete")
	}
	if _, err := canonicalMinorUnits(exception.AmountMinorUnits); err != nil {
		return err
	}
	expectedID := digestParts(
		"fec.resolved-independent-expenditure.exception.v1", exception.CalculationSetID,
		exception.ResolutionDecisionID, exception.State,
	)
	if exception.ExceptionID != expectedID {
		return fmt.Errorf("resolved independent-expenditure exception ID does not match canonical identity")
	}
	return nil
}

func validateAggregateManifest(manifest AggregateManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != AggregateManifestSchemaVersion ||
		manifest.Calculation != AggregateContractID || manifest.CalculationVersion != AggregateContractVersion ||
		manifest.PublisherVersion != AggregatePublisherVersion || manifest.ResultSchemaVersion != AggregateResultSchemaVersion ||
		manifest.ExceptionSchemaVersion != AggregateExceptionSchemaVersion || manifest.State != "published" ||
		!reflect.DeepEqual(manifest.GroupingPolicy, aggregateGroupingPolicy()) {
		return fmt.Errorf("unsupported resolved independent-expenditure calculation manifest")
	}
	if !validDigest(manifest.CalculationSetID) || !validCycle(manifest.Cycle) || !validSourceReleaseID(manifest.SourceReleaseID) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("resolved independent-expenditure calculation identity is incomplete")
	}
	reference := manifest.InputResolution
	if reference.Role != "independent_expenditure_candidate_resolution" || reference.Calculation != ContractID ||
		reference.CalculationVersion != ContractVersion || !validDigest(reference.CalculationSetID) ||
		!validDigest(reference.ManifestSHA256) || reference.DecisionSchemaVersion != DecisionSchemaVersion ||
		!validDigest(reference.DecisionsSHA256) {
		return fmt.Errorf("resolved independent-expenditure input reference is invalid")
	}
	expectedID := digestParts(
		AggregateManifestSchemaVersion, AggregateContractVersion, AggregateResultSchemaVersion,
		AggregateExceptionSchemaVersion, AggregateGroupingPolicyVersion, AggregatePublisherVersion,
		manifest.Cycle, manifest.SourceReleaseID, reference.CalculationSetID,
		reference.ManifestSHA256, reference.DecisionSchemaVersion, reference.DecisionsSHA256,
	)
	if manifest.CalculationSetID != expectedID {
		return fmt.Errorf("resolved independent-expenditure calculation-set ID does not match canonical inputs")
	}
	if aggregateStateCount(manifest.Counts) != manifest.Counts.SourceDecisions ||
		manifest.Counts.SourceDecisions != manifest.Counts.ProjectableDecisions+manifest.Counts.UnprojectableDecisions ||
		manifest.Counts.ProjectableDecisions != manifest.Counts.Confirmed+manifest.Counts.Resolved+manifest.Counts.Unverified ||
		manifest.Counts.UnprojectableDecisions != manifest.Counts.Ambiguous+manifest.Counts.Unresolved ||
		manifest.Counts.Exceptions != manifest.Counts.UnprojectableDecisions ||
		manifest.Results.RecordCount != manifest.Counts.ResultGroups || manifest.Exceptions.RecordCount != manifest.Counts.Exceptions ||
		!aggregateAmountsConserve(manifest.Amounts) {
		return fmt.Errorf("resolved independent-expenditure counts or amounts are not conserved")
	}
	for role, artifact := range map[string]storageartifact.Descriptor{"results": manifest.Results, "exceptions": manifest.Exceptions} {
		if artifact.Compression != "zstd" || artifact.CompressedBytes == 0 || !validDigest(artifact.CompressedSHA256) ||
			!validDigest(artifact.UncompressedSHA256) || !strings.HasPrefix(artifact.StorageKey, aggregateCalculationBase()+"/"+role+"/sha256/") {
			return fmt.Errorf("resolved independent-expenditure %s artifact is incomplete", role)
		}
	}
	expectedChecks := map[string]struct{}{
		"input_lineage": {}, "current_resolution_method": {}, "decision_conservation": {}, "route_conservation": {},
		"signed_conservation": {}, "result_conservation": {}, "exception_conservation": {}, "quality_preservation": {},
	}
	if len(manifest.Checks) != len(expectedChecks) {
		return fmt.Errorf("resolved independent-expenditure checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if _, ok := expectedChecks[check.ID]; !ok || check.Severity != "block" || !check.Passed {
			return fmt.Errorf("resolved independent-expenditure check %s is invalid", check.ID)
		}
		delete(expectedChecks, check.ID)
	}
	return nil
}

func validateAggregateManifestBacking(ctx context.Context, storageRoot string, manifest AggregateManifest) error {
	for role, descriptor := range map[string]storageartifact.Descriptor{"results": manifest.Results, "exceptions": manifest.Exceptions} {
		path, err := storageartifact.Resolve(storageRoot, descriptor.StorageKey)
		if err != nil {
			return err
		}
		if err := storageartifact.Verify(ctx, path, descriptor); err != nil {
			return fmt.Errorf("verify resolved independent-expenditure %s artifact: %w", role, err)
		}
	}
	return nil
}

func readAggregateManifestIfPresent(path string) (*AggregateManifest, error) {
	manifest, _, err := readStrictJSON[AggregateManifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func aggregateCalculationBase() string {
	return filepath.Join("calculations", "fec", "resolved-independent-expenditures")
}
