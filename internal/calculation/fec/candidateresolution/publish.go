package candidateresolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	feceffective "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type amountState struct {
	source     big.Int
	confirmed  big.Int
	resolved   big.Int
	unverified big.Int
	ambiguous  big.Int
	unresolved big.Int
}

// LoadPublishedManifest resolves one exact candidate-resolution calculation
// and verifies its immutable manifest and complete decision artifact.
func LoadPublishedManifest(ctx context.Context, storageRoot, path string) (Manifest, string, error) {
	if storageRoot == "" || path == "" {
		return Manifest{}, "", fmt.Errorf("storage root and candidate-resolution manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return Manifest{}, "", err
	}
	pointer, _, err := readStrictJSON[Manifest](path)
	if err != nil {
		return Manifest{}, "", err
	}
	if err := validateManifest(pointer); err != nil {
		return Manifest{}, "", err
	}
	immutablePath := filepath.Join(storageRoot, calculationBase(), "manifests", pointer.CalculationSetID+".json")
	immutable, digest, err := readStrictJSON[Manifest](immutablePath)
	if err != nil {
		return Manifest{}, "", err
	}
	if !reflect.DeepEqual(pointer, immutable) {
		return Manifest{}, "", fmt.Errorf("candidate-resolution pointer differs from immutable manifest")
	}
	if err := validateManifestBacking(ctx, storageRoot, immutable); err != nil {
		return Manifest{}, "", err
	}
	return immutable, digest, nil
}

// Publish resolves candidate references for every attributed fact in one
// accepted effective Schedule E calculation. It never rewrites a source ID or
// treats unresolved identity as a reason to discard the amount.
func Publish(ctx context.Context, input PublishInput, runID string, options PublishOptions) (Manifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.EffectiveManifestPath == "" || input.CandidateManifestPath == "" {
		return Manifest{}, fmt.Errorf("storage root, effective calculation, and candidate facts are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return Manifest{}, fmt.Errorf("run ID contains unsupported characters")
	}

	effective, effectiveDigest, err := feceffective.LoadPublishedManifest(ctx, options.StorageRoot, input.EffectiveManifestPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("load effective independent expenditures: %w", err)
	}
	scheduleEPath := filepath.Join(
		options.StorageRoot, "facts", "fec", "schedule-e", "manifests",
		effective.InputFactSet.FactSetID+".json",
	)
	scheduleE, scheduleEDigest, err := fecoccurrence.LoadPublishedScheduleEFactManifest(ctx, options.StorageRoot, scheduleEPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("load Schedule E facts: %w", err)
	}
	if scheduleEDigest != effective.InputFactSet.ManifestSHA256 || scheduleE.FactSetID != effective.InputFactSet.FactSetID ||
		scheduleE.SourceReleaseID != effective.SourceReleaseID || scheduleE.Cycle != effective.Cycle {
		return Manifest{}, fmt.Errorf("effective calculation does not match its Schedule E fact backing")
	}
	candidates, candidateDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(options.StorageRoot, input.CandidateManifestPath, "candidate-master")
	if err != nil {
		return Manifest{}, fmt.Errorf("load candidate-master facts: %w", err)
	}
	if candidates.Cycle != effective.Cycle || candidates.SourceReleaseID != effective.SourceReleaseID {
		return Manifest{}, fmt.Errorf("candidate-master facts do not share the effective calculation cycle and source release")
	}

	calculationReference := CalculationReference{
		Role: "effective_independent_expenditures", Calculation: feceffective.ContractID,
		CalculationVersion: feceffective.ContractVersion, CalculationSetID: effective.CalculationSetID,
		ManifestSHA256: effectiveDigest, ScheduleEFactSetID: scheduleE.FactSetID,
		ScheduleEManifestSHA256: scheduleEDigest,
	}
	candidateReference := CandidateFactSetReference{
		Role: "cycle_candidate_master", Dataset: candidates.Dataset, FactType: candidates.FactType,
		FactSetID: candidates.FactSetID, ManifestSHA256: candidateDigest,
	}
	calculationSetID := digestParts(
		ManifestSchemaVersion, ContractVersion, DecisionSchemaVersion, MethodVersion, PublisherVersion,
		calculationReference.CalculationSetID, calculationReference.ManifestSHA256,
		calculationReference.ScheduleEFactSetID, calculationReference.ScheduleEManifestSHA256,
		candidateReference.FactSetID, candidateReference.ManifestSHA256,
	)
	basePath := calculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", effective.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return Manifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+effective.Cycle+".lock"))
	if err != nil {
		return Manifest{}, err
	}
	defer unlock()
	current, err := readManifestIfPresent(currentPath)
	if err != nil {
		return Manifest{}, err
	}
	if current != nil {
		if err := validateManifest(*current); err != nil {
			return Manifest{}, fmt.Errorf("invalid current candidate-resolution calculation: %w", err)
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return Manifest{}, err
		}
		if current.Cycle != effective.Cycle {
			return Manifest{}, fmt.Errorf("current candidate resolution belongs to cycle %s", current.Cycle)
		}
		if current.CalculationSetID == calculationSetID {
			return *current, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", calculationSetID+".json")
	if existing, readErr := readManifestIfPresent(manifestPath); readErr != nil {
		return Manifest{}, readErr
	} else if existing != nil {
		if err := validateManifest(*existing); err != nil {
			return Manifest{}, err
		}
		if existing.InputCalculation != calculationReference || existing.InputCandidateFactSet != candidateReference {
			return Manifest{}, fmt.Errorf("immutable candidate-resolution manifest collision")
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return Manifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return Manifest{}, err
		}
		return *existing, nil
	}

	if options.Progress != nil {
		options.Progress("indexing candidate-master facts for " + effective.Cycle)
	}
	index, candidateFacts, usableCandidateFacts, err := buildCandidateIndex(ctx, options.StorageRoot, candidates)
	if err != nil {
		return Manifest{}, err
	}
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return Manifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	decisionWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "decisions")
	if err != nil {
		return Manifest{}, err
	}
	defer decisionWriter.Abort()
	if options.Progress != nil {
		options.Progress("resolving effective Schedule E candidate references for " + effective.Cycle)
	}
	counts, amounts, err := resolveEffectiveFacts(
		ctx, options.StorageRoot, scheduleE, effective, calculationSetID, index,
		func(_ fecoccurrence.ScheduleEFact, _ feceffective.FactEvaluation, decision *Decision) error {
			if decision == nil {
				return nil
			}
			return decisionWriter.WriteJSON(*decision)
		}, options.Progress,
	)
	if err != nil {
		return Manifest{}, err
	}
	counts.CandidateFacts = candidateFacts
	counts.UsableCandidateFacts = usableCandidateFacts
	decisionArtifact, err := decisionWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	if decisionArtifact.RecordCount != counts.SourceEffectiveFacts {
		return Manifest{}, fmt.Errorf("candidate-resolution decision membership is not conserved")
	}

	manifest := Manifest{
		Schema: "manifest.schema.json", SchemaVersion: ManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID, CalculationVersion: ContractVersion,
		PublisherVersion: PublisherVersion, DecisionSchemaVersion: DecisionSchemaVersion,
		Cycle: effective.Cycle, SourceReleaseID: effective.SourceReleaseID,
		InputCalculation: calculationReference, InputCandidateFactSet: candidateReference,
		RunID: runID, State: "published", PublishedAt: options.Clock().UTC(), Method: resolutionMethod(),
		Counts: counts, Amounts: amounts, Decisions: decisionArtifact,
	}
	manifest.Checks = []Check{
		{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the exact effective calculation, Schedule E facts, and candidate-master facts passed immutable-manifest and backing verification"},
		{ID: "cycle_release_coherence", Passed: true, Severity: "block", Detail: "all inputs belong to one cycle and coordinated FEC release"},
		{ID: "effective_membership_replay", Passed: counts.SourceEffectiveFacts == effective.RouteCounts.Attributed, Severity: "block", Detail: "the accepted effective predicate reproduced every attributed Schedule E fact exactly once"},
		{ID: "decision_conservation", Passed: resolutionCount(counts) == counts.SourceEffectiveFacts && decisionArtifact.RecordCount == counts.SourceEffectiveFacts, Severity: "block", Detail: "every effective attributed fact has exactly one candidate-reference state"},
		{ID: "signed_conservation", Passed: resolutionAmountsConserve(amounts), Severity: "block", Detail: "signed effective cents equal confirmed, resolved, unverified, ambiguous, and unresolved cents"},
		{ID: "no_silent_resolution", Passed: true, Severity: "block", Detail: "only one unique exact context can replace a reported ID; a present uncorroborated reported ID remains explicit as unverified"},
		{ID: "unresolved_preservation", Passed: true, Severity: "block", Detail: "unverified, ambiguous, and unresolved facts retain their exact amount and source-fact identity"},
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return Manifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func buildCandidateIndex(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest) (candidateIndex, uint64, uint64, error) {
	reader, err := storageartifact.Open[fecoccurrence.ClassicFact](ctx, storageRoot, classicDescriptor(manifest.Facts))
	if err != nil {
		return candidateIndex{}, 0, 0, err
	}
	defer reader.Abort()
	index := newCandidateIndex()
	seen := make(map[string]struct{}, manifest.Counts.Facts)
	var facts, usable uint64
	for {
		fact, ok, readErr := reader.Next()
		if readErr != nil {
			return candidateIndex{}, 0, 0, readErr
		}
		if !ok {
			break
		}
		facts++
		if fact.SchemaVersion != fecoccurrence.ClassicFactSchemaVersion || fact.Dataset != "candidate-master" ||
			fact.FactType != manifest.FactType || fact.Cycle != manifest.Cycle || fact.SourceReleaseID != manifest.SourceReleaseID {
			return candidateIndex{}, 0, 0, fmt.Errorf("candidate fact %d does not match its manifest", facts)
		}
		if _, duplicate := seen[fact.FactID]; duplicate {
			return candidateIndex{}, 0, 0, fmt.Errorf("duplicate candidate fact ID %s", fact.FactID)
		}
		seen[fact.FactID] = struct{}{}
		if fact.State != "valid" {
			continue
		}
		fields, err := decodeCandidateFields(fact.TypedFields)
		if err != nil {
			return candidateIndex{}, 0, 0, fmt.Errorf("candidate fact %s: %w", fact.FactID, err)
		}
		if index.add(fact, fields) {
			usable++
		}
	}
	if err := reader.Close(); err != nil {
		return candidateIndex{}, 0, 0, err
	}
	if facts != manifest.Counts.Facts || uint64(len(seen)) != manifest.Counts.Facts {
		return candidateIndex{}, 0, 0, fmt.Errorf("candidate fact membership is not conserved")
	}
	return index, facts, usable, nil
}

func resolveEffectiveFacts(
	ctx context.Context,
	storageRoot string,
	scheduleE fecoccurrence.ScheduleEFactManifest,
	effective feceffective.Manifest,
	calculationSetID string,
	index candidateIndex,
	visit func(fecoccurrence.ScheduleEFact, feceffective.FactEvaluation, *Decision) error,
	progress func(string),
) (Counts, Amounts, error) {
	reader, err := storageartifact.Open[fecoccurrence.ScheduleEFact](ctx, storageRoot, scheduleEDescriptor(scheduleE.Facts))
	if err != nil {
		return Counts{}, Amounts{}, err
	}
	defer reader.Abort()
	var counts Counts
	var stateAmounts amountState
	seen := make(map[string]struct{}, scheduleE.Counts.Facts)
	var scanned uint64
	for {
		fact, ok, readErr := reader.Next()
		if readErr != nil {
			return Counts{}, Amounts{}, readErr
		}
		if !ok {
			break
		}
		scanned++
		if err := validateScheduleEFact(fact, scheduleE); err != nil {
			return Counts{}, Amounts{}, fmt.Errorf("Schedule E fact %d: %w", scanned, err)
		}
		if _, duplicate := seen[fact.FactID]; duplicate {
			return Counts{}, Amounts{}, fmt.Errorf("duplicate Schedule E fact ID %s", fact.FactID)
		}
		seen[fact.FactID] = struct{}{}
		evaluation := feceffective.EvaluateFact(fact)
		if evaluation.Decision != feceffective.DecisionIncluded || len(evaluation.RouteReasons) != 0 {
			if err := visit(fact, evaluation, nil); err != nil {
				return Counts{}, Amounts{}, err
			}
			continue
		}
		counts.SourceEffectiveFacts++
		stateAmounts.source.Add(&stateAmounts.source, evaluation.Amount)
		candidateResolution := index.resolve(fact.TypedFields.Candidate)
		addResolutionCount(&counts, candidateResolution.state)
		addResolutionAmount(&stateAmounts, candidateResolution.state, evaluation.Amount)
		candidateID := *fact.TypedFields.Candidate.CandidateID
		spenderID := *fact.TypedFields.Spender.CommitteeID
		stance := *fact.TypedFields.Candidate.SupportOpposeCode
		amountText := evaluation.Amount.String()
		decision := Decision{
			SchemaVersion: DecisionSchemaVersion,
			DecisionID: digestParts(
				"fec.independent-expenditure-candidate-resolution.v1", calculationSetID, fact.FactID,
				candidateResolution.state, candidateResolution.method, pointerValue(candidateResolution.resolvedCandidateID),
				strings.Join(candidateResolution.candidateFactIDs, ","), strings.Join(candidateResolution.evidenceCodes, ","),
			),
			CalculationSetID: calculationSetID, FactID: fact.FactID, NaturalKey: fact.NaturalKey,
			Cycle: fact.Cycle, SpenderCommitteeID: spenderID, SupportOppose: stance, AmountMinorUnits: amountText,
			ReportedCandidate: ReportedCandidate{
				CandidateID: candidateID, Name: fact.TypedFields.Candidate.Name, Office: fact.TypedFields.Candidate.OfficeCode,
				OfficeState: fact.TypedFields.Candidate.OfficeState, OfficeDistrict: fact.TypedFields.Candidate.OfficeDistrict,
			},
			State: candidateResolution.state, Method: candidateResolution.method,
			ResolvedCandidateID: candidateResolution.resolvedCandidateID,
			CandidateFactIDs:    candidateResolution.candidateFactIDs, EvidenceCodes: candidateResolution.evidenceCodes,
		}
		if err := validateDecision(decision); err != nil {
			return Counts{}, Amounts{}, fmt.Errorf("candidate decision for fact %s: %w", fact.FactID, err)
		}
		if err := visit(fact, evaluation, &decision); err != nil {
			return Counts{}, Amounts{}, err
		}
		if progress != nil && counts.SourceEffectiveFacts%50_000 == 0 {
			progress(fmt.Sprintf("resolved %d effective Schedule E candidate references for %s", counts.SourceEffectiveFacts, scheduleE.Cycle))
		}
	}
	if err := reader.Close(); err != nil {
		return Counts{}, Amounts{}, err
	}
	if scanned != scheduleE.Counts.Facts || uint64(len(seen)) != scheduleE.Counts.Facts {
		return Counts{}, Amounts{}, fmt.Errorf("Schedule E fact replay is not conserved")
	}
	expectedAmount, ok := new(big.Int).SetString(effective.Amounts.AttributedMinorUnits, 10)
	if !ok || counts.SourceEffectiveFacts != effective.RouteCounts.Attributed || stateAmounts.source.Cmp(expectedAmount) != 0 {
		return Counts{}, Amounts{}, fmt.Errorf("candidate resolution did not reproduce effective attributed membership")
	}
	return counts, Amounts{
		SourceEffectiveMinorUnits: stateAmounts.source.String(), ConfirmedMinorUnits: stateAmounts.confirmed.String(),
		ResolvedMinorUnits: stateAmounts.resolved.String(), UnverifiedMinorUnits: stateAmounts.unverified.String(),
		AmbiguousMinorUnits: stateAmounts.ambiguous.String(), UnresolvedMinorUnits: stateAmounts.unresolved.String(),
	}, nil
}

func validateScheduleEFact(fact fecoccurrence.ScheduleEFact, manifest fecoccurrence.ScheduleEFactManifest) error {
	if fact.SchemaVersion != fecoccurrence.ScheduleEFactSchemaVersion || fact.FactType != fecoccurrence.ScheduleEFactType ||
		fact.Cycle != manifest.Cycle || fact.SourceReleaseID != manifest.SourceReleaseID || fact.OccurrenceSetID != manifest.OccurrenceSetID ||
		fact.SourceContract != fecoccurrence.ScheduleESourceContract || (fact.State != "valid" && fact.State != "invalid") {
		return fmt.Errorf("fact identity does not match its manifest")
	}
	if !validDigest(fact.FactID) || fact.TypedFields.Filing.SubmissionID == "" {
		return fmt.Errorf("fact identity is incomplete")
	}
	return nil
}

func resolutionMethod() ResolutionMethod {
	return ResolutionMethod{
		Version:           MethodVersion,
		NameNormalization: "uppercase Unicode alphanumeric token multiset; punctuation and token order ignored; no fuzzy, nickname, or semantic expansion",
		ContextRules: []string{
			"president requires exact normalized name and office P",
			"senate requires exact normalized name, office S, and state",
			"house requires exact normalized name, office H, state, and normalized district",
			"candidate election year is preserved but does not select identity",
		},
		DecisionOrder: []ResolutionRule{
			{State: StateConfirmed, Method: MethodReportedIDExactContext, When: "the reported ID has a candidate-master assertion with the exact context key"},
			{State: StateResolved, Method: MethodUniqueExactContext, When: "exactly one different candidate ID has the exact context key"},
			{State: StateAmbiguous, Method: MethodMultipleExactContext, When: "multiple candidate IDs have the exact context key"},
			{State: StateUnverified, Method: MethodReportedIDUnverified, When: "the reported ID exists but no candidate-master assertion matches the usable reported context"},
			{State: StateUnverified, Method: MethodReportedIDInsufficient, When: "the reported ID exists but reported context is insufficient for corroboration"},
			{State: StateUnresolved, Method: MethodInsufficientContext, When: "the reported ID is absent and reported context is insufficient"},
			{State: StateUnresolved, Method: MethodNoExactContext, When: "the reported ID is absent and no candidate-master assertion matches the exact context"},
		},
	}
}

func legacyResolutionMethod() ResolutionMethod {
	return ResolutionMethod{
		Version:           LegacyMethodVersion,
		NameNormalization: "uppercase Unicode alphanumeric token multiset; punctuation and token order ignored; no fuzzy, nickname, or semantic expansion",
		ContextRules: []string{
			"president requires exact normalized name and office P",
			"senate requires exact normalized name, office S, and state",
			"house requires exact normalized name, office H, state, and normalized district",
			"candidate election year is preserved but does not select identity",
		},
		DecisionOrder: []ResolutionRule{
			{State: StateUnresolved, Method: legacyMethodInsufficient, When: "reported name or required office context cannot form an exact context key"},
			{State: StateConfirmed, Method: MethodReportedIDExactContext, When: "the reported ID has a candidate-master assertion with the exact context key"},
			{State: StateResolved, Method: MethodUniqueExactContext, When: "exactly one different candidate ID has the exact context key"},
			{State: StateAmbiguous, Method: MethodMultipleExactContext, When: "multiple candidate IDs have the exact context key"},
			{State: legacyStateConflicting, Method: legacyMethodIDConflict, When: "the reported ID exists but no candidate-master assertion matches the exact context"},
			{State: StateUnresolved, Method: legacyMethodNoExact, When: "the reported ID is absent and no candidate-master assertion matches the exact context"},
		},
	}
}

func addResolutionCount(counts *Counts, state string) {
	switch state {
	case StateConfirmed:
		counts.Confirmed++
	case StateResolved:
		counts.Resolved++
	case StateUnverified:
		counts.Unverified++
	case StateAmbiguous:
		counts.Ambiguous++
	case StateUnresolved:
		counts.Unresolved++
	}
}

func addResolutionAmount(amounts *amountState, state string, amount *big.Int) {
	switch state {
	case StateConfirmed:
		amounts.confirmed.Add(&amounts.confirmed, amount)
	case StateResolved:
		amounts.resolved.Add(&amounts.resolved, amount)
	case StateUnverified:
		amounts.unverified.Add(&amounts.unverified, amount)
	case StateAmbiguous:
		amounts.ambiguous.Add(&amounts.ambiguous, amount)
	case StateUnresolved:
		amounts.unresolved.Add(&amounts.unresolved, amount)
	}
}

func resolutionCount(counts Counts) uint64 {
	return counts.Confirmed + counts.Resolved + counts.Unverified + counts.Ambiguous + counts.Unresolved
}

func legacyResolutionCount(counts Counts) uint64 {
	return counts.Confirmed + counts.Resolved + counts.Ambiguous + counts.LegacyConflicting + counts.Unresolved
}

func resolutionAmountsConserve(amounts Amounts) bool {
	total, ok := new(big.Int).SetString(amounts.SourceEffectiveMinorUnits, 10)
	if !ok {
		return false
	}
	var sum big.Int
	for _, text := range []string{
		amounts.ConfirmedMinorUnits, amounts.ResolvedMinorUnits, amounts.UnverifiedMinorUnits,
		amounts.AmbiguousMinorUnits, amounts.UnresolvedMinorUnits,
	} {
		value, valid := new(big.Int).SetString(text, 10)
		if !valid {
			return false
		}
		sum.Add(&sum, value)
	}
	return sum.Cmp(total) == 0
}

func legacyResolutionAmountsConserve(amounts Amounts) bool {
	total, ok := new(big.Int).SetString(amounts.SourceEffectiveMinorUnits, 10)
	if !ok {
		return false
	}
	var sum big.Int
	for _, text := range []string{
		amounts.ConfirmedMinorUnits, amounts.ResolvedMinorUnits, amounts.AmbiguousMinorUnits,
		amounts.LegacyConflictingMinorUnits, amounts.UnresolvedMinorUnits,
	} {
		value, valid := new(big.Int).SetString(text, 10)
		if !valid {
			return false
		}
		sum.Add(&sum, value)
	}
	return sum.Cmp(total) == 0
}

func validateDecision(decision Decision) error {
	if decision.SchemaVersion != DecisionSchemaVersion || !validDigest(decision.DecisionID) || !validDigest(decision.CalculationSetID) ||
		!validDigest(decision.FactID) || decision.NaturalKey == "" || !validCycle(decision.Cycle) || decision.SpenderCommitteeID == "" ||
		decision.ReportedCandidate.CandidateID == "" || (decision.SupportOppose != "S" && decision.SupportOppose != "O") {
		return fmt.Errorf("decision identity is incomplete")
	}
	if _, ok := new(big.Int).SetString(decision.AmountMinorUnits, 10); !ok {
		return fmt.Errorf("decision amount is invalid")
	}
	validMethod := map[string]string{
		StateConfirmed: MethodReportedIDExactContext,
		StateResolved:  MethodUniqueExactContext,
		StateAmbiguous: MethodMultipleExactContext,
	}
	if expected, stateHasOneMethod := validMethod[decision.State]; stateHasOneMethod {
		if decision.Method != expected {
			return fmt.Errorf("decision method does not match state")
		}
	} else if decision.State == StateUnverified {
		if decision.Method != MethodReportedIDUnverified && decision.Method != MethodReportedIDInsufficient {
			return fmt.Errorf("unverified decision method is invalid")
		}
	} else if decision.State != StateUnresolved || (decision.Method != MethodNoExactContext && decision.Method != MethodInsufficientContext) {
		return fmt.Errorf("decision state or method is invalid")
	}
	resolved := decision.State == StateConfirmed || decision.State == StateResolved || decision.State == StateUnverified
	if resolved != (decision.ResolvedCandidateID != nil && *decision.ResolvedCandidateID != "") {
		return fmt.Errorf("resolved candidate ID does not match decision state")
	}
	if decision.State == StateResolved && *decision.ResolvedCandidateID == decision.ReportedCandidate.CandidateID {
		return fmt.Errorf("resolved state did not change the reported candidate ID")
	}
	if (decision.State == StateConfirmed || decision.State == StateUnverified) && *decision.ResolvedCandidateID != decision.ReportedCandidate.CandidateID {
		return fmt.Errorf("reported-ID state changed the candidate ID")
	}
	if decision.CandidateFactIDs == nil || decision.EvidenceCodes == nil || len(decision.EvidenceCodes) == 0 {
		return fmt.Errorf("decision evidence is incomplete")
	}
	if (resolved && len(decision.CandidateFactIDs) == 0) || (decision.State == StateAmbiguous && len(decision.CandidateFactIDs) < 2) ||
		(decision.State == StateUnresolved && len(decision.CandidateFactIDs) != 0) {
		return fmt.Errorf("candidate fact evidence does not match decision state")
	}
	for index, factID := range decision.CandidateFactIDs {
		if !validDigest(factID) || (index != 0 && decision.CandidateFactIDs[index-1] >= factID) {
			return fmt.Errorf("candidate fact evidence is invalid or unordered")
		}
	}
	seenCodes := make(map[string]struct{}, len(decision.EvidenceCodes))
	for _, code := range decision.EvidenceCodes {
		if code == "" {
			return fmt.Errorf("decision evidence code is empty")
		}
		if _, duplicate := seenCodes[code]; duplicate {
			return fmt.Errorf("decision evidence code is duplicated")
		}
		seenCodes[code] = struct{}{}
	}
	expectedID := digestParts(
		"fec.independent-expenditure-candidate-resolution.v1", decision.CalculationSetID, decision.FactID,
		decision.State, decision.Method, pointerValue(decision.ResolvedCandidateID),
		strings.Join(decision.CandidateFactIDs, ","), strings.Join(decision.EvidenceCodes, ","),
	)
	if decision.DecisionID != expectedID {
		return fmt.Errorf("decision ID does not match canonical identity")
	}
	return nil
}

func classicDescriptor(artifact fecoccurrence.Artifact) storageartifact.Descriptor {
	return storageartifact.Descriptor{
		RecordCount: artifact.RecordCount, UncompressedBytes: artifact.UncompressedBytes,
		UncompressedSHA256: artifact.UncompressedSHA256, CompressedBytes: artifact.CompressedBytes,
		CompressedSHA256: artifact.CompressedSHA256, Compression: artifact.Compression, StorageKey: artifact.StorageKey,
	}
}

func scheduleEDescriptor(artifact fecoccurrence.Artifact) storageartifact.Descriptor {
	return classicDescriptor(artifact)
}

func calculationBase() string {
	return filepath.Join("calculations", "fec", "independent-expenditure-candidate-resolution")
}

func readManifestIfPresent(path string) (*Manifest, error) {
	manifest, _, err := readStrictJSON[Manifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ManifestSchemaVersion || manifest.Calculation != ContractID ||
		manifest.CalculationVersion != ContractVersion || manifest.PublisherVersion != PublisherVersion ||
		manifest.DecisionSchemaVersion != DecisionSchemaVersion || manifest.State != "published" {
		return fmt.Errorf("unsupported candidate-resolution calculation manifest")
	}
	currentMethod := reflect.DeepEqual(manifest.Method, resolutionMethod())
	legacyMethod := reflect.DeepEqual(manifest.Method, legacyResolutionMethod())
	if !currentMethod && !legacyMethod {
		return fmt.Errorf("unsupported candidate-resolution method")
	}
	if !validDigest(manifest.CalculationSetID) || !validSourceReleaseID(manifest.SourceReleaseID) || !validCycle(manifest.Cycle) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("candidate-resolution calculation identity is incomplete")
	}
	calculation := manifest.InputCalculation
	if calculation.Role != "effective_independent_expenditures" || calculation.Calculation != feceffective.ContractID ||
		calculation.CalculationVersion != feceffective.ContractVersion || !validDigest(calculation.CalculationSetID) ||
		!validDigest(calculation.ManifestSHA256) || !validDigest(calculation.ScheduleEFactSetID) ||
		!validDigest(calculation.ScheduleEManifestSHA256) {
		return fmt.Errorf("candidate-resolution effective input is invalid")
	}
	candidates := manifest.InputCandidateFactSet
	if candidates.Role != "cycle_candidate_master" || candidates.Dataset != "candidate-master" ||
		candidates.FactType != "fec.candidate_assertion.v1" || !validDigest(candidates.FactSetID) || !validDigest(candidates.ManifestSHA256) {
		return fmt.Errorf("candidate-resolution candidate input is invalid")
	}
	expectedID := digestParts(
		ManifestSchemaVersion, ContractVersion, DecisionSchemaVersion, manifest.Method.Version, PublisherVersion,
		calculation.CalculationSetID, calculation.ManifestSHA256, calculation.ScheduleEFactSetID,
		calculation.ScheduleEManifestSHA256, candidates.FactSetID, candidates.ManifestSHA256,
	)
	if manifest.CalculationSetID != expectedID {
		return fmt.Errorf("candidate-resolution calculation-set ID does not match canonical inputs")
	}
	countsConserve := resolutionCount(manifest.Counts) == manifest.Counts.SourceEffectiveFacts
	amountsConserve := resolutionAmountsConserve(manifest.Amounts)
	if legacyMethod {
		countsConserve = legacyResolutionCount(manifest.Counts) == manifest.Counts.SourceEffectiveFacts
		amountsConserve = legacyResolutionAmountsConserve(manifest.Amounts)
	}
	if !countsConserve || manifest.Decisions.RecordCount != manifest.Counts.SourceEffectiveFacts ||
		manifest.Counts.UsableCandidateFacts > manifest.Counts.CandidateFacts || !amountsConserve {
		return fmt.Errorf("candidate-resolution counts or amounts are not conserved")
	}
	if currentMethod && (manifest.Counts.LegacyConflicting != 0 || manifest.Amounts.LegacyConflictingMinorUnits != "") {
		return fmt.Errorf("current candidate-resolution manifest contains legacy conflicting state")
	}
	if legacyMethod && (manifest.Counts.Unverified != 0 || manifest.Amounts.UnverifiedMinorUnits != "") {
		return fmt.Errorf("legacy candidate-resolution manifest contains current unverified state")
	}
	if manifest.Decisions.Compression != "zstd" || manifest.Decisions.CompressedBytes == 0 || !validDigest(manifest.Decisions.CompressedSHA256) ||
		!validDigest(manifest.Decisions.UncompressedSHA256) ||
		!strings.HasPrefix(manifest.Decisions.StorageKey, calculationBase()+"/decisions/sha256/") || len(manifest.Checks) < 7 {
		return fmt.Errorf("candidate-resolution decision artifact or checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("candidate-resolution blocking check %s failed", check.ID)
		}
	}
	return nil
}

func validateManifestBacking(ctx context.Context, storageRoot string, manifest Manifest) error {
	path, err := storageartifact.Resolve(storageRoot, manifest.Decisions.StorageKey)
	if err != nil {
		return err
	}
	if err := storageartifact.Verify(ctx, path, manifest.Decisions); err != nil {
		return fmt.Errorf("verify candidate-resolution decision artifact: %w", err)
	}
	return nil
}

func readStrictJSON[T any](path string) (T, string, error) {
	var value T
	content, err := os.ReadFile(path)
	if err != nil {
		return value, "", err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, "", fmt.Errorf("multiple JSON values in %s", path)
		}
		return value, "", err
	}
	digest := sha256.Sum256(content)
	return value, hex.EncodeToString(digest[:]), nil
}

func digestParts(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func validSourceReleaseID(value string) bool {
	return strings.HasPrefix(value, "fec-") && validDigest(strings.TrimPrefix(value, "fec-"))
}

func validCycle(value string) bool {
	if len(value) != 4 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func lockContext(ctx context.Context, path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func writeAtomicJSON(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".pending-*.json")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func requirePathInside(root, path string) error {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	pathAbsolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbsolute, pathAbsolute)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("candidate-resolution path escapes storage root")
	}
	return nil
}
