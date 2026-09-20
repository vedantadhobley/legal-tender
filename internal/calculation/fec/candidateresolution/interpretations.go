package candidateresolution

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	InterpretationContractID            = "fec/independent-expenditure-candidate-interpretations"
	InterpretationContractVersion       = "1.0.0"
	InterpretationManifestSchemaVersion = "legal-tender.fec.independent-expenditure-candidate-interpretation-set.v1"
	InterpretationSchemaVersion         = "legal-tender.fec.independent-expenditure-candidate-interpretation.v1"
	InterpretationMethodVersion         = "legal-tender.fec.independent-expenditure-candidate-interpretation-method.v1"
	InterpretationPublisherVersion      = "legal-tender.fec.independent-expenditure-candidate-interpretation-publisher.v1"

	InterpretationConfirmed   = "confirmed"
	InterpretationInferred    = "inferred"
	InterpretationConflicting = "conflicting"
	InterpretationUnverified  = "unverified"
	InterpretationAmbiguous   = "ambiguous"
	InterpretationUnresolved  = "unresolved"

	ReportedEndpointConfirmed       = "present_exact_context"
	ReportedEndpointContextConflict = "present_context_conflict"
	ReportedEndpointUnverified      = "present_unverified"
	ReportedEndpointAbsent          = "absent_from_candidate_master"

	AlternativeNone                 = "none"
	AlternativeUniqueExactContext   = "unique_exact_context"
	AlternativeMultipleExactContext = "multiple_exact_context"
	AlternativeNoExactContext       = "no_exact_context"
	AlternativeInsufficientContext  = "insufficient_context"
)

type InterpretationPublishInput struct {
	CandidateResolutionManifestPath string
}

type InterpretationPublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

type InterpretationInputReference struct {
	Role                  string `json:"role"`
	Calculation           string `json:"calculation"`
	CalculationVersion    string `json:"calculation_version"`
	CalculationSetID      string `json:"calculation_set_id"`
	ManifestSHA256        string `json:"manifest_sha256"`
	MethodVersion         string `json:"method_version"`
	DecisionSchemaVersion string `json:"decision_schema_version"`
	DecisionsSHA256       string `json:"decisions_sha256"`
}

type InterpretationMethod struct {
	Version             string   `json:"version"`
	Principles          []string `json:"principles"`
	SafeDefaultEndpoint string   `json:"safe_default_endpoint"`
}

type InterpretationCounts struct {
	SourceDecisions uint64 `json:"source_decisions"`
	Confirmed       uint64 `json:"confirmed"`
	Inferred        uint64 `json:"inferred"`
	Conflicting     uint64 `json:"conflicting"`
	Unverified      uint64 `json:"unverified"`
	Ambiguous       uint64 `json:"ambiguous"`
	Unresolved      uint64 `json:"unresolved"`
	SafeDefaults    uint64 `json:"safe_default_endpoints"`
}

type InterpretationAmounts struct {
	SourceMinorUnits      string `json:"source_minor_units"`
	ConfirmedMinorUnits   string `json:"confirmed_minor_units"`
	InferredMinorUnits    string `json:"inferred_minor_units"`
	ConflictingMinorUnits string `json:"conflicting_minor_units"`
	UnverifiedMinorUnits  string `json:"unverified_minor_units"`
	AmbiguousMinorUnits   string `json:"ambiguous_minor_units"`
	UnresolvedMinorUnits  string `json:"unresolved_minor_units"`
	SafeDefaultMinorUnits string `json:"safe_default_minor_units"`
}

type CandidateInterpretation struct {
	SchemaVersion            string            `json:"schema_version"`
	InterpretationID         string            `json:"interpretation_id"`
	CalculationSetID         string            `json:"calculation_set_id"`
	SourceDecisionID         string            `json:"source_decision_id"`
	FactID                   string            `json:"fact_id"`
	NaturalKey               string            `json:"natural_key"`
	Cycle                    string            `json:"cycle"`
	SpenderCommitteeID       string            `json:"spender_committee_id"`
	SupportOppose            string            `json:"support_oppose"`
	AmountMinorUnits         string            `json:"amount_minor_units"`
	ReportedCandidate        ReportedCandidate `json:"reported_candidate"`
	State                    string            `json:"state"`
	ReportedEndpointState    string            `json:"reported_endpoint_state"`
	AlternativeEndpointState string            `json:"alternative_endpoint_state"`
	AlternativeCandidateID   *string           `json:"alternative_candidate_id"`
	SafeDefaultCandidateID   *string           `json:"safe_default_candidate_id"`
	CandidateFactIDs         []string          `json:"candidate_fact_ids"`
	EvidenceCodes            []string          `json:"evidence_codes"`
	SourceState              string            `json:"source_state"`
	SourceMethod             string            `json:"source_method"`
}

type InterpretationManifest struct {
	Schema                      string                       `json:"$schema"`
	SchemaVersion               string                       `json:"schema_version"`
	CalculationSetID            string                       `json:"calculation_set_id"`
	Calculation                 string                       `json:"calculation"`
	CalculationVersion          string                       `json:"calculation_version"`
	PublisherVersion            string                       `json:"publisher_version"`
	InterpretationSchemaVersion string                       `json:"interpretation_schema_version"`
	Cycle                       string                       `json:"cycle"`
	SourceReleaseID             string                       `json:"source_release_id"`
	InputResolution             InterpretationInputReference `json:"input_candidate_resolution"`
	RunID                       string                       `json:"run_id"`
	State                       string                       `json:"state"`
	PublishedAt                 time.Time                    `json:"published_at"`
	Method                      InterpretationMethod         `json:"method"`
	Counts                      InterpretationCounts         `json:"counts"`
	Amounts                     InterpretationAmounts        `json:"amounts"`
	Interpretations             storageartifact.Descriptor   `json:"interpretations"`
	Checks                      []Check                      `json:"checks"`
}

type interpretationAmountState struct {
	source, confirmed, inferred, conflicting, unverified, ambiguous, unresolved, safeDefault big.Int
}

// WalkPublishedInterpretations resolves an exact published interpretation set,
// verifies its immutable manifest and complete artifact, and visits each
// interpretation once. The visitor cannot bypass artifact validation: a
// successful return means the complete publication matched its manifest.
func WalkPublishedInterpretations(ctx context.Context, storageRoot, path string, visit func(CandidateInterpretation) error) (InterpretationManifest, string, error) {
	if storageRoot == "" || path == "" {
		return InterpretationManifest{}, "", fmt.Errorf("storage root and candidate-interpretation manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return InterpretationManifest{}, "", err
	}
	pointer, _, err := readStrictJSON[InterpretationManifest](path)
	if err != nil {
		return InterpretationManifest{}, "", err
	}
	if err := validateInterpretationManifest(pointer); err != nil {
		return InterpretationManifest{}, "", err
	}
	immutablePath := filepath.Join(storageRoot, interpretationCalculationBase(), "manifests", pointer.CalculationSetID+".json")
	immutable, manifestDigest, err := readStrictJSON[InterpretationManifest](immutablePath)
	if err != nil {
		return InterpretationManifest{}, "", err
	}
	if !reflect.DeepEqual(pointer, immutable) {
		return InterpretationManifest{}, "", fmt.Errorf("candidate-interpretation pointer differs from immutable manifest")
	}
	if err := scanInterpretationManifestBacking(ctx, storageRoot, immutable, visit); err != nil {
		return InterpretationManifest{}, "", err
	}
	return immutable, manifestDigest, nil
}

// PublishInterpretations reclassifies the accepted v1 candidate-resolution
// evidence without changing the source assertion or selecting inferred money.
func PublishInterpretations(ctx context.Context, input InterpretationPublishInput, runID string, options InterpretationPublishOptions) (InterpretationManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.CandidateResolutionManifestPath == "" {
		return InterpretationManifest{}, fmt.Errorf("storage root and candidate-resolution manifest are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return InterpretationManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	resolution, resolutionDigest, err := LoadPublishedManifest(ctx, options.StorageRoot, input.CandidateResolutionManifestPath)
	if err != nil {
		return InterpretationManifest{}, fmt.Errorf("load candidate resolution: %w", err)
	}
	if resolution.Method.Version != MethodVersion {
		return InterpretationManifest{}, fmt.Errorf("candidate interpretation requires resolution method %s", MethodVersion)
	}
	inputReference := InterpretationInputReference{
		Role: "candidate_resolution_evidence", Calculation: resolution.Calculation,
		CalculationVersion: resolution.CalculationVersion, CalculationSetID: resolution.CalculationSetID,
		ManifestSHA256: resolutionDigest, MethodVersion: resolution.Method.Version,
		DecisionSchemaVersion: resolution.DecisionSchemaVersion,
		DecisionsSHA256:       resolution.Decisions.UncompressedSHA256,
	}
	calculationSetID := digestParts(
		InterpretationManifestSchemaVersion, InterpretationContractVersion, InterpretationSchemaVersion,
		InterpretationMethodVersion, InterpretationPublisherVersion, inputReference.CalculationSetID,
		inputReference.ManifestSHA256, inputReference.DecisionsSHA256,
	)
	basePath := interpretationCalculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", resolution.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return InterpretationManifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+resolution.Cycle+".lock"))
	if err != nil {
		return InterpretationManifest{}, err
	}
	defer unlock()
	if existing, err := loadInterpretationManifestIfPresent(currentPath); err != nil {
		return InterpretationManifest{}, err
	} else if existing != nil && existing.CalculationSetID == calculationSetID {
		if err := validateInterpretationManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return InterpretationManifest{}, err
		}
		return *existing, nil
	}
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", calculationSetID+".json")
	if existing, err := loadInterpretationManifestIfPresent(manifestPath); err != nil {
		return InterpretationManifest{}, err
	} else if existing != nil {
		if existing.InputResolution != inputReference {
			return InterpretationManifest{}, fmt.Errorf("immutable candidate-interpretation manifest collision")
		}
		if err := validateInterpretationManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return InterpretationManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return InterpretationManifest{}, err
		}
		return *existing, nil
	}

	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return InterpretationManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	writer, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "interpretations")
	if err != nil {
		return InterpretationManifest{}, err
	}
	defer writer.Abort()
	if options.Progress != nil {
		options.Progress("interpreting candidate endpoints for " + resolution.Cycle)
	}
	counts, amounts, err := publishCandidateInterpretations(ctx, options.StorageRoot, resolution, calculationSetID, writer, options.Progress)
	if err != nil {
		return InterpretationManifest{}, err
	}
	artifact, err := writer.Finalize()
	if err != nil {
		return InterpretationManifest{}, err
	}
	if artifact.RecordCount != counts.SourceDecisions {
		return InterpretationManifest{}, fmt.Errorf("candidate interpretation artifact does not conserve decisions")
	}
	manifest := InterpretationManifest{
		Schema: "manifest.schema.json", SchemaVersion: InterpretationManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: InterpretationContractID,
		CalculationVersion: InterpretationContractVersion, PublisherVersion: InterpretationPublisherVersion,
		InterpretationSchemaVersion: InterpretationSchemaVersion, Cycle: resolution.Cycle,
		SourceReleaseID: resolution.SourceReleaseID, InputResolution: inputReference,
		RunID: runID, State: "published", PublishedAt: options.Clock().UTC(), Method: interpretationMethod(),
		Counts: counts, Amounts: amounts, Interpretations: artifact,
		Checks: []Check{
			{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the exact immutable candidate-resolution manifest and decision artifact passed verification"},
			{ID: "decision_conservation", Passed: interpretationCount(counts) == counts.SourceDecisions && artifact.RecordCount == counts.SourceDecisions, Severity: "block", Detail: "every source decision has exactly one interpretation"},
			{ID: "signed_conservation", Passed: interpretationAmountsConserve(amounts), Severity: "block", Detail: "source signed cents equal the six disjoint interpretation-state amounts"},
			{ID: "reported_assertion_preservation", Passed: true, Severity: "block", Detail: "every interpretation preserves the source-reported candidate fields and source decision identity"},
			{ID: "alternative_separation", Passed: true, Severity: "block", Detail: "unique exact-context alternatives remain separate from reported candidate endpoints"},
			{ID: "safe_default_boundary", Passed: counts.SafeDefaults == counts.Confirmed && amounts.SafeDefaultMinorUnits == amounts.ConfirmedMinorUnits, Severity: "block", Detail: "only exact reported-ID and context agreement supplies a safe default endpoint"},
			{ID: "no_financial_duplication", Passed: true, Severity: "block", Detail: "each source amount occurs once; alternatives are evidence and do not add another amount"},
		},
	}
	if err := validateInterpretationManifest(manifest); err != nil {
		return InterpretationManifest{}, err
	}
	if err := validateInterpretationManifestBacking(ctx, options.StorageRoot, manifest); err != nil {
		return InterpretationManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return InterpretationManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return InterpretationManifest{}, err
	}
	return manifest, nil
}

func publishCandidateInterpretations(ctx context.Context, storageRoot string, resolution Manifest, calculationSetID string, writer *storageartifact.Writer, progress func(string)) (InterpretationCounts, InterpretationAmounts, error) {
	reader, err := storageartifact.Open[Decision](ctx, storageRoot, resolution.Decisions)
	if err != nil {
		return InterpretationCounts{}, InterpretationAmounts{}, err
	}
	defer reader.Abort()
	var counts InterpretationCounts
	var amounts interpretationAmountState
	seen := make(map[string]struct{}, resolution.Counts.SourceEffectiveFacts)
	for {
		decision, ok, err := reader.Next()
		if err != nil {
			return InterpretationCounts{}, InterpretationAmounts{}, err
		}
		if !ok {
			break
		}
		if err := validateDecision(decision); err != nil {
			return InterpretationCounts{}, InterpretationAmounts{}, fmt.Errorf("source decision %d: %w", counts.SourceDecisions+1, err)
		}
		if decision.CalculationSetID != resolution.CalculationSetID || decision.Cycle != resolution.Cycle {
			return InterpretationCounts{}, InterpretationAmounts{}, fmt.Errorf("source decision %s does not match its manifest", decision.DecisionID)
		}
		if _, duplicate := seen[decision.DecisionID]; duplicate {
			return InterpretationCounts{}, InterpretationAmounts{}, fmt.Errorf("duplicate source decision %s", decision.DecisionID)
		}
		seen[decision.DecisionID] = struct{}{}
		interpretation, err := interpretCandidateDecision(decision, calculationSetID)
		if err != nil {
			return InterpretationCounts{}, InterpretationAmounts{}, fmt.Errorf("interpret source decision %s: %w", decision.DecisionID, err)
		}
		if err := validateCandidateInterpretation(interpretation); err != nil {
			return InterpretationCounts{}, InterpretationAmounts{}, err
		}
		amount, err := canonicalMinorUnits(decision.AmountMinorUnits)
		if err != nil {
			return InterpretationCounts{}, InterpretationAmounts{}, err
		}
		addInterpretationMeasures(&counts, &amounts, interpretation, amount)
		if err := writer.WriteJSON(interpretation); err != nil {
			return InterpretationCounts{}, InterpretationAmounts{}, err
		}
		if progress != nil && counts.SourceDecisions%50_000 == 0 {
			progress(fmt.Sprintf("interpreted %d candidate decisions for %s", counts.SourceDecisions, resolution.Cycle))
		}
	}
	if err := reader.Close(); err != nil {
		return InterpretationCounts{}, InterpretationAmounts{}, err
	}
	if counts.SourceDecisions != resolution.Counts.SourceEffectiveFacts || uint64(len(seen)) != resolution.Counts.SourceEffectiveFacts {
		return InterpretationCounts{}, InterpretationAmounts{}, fmt.Errorf("source decision membership is not conserved")
	}
	return counts, interpretationAmounts(amounts), nil
}

func interpretCandidateDecision(source Decision, calculationSetID string) (CandidateInterpretation, error) {
	result := CandidateInterpretation{
		SchemaVersion: InterpretationSchemaVersion, CalculationSetID: calculationSetID,
		SourceDecisionID: source.DecisionID, FactID: source.FactID, NaturalKey: source.NaturalKey,
		Cycle: source.Cycle, SpenderCommitteeID: source.SpenderCommitteeID, SupportOppose: source.SupportOppose,
		AmountMinorUnits: source.AmountMinorUnits, ReportedCandidate: source.ReportedCandidate,
		AlternativeEndpointState: AlternativeNone, CandidateFactIDs: append([]string{}, source.CandidateFactIDs...),
		EvidenceCodes: append([]string{}, source.EvidenceCodes...), SourceState: source.State, SourceMethod: source.Method,
	}
	present := hasEvidenceCode(source.EvidenceCodes, "reported_id_present_in_candidate_master")
	absent := hasEvidenceCode(source.EvidenceCodes, "reported_id_absent_from_candidate_master")
	conflict := hasEvidenceCode(source.EvidenceCodes, "reported_id_context_conflict")
	// The source resolver records a context conflict only after finding the
	// reported ID in the candidate master; the conflict code therefore implies
	// presence even when the redundant presence code is omitted.
	present = present || conflict
	switch source.State {
	case StateConfirmed:
		if source.ResolvedCandidateID == nil || !present || absent || conflict {
			return CandidateInterpretation{}, fmt.Errorf("confirmed evidence is inconsistent")
		}
		result.State = InterpretationConfirmed
		result.ReportedEndpointState = ReportedEndpointConfirmed
		result.SafeDefaultCandidateID = textCopyPointer(*source.ResolvedCandidateID)
	case StateResolved:
		if source.ResolvedCandidateID == nil || present == absent || conflict != present {
			return CandidateInterpretation{}, fmt.Errorf("resolved evidence does not identify an absent or conflicting reported endpoint")
		}
		result.AlternativeEndpointState = AlternativeUniqueExactContext
		result.AlternativeCandidateID = textCopyPointer(*source.ResolvedCandidateID)
		if absent {
			result.State = InterpretationInferred
			result.ReportedEndpointState = ReportedEndpointAbsent
		} else {
			result.State = InterpretationConflicting
			result.ReportedEndpointState = ReportedEndpointContextConflict
		}
	case StateUnverified:
		if source.ResolvedCandidateID == nil || !present || absent || conflict {
			return CandidateInterpretation{}, fmt.Errorf("unverified evidence is inconsistent")
		}
		result.State = InterpretationUnverified
		result.ReportedEndpointState = ReportedEndpointUnverified
		if source.Method == MethodReportedIDInsufficient {
			result.AlternativeEndpointState = AlternativeInsufficientContext
		} else {
			result.AlternativeEndpointState = AlternativeNoExactContext
		}
	case StateAmbiguous:
		if present == absent || conflict != present {
			return CandidateInterpretation{}, fmt.Errorf("ambiguous evidence does not identify reported endpoint membership")
		}
		result.State = InterpretationAmbiguous
		result.AlternativeEndpointState = AlternativeMultipleExactContext
		if present {
			result.ReportedEndpointState = ReportedEndpointContextConflict
		} else {
			result.ReportedEndpointState = ReportedEndpointAbsent
		}
	case StateUnresolved:
		if present || !absent || conflict {
			return CandidateInterpretation{}, fmt.Errorf("unresolved evidence is inconsistent")
		}
		result.State = InterpretationUnresolved
		result.ReportedEndpointState = ReportedEndpointAbsent
		if source.Method == MethodInsufficientContext {
			result.AlternativeEndpointState = AlternativeInsufficientContext
		} else {
			result.AlternativeEndpointState = AlternativeNoExactContext
		}
	default:
		return CandidateInterpretation{}, fmt.Errorf("unsupported source state %s", source.State)
	}
	result.InterpretationID = digestParts(
		"fec.independent-expenditure-candidate-interpretation.v1", calculationSetID, source.DecisionID,
		result.State, result.ReportedEndpointState, result.AlternativeEndpointState,
		pointerValue(result.AlternativeCandidateID), pointerValue(result.SafeDefaultCandidateID),
	)
	return result, nil
}

func addInterpretationMeasures(counts *InterpretationCounts, amounts *interpretationAmountState, interpretation CandidateInterpretation, amount *big.Int) {
	counts.SourceDecisions++
	amounts.source.Add(&amounts.source, amount)
	switch interpretation.State {
	case InterpretationConfirmed:
		counts.Confirmed++
		amounts.confirmed.Add(&amounts.confirmed, amount)
	case InterpretationInferred:
		counts.Inferred++
		amounts.inferred.Add(&amounts.inferred, amount)
	case InterpretationConflicting:
		counts.Conflicting++
		amounts.conflicting.Add(&amounts.conflicting, amount)
	case InterpretationUnverified:
		counts.Unverified++
		amounts.unverified.Add(&amounts.unverified, amount)
	case InterpretationAmbiguous:
		counts.Ambiguous++
		amounts.ambiguous.Add(&amounts.ambiguous, amount)
	case InterpretationUnresolved:
		counts.Unresolved++
		amounts.unresolved.Add(&amounts.unresolved, amount)
	}
	if interpretation.SafeDefaultCandidateID != nil {
		counts.SafeDefaults++
		amounts.safeDefault.Add(&amounts.safeDefault, amount)
	}
}

func interpretationAmounts(state interpretationAmountState) InterpretationAmounts {
	return InterpretationAmounts{
		SourceMinorUnits: state.source.String(), ConfirmedMinorUnits: state.confirmed.String(),
		InferredMinorUnits: state.inferred.String(), ConflictingMinorUnits: state.conflicting.String(),
		UnverifiedMinorUnits: state.unverified.String(), AmbiguousMinorUnits: state.ambiguous.String(),
		UnresolvedMinorUnits: state.unresolved.String(), SafeDefaultMinorUnits: state.safeDefault.String(),
	}
}

func interpretationMethod() InterpretationMethod {
	return InterpretationMethod{
		Version: InterpretationMethodVersion,
		Principles: []string{
			"preserve the filer-reported candidate assertion without rewrite",
			"publish a unique exact-context endpoint as a separate alternative",
			"distinguish absent-master inference from present-ID context conflict",
			"carry each source amount once and never add money for an alternative endpoint",
		},
		SafeDefaultEndpoint: "only a reported candidate ID corroborated by exact normalized name and office context",
	}
}

func validateCandidateInterpretation(value CandidateInterpretation) error {
	if value.SchemaVersion != InterpretationSchemaVersion || !validDigest(value.InterpretationID) || !validDigest(value.CalculationSetID) ||
		!validDigest(value.SourceDecisionID) || !validDigest(value.FactID) || value.NaturalKey == "" || !validCycle(value.Cycle) ||
		value.SpenderCommitteeID == "" || value.ReportedCandidate.CandidateID == "" ||
		(value.SupportOppose != "S" && value.SupportOppose != "O") || value.CandidateFactIDs == nil ||
		value.EvidenceCodes == nil || len(value.EvidenceCodes) == 0 || value.SourceState == "" || value.SourceMethod == "" {
		return fmt.Errorf("candidate interpretation identity or evidence is incomplete")
	}
	if _, err := canonicalMinorUnits(value.AmountMinorUnits); err != nil {
		return err
	}
	hasAlternative := value.AlternativeCandidateID != nil && *value.AlternativeCandidateID != ""
	hasDefault := value.SafeDefaultCandidateID != nil && *value.SafeDefaultCandidateID != ""
	switch value.State {
	case InterpretationConfirmed:
		if value.ReportedEndpointState != ReportedEndpointConfirmed || value.AlternativeEndpointState != AlternativeNone || hasAlternative || !hasDefault || *value.SafeDefaultCandidateID != value.ReportedCandidate.CandidateID {
			return fmt.Errorf("confirmed interpretation endpoints are inconsistent")
		}
	case InterpretationInferred:
		if value.ReportedEndpointState != ReportedEndpointAbsent || value.AlternativeEndpointState != AlternativeUniqueExactContext || !hasAlternative || hasDefault {
			return fmt.Errorf("inferred interpretation endpoints are inconsistent")
		}
	case InterpretationConflicting:
		if value.ReportedEndpointState != ReportedEndpointContextConflict || value.AlternativeEndpointState != AlternativeUniqueExactContext || !hasAlternative || hasDefault {
			return fmt.Errorf("conflicting interpretation endpoints are inconsistent")
		}
	case InterpretationUnverified:
		if value.ReportedEndpointState != ReportedEndpointUnverified || hasAlternative || hasDefault || (value.AlternativeEndpointState != AlternativeNoExactContext && value.AlternativeEndpointState != AlternativeInsufficientContext) {
			return fmt.Errorf("unverified interpretation endpoints are inconsistent")
		}
	case InterpretationAmbiguous:
		if (value.ReportedEndpointState != ReportedEndpointAbsent && value.ReportedEndpointState != ReportedEndpointContextConflict) || value.AlternativeEndpointState != AlternativeMultipleExactContext || hasAlternative || hasDefault {
			return fmt.Errorf("ambiguous interpretation endpoints are inconsistent")
		}
	case InterpretationUnresolved:
		if value.ReportedEndpointState != ReportedEndpointAbsent || hasAlternative || hasDefault || (value.AlternativeEndpointState != AlternativeNoExactContext && value.AlternativeEndpointState != AlternativeInsufficientContext) {
			return fmt.Errorf("unresolved interpretation endpoints are inconsistent")
		}
	default:
		return fmt.Errorf("unsupported candidate interpretation state %s", value.State)
	}
	expectedID := digestParts(
		"fec.independent-expenditure-candidate-interpretation.v1", value.CalculationSetID, value.SourceDecisionID,
		value.State, value.ReportedEndpointState, value.AlternativeEndpointState,
		pointerValue(value.AlternativeCandidateID), pointerValue(value.SafeDefaultCandidateID),
	)
	if value.InterpretationID != expectedID {
		return fmt.Errorf("candidate interpretation ID does not match canonical identity")
	}
	for index, factID := range value.CandidateFactIDs {
		if !validDigest(factID) || (index != 0 && value.CandidateFactIDs[index-1] >= factID) {
			return fmt.Errorf("candidate interpretation fact evidence is invalid or unordered")
		}
	}
	seenCodes := make(map[string]struct{}, len(value.EvidenceCodes))
	for _, code := range value.EvidenceCodes {
		if code == "" {
			return fmt.Errorf("candidate interpretation evidence code is empty")
		}
		if _, duplicate := seenCodes[code]; duplicate {
			return fmt.Errorf("candidate interpretation evidence code is duplicated")
		}
		seenCodes[code] = struct{}{}
	}
	wantSourceState := map[string]string{
		InterpretationConfirmed: StateConfirmed, InterpretationInferred: StateResolved,
		InterpretationConflicting: StateResolved, InterpretationUnverified: StateUnverified,
		InterpretationAmbiguous: StateAmbiguous, InterpretationUnresolved: StateUnresolved,
	}[value.State]
	if value.SourceState != wantSourceState {
		return fmt.Errorf("candidate interpretation source state is inconsistent")
	}
	return nil
}

func validateInterpretationManifest(manifest InterpretationManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != InterpretationManifestSchemaVersion ||
		manifest.Calculation != InterpretationContractID || manifest.CalculationVersion != InterpretationContractVersion ||
		manifest.PublisherVersion != InterpretationPublisherVersion || manifest.InterpretationSchemaVersion != InterpretationSchemaVersion ||
		manifest.State != "published" || !reflect.DeepEqual(manifest.Method, interpretationMethod()) {
		return fmt.Errorf("unsupported candidate-interpretation manifest")
	}
	if !validDigest(manifest.CalculationSetID) || !validCycle(manifest.Cycle) || !validSourceReleaseID(manifest.SourceReleaseID) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("candidate-interpretation manifest identity is incomplete")
	}
	input := manifest.InputResolution
	if input.Role != "candidate_resolution_evidence" || input.Calculation != ContractID || input.CalculationVersion != ContractVersion ||
		input.MethodVersion != MethodVersion || input.DecisionSchemaVersion != DecisionSchemaVersion ||
		!validDigest(input.CalculationSetID) || !validDigest(input.ManifestSHA256) || !validDigest(input.DecisionsSHA256) {
		return fmt.Errorf("candidate-interpretation input is invalid")
	}
	expectedID := digestParts(
		InterpretationManifestSchemaVersion, InterpretationContractVersion, InterpretationSchemaVersion,
		InterpretationMethodVersion, InterpretationPublisherVersion, input.CalculationSetID,
		input.ManifestSHA256, input.DecisionsSHA256,
	)
	if manifest.CalculationSetID != expectedID || interpretationCount(manifest.Counts) != manifest.Counts.SourceDecisions ||
		manifest.Counts.SafeDefaults != manifest.Counts.Confirmed || !interpretationAmountsConserve(manifest.Amounts) ||
		manifest.Amounts.SafeDefaultMinorUnits != manifest.Amounts.ConfirmedMinorUnits ||
		manifest.Interpretations.RecordCount != manifest.Counts.SourceDecisions {
		return fmt.Errorf("candidate-interpretation identity, counts, or amounts are inconsistent")
	}
	if manifest.Interpretations.Compression != "zstd" || manifest.Interpretations.CompressedBytes == 0 ||
		!validDigest(manifest.Interpretations.CompressedSHA256) || !validDigest(manifest.Interpretations.UncompressedSHA256) ||
		!strings.HasPrefix(manifest.Interpretations.StorageKey, interpretationCalculationBase()+"/interpretations/sha256/") || len(manifest.Checks) < 7 {
		return fmt.Errorf("candidate-interpretation artifact or checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("candidate-interpretation blocking check %s failed", check.ID)
		}
	}
	return nil
}

func interpretationCount(counts InterpretationCounts) uint64 {
	return counts.Confirmed + counts.Inferred + counts.Conflicting + counts.Unverified + counts.Ambiguous + counts.Unresolved
}

func interpretationAmountsConserve(amounts InterpretationAmounts) bool {
	total, ok := new(big.Int).SetString(amounts.SourceMinorUnits, 10)
	if !ok {
		return false
	}
	var sum big.Int
	for _, text := range []string{amounts.ConfirmedMinorUnits, amounts.InferredMinorUnits, amounts.ConflictingMinorUnits, amounts.UnverifiedMinorUnits, amounts.AmbiguousMinorUnits, amounts.UnresolvedMinorUnits} {
		value, valid := new(big.Int).SetString(text, 10)
		if !valid {
			return false
		}
		sum.Add(&sum, value)
	}
	return sum.Cmp(total) == 0
}

func interpretationCalculationBase() string {
	return filepath.Join("calculations", "fec", "independent-expenditure-candidate-interpretations", "v1")
}

func loadInterpretationManifestIfPresent(path string) (*InterpretationManifest, error) {
	manifest, _, err := readStrictJSON[InterpretationManifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := validateInterpretationManifest(manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func validateInterpretationManifestBacking(ctx context.Context, storageRoot string, manifest InterpretationManifest) error {
	return scanInterpretationManifestBacking(ctx, storageRoot, manifest, nil)
}

func scanInterpretationManifestBacking(ctx context.Context, storageRoot string, manifest InterpretationManifest, visit func(CandidateInterpretation) error) error {
	if err := validateInterpretationManifest(manifest); err != nil {
		return err
	}
	path, err := storageartifact.Resolve(storageRoot, manifest.Interpretations.StorageKey)
	if err != nil {
		return err
	}
	if err := storageartifact.Verify(ctx, path, manifest.Interpretations); err != nil {
		return fmt.Errorf("verify candidate-interpretation artifact: %w", err)
	}
	reader, err := storageartifact.Open[CandidateInterpretation](ctx, storageRoot, manifest.Interpretations)
	if err != nil {
		return err
	}
	defer reader.Abort()
	var counts InterpretationCounts
	var amountState interpretationAmountState
	seen := make(map[string]struct{}, manifest.Interpretations.RecordCount)
	for {
		value, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := validateCandidateInterpretation(value); err != nil {
			return err
		}
		if value.CalculationSetID != manifest.CalculationSetID || value.Cycle != manifest.Cycle {
			return fmt.Errorf("candidate interpretation does not match its manifest")
		}
		if _, duplicate := seen[value.InterpretationID]; duplicate {
			return fmt.Errorf("duplicate candidate interpretation %s", value.InterpretationID)
		}
		seen[value.InterpretationID] = struct{}{}
		if visit != nil {
			if err := visit(value); err != nil {
				return fmt.Errorf("visit candidate interpretation %s: %w", value.InterpretationID, err)
			}
		}
		amount, err := canonicalMinorUnits(value.AmountMinorUnits)
		if err != nil {
			return err
		}
		addInterpretationMeasures(&counts, &amountState, value, amount)
	}
	if err := reader.Close(); err != nil {
		return err
	}
	if uint64(len(seen)) != manifest.Interpretations.RecordCount || counts != manifest.Counts || interpretationAmounts(amountState) != manifest.Amounts {
		return fmt.Errorf("candidate-interpretation readback differs from manifest")
	}
	return nil
}

func hasEvidenceCode(codes []string, wanted string) bool {
	for _, code := range codes {
		if code == wanted {
			return true
		}
	}
	return false
}

func textCopyPointer(value string) *string {
	copy := value
	return &copy
}
