package independentexpenditures

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
	"sort"
	"strings"
	"syscall"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type resultKey struct {
	spender   string
	candidate string
	stance    string
}

type resultAccumulator struct {
	amount                 big.Int
	count                  uint64
	positive               uint64
	negative               uint64
	zero                   uint64
	missingExpenditureType uint64
}

type scanResult struct {
	decisions    DecisionCounts
	routes       RouteCounts
	shapes       SourceShapeCounts
	included     big.Int
	excludedMemo big.Int
	attributed   big.Int
	unattributed big.Int
	groups       map[resultKey]*resultAccumulator
	resultAmount big.Int
	resultRows   uint64
}

// LoadPublishedManifest resolves one exact effective independent-expenditure
// calculation and verifies both immutable backing artifacts before returning
// it to a downstream projection.
func LoadPublishedManifest(ctx context.Context, storageRoot, path string) (Manifest, string, error) {
	if storageRoot == "" || path == "" {
		return Manifest{}, "", fmt.Errorf("storage root and calculation manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return Manifest{}, "", err
	}
	manifest, _, err := readStrictJSON[Manifest](path)
	if err != nil {
		return Manifest{}, "", err
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, "", err
	}
	immutablePath := filepath.Join(storageRoot, calculationBase(), "manifests", manifest.CalculationSetID+".json")
	immutable, digest, err := readStrictJSON[Manifest](immutablePath)
	if err != nil {
		return Manifest{}, "", fmt.Errorf("load immutable independent-expenditure calculation manifest: %w", err)
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return Manifest{}, "", fmt.Errorf("independent-expenditure calculation pointer differs from immutable manifest")
	}
	if err := validateManifestBacking(ctx, storageRoot, manifest); err != nil {
		return Manifest{}, "", err
	}
	return immutable, digest, nil
}

// Publish evaluates the accepted compact membership predicate over one exact
// Schedule E fact set. Ordinary decisions remain reconstructable from the
// predicate; only unresolved amount/route exceptions and grouped results are
// materialized.
func Publish(ctx context.Context, input PublishInput, runID string, options PublishOptions) (Manifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.ScheduleEFactManifestPath == "" {
		return Manifest{}, fmt.Errorf("storage root and Schedule E fact manifest path are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return Manifest{}, fmt.Errorf("run ID contains unsupported characters")
	}

	factManifest, factManifestDigest, err := loadFactManifest(ctx, options.StorageRoot, input.ScheduleEFactManifestPath)
	if err != nil {
		return Manifest{}, err
	}
	reference := FactSetReference{
		Role: "schedule_e_independent_expenditures", Dataset: "schedule-e",
		FactType: factManifest.FactType, FactSetID: factManifest.FactSetID,
		ManifestSHA256: factManifestDigest,
	}
	calculationSetID := digestParts(
		ManifestSchemaVersion, ContractVersion, PredicateVersion,
		ResultSchemaVersion, ExceptionSchemaVersion, PublisherVersion,
		reference.Role, reference.FactSetID, reference.ManifestSHA256,
	)
	basePath := calculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", factManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return Manifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+factManifest.Cycle+".lock"))
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
			return Manifest{}, fmt.Errorf("invalid current independent-expenditure calculation: %w", err)
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return Manifest{}, err
		}
		if current.Cycle != factManifest.Cycle {
			return Manifest{}, fmt.Errorf("current independent-expenditure calculation belongs to cycle %s", current.Cycle)
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
		if !reflect.DeepEqual(existing.InputFactSet, reference) {
			return Manifest{}, fmt.Errorf("immutable independent-expenditure calculation manifest collision")
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return Manifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return Manifest{}, err
		}
		return *existing, nil
	}

	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return Manifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	exceptionWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "exceptions")
	if err != nil {
		return Manifest{}, err
	}
	defer exceptionWriter.Abort()

	if options.Progress != nil {
		options.Progress("calculating effective independent expenditures for " + factManifest.Cycle)
	}
	scan, err := scanFacts(ctx, options.StorageRoot, factManifest, calculationSetID, exceptionWriter, options.Progress)
	if err != nil {
		return Manifest{}, err
	}
	if scan.shapes.NoticeLikeFacts != 0 {
		return Manifest{}, fmt.Errorf("Schedule E fact set contains %d notice-like facts", scan.shapes.NoticeLikeFacts)
	}
	exceptionArtifact, err := exceptionWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	expectedExceptions := scan.decisions.UnresolvedAmount + scan.routes.Unattributed
	if exceptionArtifact.RecordCount != expectedExceptions {
		return Manifest{}, fmt.Errorf("independent-expenditure exceptions are not conserved")
	}

	resultWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "results")
	if err != nil {
		return Manifest{}, err
	}
	defer resultWriter.Abort()
	keys := sortedResultKeys(scan.groups)
	for _, key := range keys {
		accumulator := scan.groups[key]
		result := Result{
			SchemaVersion: ResultSchemaVersion,
			ResultID: digestParts(
				"fec.effective-independent-expenditure.result.v1", calculationSetID,
				factManifest.Cycle, key.spender, key.candidate, key.stance,
			),
			CalculationSetID: calculationSetID, Cycle: factManifest.Cycle,
			SpenderCommitteeID: key.spender, CandidateID: key.candidate, SupportOppose: key.stance,
			SignedAmountMinorUnits: accumulator.amount.String(), ExpenditureCount: accumulator.count,
			PositiveCount: accumulator.positive, NegativeCount: accumulator.negative, ZeroCount: accumulator.zero,
			MissingExpenditureTypeCount: accumulator.missingExpenditureType,
		}
		if result.ExpenditureCount != result.PositiveCount+result.NegativeCount+result.ZeroCount {
			return Manifest{}, fmt.Errorf("result sign counts are not conserved")
		}
		if err := resultWriter.WriteJSON(result); err != nil {
			return Manifest{}, err
		}
		scan.resultAmount.Add(&scan.resultAmount, &accumulator.amount)
		scan.resultRows += accumulator.count
	}
	resultArtifact, err := resultWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	scan.routes.ResultGroups = uint64(len(keys))
	if resultArtifact.RecordCount != scan.routes.ResultGroups || scan.resultRows != scan.routes.Attributed || scan.resultAmount.Cmp(&scan.attributed) != 0 {
		return Manifest{}, fmt.Errorf("independent-expenditure results are not conserved")
	}

	manifest := Manifest{
		Schema: "manifest.schema.json", SchemaVersion: ManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID, CalculationVersion: ContractVersion,
		PublisherVersion: PublisherVersion, ResultSchemaVersion: ResultSchemaVersion, ExceptionSchemaVersion: ExceptionSchemaVersion,
		Cycle: factManifest.Cycle, SourceReleaseID: factManifest.SourceReleaseID, InputFactSet: reference,
		RunID: runID, State: "published", PublishedAt: options.Clock().UTC(), Predicate: membershipPredicate(),
		DecisionCounts: scan.decisions, RouteCounts: scan.routes, SourceShapeCounts: scan.shapes,
		Amounts: AmountTotals{
			IncludedMinorUnits: scan.included.String(), ExcludedMemoMinorUnits: scan.excludedMemo.String(),
			AttributedMinorUnits: scan.attributed.String(), UnattributedMinorUnits: scan.unattributed.String(),
		},
		Exceptions: exceptionArtifact, Results: resultArtifact,
	}
	manifest.Checks = []Check{
		{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the exact immutable Schedule E fact manifest and digest-verified artifact define one cycle input"},
		{ID: "notice_separation", Passed: scan.shapes.NoticeLikeFacts == 0, Severity: "block", Detail: "the processed fact set contains no report type 24 or 48 and no F24 filing"},
		{ID: "decision_conservation", Passed: decisionTotal(scan.decisions) == scan.decisions.SourceFacts, Severity: "block", Detail: "every Schedule E fact is included, excluded as memo X, or unresolved for amount exactly once"},
		{ID: "route_conservation", Passed: scan.decisions.Included == scan.routes.Attributed+scan.routes.Unattributed, Severity: "block", Detail: "every included amount is attributed once or preserved as an unattributed exception"},
		{ID: "signed_conservation", Passed: amountConserves(&scan.included, &scan.attributed, &scan.unattributed) && scan.resultAmount.Cmp(&scan.attributed) == 0, Severity: "block", Detail: "exact signed included cents conserve through attributed, unattributed, and grouped results"},
		{ID: "result_conservation", Passed: scan.resultRows == scan.routes.Attributed && resultArtifact.RecordCount == scan.routes.ResultGroups, Severity: "block", Detail: "every attributed fact contributes once to one spender-candidate-stance result"},
		{ID: "repeated_key_observation", Passed: true, Severity: "warning", Detail: fmt.Sprintf("retained %d occurrences across %d repeated spender-transaction keys without deduplication", scan.shapes.RepeatedTransactionOccurrences, scan.shapes.RepeatedTransactionKeys)},
		{ID: "missing_type_observation", Passed: true, Severity: "warning", Detail: fmt.Sprintf("retained %d facts without an expenditure type", scan.shapes.MissingExpenditureType)},
		{ID: "no_dense_decision_artifact", Passed: true, Severity: "block", Detail: "ordinary decisions are the exact fact-set identity plus the versioned predicate; only sparse exceptions are materialized"},
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

func scanFacts(
	ctx context.Context,
	storageRoot string,
	manifest fecoccurrence.ScheduleEFactManifest,
	calculationSetID string,
	exceptionWriter *storageartifact.Writer,
	progress func(string),
) (scanResult, error) {
	reader, err := storageartifact.Open[fecoccurrence.ScheduleEFact](ctx, storageRoot, factDescriptor(manifest.Facts))
	if err != nil {
		return scanResult{}, err
	}
	defer reader.Abort()
	result := scanResult{groups: make(map[resultKey]*resultAccumulator)}
	factIDs := make(map[string]struct{}, manifest.Counts.Facts)
	transactionCounts := make(map[string]uint64)
	for {
		fact, ok, err := reader.Next()
		if err != nil {
			return result, err
		}
		if !ok {
			break
		}
		result.decisions.SourceFacts++
		if err := validateInputFact(fact, manifest); err != nil {
			return result, fmt.Errorf("Schedule E fact %d: %w", result.decisions.SourceFacts, err)
		}
		if _, duplicate := factIDs[fact.FactID]; duplicate {
			return result, fmt.Errorf("Schedule E fact set contains duplicate fact ID %s", fact.FactID)
		}
		factIDs[fact.FactID] = struct{}{}
		profileFact(fact, &result.shapes, transactionCounts)

		evaluation := EvaluateFact(fact)
		if evaluation.Decision == DecisionExcludedMemo {
			result.decisions.ExcludedMemo++
			if evaluation.Amount != nil {
				result.excludedMemo.Add(&result.excludedMemo, evaluation.Amount)
			} else {
				result.decisions.ExcludedMemoAmountUnresolved++
			}
			continue
		}
		if evaluation.Decision == DecisionUnresolvedAmount {
			result.decisions.UnresolvedAmount++
			exception := newException(calculationSetID, fact, DecisionUnresolvedAmount, nil, []string{evaluation.AmountReason})
			if err := exceptionWriter.WriteJSON(exception); err != nil {
				return result, err
			}
			continue
		}

		result.decisions.Included++
		amount := evaluation.Amount
		result.included.Add(&result.included, amount)
		reasons := evaluation.RouteReasons
		if len(reasons) != 0 {
			result.routes.Unattributed++
			result.unattributed.Add(&result.unattributed, amount)
			for _, reason := range reasons {
				switch reason {
				case "missing_spender":
					result.routes.MissingSpender++
				case "missing_candidate":
					result.routes.MissingCandidate++
				case "invalid_support_oppose":
					result.routes.InvalidSupportOppose++
				}
			}
			amountText := amount.String()
			exception := newException(calculationSetID, fact, "included_unattributed", &amountText, reasons)
			if err := exceptionWriter.WriteJSON(exception); err != nil {
				return result, err
			}
			continue
		}

		result.routes.Attributed++
		result.attributed.Add(&result.attributed, amount)
		key := resultKey{
			spender:   *fact.TypedFields.Spender.CommitteeID,
			candidate: *fact.TypedFields.Candidate.CandidateID,
			stance:    *fact.TypedFields.Candidate.SupportOpposeCode,
		}
		accumulator := result.groups[key]
		if accumulator == nil {
			accumulator = &resultAccumulator{}
			result.groups[key] = accumulator
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
		if fact.TypedFields.Expenditure.ExpenditureTypeCode == nil {
			accumulator.missingExpenditureType++
		}
		if progress != nil && result.decisions.SourceFacts%50_000 == 0 {
			progress(fmt.Sprintf("classified %d Schedule E facts for %s", result.decisions.SourceFacts, manifest.Cycle))
		}
	}
	if err := reader.Close(); err != nil {
		return result, err
	}
	result.shapes.DistinctTransactionKeys = uint64(len(transactionCounts))
	for _, count := range transactionCounts {
		if count > 1 {
			result.shapes.RepeatedTransactionKeys++
			result.shapes.RepeatedTransactionOccurrences += count
		}
	}
	if result.decisions.SourceFacts != manifest.Counts.Facts || uint64(len(factIDs)) != manifest.Counts.Facts {
		return result, fmt.Errorf("Schedule E fact membership is not conserved")
	}
	return result, nil
}

func validateInputFact(fact fecoccurrence.ScheduleEFact, manifest fecoccurrence.ScheduleEFactManifest) error {
	if fact.SchemaVersion != fecoccurrence.ScheduleEFactSchemaVersion || fact.FactType != fecoccurrence.ScheduleEFactType ||
		fact.Cycle != manifest.Cycle || fact.SourceReleaseID != manifest.SourceReleaseID || fact.OccurrenceSetID != manifest.OccurrenceSetID ||
		fact.SourceContract != fecoccurrence.ScheduleESourceContract || (fact.State != "valid" && fact.State != "invalid") {
		return fmt.Errorf("fact identity does not match its manifest")
	}
	if !validDigest(fact.FactID) || !strings.HasPrefix(fact.NaturalKey, "fec:schedule-e:"+manifest.Cycle+":") {
		return fmt.Errorf("fact key is invalid")
	}
	if fact.TypedFields.Election.Cycle != mustCycle(manifest.Cycle) || fact.TypedFields.Filing.SubmissionID == "" {
		return fmt.Errorf("typed cycle or submission identity is invalid")
	}
	return nil
}

func mustCycle(cycle string) int64 {
	var value int64
	for _, character := range cycle {
		value = value*10 + int64(character-'0')
	}
	return value
}

func profileFact(fact fecoccurrence.ScheduleEFact, shapes *SourceShapeCounts, transactionCounts map[string]uint64) {
	if fact.State == "valid" {
		shapes.ValidFacts++
	} else {
		shapes.InvalidFacts++
	}
	if fact.TypedFields.Filing.ActionCode == nil {
		shapes.ActionNull++
	} else {
		switch *fact.TypedFields.Filing.ActionCode {
		case "A":
			shapes.ActionAdd++
		case "C":
			shapes.ActionChange++
		case "N":
			shapes.ActionNoChange++
		case "T":
			shapes.ActionTerminate++
		default:
			shapes.ActionOther++
		}
	}
	if fact.TypedFields.Expenditure.ExpenditureTypeCode == nil {
		shapes.MissingExpenditureType++
	}
	reportType := fact.TypedFields.Filing.ReportTypeCode
	if (reportType != nil && (*reportType == "24" || *reportType == "48")) || fact.TypedFields.Filing.FilingForm == "F24" {
		shapes.NoticeLikeFacts++
	}
	transactionID := fact.TypedFields.Filing.TransactionID
	if transactionID == nil || *transactionID == "" {
		shapes.TransactionIDMissing++
		return
	}
	spender := fact.TypedFields.Spender.CommitteeID
	if spender == nil || *spender == "" {
		return
	}
	shapes.TransactionKeyedFacts++
	transactionCounts[*spender+"\x00"+*transactionID]++
}

func factAmount(fact fecoccurrence.ScheduleEFact) (*big.Int, string, bool) {
	observation := fact.TypedFields.Expenditure.Amount
	if observation.ObservationState == "source_null" {
		return nil, "amount_source_null", false
	}
	if observation.ObservationState != "reported_value" || observation.ReportedMinorUnits == nil {
		return nil, "amount_invalid", false
	}
	value, ok := new(big.Int).SetString(*observation.ReportedMinorUnits, 10)
	if !ok || value.String() != *observation.ReportedMinorUnits {
		return nil, "amount_invalid", false
	}
	return value, "", true
}

func routeReasons(fact fecoccurrence.ScheduleEFact) []string {
	reasons := make([]string, 0, 3)
	if fact.TypedFields.Spender.CommitteeID == nil || *fact.TypedFields.Spender.CommitteeID == "" {
		reasons = append(reasons, "missing_spender")
	}
	if fact.TypedFields.Candidate.CandidateID == nil || *fact.TypedFields.Candidate.CandidateID == "" {
		reasons = append(reasons, "missing_candidate")
	}
	stance := fact.TypedFields.Candidate.SupportOpposeCode
	if stance == nil || (*stance != "S" && *stance != "O") {
		reasons = append(reasons, "invalid_support_oppose")
	}
	return reasons
}

func newException(calculationSetID string, fact fecoccurrence.ScheduleEFact, state string, amount *string, reasons []string) Exception {
	return Exception{
		SchemaVersion: ExceptionSchemaVersion,
		ExceptionID: digestParts(
			"fec.effective-independent-expenditure.exception.v1", calculationSetID,
			fact.FactID, state, strings.Join(reasons, ","),
		),
		CalculationSetID: calculationSetID, FactID: fact.FactID, NaturalKey: fact.NaturalKey,
		State: state, AmountMinorUnits: amount, ReasonCodes: reasons,
	}
}

func sortedResultKeys(groups map[resultKey]*resultAccumulator) []resultKey {
	keys := make([]resultKey, 0, len(groups))
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

func loadFactManifest(ctx context.Context, storageRoot, path string) (fecoccurrence.ScheduleEFactManifest, string, error) {
	manifest, _, err := readStrictJSON[fecoccurrence.ScheduleEFactManifest](path)
	if err != nil {
		return manifest, "", fmt.Errorf("load Schedule E fact manifest: %w", err)
	}
	if err := validateFactManifest(manifest); err != nil {
		return manifest, "", err
	}
	immutablePath := filepath.Join(storageRoot, "facts", "fec", "schedule-e", "manifests", manifest.FactSetID+".json")
	immutable, digest, err := readStrictJSON[fecoccurrence.ScheduleEFactManifest](immutablePath)
	if err != nil {
		return manifest, "", fmt.Errorf("load immutable Schedule E fact manifest: %w", err)
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return manifest, "", fmt.Errorf("Schedule E fact pointer differs from immutable manifest")
	}
	factPath, err := storageartifact.Resolve(storageRoot, manifest.Facts.StorageKey)
	if err != nil {
		return manifest, "", err
	}
	if err := storageartifact.Verify(ctx, factPath, factDescriptor(manifest.Facts)); err != nil {
		return manifest, "", fmt.Errorf("verify Schedule E fact artifact: %w", err)
	}
	return manifest, digest, nil
}

func validateFactManifest(manifest fecoccurrence.ScheduleEFactManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != fecoccurrence.ScheduleEFactSetSchemaVersion ||
		manifest.FactType != fecoccurrence.ScheduleEFactType || manifest.FactSchemaVersion != fecoccurrence.ScheduleEFactSchemaVersion ||
		manifest.NormalizerVersion != fecoccurrence.ScheduleENormalizerVersion || manifest.SourceContract != fecoccurrence.ScheduleESourceContract ||
		manifest.State != "published" || !validCycle(manifest.Cycle) || !validDigest(manifest.FactSetID) || !validSourceReleaseID(manifest.SourceReleaseID) {
		return fmt.Errorf("unsupported Schedule E fact manifest")
	}
	if manifest.Counts.Facts != manifest.Counts.SourceOccurrences || manifest.Counts.Facts != manifest.Counts.ValidFacts+manifest.Counts.InvalidFacts ||
		manifest.Facts.RecordCount != manifest.Counts.Facts {
		return fmt.Errorf("Schedule E fact manifest counts are not conserved")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("Schedule E fact blocking check %s failed", check.ID)
		}
	}
	return nil
}

func factDescriptor(artifact fecoccurrence.Artifact) storageartifact.Descriptor {
	return storageartifact.Descriptor{
		RecordCount: artifact.RecordCount, UncompressedBytes: artifact.UncompressedBytes,
		UncompressedSHA256: artifact.UncompressedSHA256, CompressedBytes: artifact.CompressedBytes,
		CompressedSHA256: artifact.CompressedSHA256, Compression: artifact.Compression, StorageKey: artifact.StorageKey,
	}
}

func decisionTotal(counts DecisionCounts) uint64 {
	return counts.Included + counts.ExcludedMemo + counts.UnresolvedAmount
}

func amountConserves(total, left, right *big.Int) bool {
	var combined big.Int
	combined.Add(left, right)
	return combined.Cmp(total) == 0
}

func calculationBase() string {
	return filepath.Join("calculations", "fec", "effective-independent-expenditures")
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
		manifest.ResultSchemaVersion != ResultSchemaVersion || manifest.ExceptionSchemaVersion != ExceptionSchemaVersion ||
		manifest.State != "published" || !reflect.DeepEqual(manifest.Predicate, membershipPredicate()) {
		return fmt.Errorf("unsupported independent-expenditure calculation manifest")
	}
	if !validDigest(manifest.CalculationSetID) || !validSourceReleaseID(manifest.SourceReleaseID) || !validCycle(manifest.Cycle) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("independent-expenditure calculation identity is incomplete")
	}
	reference := manifest.InputFactSet
	if reference.Role != "schedule_e_independent_expenditures" || reference.Dataset != "schedule-e" ||
		reference.FactType != fecoccurrence.ScheduleEFactType || !validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) {
		return fmt.Errorf("independent-expenditure input fact-set identity is invalid")
	}
	expectedID := digestParts(
		ManifestSchemaVersion, ContractVersion, PredicateVersion,
		ResultSchemaVersion, ExceptionSchemaVersion, PublisherVersion,
		reference.Role, reference.FactSetID, reference.ManifestSHA256,
	)
	if manifest.CalculationSetID != expectedID {
		return fmt.Errorf("independent-expenditure calculation-set ID does not match canonical inputs")
	}
	if decisionTotal(manifest.DecisionCounts) != manifest.DecisionCounts.SourceFacts ||
		manifest.DecisionCounts.ExcludedMemoAmountUnresolved > manifest.DecisionCounts.ExcludedMemo ||
		manifest.DecisionCounts.Included != manifest.RouteCounts.Attributed+manifest.RouteCounts.Unattributed ||
		manifest.SourceShapeCounts.ValidFacts+manifest.SourceShapeCounts.InvalidFacts != manifest.DecisionCounts.SourceFacts ||
		actionTotal(manifest.SourceShapeCounts) != manifest.DecisionCounts.SourceFacts || manifest.SourceShapeCounts.NoticeLikeFacts != 0 ||
		manifest.Results.RecordCount != manifest.RouteCounts.ResultGroups ||
		manifest.Exceptions.RecordCount != manifest.DecisionCounts.UnresolvedAmount+manifest.RouteCounts.Unattributed {
		return fmt.Errorf("independent-expenditure calculation counts are not conserved")
	}
	included, okIncluded := new(big.Int).SetString(manifest.Amounts.IncludedMinorUnits, 10)
	attributed, okAttributed := new(big.Int).SetString(manifest.Amounts.AttributedMinorUnits, 10)
	unattributed, okUnattributed := new(big.Int).SetString(manifest.Amounts.UnattributedMinorUnits, 10)
	_, okMemo := new(big.Int).SetString(manifest.Amounts.ExcludedMemoMinorUnits, 10)
	if !okIncluded || !okAttributed || !okUnattributed || !okMemo || !amountConserves(included, attributed, unattributed) {
		return fmt.Errorf("independent-expenditure calculation amounts are invalid")
	}
	if len(manifest.Checks) < 8 {
		return fmt.Errorf("independent-expenditure calculation checks are incomplete")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("independent-expenditure blocking check %s failed", check.ID)
		}
	}
	return nil
}

func actionTotal(shapes SourceShapeCounts) uint64 {
	return shapes.ActionAdd + shapes.ActionChange + shapes.ActionNoChange + shapes.ActionTerminate + shapes.ActionNull + shapes.ActionOther
}

func validateManifestBacking(ctx context.Context, storageRoot string, manifest Manifest) error {
	for kind, descriptor := range map[string]storageartifact.Descriptor{"exceptions": manifest.Exceptions, "results": manifest.Results} {
		path, err := storageartifact.Resolve(storageRoot, descriptor.StorageKey)
		if err != nil {
			return err
		}
		if err := storageartifact.Verify(ctx, path, descriptor); err != nil {
			return fmt.Errorf("verify independent-expenditure %s artifact: %w", kind, err)
		}
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
		return fmt.Errorf("calculation current-manifest path escapes storage root")
	}
	return nil
}
