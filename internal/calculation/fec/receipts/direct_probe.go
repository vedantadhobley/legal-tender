package receipts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/klauspost/compress/zstd"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const DirectProbeSchemaVersion = "legal-tender.fec.candidate-itemized-individual-receipts-direct-probe.v2"

type DirectProbeInput struct {
	Cycle                         string
	ReleaseManifestPath           string
	OccurrenceManifestPath        string
	LinkageFactManifestPath       string
	AllCandidatesFactManifestPath string
	CurrentCampaignsManifestPath  string
}

type DirectProbeOptions struct {
	StorageRoot string
	Clock       func() time.Time
	Progress    func(string)
}

// DirectProbeManifest records one non-production calculation probe. It has no
// active pointer and emits no receipt facts or per-row decision artifact.
type DirectProbeManifest struct {
	SchemaVersion               string                       `json:"schema_version"`
	ProbeID                     string                       `json:"probe_id"`
	RunID                       string                       `json:"run_id"`
	Cycle                       string                       `json:"cycle"`
	Calculation                 string                       `json:"calculation"`
	CalculationVersion          string                       `json:"calculation_version"`
	SourceReleaseID             string                       `json:"source_release_id"`
	SourceReleaseManifestSHA256 string                       `json:"source_release_manifest_sha256"`
	OccurrenceSetID             string                       `json:"occurrence_set_id"`
	OccurrenceManifestSHA256    string                       `json:"occurrence_manifest_sha256"`
	InputFactSets               []FactSetReference           `json:"input_fact_sets"`
	StartedAt                   time.Time                    `json:"started_at"`
	CompletedAt                 time.Time                    `json:"completed_at"`
	ElapsedMilliseconds         int64                        `json:"elapsed_milliseconds"`
	Counts                      DirectProbeCounts            `json:"counts"`
	SourceDecisions             DirectProbeDecisionCounts    `json:"source_decisions"`
	Reconciliations             DirectProbeReconciliationSet `json:"reconciliations"`
	Results                     storageartifact.Descriptor   `json:"results"`
	ManifestStorageKey          string                       `json:"manifest_storage_key"`
	Checks                      []Check                      `json:"checks"`
}

type DirectProbeCounts struct {
	SourceRows                uint64 `json:"source_rows"`
	ValidatedRows             uint64 `json:"validated_rows"`
	RoutedRows                uint64 `json:"candidate_routed_rows"`
	RowsWithoutCandidateRoute uint64 `json:"rows_without_candidate_route"`
	RoutedInvalidReceiptDates uint64 `json:"routed_invalid_receipt_dates"`
	LinkageFacts              uint64 `json:"linkage_facts"`
	SummaryFacts              uint64 `json:"summary_facts"`
	Candidates                uint64 `json:"candidates"`
	CompleteCandidates        uint64 `json:"complete_candidates"`
	PartialCandidates         uint64 `json:"partial_candidates"`
	NotComparableCandidates   uint64 `json:"not_comparable_candidates"`
}

type DirectProbeDecisionCounts struct {
	Included                  uint64 `json:"included"`
	ExcludedNonIndividual     uint64 `json:"excluded_non_individual"`
	ExcludedMemoSubtotal      uint64 `json:"excluded_memo_subtotal"`
	UnresolvedIndividualClass uint64 `json:"unresolved_individual_class"`
	UnresolvedAmount          uint64 `json:"unresolved_amount"`
	IncludedAmountMinorUnits  string `json:"included_amount_minor_units"`
}

type DirectProbeReconciliationCounts struct {
	Total                   uint64 `json:"total"`
	Comparable              uint64 `json:"comparable"`
	Exact                   uint64 `json:"exact"`
	WithinFivePercent       uint64 `json:"within_five_percent"`
	WithinTenPercent        uint64 `json:"within_ten_percent"`
	WithinTwentyFivePercent uint64 `json:"within_twenty_five_percent"`
	NonzeroDifference       uint64 `json:"nonzero_difference"`
	ZeroSummary             uint64 `json:"zero_summary"`
	NonzeroSummary          uint64 `json:"nonzero_summary"`
	NotComparable           uint64 `json:"not_comparable"`
}

type DirectProbeReconciliationSet struct {
	Overall    DirectProbeReconciliationCounts            `json:"overall"`
	ByFactType map[string]DirectProbeReconciliationCounts `json:"by_summary_fact_type"`
}

type directProbeScan struct {
	SourceRows               uint64
	ValidatedRows            uint64
	RoutedRows               uint64
	InvalidReceiptDates      uint64
	Decisions                DirectProbeDecisionCounts
	includedAmountMinorUnits int64
}

type directScheduleAIndexes struct {
	committeeID  int
	receivedAt   int
	amount       int
	memoCode     int
	isIndividual int
}

// ProbeDirect runs the accepted candidate itemized-individual calculation
// directly over one staged Schedule A COPY relation. It validates the exact
// source stream but deliberately does not publish receipt facts, row decisions,
// or a production current pointer.
func ProbeDirect(ctx context.Context, input DirectProbeInput, runID string, options DirectProbeOptions) (DirectProbeManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" {
		return DirectProbeManifest{}, fmt.Errorf("storage root is required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return DirectProbeManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	for name, path := range map[string]string{
		"release manifest":                input.ReleaseManifestPath,
		"occurrence manifest":             input.OccurrenceManifestPath,
		"linkage fact manifest":           input.LinkageFactManifestPath,
		"all-candidates fact manifest":    input.AllCandidatesFactManifestPath,
		"current-campaigns fact manifest": input.CurrentCampaignsManifestPath,
	} {
		if path == "" {
			return DirectProbeManifest{}, fmt.Errorf("%s path is required", name)
		}
	}
	if input.Cycle == "" {
		return DirectProbeManifest{}, fmt.Errorf("cycle is required")
	}

	started := options.Clock().UTC()
	releaseManifest, releaseDigest, output, err := loadDirectProbeRelease(options.StorageRoot, input.ReleaseManifestPath, input.Cycle)
	if err != nil {
		return DirectProbeManifest{}, err
	}
	occurrenceManifest, occurrenceDigest, err := loadDirectProbeOccurrence(options.StorageRoot, input.OccurrenceManifestPath, input.Cycle)
	if err != nil {
		return DirectProbeManifest{}, err
	}
	if occurrenceManifest.SourceReleaseID != releaseManifest.ReleaseID || occurrenceManifest.SourceReleaseManifestSHA256 != releaseDigest {
		return DirectProbeManifest{}, fmt.Errorf("Schedule A occurrence set does not name the exact source release manifest")
	}
	if occurrenceManifest.StagedOutputSHA256 != output.CompressedSHA256 || occurrenceManifest.SourceArtifactSHA256 != output.SourceArtifactSHA256 || occurrenceManifest.Relation != output.Selection {
		return DirectProbeManifest{}, fmt.Errorf("Schedule A occurrence set does not describe the selected staged relation")
	}
	if output.RowCount == nil || *output.RowCount != occurrenceManifest.Counts.Total {
		return DirectProbeManifest{}, fmt.Errorf("staged Schedule A row count does not match occurrence evidence")
	}

	classicInputs := []struct {
		role, dataset, path string
	}{
		{"candidate_committee_linkage", "candidate-committee-linkage", input.LinkageFactManifestPath},
		{"all_candidates_summary", "all-candidates-summary", input.AllCandidatesFactManifestPath},
		{"current_campaigns_summary", "current-campaigns-summary", input.CurrentCampaignsManifestPath},
	}
	classicManifests := make(map[string]fecoccurrence.ClassicFactManifest, len(classicInputs))
	references := make([]FactSetReference, 0, len(classicInputs))
	for _, selected := range classicInputs {
		manifest, digest, loadErr := loadClassicFactManifest(options.StorageRoot, selected.path, selected.dataset)
		if loadErr != nil {
			return DirectProbeManifest{}, fmt.Errorf("load %s facts: %w", selected.dataset, loadErr)
		}
		if manifest.Cycle != occurrenceManifest.Cycle || manifest.SourceReleaseID != releaseManifest.ReleaseID {
			return DirectProbeManifest{}, fmt.Errorf("%s facts do not share the Schedule A cycle and source release", selected.dataset)
		}
		if manifest.SourceReleaseManifestSHA256 != "" && manifest.SourceReleaseManifestSHA256 != releaseDigest {
			return DirectProbeManifest{}, fmt.Errorf("%s facts do not name the exact source release manifest", selected.dataset)
		}
		classicManifests[selected.dataset] = manifest
		references = append(references, FactSetReference{
			Role: selected.role, Dataset: selected.dataset, FactType: manifest.FactType,
			FactSetID: manifest.FactSetID, ManifestSHA256: digest,
		})
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Role < references[right].Role })
	identityParts := []string{DirectProbeSchemaVersion, runID, releaseDigest, occurrenceDigest, ContractVersion}
	for _, reference := range references {
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	probeID := digestParts(identityParts...)
	basePath := filepath.Join("probes", "fec", "candidate-itemized-individual-receipts")
	manifestStorageKey := filepath.ToSlash(filepath.Join(basePath, "manifests", probeID+".json"))
	manifestPath, err := storageartifact.Resolve(options.StorageRoot, manifestStorageKey)
	if err != nil {
		return DirectProbeManifest{}, err
	}
	if existing, _, readErr := readStrictJSON[DirectProbeManifest](manifestPath); readErr == nil {
		if err := validateReusableDirectProbe(ctx, options.StorageRoot, existing, probeID, runID, occurrenceManifest.Cycle, releaseManifest.ReleaseID, releaseDigest, occurrenceManifest.OccurrenceSetID, occurrenceDigest, references); err != nil {
			return DirectProbeManifest{}, err
		}
		return existing, nil
	} else if !os.IsNotExist(readErr) {
		return DirectProbeManifest{}, readErr
	}

	linkages, err := loadLinkages(ctx, options.StorageRoot, classicManifests["candidate-committee-linkage"])
	if err != nil {
		return DirectProbeManifest{}, err
	}
	summaries, err := loadSummaries(ctx, options.StorageRoot, classicManifests["all-candidates-summary"])
	if err != nil {
		return DirectProbeManifest{}, err
	}
	currentSummaries, err := loadSummaries(ctx, options.StorageRoot, classicManifests["current-campaigns-summary"])
	if err != nil {
		return DirectProbeManifest{}, err
	}
	summaries = append(summaries, currentSummaries...)
	calculator, err := NewCycleCalculator(occurrenceManifest.Cycle, nil, linkages, summaries)
	if err != nil {
		return DirectProbeManifest{}, err
	}

	stagedPath, err := storageartifact.Resolve(options.StorageRoot, output.StorageKey)
	if err != nil {
		return DirectProbeManifest{}, err
	}
	if options.Progress != nil {
		options.Progress("probing candidate itemized-individual receipts directly from staged Schedule A " + occurrenceManifest.Cycle)
	}
	scan, err := scanDirectScheduleA(ctx, stagedPath, output, occurrenceManifest.Cycle, calculator, options.Progress)
	if err != nil {
		return DirectProbeManifest{}, err
	}
	if scan.SourceRows != occurrenceManifest.Counts.Total || scan.ValidatedRows != occurrenceManifest.Counts.Valid {
		return DirectProbeManifest{}, fmt.Errorf("direct probe row conservation does not match occurrence evidence")
	}

	results, err := calculator.Results()
	if err != nil {
		return DirectProbeManifest{}, err
	}
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", probeID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return DirectProbeManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	resultWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "results")
	if err != nil {
		return DirectProbeManifest{}, err
	}
	for _, result := range results {
		if err := resultWriter.WriteJSON(result); err != nil {
			resultWriter.Abort()
			return DirectProbeManifest{}, err
		}
	}
	resultArtifact, err := resultWriter.Finalize()
	if err != nil {
		return DirectProbeManifest{}, err
	}

	counts := directProbeResultCounts(scan, uint64(len(linkages)), uint64(len(summaries)), results)
	reconciliations := directProbeReconciliations(results)
	completed := options.Clock().UTC()
	manifest := DirectProbeManifest{
		SchemaVersion: DirectProbeSchemaVersion, ProbeID: probeID, RunID: runID, Cycle: occurrenceManifest.Cycle,
		Calculation: ContractID, CalculationVersion: ContractVersion,
		SourceReleaseID: releaseManifest.ReleaseID, SourceReleaseManifestSHA256: releaseDigest,
		OccurrenceSetID: occurrenceManifest.OccurrenceSetID, OccurrenceManifestSHA256: occurrenceDigest,
		InputFactSets: references, StartedAt: started, CompletedAt: completed,
		ElapsedMilliseconds: completed.Sub(started).Milliseconds(), Counts: counts, SourceDecisions: scan.Decisions,
		Reconciliations: reconciliations, Results: resultArtifact, ManifestStorageKey: manifestStorageKey,
	}
	manifest.Checks = []Check{
		{ID: "source_identity", Passed: true, Severity: "block", Detail: "the exact staged Schedule A relation matched its coordinated release digests"},
		{ID: "occurrence_uniqueness", Passed: occurrenceManifest.Counts.Total == occurrenceManifest.Counts.UniqueKeys, Severity: "block", Detail: "published occurrence evidence proves every source row has one unique SUB_ID"},
		{ID: "row_conservation", Passed: counts.SourceRows == counts.ValidatedRows, Severity: "block", Detail: "every staged source row passed the contracted 81-field validator"},
		{ID: "decision_conservation", Passed: directProbeDecisionTotal(scan.Decisions) == counts.SourceRows, Severity: "block", Detail: "the accepted source predicate classified every Schedule A row exactly once"},
		{ID: "route_conservation", Passed: counts.SourceRows == counts.RoutedRows+counts.RowsWithoutCandidateRoute, Severity: "block", Detail: "every source row was either routed through a candidate committee relationship or explicitly skipped"},
		{ID: "candidate_conservation", Passed: counts.Candidates == counts.CompleteCandidates+counts.PartialCandidates+counts.NotComparableCandidates, Severity: "block", Detail: "every candidate result has one calculation state"},
		{ID: "no_row_materialization", Passed: true, Severity: "block", Detail: "the probe emitted candidate results only; no receipt facts or per-row decisions were written"},
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return DirectProbeManifest{}, fmt.Errorf("direct probe blocking check %s failed", check.ID)
		}
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return DirectProbeManifest{}, err
	}
	return manifest, nil
}

func validateReusableDirectProbe(
	ctx context.Context,
	storageRoot string,
	manifest DirectProbeManifest,
	probeID string,
	runID string,
	cycle string,
	releaseID string,
	releaseDigest string,
	occurrenceSetID string,
	occurrenceDigest string,
	references []FactSetReference,
) error {
	if manifest.SchemaVersion != DirectProbeSchemaVersion || manifest.ProbeID != probeID || manifest.RunID != runID || manifest.Cycle != cycle || manifest.Calculation != ContractID || manifest.CalculationVersion != ContractVersion {
		return fmt.Errorf("existing direct probe manifest has incompatible identity")
	}
	if manifest.SourceReleaseID != releaseID || manifest.SourceReleaseManifestSHA256 != releaseDigest || manifest.OccurrenceSetID != occurrenceSetID || manifest.OccurrenceManifestSHA256 != occurrenceDigest || !reflect.DeepEqual(manifest.InputFactSets, references) {
		return fmt.Errorf("existing direct probe manifest has incompatible inputs")
	}
	if manifest.Results.RecordCount != manifest.Counts.Candidates || manifest.Results.Compression != "zstd" || manifest.Results.CompressedBytes == 0 {
		return fmt.Errorf("existing direct probe result descriptor is invalid")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("existing direct probe blocking check %s failed", check.ID)
		}
	}
	resultPath, err := storageartifact.Resolve(storageRoot, manifest.Results.StorageKey)
	if err != nil {
		return err
	}
	if err := storageartifact.Verify(ctx, resultPath, manifest.Results); err != nil {
		return fmt.Errorf("verify existing direct probe results: %w", err)
	}
	return nil
}

func loadDirectProbeRelease(storageRoot, path, cycle string) (fecrelease.ReleaseManifest, string, fecrelease.StagedOutput, error) {
	manifest, digest, err := readStrictJSON[fecrelease.ReleaseManifest](path)
	if err != nil {
		return manifest, "", fecrelease.StagedOutput{}, err
	}
	if issues := fecrelease.ValidateKnownManifest(manifest); len(issues) != 0 {
		return manifest, "", fecrelease.StagedOutput{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}
	immutablePath := filepath.Join(storageRoot, "releases", "fec", "manifests", manifest.ReleaseID+".json")
	immutable, _, err := readStrictJSON[fecrelease.ReleaseManifest](immutablePath)
	if err != nil {
		return manifest, "", fecrelease.StagedOutput{}, err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return manifest, "", fecrelease.StagedOutput{}, fmt.Errorf("source release pointer differs from immutable manifest")
	}
	var output *fecrelease.StagedOutput
	for index := range manifest.StagedOutputs {
		candidate := &manifest.StagedOutputs[index]
		if candidate.SourceID == fecrelease.ScheduleASourceID && candidate.SelectionKind == "relation" && candidate.Period == cycle {
			if output != nil {
				return manifest, "", fecrelease.StagedOutput{}, fmt.Errorf("source release has multiple %s Schedule A outputs", cycle)
			}
			output = candidate
		}
	}
	if output == nil {
		return manifest, "", fecrelease.StagedOutput{}, fmt.Errorf("source release has no %s Schedule A output", cycle)
	}
	return manifest, digest, *output, nil
}

func loadDirectProbeOccurrence(storageRoot, path, cycle string) (fecoccurrence.Manifest, string, error) {
	manifest, _, err := readStrictJSON[fecoccurrence.Manifest](path)
	if err != nil {
		return manifest, "", err
	}
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != fecoccurrence.ManifestSchemaVersion || manifest.State != "published" || manifest.Cycle != cycle {
		return manifest, "", fmt.Errorf("unsupported Schedule A occurrence manifest")
	}
	if manifest.Counts.Total == 0 || manifest.Counts.Invalid != 0 || manifest.Counts.InvalidKeys != 0 || manifest.Counts.DuplicateKeys != 0 || manifest.Counts.DuplicateOccurrences != 0 || manifest.Counts.Total != manifest.Counts.Valid || manifest.Counts.Total != manifest.Counts.Keyed || manifest.Counts.Total != manifest.Counts.UniqueKeys {
		return manifest, "", fmt.Errorf("direct probe requires complete, valid, unique Schedule A occurrence evidence")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return manifest, "", fmt.Errorf("Schedule A occurrence blocking check %s failed", check.ID)
		}
	}
	immutablePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", manifest.OccurrenceSetID+".json")
	immutable, digest, err := readStrictJSON[fecoccurrence.Manifest](immutablePath)
	if err != nil {
		return manifest, "", err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return manifest, "", fmt.Errorf("Schedule A occurrence pointer differs from immutable manifest")
	}
	return manifest, digest, nil
}

func scanDirectScheduleA(ctx context.Context, path string, expected fecrelease.StagedOutput, cycle string, calculator *CycleCalculator, progress func(string)) (directProbeScan, error) {
	var result directProbeScan
	indexes, err := directProbeIndexes()
	if err != nil {
		return result, err
	}
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer func() { _ = file.Close() }()
	compressed := &directProbeCountingReader{reader: &directProbeContextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return result, err
	}
	defer decoder.Close()
	uncompressed := &directProbeCountingReader{reader: decoder, hash: sha256.New()}
	rows := schedulea.NewDecoder(uncompressed)
	for rows.Scan() {
		result.SourceRows++
		row := rows.Row()
		if err := schedulea.Validate(row, cycle); err != nil {
			return result, fmt.Errorf("direct probe source row %d: %w", row.Number(), err)
		}
		result.ValidatedRows++
		committeeField, ok := row.Field(indexes.committeeID)
		if !ok {
			return result, fmt.Errorf("direct probe source row %d: missing cmte_id field", row.Number())
		}
		routed := !committeeField.IsNull() && calculator.hasCommitteeRouteBytes(committeeField.Bytes())
		committeeID := ""
		if routed {
			committeeID = string(committeeField.Bytes())
		}
		input, err := directReceiptInput(row, indexes, cycle, committeeID)
		if err != nil {
			return result, fmt.Errorf("direct probe source row %d: %w", row.Number(), err)
		}
		if routed {
			result.RoutedRows++
		}
		decision := decideReceipt(input)
		if err := addDirectProbeDecision(&result, decision); err != nil {
			return result, err
		}
		if routed {
			if decision.State == "included" {
				input.ReceivedOn, err = directReceiptDate(row, indexes.receivedAt)
				if err != nil {
					result.InvalidReceiptDates++
					input.ReceivedOn = nil
				}
			}
			if _, err := calculator.addReceiptInput(input); err != nil {
				return result, err
			}
		}
		if progress != nil && result.SourceRows%10_000_000 == 0 {
			progress(fmt.Sprintf("direct Schedule A probe validated %d rows for %s", result.SourceRows, cycle))
		}
		if result.SourceRows&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return result, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if expected.RowCount == nil || result.SourceRows != *expected.RowCount {
		return result, fmt.Errorf("direct probe read %d rows; want %d", result.SourceRows, pointerUint64(expected.RowCount))
	}
	if compressed.bytes != expected.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != expected.CompressedSHA256 {
		return result, fmt.Errorf("direct probe compressed source identity mismatch")
	}
	if uncompressed.bytes != expected.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != expected.UncompressedSHA256 {
		return result, fmt.Errorf("direct probe uncompressed source identity mismatch")
	}
	result.Decisions.IncludedAmountMinorUnits = strconv.FormatInt(result.includedAmountMinorUnits, 10)
	return result, nil
}

func directProbeIndexes() (directScheduleAIndexes, error) {
	resolve := func(name string) (int, error) {
		index, ok := schedulea.ColumnIndex(name)
		if !ok {
			return 0, fmt.Errorf("Schedule A schema does not contain %s", name)
		}
		return index, nil
	}
	var result directScheduleAIndexes
	for name, destination := range map[string]*int{
		"cmte_id": &result.committeeID, "contb_receipt_dt": &result.receivedAt,
		"contb_receipt_amt": &result.amount, "memo_cd": &result.memoCode, "is_individual": &result.isIndividual,
	} {
		index, err := resolve(name)
		if err != nil {
			return result, err
		}
		*destination = index
	}
	return result, nil
}

func directReceiptInput(row *schedulea.Row, indexes directScheduleAIndexes, cycle, committeeID string) (receiptInput, error) {
	input := receiptInput{Cycle: cycle, CommitteeID: committeeID}
	individual, ok := row.Field(indexes.isIndividual)
	if !ok {
		return input, fmt.Errorf("missing is_individual field")
	}
	if !individual.IsNull() {
		value := len(individual.Bytes()) == 1 && individual.Bytes()[0] == 't'
		input.PublisherClassedIndividual = &value
	}
	memo, ok := row.Field(indexes.memoCode)
	if !ok {
		return input, fmt.Errorf("missing memo_cd field")
	}
	input.MemoedSubtotal = !memo.IsNull() && len(memo.Bytes()) == 1 && memo.Bytes()[0] == 'X'
	if input.PublisherClassedIndividual == nil || !*input.PublisherClassedIndividual || input.MemoedSubtotal {
		return input, nil
	}
	amount, ok := row.Field(indexes.amount)
	if !ok {
		return input, fmt.Errorf("missing contb_receipt_amt field")
	}
	if amount.IsNull() {
		input.AmountObservationState = "source_null"
		return input, nil
	}
	minorUnits, _, issue := schedulea.ParseUSDMinorUnits(string(amount.Bytes()))
	if issue != "" {
		input.AmountObservationState = "invalid"
		return input, nil
	}
	input.AmountObservationState = "reported_value"
	input.AmountMinorUnits = &minorUnits
	return input, nil
}

func directReceiptDate(row *schedulea.Row, index int) (*string, error) {
	value, null, err := directRowText(row, index)
	if err != nil || null {
		return nil, err
	}
	parsed, err := time.Parse("2006-01-02 15:04:05.999999999", value)
	if err != nil {
		return nil, err
	}
	date := parsed.Format("2006-01-02")
	return &date, nil
}

func directRowText(row *schedulea.Row, index int) (string, bool, error) {
	field, ok := row.Field(index)
	if !ok {
		return "", false, fmt.Errorf("missing Schedule A field %d", index)
	}
	if field.IsNull() {
		return "", true, nil
	}
	return string(field.Bytes()), false, nil
}

func addDirectProbeDecision(result *directProbeScan, decision receiptDecision) error {
	switch decision.State {
	case "included":
		result.Decisions.Included++
		value, err := checkedAdd(result.includedAmountMinorUnits, decision.Amount)
		if err != nil {
			return fmt.Errorf("direct probe included source amount: %w", err)
		}
		result.includedAmountMinorUnits = value
	case "excluded_non_individual":
		result.Decisions.ExcludedNonIndividual++
	case "excluded_memo_subtotal":
		result.Decisions.ExcludedMemoSubtotal++
	case "unresolved_individual_class":
		result.Decisions.UnresolvedIndividualClass++
	case "unresolved_amount":
		result.Decisions.UnresolvedAmount++
	default:
		return fmt.Errorf("unsupported direct probe receipt decision %q", decision.State)
	}
	return nil
}

func directProbeDecisionTotal(counts DirectProbeDecisionCounts) uint64 {
	return counts.Included + counts.ExcludedNonIndividual + counts.ExcludedMemoSubtotal + counts.UnresolvedIndividualClass + counts.UnresolvedAmount
}

func directProbeResultCounts(scan directProbeScan, linkageFacts, summaryFacts uint64, results []Result) DirectProbeCounts {
	counts := DirectProbeCounts{
		SourceRows: scan.SourceRows, ValidatedRows: scan.ValidatedRows, RoutedRows: scan.RoutedRows,
		RowsWithoutCandidateRoute: scan.SourceRows - scan.RoutedRows, RoutedInvalidReceiptDates: scan.InvalidReceiptDates,
		LinkageFacts: linkageFacts, SummaryFacts: summaryFacts, Candidates: uint64(len(results)),
	}
	for _, result := range results {
		switch result.State {
		case "complete":
			counts.CompleteCandidates++
		case "partial":
			counts.PartialCandidates++
		case "not_comparable":
			counts.NotComparableCandidates++
		}
	}
	return counts
}

func directProbeReconciliations(results []Result) DirectProbeReconciliationSet {
	set := DirectProbeReconciliationSet{ByFactType: make(map[string]DirectProbeReconciliationCounts)}
	for _, result := range results {
		for _, reconciliation := range result.Reconciliations {
			overall := set.Overall
			addDirectProbeReconciliation(&overall, reconciliation)
			set.Overall = overall
			byType := set.ByFactType[reconciliation.SummaryFactType]
			addDirectProbeReconciliation(&byType, reconciliation)
			set.ByFactType[reconciliation.SummaryFactType] = byType
		}
	}
	return set
}

func addDirectProbeReconciliation(counts *DirectProbeReconciliationCounts, reconciliation SummaryReconciliation) {
	counts.Total++
	if reconciliation.SummaryMinorUnits == nil || reconciliation.ResolvedDetailMinorUnits == nil || reconciliation.DifferenceMinorUnits == nil {
		counts.NotComparable++
		return
	}
	summary, summaryErr := strconv.ParseInt(*reconciliation.SummaryMinorUnits, 10, 64)
	difference, differenceErr := strconv.ParseInt(*reconciliation.DifferenceMinorUnits, 10, 64)
	if summaryErr != nil || differenceErr != nil {
		counts.NotComparable++
		return
	}
	counts.Comparable++
	if difference == 0 {
		counts.Exact++
	} else {
		counts.NonzeroDifference++
	}
	if summary == 0 {
		counts.ZeroSummary++
		if difference == 0 {
			counts.WithinFivePercent++
			counts.WithinTenPercent++
			counts.WithinTwentyFivePercent++
		}
		return
	}
	counts.NonzeroSummary++
	ratio := math.Abs(float64(difference)) / math.Abs(float64(summary))
	if ratio <= 0.05 {
		counts.WithinFivePercent++
	}
	if ratio <= 0.10 {
		counts.WithinTenPercent++
	}
	if ratio <= 0.25 {
		counts.WithinTwentyFivePercent++
	}
}

func pointerUint64(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

type directProbeCountingReader struct {
	reader io.Reader
	hash   hash.Hash
	bytes  uint64
}

func (reader *directProbeCountingReader) Read(destination []byte) (int, error) {
	read, err := reader.reader.Read(destination)
	if read > 0 {
		reader.bytes += uint64(read)
		_, _ = reader.hash.Write(destination[:read])
	}
	return read, err
}

type directProbeContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *directProbeContextReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(destination)
}
