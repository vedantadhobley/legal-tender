package receipts

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// PublishCompact evaluates the accepted receipt calculation directly over the
// narrow Parquet projection. Ordinary membership is the versioned predicate;
// only unresolved/invalid membership and candidate results are materialized.
func PublishCompact(ctx context.Context, input CompactPublishInput, runID string, options CompactPublishOptions) (CompactManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" {
		return CompactManifest{}, fmt.Errorf("storage root is required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return CompactManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	for name, path := range map[string]string{
		"columnar Schedule A fact manifest": input.ColumnarFactManifestPath,
		"linkage fact manifest":             input.LinkageFactManifestPath,
		"all-candidates fact manifest":      input.AllCandidatesFactManifestPath,
		"current-campaigns fact manifest":   input.CurrentCampaignsManifestPath,
	} {
		if path == "" {
			return CompactManifest{}, fmt.Errorf("%s path is required", name)
		}
	}

	scheduleManifest, scheduleDigest, err := loadCompactColumnarManifest(ctx, options.StorageRoot, input.ColumnarFactManifestPath)
	if err != nil {
		return CompactManifest{}, fmt.Errorf("load columnar Schedule A facts: %w", err)
	}
	classicInputs := []struct {
		role, dataset, path string
	}{
		{"candidate_committee_linkage", "candidate-committee-linkage", input.LinkageFactManifestPath},
		{"all_candidates_summary", "all-candidates-summary", input.AllCandidatesFactManifestPath},
		{"current_campaigns_summary", "current-campaigns-summary", input.CurrentCampaignsManifestPath},
	}
	classicManifests := make(map[string]fecoccurrence.ClassicFactManifest, len(classicInputs))
	references := []FactSetReference{{
		Role: "schedule_a_receipts", Dataset: "schedule-a", FactType: scheduleManifest.FactType,
		FactSetID: scheduleManifest.FactSetID, ManifestSHA256: scheduleDigest,
	}}
	for _, selected := range classicInputs {
		manifest, digest, loadErr := loadClassicFactManifest(options.StorageRoot, selected.path, selected.dataset)
		if loadErr != nil {
			return CompactManifest{}, fmt.Errorf("load %s facts: %w", selected.dataset, loadErr)
		}
		if manifest.Cycle != scheduleManifest.Cycle || manifest.SourceReleaseID != scheduleManifest.SourceReleaseID {
			return CompactManifest{}, fmt.Errorf("%s facts do not share columnar Schedule A cycle and source release", selected.dataset)
		}
		classicManifests[selected.dataset] = manifest
		references = append(references, FactSetReference{
			Role: selected.role, Dataset: selected.dataset, FactType: manifest.FactType,
			FactSetID: manifest.FactSetID, ManifestSHA256: digest,
		})
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Role < references[right].Role })
	identityParts := []string{CompactManifestSchemaVersion, ContractVersion, CompactMembershipPredicateV1, ResultSchemaVersion}
	for _, reference := range references {
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	calculationSetID := digestParts(identityParts...)
	basePath := compactCalculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", scheduleManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return CompactManifest{}, err
	}
	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+scheduleManifest.Cycle+".lock"))
	if err != nil {
		return CompactManifest{}, err
	}
	defer unlock()
	current, err := readCompactManifestIfPresent(currentPath)
	if err != nil {
		return CompactManifest{}, err
	}
	if current != nil {
		if err := validateCompactManifest(*current); err != nil {
			return CompactManifest{}, fmt.Errorf("invalid current compact calculation manifest: %w", err)
		}
		if err := validateCompactManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return CompactManifest{}, err
		}
		if current.Cycle != scheduleManifest.Cycle {
			return CompactManifest{}, fmt.Errorf("current compact calculation belongs to cycle %s", current.Cycle)
		}
		if current.CalculationSetID == calculationSetID {
			return *current, nil
		}
	}
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", calculationSetID+".json")
	if existing, readErr := readCompactManifestIfPresent(manifestPath); readErr != nil {
		return CompactManifest{}, readErr
	} else if existing != nil {
		if err := validateCompactManifest(*existing); err != nil {
			return CompactManifest{}, err
		}
		if !reflect.DeepEqual(existing.InputFactSets, references) {
			return CompactManifest{}, fmt.Errorf("immutable compact calculation manifest collision")
		}
		if err := validateCompactManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return CompactManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return CompactManifest{}, err
		}
		return *existing, nil
	}

	linkages, err := loadLinkages(ctx, options.StorageRoot, classicManifests["candidate-committee-linkage"])
	if err != nil {
		return CompactManifest{}, err
	}
	summaries, err := loadSummaries(ctx, options.StorageRoot, classicManifests["all-candidates-summary"])
	if err != nil {
		return CompactManifest{}, err
	}
	currentSummaries, err := loadSummaries(ctx, options.StorageRoot, classicManifests["current-campaigns-summary"])
	if err != nil {
		return CompactManifest{}, err
	}
	summaries = append(summaries, currentSummaries...)
	calculator, err := NewCycleCalculator(scheduleManifest.Cycle, nil, linkages, summaries)
	if err != nil {
		return CompactManifest{}, err
	}
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return CompactManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	exceptionWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "exceptions")
	if err != nil {
		return CompactManifest{}, err
	}
	defer exceptionWriter.Abort()
	if options.Progress != nil {
		options.Progress("evaluating compact candidate itemized-individual membership for " + scheduleManifest.Cycle)
	}
	scan, err := scanCompactMembership(ctx, options.StorageRoot, scheduleManifest, calculationSetID, calculator, exceptionWriter, options.Progress)
	if err != nil {
		return CompactManifest{}, err
	}
	if scan.SourceRows != scheduleManifest.Counts.Facts || directProbeDecisionTotal(scan.Decisions) != scan.SourceRows {
		return CompactManifest{}, fmt.Errorf("compact receipt membership does not conserve columnar facts")
	}
	exceptionArtifact, err := exceptionWriter.Finalize()
	if err != nil {
		return CompactManifest{}, err
	}
	if exceptionArtifact.RecordCount != scan.ExceptionRecords {
		return CompactManifest{}, fmt.Errorf("compact membership exception records are not conserved")
	}
	results, err := calculator.Results()
	if err != nil {
		return CompactManifest{}, err
	}
	resultWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "results")
	if err != nil {
		return CompactManifest{}, err
	}
	defer resultWriter.Abort()
	for _, result := range results {
		if err := resultWriter.WriteJSON(result); err != nil {
			return CompactManifest{}, err
		}
	}
	resultArtifact, err := resultWriter.Finalize()
	if err != nil {
		return CompactManifest{}, err
	}
	resultCounts := directProbeResultCounts(scan.directProbeScan, uint64(len(linkages)), uint64(len(summaries)), results)
	reconciliations := directProbeReconciliations(results)
	manifest := CompactManifest{
		Schema: "manifest.schema.json", SchemaVersion: CompactManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID, CalculationVersion: ContractVersion,
		PublisherVersion: CompactCalculationPublisherV1, ResultSchemaVersion: ResultSchemaVersion,
		Cycle: scheduleManifest.Cycle, SourceReleaseID: scheduleManifest.SourceReleaseID,
		InputFactSets: references, RunID: runID, State: "published", PublishedAt: options.Clock().UTC(),
		Predicate: compactMembershipPredicate(), DecisionCounts: scan.Decisions, ResultCounts: resultCounts,
		Reconciliations: reconciliations, Exceptions: exceptionArtifact, Results: resultArtifact,
	}
	manifest.Checks = []Check{
		{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the exact columnar Schedule A and three classic fact sets share one cycle and source release"},
		{ID: "columnar_integrity", Passed: scan.SourceRows == scheduleManifest.Counts.Facts, Severity: "block", Detail: "every digest-verified columnar fact was read once through the declared predicate columns"},
		{ID: "decision_conservation", Passed: directProbeDecisionTotal(scan.Decisions) == scan.SourceRows, Severity: "block", Detail: "the versioned predicate classifies every input fact exactly once"},
		{ID: "exception_conservation", Passed: exceptionArtifact.RecordCount == scan.ExceptionRecords, Severity: "block", Detail: "all unresolved decision and invalid routed-date memberships are explicit sparse evidence"},
		{ID: "route_conservation", Passed: resultCounts.SourceRows == resultCounts.RoutedRows+resultCounts.RowsWithoutCandidateRoute, Severity: "block", Detail: "every fact was routed through exact linkage evidence or explicitly left without a candidate route"},
		{ID: "candidate_conservation", Passed: resultCounts.Candidates == resultCounts.CompleteCandidates+resultCounts.PartialCandidates+resultCounts.NotComparableCandidates, Severity: "block", Detail: "every candidate result has one explicit calculation state"},
		{ID: "result_conservation", Passed: resultArtifact.RecordCount == resultCounts.Candidates, Severity: "block", Detail: "one materialized result exists per calculated candidate"},
		{ID: "no_dense_decision_artifact", Passed: true, Severity: "block", Detail: "ordinary calculation membership is the exact predicate plus input fact index; only exceptions are materialized"},
	}
	if err := validateCompactManifest(manifest); err != nil {
		return CompactManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return CompactManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return CompactManifest{}, err
	}
	return manifest, nil
}

func loadCompactColumnarManifest(ctx context.Context, storageRoot, path string) (fecoccurrence.ScheduleAColumnarManifest, string, error) {
	manifest, _, err := readStrictJSON[fecoccurrence.ScheduleAColumnarManifest](path)
	if err != nil {
		return manifest, "", err
	}
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != fecoccurrence.ScheduleAColumnarFactSetSchemaVersion ||
		manifest.FactType != fecoccurrence.ScheduleAFactType || manifest.PhysicalSchemaVersion != scheduleaparquet.PhysicalSchemaVersion ||
		manifest.State != "published" || manifest.Cycle == "" || manifest.SourceReleaseID == "" {
		return manifest, "", fmt.Errorf("unsupported Schedule A columnar fact manifest")
	}
	if manifest.Counts.SourceOccurrences == 0 || manifest.Counts.ExcludedOccurrences != 0 || manifest.Counts.InvalidFacts != 0 ||
		manifest.Counts.SourceOccurrences != manifest.Counts.Facts || manifest.Counts.Facts != manifest.Counts.ValidFacts {
		return manifest, "", fmt.Errorf("compact receipt calculation v1 requires complete valid columnar fact membership")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return manifest, "", fmt.Errorf("Schedule A columnar blocking check %s failed", check.ID)
		}
	}
	immutablePath := filepath.Join(storageRoot, "facts", "fec", "schedule-a", "columnar", "manifests", manifest.FactSetID+".json")
	immutable, digest, err := readStrictJSON[fecoccurrence.ScheduleAColumnarManifest](immutablePath)
	if err != nil {
		return manifest, "", err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return manifest, "", fmt.Errorf("Schedule A columnar pointer differs from immutable manifest")
	}
	var rows, facts, bytes uint64
	for _, shard := range manifest.Shards {
		path, err := storageartifact.Resolve(storageRoot, shard.StorageKey)
		if err != nil {
			return manifest, "", err
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != shard.Bytes {
			return manifest, "", fmt.Errorf("Schedule A columnar shard %d size is invalid", shard.Index)
		}
		if err := verifyColumnarShardSHA256(ctx, path, shard.SHA256); err != nil {
			return manifest, "", fmt.Errorf("verify Schedule A columnar shard %d: %w", shard.Index, err)
		}
		rows += shard.SourceRows
		facts += shard.Facts
		bytes += shard.Bytes
	}
	if rows != manifest.Counts.SourceOccurrences || facts != manifest.Counts.Facts || bytes == 0 {
		return manifest, "", fmt.Errorf("Schedule A columnar shard totals are not conserved")
	}
	return manifest, digest, nil
}

func compactCalculationBase() string {
	return filepath.Join("calculations", "fec", "candidate-itemized-individual-receipts", "compact")
}

func readCompactManifestIfPresent(path string) (*CompactManifest, error) {
	manifest, _, err := readStrictJSON[CompactManifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func validateCompactManifest(manifest CompactManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != CompactManifestSchemaVersion || manifest.Calculation != ContractID ||
		manifest.CalculationVersion != ContractVersion || manifest.PublisherVersion != CompactCalculationPublisherV1 ||
		manifest.ResultSchemaVersion != ResultSchemaVersion || manifest.State != "published" || !reflect.DeepEqual(manifest.Predicate, compactMembershipPredicate()) {
		return fmt.Errorf("unsupported compact calculation manifest")
	}
	if !validDigest(manifest.CalculationSetID) || manifest.Cycle == "" || !strings.HasPrefix(manifest.SourceReleaseID, "fec-") ||
		!validDigest(strings.TrimPrefix(manifest.SourceReleaseID, "fec-")) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() || len(manifest.InputFactSets) != 4 {
		return fmt.Errorf("compact calculation manifest identity is incomplete")
	}
	expectedRoles := []string{"all_candidates_summary", "candidate_committee_linkage", "current_campaigns_summary", "schedule_a_receipts"}
	identityParts := []string{CompactManifestSchemaVersion, manifest.CalculationVersion, manifest.Predicate.Version, manifest.ResultSchemaVersion}
	for index, reference := range manifest.InputFactSets {
		if reference.Role != expectedRoles[index] || !validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) {
			return fmt.Errorf("compact calculation input fact-set identity is invalid")
		}
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	if manifest.CalculationSetID != digestParts(identityParts...) {
		return fmt.Errorf("compact calculation-set ID does not match canonical inputs")
	}
	if directProbeDecisionTotal(manifest.DecisionCounts) != manifest.ResultCounts.SourceRows ||
		manifest.ResultCounts.SourceRows != manifest.ResultCounts.ValidatedRows ||
		manifest.ResultCounts.SourceRows != manifest.ResultCounts.RoutedRows+manifest.ResultCounts.RowsWithoutCandidateRoute ||
		manifest.ResultCounts.Candidates != manifest.ResultCounts.CompleteCandidates+manifest.ResultCounts.PartialCandidates+manifest.ResultCounts.NotComparableCandidates {
		return fmt.Errorf("compact calculation counts are not conserved")
	}
	expectedExceptions := manifest.DecisionCounts.UnresolvedIndividualClass + manifest.DecisionCounts.UnresolvedAmount + manifest.ResultCounts.RoutedInvalidReceiptDates
	if manifest.Exceptions.Compression != "zstd" || manifest.Exceptions.RecordCount != expectedExceptions || manifest.Exceptions.CompressedBytes == 0 ||
		!validDigest(manifest.Exceptions.CompressedSHA256) || !validDigest(manifest.Exceptions.UncompressedSHA256) {
		return fmt.Errorf("compact calculation exception artifact is invalid")
	}
	if manifest.Results.Compression != "zstd" || manifest.Results.RecordCount != manifest.ResultCounts.Candidates || manifest.Results.CompressedBytes == 0 ||
		!validDigest(manifest.Results.CompressedSHA256) || !validDigest(manifest.Results.UncompressedSHA256) {
		return fmt.Errorf("compact calculation result artifact is invalid")
	}
	exceptionPrefix := filepath.ToSlash(filepath.Join(compactCalculationBase(), "exceptions", "sha256", manifest.Exceptions.CompressedSHA256[:2])) + "/"
	resultPrefix := filepath.ToSlash(filepath.Join(compactCalculationBase(), "results", "sha256", manifest.Results.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Exceptions.StorageKey, exceptionPrefix) || !strings.HasPrefix(manifest.Results.StorageKey, resultPrefix) {
		return fmt.Errorf("compact calculation artifact storage identity is invalid")
	}
	if len(manifest.Checks) < 8 {
		return fmt.Errorf("compact calculation manifest is missing checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking compact calculation check %s failed", check.ID)
		}
	}
	return nil
}

func validateCompactManifestBacking(ctx context.Context, storageRoot string, manifest CompactManifest) error {
	immutablePath := filepath.Join(storageRoot, compactCalculationBase(), "manifests", manifest.CalculationSetID+".json")
	immutable, _, err := readStrictJSON[CompactManifest](immutablePath)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return fmt.Errorf("compact calculation pointer differs from immutable manifest")
	}
	for name, descriptor := range map[string]storageartifact.Descriptor{"exceptions": manifest.Exceptions, "results": manifest.Results} {
		path, err := storageartifact.Resolve(storageRoot, descriptor.StorageKey)
		if err != nil {
			return err
		}
		if err := storageartifact.Verify(ctx, path, descriptor); err != nil {
			return fmt.Errorf("verify compact calculation %s: %w", name, err)
		}
	}
	return nil
}
