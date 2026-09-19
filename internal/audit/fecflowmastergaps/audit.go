package fecflowmastergaps

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecclassic "github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	stateFoundSameCycle = "found_in_different_same_cycle_master"
	stateFoundHistory   = "found_only_in_historical_master"
	stateFoundBoth      = "found_in_same_cycle_and_historical_masters"
	stateAbsentAll      = "absent_from_all_audited_masters"
)

type loadedMaster struct {
	manifest fecoccurrence.ClassicFactManifest
	digest   string
}

type exposureAccumulator struct {
	resultGroups uint64
	receiptRows  uint64
	positiveRows uint64
	negativeRows uint64
	zeroRows     uint64
	amount       big.Int
	roles        map[string]*roleAccumulator
}

type roleAccumulator struct {
	resultGroups uint64
	receiptRows  uint64
	amount       big.Int
}

type gapAccumulator struct {
	endpointRoles map[string]struct{}
	outgoing      exposureAccumulator
	incoming      exposureAccumulator
}

type auditAccumulator struct {
	references map[string]struct{}
	gaps       map[string]*gapAccumulator
	all        exposureAccumulator
	anyMissing exposureAccumulator
	onlySource exposureAccumulator
	onlyTarget exposureAccumulator
	both       exposureAccumulator
}

// Audit classifies every committee ID omitted from the exact graph master
// against explicit same-cycle, other-cycle, and historical official evidence.
// Comparison assertions never mutate or backfill the graph input master.
func Audit(ctx context.Context, options Options) (Report, error) {
	if options.StorageRoot == "" || options.Cycle == "" || options.ReadinessBundlePath == "" {
		return Report{}, fmt.Errorf("storage root, cycle, and receiver-flow readiness bundle are required")
	}
	if len(options.CommitteeComparisonManifestPaths) == 0 {
		return Report{}, fmt.Errorf("at least one different-release same-cycle committee master is required")
	}
	if len(options.CommitteeHistoryManifestPaths) == 0 && len(options.RawHistoryArchives) == 0 {
		return Report{}, fmt.Errorf("at least one normalized or raw historical committee master is required")
	}
	if options.LinkageManifestPath == "" || len(options.SummaryManifestPaths) == 0 {
		return Report{}, fmt.Errorf("same-release linkage and summary fact manifests are required")
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	progress := options.Progress
	if progress == nil {
		progress = func(string) {}
	}

	progress("loading exact receiver-flow projection bundle and calculation")
	bundle, bundleDigest, resolved, err := fecflows.LoadProjectionBundle(ctx, options.StorageRoot, options.ReadinessBundlePath)
	if err != nil {
		return Report{}, fmt.Errorf("load receiver-flow readiness bundle: %w", err)
	}
	if bundle.Cycle != options.Cycle {
		return Report{}, fmt.Errorf("readiness bundle belongs to cycle %s, expected %s", bundle.Cycle, options.Cycle)
	}
	calculation, calculationDigest, err := fecflows.LoadPublishedManifest(ctx, options.StorageRoot, resolved.CalculationManifestPath)
	if err != nil {
		return Report{}, fmt.Errorf("load receiver-flow calculation: %w", err)
	}
	current, err := loadMaster(options.StorageRoot, resolved.CommitteeManifestPath, "committee-master")
	if err != nil {
		return Report{}, fmt.Errorf("load graph committee master: %w", err)
	}
	if current.manifest.Cycle != bundle.Cycle || current.manifest.SourceReleaseID != bundle.SourceReleaseID {
		return Report{}, fmt.Errorf("graph committee master does not match readiness bundle cycle and release")
	}

	comparisons, err := loadSameCycleComparisons(options.StorageRoot, options.CommitteeComparisonManifestPaths, options.Cycle, current.manifest.FactSetID)
	if err != nil {
		return Report{}, err
	}
	history, err := loadNormalizedHistory(options.StorageRoot, options.CommitteeHistoryManifestPaths, options.Cycle)
	if err != nil {
		return Report{}, err
	}

	progress("indexing selected, comparison, and normalized historical committee masters")
	currentIndex, _, err := indexNormalizedMasters(ctx, options.StorageRoot, []loadedMaster{current})
	if err != nil {
		return Report{}, err
	}
	_, comparisonIndex, err := indexNormalizedMasters(ctx, options.StorageRoot, comparisons)
	if err != nil {
		return Report{}, err
	}
	_, historyIndex, err := indexNormalizedMasters(ctx, options.StorageRoot, history)
	if err != nil {
		return Report{}, err
	}

	progress("verifying and indexing official historical committee-master archives")
	rawReferences, rawIndex, err := indexRawHistory(ctx, options.StorageRoot, options.RawHistoryArchives)
	if err != nil {
		return Report{}, err
	}
	for _, reference := range rawReferences {
		if reference.Cycle == options.Cycle {
			return Report{}, fmt.Errorf("raw committee history repeats current cycle %s", options.Cycle)
		}
	}
	if err := rejectHistoryCycleOverlap(history, rawReferences); err != nil {
		return Report{}, err
	}
	for committeeID, assertions := range rawIndex {
		historyIndex[committeeID] = append(historyIndex[committeeID], assertions...)
	}
	for committeeID := range historyIndex {
		sortAssertions(historyIndex[committeeID])
	}

	progress("loading same-release candidate linkage and summary evidence")
	linkageMaster, linkageIndex, err := loadLinkageFacts(ctx, options.StorageRoot, options.LinkageManifestPath, bundle.Cycle, bundle.SourceReleaseID)
	if err != nil {
		return Report{}, err
	}
	summaryMasters, summaryIndex, err := loadSummaryFacts(ctx, options.StorageRoot, options.SummaryManifestPaths, bundle.Cycle, bundle.SourceReleaseID)
	if err != nil {
		return Report{}, err
	}

	progress("scanning receiver-flow results and reconstructing missing-master exposure")
	accumulator, err := analyzeCalculation(ctx, options.StorageRoot, calculation, currentIndex)
	if err != nil {
		return Report{}, err
	}
	var scheduleAReference *ScheduleAFactReference
	sourceReceipts := make(map[string][]SourceReceiptAssertion)
	if options.TraceSourceReceipts {
		progress("tracing source rows for IDs absent from every audited committee master")
		reference, evidence, scanErr := scanAbsentMasterSourceReceipts(
			ctx, options.StorageRoot, calculation, accumulator, comparisonIndex, historyIndex, progress,
		)
		if scanErr != nil {
			return Report{}, scanErr
		}
		scheduleAReference = &reference
		sourceReceipts = evidence
	}
	committees, counts := buildGaps(accumulator, comparisonIndex, historyIndex, linkageIndex, summaryIndex, sourceReceipts)
	report := Report{
		SchemaVersion: SchemaVersion, AuditVersion: AuditVersion,
		Cycle: bundle.Cycle, SourceReleaseID: bundle.SourceReleaseID,
		AuditedAt: clock().UTC(),
		Inputs: Inputs{
			ReadinessBundle: BundleReference{BundleID: bundle.BundleID, ManifestSHA256: bundleDigest},
			Calculation: CalculationReference{
				CalculationSetID: calculation.CalculationSetID, ManifestSHA256: calculationDigest,
				ResultsSHA256: calculation.Results.CompressedSHA256,
			},
			ScheduleAFacts:         scheduleAReference,
			CurrentCommitteeMaster: masterReference(current),
			SameCycleComparisons:   masterReferences(comparisons),
			NormalizedHistory:      masterReferences(history),
			RawHistory:             rawReferences,
			LinkageFacts:           masterReference(linkageMaster),
			SummaryFacts:           masterReferences(summaryMasters),
		},
		Counts: counts,
		Exposure: ExposureReport{
			AllCalculationResults: accumulator.all.snapshot(),
			AnyMissingEndpoint:    accumulator.anyMissing.snapshot(),
			OnlyMissingSource:     accumulator.onlySource.snapshot(),
			OnlyMissingRecipient:  accumulator.onlyTarget.snapshot(),
			BothMissingEndpoints:  accumulator.both.snapshot(),
		},
		Committees: committees,
	}
	report.Checks = auditChecks(report, calculation)
	for _, check := range report.Checks {
		if check.Severity == "block" && !check.Passed {
			return report, fmt.Errorf("audit check %s failed: %s", check.ID, check.Detail)
		}
	}
	return report, nil
}

func loadMaster(storageRoot, path, dataset string) (loadedMaster, error) {
	manifest, digest, err := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, path, dataset)
	if err != nil {
		return loadedMaster{}, err
	}
	return loadedMaster{manifest: manifest, digest: digest}, nil
}

func loadSameCycleComparisons(storageRoot string, paths []string, cycle, currentFactSetID string) ([]loadedMaster, error) {
	result := make([]loadedMaster, 0, len(paths))
	seen := make(map[string]struct{})
	for _, path := range paths {
		master, err := loadMaster(storageRoot, path, "committee-master")
		if err != nil {
			return nil, fmt.Errorf("load same-cycle committee comparison: %w", err)
		}
		if master.manifest.Cycle != cycle || master.manifest.FactSetID == currentFactSetID {
			return nil, fmt.Errorf("committee comparison must be a different fact set for cycle %s", cycle)
		}
		if _, exists := seen[master.manifest.FactSetID]; exists {
			return nil, fmt.Errorf("duplicate committee comparison fact set %s", master.manifest.FactSetID)
		}
		seen[master.manifest.FactSetID] = struct{}{}
		result = append(result, master)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].manifest.FactSetID < result[right].manifest.FactSetID })
	return result, nil
}

func loadNormalizedHistory(storageRoot string, paths []string, currentCycle string) ([]loadedMaster, error) {
	result := make([]loadedMaster, 0, len(paths))
	seenCycles := make(map[string]struct{})
	for _, path := range paths {
		master, err := loadMaster(storageRoot, path, "committee-master")
		if err != nil {
			return nil, fmt.Errorf("load normalized committee history: %w", err)
		}
		if master.manifest.Cycle == currentCycle {
			return nil, fmt.Errorf("normalized committee history repeats current cycle %s", currentCycle)
		}
		if _, exists := seenCycles[master.manifest.Cycle]; exists {
			return nil, fmt.Errorf("duplicate normalized committee history cycle %s", master.manifest.Cycle)
		}
		seenCycles[master.manifest.Cycle] = struct{}{}
		result = append(result, master)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].manifest.Cycle < result[right].manifest.Cycle })
	return result, nil
}

func indexNormalizedMasters(ctx context.Context, storageRoot string, masters []loadedMaster) (map[string]struct{}, map[string][]CommitteeHistoricalAssertion, error) {
	ids := make(map[string]struct{})
	assertions := make(map[string][]CommitteeHistoricalAssertion)
	for _, master := range masters {
		err := streamClassicFacts(ctx, storageRoot, master.manifest, func(fact fecoccurrence.ClassicFact) error {
			if fact.State != "valid" {
				return nil
			}
			fields, err := decodeTypedFields[fecoccurrence.CommitteeTypedFields](fact.TypedFields)
			if err != nil {
				return fmt.Errorf("committee fact %s: %w", fact.FactID, err)
			}
			if fields.CommitteeID == "" {
				return fmt.Errorf("committee fact %s has no committee ID", fact.FactID)
			}
			if _, exists := ids[fields.CommitteeID]; exists && len(masters) == 1 {
				return fmt.Errorf("duplicate committee %s in fact set %s", fields.CommitteeID, master.manifest.FactSetID)
			}
			ids[fields.CommitteeID] = struct{}{}
			assertions[fields.CommitteeID] = append(assertions[fields.CommitteeID], CommitteeHistoricalAssertion{
				Cycle: master.manifest.Cycle, SourceKind: "normalized_fact",
				SourceReleaseID: master.manifest.SourceReleaseID, FactSetID: master.manifest.FactSetID, FactID: fact.FactID,
				IssueCodes: []string{},
				Name:       fields.Name, PartyAffiliation: fields.PartyAffiliation,
				DesignationCode: fields.DesignationCode, CommitteeTypeCode: fields.CommitteeTypeCode,
				OrganizationTypeCode: fields.OrganizationTypeCode, ConnectedOrganization: fields.ConnectedOrganization,
				CandidateID: fields.CandidateID,
			})
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	for committeeID := range assertions {
		sortAssertions(assertions[committeeID])
	}
	return ids, assertions, nil
}

func indexRawHistory(ctx context.Context, storageRoot string, inputs []RawHistoryArchiveInput) ([]RawHistoryArchive, map[string][]CommitteeHistoricalAssertion, error) {
	references := make([]RawHistoryArchive, 0, len(inputs))
	index := make(map[string][]CommitteeHistoricalAssertion)
	seenCycles := make(map[string]struct{})
	for _, input := range inputs {
		if input.Cycle == "" || input.Path == "" {
			return nil, nil, fmt.Errorf("raw history cycle and path are required")
		}
		if len(input.Cycle) != 4 || input.Cycle[0] < '0' || input.Cycle[0] > '9' ||
			input.Cycle[1] < '0' || input.Cycle[1] > '9' || input.Cycle[2] < '0' || input.Cycle[2] > '9' ||
			input.Cycle[3] < '0' || input.Cycle[3] > '9' || (input.Cycle[3]-'0')%2 != 0 {
			return nil, nil, fmt.Errorf("raw history cycle %q is not a four-digit even year", input.Cycle)
		}
		if _, exists := seenCycles[input.Cycle]; exists {
			return nil, nil, fmt.Errorf("duplicate raw history cycle %s", input.Cycle)
		}
		seenCycles[input.Cycle] = struct{}{}
		reference, assertions, err := readRawHistoryArchive(ctx, storageRoot, input)
		if err != nil {
			return nil, nil, err
		}
		references = append(references, reference)
		for committeeID, values := range assertions {
			index[committeeID] = append(index[committeeID], values...)
		}
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Cycle < references[right].Cycle })
	return references, index, nil
}

func readRawHistoryArchive(ctx context.Context, storageRoot string, input RawHistoryArchiveInput) (RawHistoryArchive, map[string][]CommitteeHistoricalAssertion, error) {
	storageKey, err := relativeStorageKey(storageRoot, input.Path)
	if err != nil {
		return RawHistoryArchive{}, nil, err
	}
	file, err := os.Open(input.Path)
	if err != nil {
		return RawHistoryArchive{}, nil, fmt.Errorf("open committee history %s: %w", input.Cycle, err)
	}
	hasher := sha256.New()
	archiveBytes, err := io.Copy(hasher, file)
	closeErr := file.Close()
	if err != nil {
		return RawHistoryArchive{}, nil, fmt.Errorf("hash committee history %s: %w", input.Cycle, err)
	}
	if closeErr != nil {
		return RawHistoryArchive{}, nil, closeErr
	}
	archiveDigest := hex.EncodeToString(hasher.Sum(nil))
	reader, err := zip.OpenReader(input.Path)
	if err != nil {
		return RawHistoryArchive{}, nil, fmt.Errorf("open committee history ZIP %s: %w", input.Cycle, err)
	}
	defer func() { _ = reader.Close() }()
	files := make([]*zip.File, 0, len(reader.File))
	for _, candidate := range reader.File {
		if !candidate.FileInfo().IsDir() {
			files = append(files, candidate)
		}
	}
	if len(files) != 1 {
		return RawHistoryArchive{}, nil, fmt.Errorf("committee history %s contains %d files, expected 1", input.Cycle, len(files))
	}
	member := files[0]
	content, err := member.Open()
	if err != nil {
		return RawHistoryArchive{}, nil, err
	}
	defer func() { _ = content.Close() }()
	spec, _ := fecclassic.Lookup("committee-master")
	decoder := fecclassic.NewDecoder(content)
	assertions := make(map[string][]CommitteeHistoricalAssertion)
	issueCounts := make(map[string]uint64)
	var records uint64
	var cleanRecords uint64
	var issueRecords uint64
	for decoder.Scan() {
		if err := ctx.Err(); err != nil {
			return RawHistoryArchive{}, nil, err
		}
		row := decoder.Row()
		issues := row.Validate(spec, input.Cycle)
		issueCodes := make([]string, 0, len(issues))
		seenIssues := make(map[string]struct{}, len(issues))
		for _, issue := range issues {
			if _, exists := seenIssues[issue.Code]; !exists {
				issueCodes = append(issueCodes, issue.Code)
				issueCounts[issue.Code]++
				seenIssues[issue.Code] = struct{}{}
			}
		}
		sort.Strings(issueCodes)
		if len(issueCodes) == 0 {
			cleanRecords++
		} else {
			issueRecords++
		}
		committeeID, usableID := row.NaturalKeyValue(spec)
		if !usableID || !spec.ValidNaturalKey(committeeID) {
			records++
			continue
		}
		fields, ok := row.CanonicalMap(spec)
		if !ok {
			fields = map[string]string{"CMTE_ID": committeeID}
		}
		var candidateID *string
		if fields["CAND_ID"] != "" {
			value := fields["CAND_ID"]
			candidateID = &value
		}
		rowDigest := sha256.Sum256(row.Raw())
		assertions[committeeID] = append(assertions[committeeID], CommitteeHistoricalAssertion{
			Cycle: input.Cycle, SourceKind: "official_bulk_archive", ArchiveSHA256: archiveDigest, SourceRow: row.Number(),
			SourceRowSHA256: hex.EncodeToString(rowDigest[:]), IssueCodes: issueCodes,
			Name: fields["CMTE_NM"], PartyAffiliation: fields["CMTE_PTY_AFFILIATION"],
			DesignationCode: fields["CMTE_DSGN"], CommitteeTypeCode: fields["CMTE_TP"],
			OrganizationTypeCode: fields["ORG_TP"], ConnectedOrganization: fields["CONNECTED_ORG_NM"], CandidateID: candidateID,
		})
		records++
	}
	if err := decoder.Err(); err != nil {
		return RawHistoryArchive{}, nil, fmt.Errorf("read committee history %s: %w", input.Cycle, err)
	}
	if err := content.Close(); err != nil {
		return RawHistoryArchive{}, nil, err
	}
	issueSummary := make([]SourceIssueCount, 0, len(issueCounts))
	for code, rows := range issueCounts {
		issueSummary = append(issueSummary, SourceIssueCount{Code: code, Rows: rows})
	}
	sort.Slice(issueSummary, func(left, right int) bool { return issueSummary[left].Code < issueSummary[right].Code })
	return RawHistoryArchive{
		Cycle: input.Cycle, SourceURL: officialCommitteeMasterURL(input.Cycle),
		StorageKey:    storageKey,
		ArchiveSHA256: archiveDigest, ArchiveBytes: uint64(archiveBytes), Member: member.Name,
		MemberBytes: member.UncompressedSize64, CommitteeRecords: records,
		CleanRecords: cleanRecords, IssueRecords: issueRecords, IssueCounts: issueSummary,
	}, assertions, nil
}

func relativeStorageKey(storageRoot, path string) (string, error) {
	root, err := filepath.Abs(storageRoot)
	if err != nil {
		return "", err
	}
	target, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("committee history path %q is outside storage root", path)
	}
	return filepath.ToSlash(relative), nil
}

func rejectHistoryCycleOverlap(normalized []loadedMaster, raw []RawHistoryArchive) error {
	seen := make(map[string]struct{}, len(normalized))
	for _, master := range normalized {
		seen[master.manifest.Cycle] = struct{}{}
	}
	for _, archive := range raw {
		if _, exists := seen[archive.Cycle]; exists {
			return fmt.Errorf("history cycle %s appears as both normalized facts and raw archive", archive.Cycle)
		}
	}
	return nil
}

func officialCommitteeMasterURL(cycle string) string {
	return "https://www.fec.gov/files/bulk-downloads/" + cycle + "/cm" + cycle[2:] + ".zip"
}

func loadLinkageFacts(ctx context.Context, storageRoot, path, cycle, releaseID string) (loadedMaster, map[string][]LinkageAssertion, error) {
	master, err := loadMaster(storageRoot, path, "candidate-committee-linkage")
	if err != nil {
		return loadedMaster{}, nil, fmt.Errorf("load linkage facts: %w", err)
	}
	if master.manifest.Cycle != cycle || master.manifest.SourceReleaseID != releaseID {
		return loadedMaster{}, nil, fmt.Errorf("linkage facts do not match graph cycle and source release")
	}
	index := make(map[string][]LinkageAssertion)
	err = streamClassicFacts(ctx, storageRoot, master.manifest, func(fact fecoccurrence.ClassicFact) error {
		if fact.State != "valid" {
			return nil
		}
		fields, err := decodeTypedFields[fecoccurrence.LinkageTypedFields](fact.TypedFields)
		if err != nil {
			return err
		}
		index[fields.CommitteeID] = append(index[fields.CommitteeID], LinkageAssertion{
			FactID: fact.FactID, FactSetID: master.manifest.FactSetID,
			CandidateID: fields.CandidateID, CandidateElectionYear: fields.CandidateElectionYear,
			FECElectionYear: fields.FECElectionYear, SourceCycle: fields.SourceCycle,
			CommitteeID: fields.CommitteeID, CommitteeTypeCode: fields.CommitteeTypeCode,
			DesignationCode: fields.DesignationCode, LinkageID: fields.LinkageID,
		})
		return nil
	})
	if err != nil {
		return loadedMaster{}, nil, err
	}
	for committeeID := range index {
		sort.Slice(index[committeeID], func(left, right int) bool { return index[committeeID][left].FactID < index[committeeID][right].FactID })
	}
	return master, index, nil
}

func loadSummaryFacts(ctx context.Context, storageRoot string, paths []string, cycle, releaseID string) ([]loadedMaster, map[string][]CandidateSummaryAssertion, error) {
	masters := make([]loadedMaster, 0, len(paths))
	index := make(map[string][]CandidateSummaryAssertion)
	seenDatasets := make(map[string]struct{})
	for _, path := range paths {
		manifest, _, err := fecoccurrence.LoadPublishedClassicFactManifest(storageRoot, path, "")
		if err != nil {
			return nil, nil, fmt.Errorf("load candidate summary facts: %w", err)
		}
		if manifest.Dataset != "all-candidates-summary" && manifest.Dataset != "current-campaigns-summary" {
			return nil, nil, fmt.Errorf("unsupported candidate summary dataset %s", manifest.Dataset)
		}
		master, err := loadMaster(storageRoot, path, manifest.Dataset)
		if err != nil {
			return nil, nil, err
		}
		if master.manifest.Cycle != cycle || master.manifest.SourceReleaseID != releaseID {
			return nil, nil, fmt.Errorf("%s facts do not match graph cycle and source release", master.manifest.Dataset)
		}
		if _, exists := seenDatasets[master.manifest.Dataset]; exists {
			return nil, nil, fmt.Errorf("duplicate candidate summary dataset %s", master.manifest.Dataset)
		}
		seenDatasets[master.manifest.Dataset] = struct{}{}
		masters = append(masters, master)
		err = streamClassicFacts(ctx, storageRoot, master.manifest, func(fact fecoccurrence.ClassicFact) error {
			if fact.State != "valid" {
				return nil
			}
			fields, err := decodeTypedFields[fecoccurrence.SummaryTypedFields](fact.TypedFields)
			if err != nil {
				return err
			}
			index[fields.CandidateID] = append(index[fields.CandidateID], CandidateSummaryAssertion{
				FactID: fact.FactID, FactSetID: master.manifest.FactSetID, Dataset: master.manifest.Dataset,
				CandidateID: fields.CandidateID, Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
				OfficeState: fields.OfficeState, OfficeDistrict: fields.OfficeDistrict,
				SourceCycle: fields.SourceCycle, CoverageThrough: fields.CoverageThrough,
			})
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	if len(seenDatasets) != 2 {
		return nil, nil, fmt.Errorf("candidate summary audit requires all-candidates and current-campaigns fact sets")
	}
	sort.Slice(masters, func(left, right int) bool { return masters[left].manifest.Dataset < masters[right].manifest.Dataset })
	for candidateID := range index {
		sort.Slice(index[candidateID], func(left, right int) bool {
			return index[candidateID][left].Dataset < index[candidateID][right].Dataset
		})
	}
	return masters, index, nil
}

func analyzeCalculation(ctx context.Context, storageRoot string, calculation fecflows.Manifest, current map[string]struct{}) (*auditAccumulator, error) {
	accumulator := &auditAccumulator{references: make(map[string]struct{}), gaps: make(map[string]*gapAccumulator)}
	reader, err := storageartifact.Open[fecflows.Result](ctx, storageRoot, calculation.Results)
	if err != nil {
		return nil, fmt.Errorf("open receiver-flow results: %w", err)
	}
	for {
		result, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return nil, fmt.Errorf("read receiver-flow results: %w", readErr)
		}
		if !ok {
			break
		}
		if result.Cycle != calculation.Cycle || result.CalculationSetID != calculation.CalculationSetID {
			reader.Abort()
			return nil, fmt.Errorf("receiver-flow result %s has conflicting calculation lineage", result.ResultID)
		}
		if err := accumulator.addResult(result, current); err != nil {
			reader.Abort()
			return nil, err
		}
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return accumulator, nil
}

func (accumulator *auditAccumulator) addResult(result fecflows.Result, current map[string]struct{}) error {
	accumulator.references[result.SourceCommitteeID] = struct{}{}
	accumulator.references[result.RecipientCommitteeID] = struct{}{}
	if err := accumulator.all.add(result); err != nil {
		return err
	}
	_, sourcePresent := current[result.SourceCommitteeID]
	_, targetPresent := current[result.RecipientCommitteeID]
	if sourcePresent && targetPresent {
		return nil
	}
	if err := accumulator.anyMissing.add(result); err != nil {
		return err
	}
	switch {
	case !sourcePresent && !targetPresent:
		if err := accumulator.both.add(result); err != nil {
			return err
		}
	case !sourcePresent:
		if err := accumulator.onlySource.add(result); err != nil {
			return err
		}
	default:
		if err := accumulator.onlyTarget.add(result); err != nil {
			return err
		}
	}
	if !sourcePresent {
		gap := accumulator.gap(result.SourceCommitteeID)
		gap.endpointRoles["source"] = struct{}{}
		if err := gap.outgoing.add(result); err != nil {
			return err
		}
	}
	if !targetPresent {
		gap := accumulator.gap(result.RecipientCommitteeID)
		gap.endpointRoles["recipient"] = struct{}{}
		if err := gap.incoming.add(result); err != nil {
			return err
		}
	}
	return nil
}

func (accumulator *auditAccumulator) gap(committeeID string) *gapAccumulator {
	gap := accumulator.gaps[committeeID]
	if gap == nil {
		gap = &gapAccumulator{endpointRoles: make(map[string]struct{})}
		accumulator.gaps[committeeID] = gap
	}
	return gap
}

func (accumulator *exposureAccumulator) add(result fecflows.Result) error {
	amount, ok := new(big.Int).SetString(result.SignedAmountMinorUnits, 10)
	if !ok || amount.String() != result.SignedAmountMinorUnits {
		return fmt.Errorf("receiver-flow result %s has invalid exact amount %q", result.ResultID, result.SignedAmountMinorUnits)
	}
	if result.ReceiptCount == 0 || result.ReceiptCount != result.PositiveCount+result.NegativeCount+result.ZeroCount {
		return fmt.Errorf("receiver-flow result %s has invalid receipt counts", result.ResultID)
	}
	accumulator.resultGroups++
	accumulator.receiptRows += result.ReceiptCount
	accumulator.positiveRows += result.PositiveCount
	accumulator.negativeRows += result.NegativeCount
	accumulator.zeroRows += result.ZeroCount
	accumulator.amount.Add(&accumulator.amount, amount)
	if accumulator.roles == nil {
		accumulator.roles = make(map[string]*roleAccumulator)
	}
	role := accumulator.roles[result.ReceiptRole]
	if role == nil {
		role = &roleAccumulator{}
		accumulator.roles[result.ReceiptRole] = role
	}
	role.resultGroups++
	role.receiptRows += result.ReceiptCount
	role.amount.Add(&role.amount, amount)
	return nil
}

func (accumulator exposureAccumulator) snapshot() EdgeExposure {
	result := EdgeExposure{
		ResultGroups: accumulator.resultGroups, ReceiptRows: accumulator.receiptRows,
		PositiveRows: accumulator.positiveRows, NegativeRows: accumulator.negativeRows, ZeroRows: accumulator.zeroRows,
		SignedAmountMinorUnits: accumulator.amount.String(), Roles: make([]RoleExposure, 0, len(accumulator.roles)),
	}
	for receiptRole, role := range accumulator.roles {
		result.Roles = append(result.Roles, RoleExposure{
			ReceiptRole: receiptRole, ResultGroups: role.resultGroups,
			ReceiptRows: role.receiptRows, SignedAmountMinorUnits: role.amount.String(),
		})
	}
	sort.Slice(result.Roles, func(left, right int) bool { return result.Roles[left].ReceiptRole < result.Roles[right].ReceiptRole })
	return result
}

func buildGaps(accumulator *auditAccumulator, comparisons, history map[string][]CommitteeHistoricalAssertion, linkages map[string][]LinkageAssertion, summaries map[string][]CandidateSummaryAssertion, sourceReceipts map[string][]SourceReceiptAssertion) ([]CommitteeGap, Counts) {
	committees := make([]CommitteeGap, 0, len(accumulator.gaps))
	linkedCandidates := make(map[string]struct{})
	for committeeID, source := range accumulator.gaps {
		candidateIDs := make(map[string]struct{})
		gapLinkages := append([]LinkageAssertion(nil), linkages[committeeID]...)
		for _, linkage := range gapLinkages {
			candidateIDs[linkage.CandidateID] = struct{}{}
			linkedCandidates[linkage.CandidateID] = struct{}{}
		}
		gapSummaries := make([]CandidateSummaryAssertion, 0)
		for candidateID := range candidateIDs {
			gapSummaries = append(gapSummaries, summaries[candidateID]...)
		}
		sort.Slice(gapSummaries, func(left, right int) bool {
			if gapSummaries[left].CandidateID != gapSummaries[right].CandidateID {
				return gapSummaries[left].CandidateID < gapSummaries[right].CandidateID
			}
			return gapSummaries[left].Dataset < gapSummaries[right].Dataset
		})
		committees = append(committees, CommitteeGap{
			CommitteeID: committeeID, State: comparisonState(len(comparisons[committeeID]), len(history[committeeID])),
			EndpointRoles: sortedKeys(source.endpointRoles), Outgoing: source.outgoing.snapshot(), Incoming: source.incoming.snapshot(),
			CandidateIDs: sortedKeys(candidateIDs), LinkageFacts: nonNilLinkages(gapLinkages), CandidateSummaries: nonNilSummaries(gapSummaries),
			SourceReceipts:             nonNilSourceReceipts(append([]SourceReceiptAssertion(nil), sourceReceipts[committeeID]...)),
			SameCycleComparisonMasters: nonNilAssertions(append([]CommitteeHistoricalAssertion(nil), comparisons[committeeID]...)),
			HistoricalMasters:          nonNilAssertions(append([]CommitteeHistoricalAssertion(nil), history[committeeID]...)),
		})
	}
	sort.Slice(committees, func(left, right int) bool { return committees[left].CommitteeID < committees[right].CommitteeID })
	counts := Counts{
		CalculationResults: accumulator.all.resultGroups, ReferencedCommittees: uint64(len(accumulator.references)),
		CommitteesMissingCurrentMaster: uint64(len(committees)), LinkedCandidateReferences: uint64(len(linkedCandidates)),
	}
	for _, gap := range committees {
		switch gap.State {
		case stateFoundSameCycle:
			counts.FoundInSameCycleComparison++
		case stateFoundHistory:
			counts.FoundOnlyInHistory++
		case stateFoundBoth:
			counts.FoundInSameCycleComparisonAndHistory++
		case stateAbsentAll:
			counts.AbsentFromAllAuditedMasters++
		}
		if contains(gap.EndpointRoles, "source") {
			counts.MissingSourceCommittees++
		}
		if contains(gap.EndpointRoles, "recipient") {
			counts.MissingRecipientCommittees++
		}
		if len(gap.EndpointRoles) == 2 {
			counts.MissingCommitteesInBothEndpointRoles++
		}
		if len(gap.LinkageFacts) == 0 {
			counts.MissingCommitteesWithoutLinkage++
		} else {
			counts.MissingCommitteesWithLinkage++
		}
		counts.AbsentMasterSourceReceiptEvidence += uint64(len(gap.SourceReceipts))
	}
	for candidateID := range linkedCandidates {
		hasAll, hasCurrent := false, false
		for _, summary := range summaries[candidateID] {
			switch summary.Dataset {
			case "all-candidates-summary":
				hasAll = true
			case "current-campaigns-summary":
				hasCurrent = true
			}
		}
		if hasAll {
			counts.LinkedCandidatesInAllCandidatesSummary++
		}
		if hasCurrent {
			counts.LinkedCandidatesInCurrentCampaignsSummary++
		}
		if !hasAll && !hasCurrent {
			counts.LinkedCandidatesAbsentFromSummaries++
		}
	}
	return committees, counts
}

func auditChecks(report Report, calculation fecflows.Manifest) []Check {
	classified := report.Counts.FoundInSameCycleComparison + report.Counts.FoundOnlyInHistory +
		report.Counts.FoundInSameCycleComparisonAndHistory + report.Counts.AbsentFromAllAuditedMasters
	partitionGroups := report.Exposure.OnlyMissingSource.ResultGroups + report.Exposure.OnlyMissingRecipient.ResultGroups + report.Exposure.BothMissingEndpoints.ResultGroups
	partitionAmount := addMinorUnits(
		report.Exposure.OnlyMissingSource.SignedAmountMinorUnits,
		report.Exposure.OnlyMissingRecipient.SignedAmountMinorUnits,
		report.Exposure.BothMissingEndpoints.SignedAmountMinorUnits,
	)
	checks := []Check{
		{ID: "calculation_result_conservation", Passed: report.Exposure.AllCalculationResults.ResultGroups == calculation.ResultCounts.ResultGroups && report.Exposure.AllCalculationResults.SignedAmountMinorUnits == calculation.Amounts.IncludedMinorUnits, Severity: "block", Detail: fmt.Sprintf("groups=%d amount_minor_units=%s", report.Exposure.AllCalculationResults.ResultGroups, report.Exposure.AllCalculationResults.SignedAmountMinorUnits)},
		{ID: "missing_committee_conservation", Passed: report.Counts.CommitteesMissingCurrentMaster == uint64(len(report.Committees)), Severity: "block", Detail: fmt.Sprintf("count=%d rows=%d", report.Counts.CommitteesMissingCurrentMaster, len(report.Committees))},
		{ID: "classification_conservation", Passed: report.Counts.CommitteesMissingCurrentMaster == classified, Severity: "block", Detail: fmt.Sprintf("missing=%d classified=%d", report.Counts.CommitteesMissingCurrentMaster, classified)},
		{ID: "endpoint_role_conservation", Passed: report.Counts.MissingSourceCommittees+report.Counts.MissingRecipientCommittees-report.Counts.MissingCommitteesInBothEndpointRoles == report.Counts.CommitteesMissingCurrentMaster, Severity: "block", Detail: "source and recipient populations conserve after overlap"},
		{ID: "edge_exposure_conservation", Passed: report.Exposure.AnyMissingEndpoint.ResultGroups == partitionGroups && report.Exposure.AnyMissingEndpoint.SignedAmountMinorUnits == partitionAmount, Severity: "block", Detail: fmt.Sprintf("missing_endpoint_groups=%d partition_groups=%d", report.Exposure.AnyMissingEndpoint.ResultGroups, partitionGroups)},
		{ID: "linkage_partition_conservation", Passed: report.Counts.MissingCommitteesWithLinkage+report.Counts.MissingCommitteesWithoutLinkage == report.Counts.CommitteesMissingCurrentMaster, Severity: "block", Detail: "linkage presence is exhaustive and mutually exclusive"},
		{ID: "comparison_assertion_boundary", Passed: true, Severity: "block", Detail: "same-cycle and historical masters remain attached assertions and never backfill the graph input master"},
	}
	if report.Inputs.ScheduleAFacts != nil {
		checks = append(checks, Check{ID: "absent_master_source_evidence", Passed: absentMasterSourceEvidenceConserves(report), Severity: "block", Detail: fmt.Sprintf("source_receipts=%d", report.Counts.AbsentMasterSourceReceiptEvidence)})
	}
	return checks
}

func streamClassicFacts(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest, consume func(fecoccurrence.ClassicFact) error) error {
	descriptor := storageartifact.Descriptor{
		RecordCount: manifest.Facts.RecordCount, UncompressedBytes: manifest.Facts.UncompressedBytes,
		UncompressedSHA256: manifest.Facts.UncompressedSHA256, CompressedBytes: manifest.Facts.CompressedBytes,
		CompressedSHA256: manifest.Facts.CompressedSHA256, Compression: manifest.Facts.Compression, StorageKey: manifest.Facts.StorageKey,
	}
	reader, err := storageartifact.Open[fecoccurrence.ClassicFact](ctx, storageRoot, descriptor)
	if err != nil {
		return err
	}
	for {
		fact, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return readErr
		}
		if !ok {
			break
		}
		if fact.Dataset != manifest.Dataset || fact.Cycle != manifest.Cycle || fact.SourceReleaseID != manifest.SourceReleaseID {
			reader.Abort()
			return fmt.Errorf("classic fact %s does not match its manifest", fact.FactID)
		}
		if err := consume(fact); err != nil {
			reader.Abort()
			return err
		}
	}
	return reader.Close()
}

func decodeTypedFields[T any](value any) (T, error) {
	var result T
	content, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}

func masterReference(master loadedMaster) FactSetReference {
	return FactSetReference{
		Dataset: master.manifest.Dataset, Cycle: master.manifest.Cycle,
		SourceReleaseID: master.manifest.SourceReleaseID, FactSetID: master.manifest.FactSetID,
		ManifestSHA256: master.digest, FactsSHA256: master.manifest.Facts.CompressedSHA256,
	}
}

func masterReferences(masters []loadedMaster) []FactSetReference {
	result := make([]FactSetReference, 0, len(masters))
	for _, master := range masters {
		result = append(result, masterReference(master))
	}
	return result
}

func sortAssertions(values []CommitteeHistoricalAssertion) {
	sort.Slice(values, func(left, right int) bool {
		if values[left].Cycle != values[right].Cycle {
			return values[left].Cycle < values[right].Cycle
		}
		if values[left].SourceKind != values[right].SourceKind {
			return values[left].SourceKind < values[right].SourceKind
		}
		return values[left].FactID < values[right].FactID
	})
}

func comparisonState(sameCycle, history int) string {
	switch {
	case sameCycle > 0 && history > 0:
		return stateFoundBoth
	case sameCycle > 0:
		return stateFoundSameCycle
	case history > 0:
		return stateFoundHistory
	default:
		return stateAbsentAll
	}
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func addMinorUnits(values ...string) string {
	var total big.Int
	for _, value := range values {
		parsed, ok := new(big.Int).SetString(value, 10)
		if !ok {
			return "invalid"
		}
		total.Add(&total, parsed)
	}
	return total.String()
}

func nonNilAssertions(values []CommitteeHistoricalAssertion) []CommitteeHistoricalAssertion {
	if values == nil {
		return []CommitteeHistoricalAssertion{}
	}
	return values
}

func nonNilLinkages(values []LinkageAssertion) []LinkageAssertion {
	if values == nil {
		return []LinkageAssertion{}
	}
	return values
}

func nonNilSummaries(values []CandidateSummaryAssertion) []CandidateSummaryAssertion {
	if values == nil {
		return []CandidateSummaryAssertion{}
	}
	return values
}

func nonNilSourceReceipts(values []SourceReceiptAssertion) []SourceReceiptAssertion {
	if values == nil {
		return []SourceReceiptAssertion{}
	}
	return values
}
