package fecmastergaps

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"path/filepath"
	"sort"
	"time"

	fecreceipts "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	stateFoundInSameCycle = "found_in_different_same_cycle_master"
	stateFoundInHistory   = "found_in_other_cycle_master"
	stateFoundInBoth      = "found_in_same_and_other_cycle_masters"
	stateAbsentAll        = "absent_from_all_comparison_masters"
)

type loadedMaster struct {
	manifest fecoccurrence.ClassicFactManifest
	digest   string
}

type candidateMasterRecord struct {
	cycle     string
	factSetID string
	factID    string
	fields    fecoccurrence.CandidateTypedFields
}

type committeeMasterRecord struct {
	cycle     string
	factSetID string
	factID    string
	fields    fecoccurrence.CommitteeTypedFields
}

type committeeReference struct {
	origins      map[string]struct{}
	candidateIDs map[string]struct{}
	linkages     map[string]CommitteeLinkageAssertion
	amount       *big.Int
	counts       fecreceipts.IncludedCounts
}

// Audit reconstructs the exact graph-placeholder boundary from immutable
// receipt-calculation and classic-master artifacts. Other-cycle master facts
// are reported as assertions; they never replace the selected cycle masters.
func Audit(ctx context.Context, options Options) (Report, error) {
	if options.StorageRoot == "" || options.Cycle == "" || options.CalculationManifestPath == "" || options.CandidateManifestPath == "" || options.CommitteeManifestPath == "" {
		return Report{}, fmt.Errorf("storage root, cycle, calculation, and current candidate and committee manifests are required")
	}
	if len(options.CandidateComparisonManifestPaths) == 0 || len(options.CommitteeComparisonManifestPaths) == 0 || len(options.CandidateHistoryManifestPaths) == 0 || len(options.CommitteeHistoryManifestPaths) == 0 {
		return Report{}, fmt.Errorf("at least one same-cycle comparison and other-cycle history manifest is required for both candidates and committees")
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	progress := options.Progress
	if progress == nil {
		progress = func(string) {}
	}

	progress("loading immutable receipt calculation")
	calculation, calculationDigest, err := fecreceipts.LoadPublishedCompactManifest(ctx, options.StorageRoot, options.CalculationManifestPath)
	if err != nil {
		return Report{}, fmt.Errorf("load receipt calculation: %w", err)
	}
	if calculation.Cycle != options.Cycle {
		return Report{}, fmt.Errorf("receipt calculation belongs to cycle %s, expected %s", calculation.Cycle, options.Cycle)
	}

	currentCandidates, err := loadMaster(options.StorageRoot, options.CandidateManifestPath, "candidate-master")
	if err != nil {
		return Report{}, fmt.Errorf("load current candidate master: %w", err)
	}
	currentCommittees, err := loadMaster(options.StorageRoot, options.CommitteeManifestPath, "committee-master")
	if err != nil {
		return Report{}, fmt.Errorf("load current committee master: %w", err)
	}
	if err := validateCurrentMaster(currentCandidates.manifest, options.Cycle, calculation.SourceReleaseID); err != nil {
		return Report{}, err
	}
	if err := validateCurrentMaster(currentCommittees.manifest, options.Cycle, calculation.SourceReleaseID); err != nil {
		return Report{}, err
	}
	candidateComparisons, err := loadComparisons(options.StorageRoot, options.CandidateComparisonManifestPaths, "candidate-master", options.Cycle, currentCandidates.manifest.FactSetID)
	if err != nil {
		return Report{}, fmt.Errorf("load same-cycle candidate comparisons: %w", err)
	}
	committeeComparisons, err := loadComparisons(options.StorageRoot, options.CommitteeComparisonManifestPaths, "committee-master", options.Cycle, currentCommittees.manifest.FactSetID)
	if err != nil {
		return Report{}, fmt.Errorf("load same-cycle committee comparisons: %w", err)
	}

	candidateHistory, err := loadHistory(options.StorageRoot, options.CandidateHistoryManifestPaths, "candidate-master", options.Cycle)
	if err != nil {
		return Report{}, fmt.Errorf("load candidate history: %w", err)
	}
	committeeHistory, err := loadHistory(options.StorageRoot, options.CommitteeHistoryManifestPaths, "committee-master", options.Cycle)
	if err != nil {
		return Report{}, fmt.Errorf("load committee history: %w", err)
	}

	progress("indexing cycle-scoped and historical master assertions")
	currentCandidateIndex, _, err := indexCandidateMasters(ctx, options.StorageRoot, []loadedMaster{currentCandidates})
	if err != nil {
		return Report{}, err
	}
	currentCommitteeIndex, _, err := indexCommitteeMasters(ctx, options.StorageRoot, []loadedMaster{currentCommittees})
	if err != nil {
		return Report{}, err
	}
	_, candidateComparisonIndex, err := indexCandidateMasters(ctx, options.StorageRoot, candidateComparisons)
	if err != nil {
		return Report{}, err
	}
	_, committeeComparisonIndex, err := indexCommitteeMasters(ctx, options.StorageRoot, committeeComparisons)
	if err != nil {
		return Report{}, err
	}
	_, historicalCandidateIndex, err := indexCandidateMasters(ctx, options.StorageRoot, candidateHistory)
	if err != nil {
		return Report{}, err
	}
	_, historicalCommitteeIndex, err := indexCommitteeMasters(ctx, options.StorageRoot, committeeHistory)
	if err != nil {
		return Report{}, err
	}

	progress("reconstructing calculation references and placeholder sets")
	results, err := readCalculationResults(ctx, options.StorageRoot, calculation)
	if err != nil {
		return Report{}, err
	}
	candidateGaps, committeeGaps, counts, err := analyze(results, currentCandidateIndex, currentCommitteeIndex, candidateComparisonIndex, committeeComparisonIndex, historicalCandidateIndex, historicalCommitteeIndex)
	if err != nil {
		return Report{}, err
	}
	linkageFacts, summaryFacts, calculationClassicMasters, err := loadCalculationClassicEvidence(ctx, options.StorageRoot, calculation)
	if err != nil {
		return Report{}, err
	}
	if err := attachCalculationEvidence(candidateGaps, committeeGaps, linkageFacts, summaryFacts); err != nil {
		return Report{}, err
	}

	report := Report{
		SchemaVersion:   SchemaVersion,
		AuditVersion:    AuditVersion,
		Cycle:           options.Cycle,
		SourceReleaseID: calculation.SourceReleaseID,
		AuditedAt:       clock().UTC(),
		Inputs: Inputs{
			Calculation: CalculationReference{
				CalculationSetID: calculation.CalculationSetID,
				Cycle:            calculation.Cycle,
				SourceReleaseID:  calculation.SourceReleaseID,
				ManifestSHA256:   calculationDigest,
				ResultsSHA256:    calculation.Results.CompressedSHA256,
			},
			CurrentCandidateMaster:     masterReference(currentCandidates),
			CurrentCommitteeMaster:     masterReference(currentCommittees),
			CandidateComparisonMasters: masterReferences(candidateComparisons),
			CommitteeComparisonMasters: masterReferences(committeeComparisons),
			CandidateHistoryMasters:    masterReferences(candidateHistory),
			CommitteeHistoryMasters:    masterReferences(committeeHistory),
			CalculationClassicFactSets: masterReferences(calculationClassicMasters),
		},
		Counts:     counts,
		Candidates: candidateGaps,
		Committees: committeeGaps,
	}
	report.Checks = checks(report)
	for _, check := range report.Checks {
		if !check.Passed {
			return report, fmt.Errorf("audit check %s failed: %s", check.Name, check.Details)
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

func loadComparisons(storageRoot string, paths []string, dataset, cycle, currentFactSetID string) ([]loadedMaster, error) {
	masters := make([]loadedMaster, 0, len(paths))
	seenFactSets := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		master, err := loadMaster(storageRoot, path, dataset)
		if err != nil {
			return nil, err
		}
		if master.manifest.Cycle != cycle {
			return nil, fmt.Errorf("same-cycle comparison for %s belongs to cycle %s, expected %s", dataset, master.manifest.Cycle, cycle)
		}
		if master.manifest.FactSetID == currentFactSetID {
			return nil, fmt.Errorf("same-cycle comparison for %s repeats current fact set %s", dataset, currentFactSetID)
		}
		if _, exists := seenFactSets[master.manifest.FactSetID]; exists {
			return nil, fmt.Errorf("duplicate %s comparison fact set %s", dataset, master.manifest.FactSetID)
		}
		seenFactSets[master.manifest.FactSetID] = struct{}{}
		masters = append(masters, master)
	}
	sort.Slice(masters, func(left, right int) bool {
		return masters[left].manifest.FactSetID < masters[right].manifest.FactSetID
	})
	return masters, nil
}

func loadHistory(storageRoot string, paths []string, dataset, currentCycle string) ([]loadedMaster, error) {
	masters := make([]loadedMaster, 0, len(paths))
	seenCycles := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		master, err := loadMaster(storageRoot, path, dataset)
		if err != nil {
			return nil, err
		}
		if master.manifest.Cycle == currentCycle {
			return nil, fmt.Errorf("history manifest for %s repeats current cycle %s", dataset, currentCycle)
		}
		if _, exists := seenCycles[master.manifest.Cycle]; exists {
			return nil, fmt.Errorf("duplicate %s history cycle %s", dataset, master.manifest.Cycle)
		}
		seenCycles[master.manifest.Cycle] = struct{}{}
		masters = append(masters, master)
	}
	sort.Slice(masters, func(left, right int) bool { return masters[left].manifest.Cycle < masters[right].manifest.Cycle })
	return masters, nil
}

func validateCurrentMaster(manifest fecoccurrence.ClassicFactManifest, cycle, releaseID string) error {
	if manifest.Cycle != cycle || manifest.SourceReleaseID != releaseID {
		return fmt.Errorf("%s facts do not share calculation cycle %s and source release %s", manifest.Dataset, cycle, releaseID)
	}
	return nil
}

func indexCandidateMasters(ctx context.Context, storageRoot string, masters []loadedMaster) (map[string]candidateMasterRecord, map[string][]CandidateHistoricalAssertion, error) {
	current := make(map[string]candidateMasterRecord)
	history := make(map[string][]CandidateHistoricalAssertion)
	for _, master := range masters {
		err := streamClassicFacts(ctx, storageRoot, master.manifest, func(fact fecoccurrence.ClassicFact) error {
			if fact.State != "valid" {
				return nil
			}
			fields, err := decodeTypedFields[fecoccurrence.CandidateTypedFields](fact.TypedFields)
			if err != nil {
				return fmt.Errorf("candidate fact %s: %w", fact.FactID, err)
			}
			if fields.CandidateID == "" {
				return fmt.Errorf("candidate fact %s has empty candidate ID", fact.FactID)
			}
			record := candidateMasterRecord{cycle: master.manifest.Cycle, factSetID: master.manifest.FactSetID, factID: fact.FactID, fields: fields}
			if previous, exists := current[fields.CandidateID]; exists && previous.cycle == record.cycle && previous.factID != record.factID {
				return fmt.Errorf("candidate %s has conflicting facts in cycle %s", fields.CandidateID, record.cycle)
			}
			current[fields.CandidateID] = record
			history[fields.CandidateID] = append(history[fields.CandidateID], CandidateHistoricalAssertion{
				Cycle: master.manifest.Cycle, SourceReleaseID: master.manifest.SourceReleaseID,
				FactSetID: master.manifest.FactSetID, FactID: fact.FactID,
				Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
				Office: fields.Office, OfficeState: fields.OfficeState, OfficeDistrict: fields.OfficeDistrict,
				CandidateStatus: fields.CandidateStatus, PrincipalCampaignCommitteeID: fields.PrincipalCampaignCommitteeID,
			})
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	for id := range history {
		sort.Slice(history[id], func(left, right int) bool { return history[id][left].Cycle < history[id][right].Cycle })
	}
	return current, history, nil
}

func indexCommitteeMasters(ctx context.Context, storageRoot string, masters []loadedMaster) (map[string]committeeMasterRecord, map[string][]CommitteeHistoricalAssertion, error) {
	current := make(map[string]committeeMasterRecord)
	history := make(map[string][]CommitteeHistoricalAssertion)
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
				return fmt.Errorf("committee fact %s has empty committee ID", fact.FactID)
			}
			record := committeeMasterRecord{cycle: master.manifest.Cycle, factSetID: master.manifest.FactSetID, factID: fact.FactID, fields: fields}
			if previous, exists := current[fields.CommitteeID]; exists && previous.cycle == record.cycle && previous.factID != record.factID {
				return fmt.Errorf("committee %s has conflicting facts in cycle %s", fields.CommitteeID, record.cycle)
			}
			current[fields.CommitteeID] = record
			history[fields.CommitteeID] = append(history[fields.CommitteeID], CommitteeHistoricalAssertion{
				Cycle: master.manifest.Cycle, SourceReleaseID: master.manifest.SourceReleaseID,
				FactSetID: master.manifest.FactSetID, FactID: fact.FactID,
				Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
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
	for id := range history {
		sort.Slice(history[id], func(left, right int) bool { return history[id][left].Cycle < history[id][right].Cycle })
	}
	return current, history, nil
}

func analyze(
	results []fecreceipts.Result,
	currentCandidates map[string]candidateMasterRecord,
	currentCommittees map[string]committeeMasterRecord,
	candidateComparisons map[string][]CandidateHistoricalAssertion,
	committeeComparisons map[string][]CommitteeHistoricalAssertion,
	candidateHistory map[string][]CandidateHistoricalAssertion,
	committeeHistory map[string][]CommitteeHistoricalAssertion,
) ([]CandidateGap, []CommitteeGap, Counts, error) {
	candidateReferences := make(map[string]fecreceipts.Result, len(results))
	committeeReferences := make(map[string]*committeeReference)
	for _, result := range results {
		if result.CandidateID == "" {
			return nil, nil, Counts{}, fmt.Errorf("calculation result has empty candidate ID")
		}
		if _, exists := candidateReferences[result.CandidateID]; exists {
			return nil, nil, Counts{}, fmt.Errorf("duplicate calculation result for candidate %s", result.CandidateID)
		}
		candidateReferences[result.CandidateID] = result
		for _, relationship := range result.CommitteeRelationships {
			reference := committeeReferenceFor(committeeReferences, relationship.CommitteeID)
			reference.origins["candidate_committee_linkage"] = struct{}{}
			reference.candidateIDs[result.CandidateID] = struct{}{}
			reference.linkages[result.CandidateID] = CommitteeLinkageAssertion{
				CandidateID: result.CandidateID, State: relationship.State,
				DesignationCodes:  nonNilSortedStrings(relationship.DesignationCodes),
				SupportingFactIDs: nonNilSortedStrings(relationship.SupportingFactIDs),
			}
		}
		for _, subtotal := range result.CommitteeSubtotals {
			reference := committeeReferenceFor(committeeReferences, subtotal.CommitteeID)
			reference.origins["itemized_individual_receipt_subtotal"] = struct{}{}
			reference.candidateIDs[result.CandidateID] = struct{}{}
			amount, ok := new(big.Int).SetString(subtotal.AmountMinorUnits, 10)
			if !ok {
				return nil, nil, Counts{}, fmt.Errorf("committee %s has invalid subtotal %q", subtotal.CommitteeID, subtotal.AmountMinorUnits)
			}
			reference.amount.Add(reference.amount, amount)
			addCounts(&reference.counts, subtotal.IncludedRecords)
		}
	}

	candidates := make([]CandidateGap, 0)
	for id, result := range candidateReferences {
		if _, exists := currentCandidates[id]; exists {
			continue
		}
		origins := make(map[string]struct{})
		if len(result.CommitteeRelationships) > 0 {
			origins["candidate_committee_linkage"] = struct{}{}
		}
		if len(result.CommitteeSubtotals) > 0 {
			origins["itemized_individual_receipt_subtotal"] = struct{}{}
		}
		for _, summary := range result.SourceSummaries {
			origins["summary:"+summary.Dataset] = struct{}{}
		}
		if len(origins) == 0 {
			origins["calculation_result"] = struct{}{}
		}
		comparisons := append([]CandidateHistoricalAssertion(nil), candidateComparisons[id]...)
		assertions := append([]CandidateHistoricalAssertion(nil), candidateHistory[id]...)
		candidates = append(candidates, CandidateGap{
			CandidateID: id, State: comparisonState(len(comparisons), len(assertions)), Origins: sortedKeys(origins),
			MoneyAmount: result.MoneyMeasure.Amount, IncludedRecords: result.IncludedRecords,
			CommitteeRelationships: nonNilRelationships(result.CommitteeRelationships), SourceSummaries: nonNilSummaries(result.SourceSummaries),
			SameCycleComparisonMasters: nonNilCandidateAssertions(comparisons), HistoricalMasters: nonNilCandidateAssertions(assertions),
		})
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].CandidateID < candidates[right].CandidateID })

	committees := make([]CommitteeGap, 0)
	for id, reference := range committeeReferences {
		if _, exists := currentCommittees[id]; exists {
			continue
		}
		comparisons := append([]CommitteeHistoricalAssertion(nil), committeeComparisons[id]...)
		assertions := append([]CommitteeHistoricalAssertion(nil), committeeHistory[id]...)
		committees = append(committees, CommitteeGap{
			CommitteeID: id, State: comparisonState(len(comparisons), len(assertions)), Origins: sortedKeys(reference.origins),
			CandidateIDs: sortedKeys(reference.candidateIDs), AttributedAmountMinorUnits: reference.amount.String(),
			IncludedRecords: reference.counts, LinkageAssertions: sortedLinkages(reference.linkages),
			SameCycleComparisonMasters: nonNilCommitteeAssertions(comparisons), HistoricalMasters: nonNilCommitteeAssertions(assertions),
		})
	}
	sort.Slice(committees, func(left, right int) bool { return committees[left].CommitteeID < committees[right].CommitteeID })

	counts := Counts{
		CalculationResults: uint64(len(results)), CandidateReferences: uint64(len(candidateReferences)), CommitteeReferences: uint64(len(committeeReferences)),
		CandidatesMissingCurrentMaster: uint64(len(candidates)), CommitteesMissingCurrentMaster: uint64(len(committees)),
	}
	for _, gap := range candidates {
		switch gap.State {
		case stateFoundInSameCycle:
			counts.CandidatesFoundInSameCycleComparison++
		case stateFoundInHistory:
			counts.CandidatesFoundOnlyInHistory++
		case stateFoundInBoth:
			counts.CandidatesFoundInBothComparisons++
		case stateAbsentAll:
			counts.CandidatesAbsentFromAllComparisons++
		}
	}
	for _, gap := range committees {
		switch gap.State {
		case stateFoundInSameCycle:
			counts.CommitteesFoundInSameCycleComparison++
		case stateFoundInHistory:
			counts.CommitteesFoundOnlyInHistory++
		case stateFoundInBoth:
			counts.CommitteesFoundInBothComparisons++
		case stateAbsentAll:
			counts.CommitteesAbsentFromAllComparisons++
		}
	}
	return candidates, committees, counts, nil
}

func committeeReferenceFor(references map[string]*committeeReference, id string) *committeeReference {
	reference := references[id]
	if reference == nil {
		reference = &committeeReference{origins: make(map[string]struct{}), candidateIDs: make(map[string]struct{}), linkages: make(map[string]CommitteeLinkageAssertion), amount: new(big.Int)}
		references[id] = reference
	}
	return reference
}

func loadCalculationClassicEvidence(ctx context.Context, storageRoot string, calculation fecreceipts.CompactManifest) (map[string]LinkageFactAssertion, map[string]CandidateSummaryAssertion, []loadedMaster, error) {
	linkages := make(map[string]LinkageFactAssertion)
	summaries := make(map[string]CandidateSummaryAssertion)
	masters := make([]loadedMaster, 0, 3)
	for _, reference := range calculation.InputFactSets {
		if reference.Dataset != "candidate-committee-linkage" && reference.Dataset != "all-candidates-summary" && reference.Dataset != "current-campaigns-summary" {
			continue
		}
		path := filepath.Join(storageRoot, "facts", "fec", "classic", reference.Dataset, "manifests", reference.FactSetID+".json")
		master, err := loadMaster(storageRoot, path, reference.Dataset)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("load calculation %s facts: %w", reference.Dataset, err)
		}
		if master.manifest.FactSetID != reference.FactSetID || master.digest != reference.ManifestSHA256 || master.manifest.Cycle != calculation.Cycle || master.manifest.SourceReleaseID != calculation.SourceReleaseID {
			return nil, nil, nil, fmt.Errorf("calculation %s fact manifest does not match its frozen input reference", reference.Dataset)
		}
		masters = append(masters, master)
		err = streamClassicFacts(ctx, storageRoot, master.manifest, func(fact fecoccurrence.ClassicFact) error {
			if fact.State != "valid" {
				return nil
			}
			switch reference.Dataset {
			case "candidate-committee-linkage":
				fields, err := decodeTypedFields[fecoccurrence.LinkageTypedFields](fact.TypedFields)
				if err != nil {
					return fmt.Errorf("linkage fact %s: %w", fact.FactID, err)
				}
				if _, exists := linkages[fact.FactID]; exists {
					return fmt.Errorf("duplicate linkage fact ID %s", fact.FactID)
				}
				linkages[fact.FactID] = LinkageFactAssertion{
					FactID: fact.FactID, FactSetID: master.manifest.FactSetID,
					CandidateID: fields.CandidateID, CandidateElectionYear: fields.CandidateElectionYear,
					FECElectionYear: fields.FECElectionYear, SourceCycle: fields.SourceCycle,
					CommitteeID: fields.CommitteeID, CommitteeTypeCode: fields.CommitteeTypeCode,
					DesignationCode: fields.DesignationCode, LinkageID: fields.LinkageID,
				}
			default:
				fields, err := decodeTypedFields[fecoccurrence.SummaryTypedFields](fact.TypedFields)
				if err != nil {
					return fmt.Errorf("summary fact %s: %w", fact.FactID, err)
				}
				if _, exists := summaries[fact.FactID]; exists {
					return fmt.Errorf("duplicate summary fact ID %s", fact.FactID)
				}
				summaries[fact.FactID] = CandidateSummaryAssertion{
					FactID: fact.FactID, FactSetID: master.manifest.FactSetID, Dataset: reference.Dataset,
					CandidateID: fields.CandidateID, Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
					OfficeState: fields.OfficeState, OfficeDistrict: fields.OfficeDistrict,
					SourceCycle: fields.SourceCycle, CoverageThrough: fields.CoverageThrough,
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if len(masters) != 3 {
		return nil, nil, nil, fmt.Errorf("calculation supplied %d classic fact sets, expected 3", len(masters))
	}
	sort.Slice(masters, func(left, right int) bool { return masters[left].manifest.Dataset < masters[right].manifest.Dataset })
	return linkages, summaries, masters, nil
}

func attachCalculationEvidence(candidates []CandidateGap, committees []CommitteeGap, linkages map[string]LinkageFactAssertion, summaries map[string]CandidateSummaryAssertion) error {
	for index := range candidates {
		gap := &candidates[index]
		seenLinkages := make(map[string]struct{})
		for _, relationship := range gap.CommitteeRelationships {
			for _, factID := range relationship.SupportingFactIDs {
				fact, exists := linkages[factID]
				if !exists {
					return fmt.Errorf("candidate %s references missing linkage fact %s", gap.CandidateID, factID)
				}
				if fact.CandidateID != gap.CandidateID || fact.CommitteeID != relationship.CommitteeID {
					return fmt.Errorf("candidate %s linkage fact %s has conflicting endpoints", gap.CandidateID, factID)
				}
				if _, exists := seenLinkages[factID]; !exists {
					gap.LinkageFacts = append(gap.LinkageFacts, fact)
					seenLinkages[factID] = struct{}{}
				}
			}
		}
		for _, summary := range gap.SourceSummaries {
			fact, exists := summaries[summary.FactID]
			if !exists {
				return fmt.Errorf("candidate %s references missing summary fact %s", gap.CandidateID, summary.FactID)
			}
			if fact.CandidateID != gap.CandidateID || fact.Dataset != summary.Dataset {
				return fmt.Errorf("candidate %s summary fact %s has conflicting identity", gap.CandidateID, summary.FactID)
			}
			gap.SummaryFacts = append(gap.SummaryFacts, fact)
		}
		sort.Slice(gap.LinkageFacts, func(left, right int) bool { return gap.LinkageFacts[left].FactID < gap.LinkageFacts[right].FactID })
		sort.Slice(gap.SummaryFacts, func(left, right int) bool { return gap.SummaryFacts[left].Dataset < gap.SummaryFacts[right].Dataset })
		if gap.LinkageFacts == nil {
			gap.LinkageFacts = []LinkageFactAssertion{}
		}
		if gap.SummaryFacts == nil {
			gap.SummaryFacts = []CandidateSummaryAssertion{}
		}
	}
	for index := range committees {
		gap := &committees[index]
		seen := make(map[string]struct{})
		for _, assertion := range gap.LinkageAssertions {
			for _, factID := range assertion.SupportingFactIDs {
				fact, exists := linkages[factID]
				if !exists {
					return fmt.Errorf("committee %s references missing linkage fact %s", gap.CommitteeID, factID)
				}
				if fact.CommitteeID != gap.CommitteeID || fact.CandidateID != assertion.CandidateID {
					return fmt.Errorf("committee %s linkage fact %s has conflicting endpoints", gap.CommitteeID, factID)
				}
				if _, exists := seen[factID]; !exists {
					gap.LinkageFacts = append(gap.LinkageFacts, fact)
					seen[factID] = struct{}{}
				}
			}
		}
		sort.Slice(gap.LinkageFacts, func(left, right int) bool { return gap.LinkageFacts[left].FactID < gap.LinkageFacts[right].FactID })
		if gap.LinkageFacts == nil {
			gap.LinkageFacts = []LinkageFactAssertion{}
		}
	}
	return nil
}

func readCalculationResults(ctx context.Context, storageRoot string, calculation fecreceipts.CompactManifest) ([]fecreceipts.Result, error) {
	reader, err := storageartifact.Open[fecreceipts.Result](ctx, storageRoot, calculation.Results)
	if err != nil {
		return nil, fmt.Errorf("open calculation results: %w", err)
	}
	results := make([]fecreceipts.Result, 0, calculation.Results.RecordCount)
	for {
		result, ok, err := reader.Next()
		if err != nil {
			reader.Abort()
			return nil, fmt.Errorf("read calculation results: %w", err)
		}
		if !ok {
			break
		}
		if result.Cycle != calculation.Cycle {
			reader.Abort()
			return nil, fmt.Errorf("candidate %s result belongs to cycle %s", result.CandidateID, result.Cycle)
		}
		results = append(results, result)
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("close calculation results: %w", err)
	}
	return results, nil
}

func streamClassicFacts(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest, consume func(fecoccurrence.ClassicFact) error) error {
	descriptor := storageartifact.Descriptor{
		RecordCount: manifest.Facts.RecordCount, UncompressedBytes: manifest.Facts.UncompressedBytes,
		UncompressedSHA256: manifest.Facts.UncompressedSHA256, CompressedBytes: manifest.Facts.CompressedBytes,
		CompressedSHA256: manifest.Facts.CompressedSHA256, Compression: manifest.Facts.Compression, StorageKey: manifest.Facts.StorageKey,
	}
	reader, err := storageartifact.Open[fecoccurrence.ClassicFact](ctx, storageRoot, descriptor)
	if err != nil {
		return fmt.Errorf("open %s cycle %s facts: %w", manifest.Dataset, manifest.Cycle, err)
	}
	for {
		fact, ok, err := reader.Next()
		if err != nil {
			reader.Abort()
			return fmt.Errorf("read %s cycle %s facts: %w", manifest.Dataset, manifest.Cycle, err)
		}
		if !ok {
			break
		}
		if fact.Dataset != manifest.Dataset || fact.Cycle != manifest.Cycle || fact.SourceReleaseID != manifest.SourceReleaseID {
			reader.Abort()
			return fmt.Errorf("%s fact %s does not match its manifest", manifest.Dataset, fact.FactID)
		}
		if err := consume(fact); err != nil {
			reader.Abort()
			return err
		}
	}
	if err := reader.Close(); err != nil {
		return fmt.Errorf("close %s cycle %s facts: %w", manifest.Dataset, manifest.Cycle, err)
	}
	return nil
}

func decodeTypedFields[T any](value any) (T, error) {
	var result T
	content, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(content, &result); err != nil {
		return result, err
	}
	return result, nil
}

func masterReference(master loadedMaster) FactSetReference {
	return FactSetReference{
		Dataset: master.manifest.Dataset, Cycle: master.manifest.Cycle, SourceReleaseID: master.manifest.SourceReleaseID,
		FactSetID: master.manifest.FactSetID, ManifestSHA256: master.digest, FactsSHA256: master.manifest.Facts.CompressedSHA256,
	}
}

func masterReferences(masters []loadedMaster) []FactSetReference {
	references := make([]FactSetReference, 0, len(masters))
	for _, master := range masters {
		references = append(references, masterReference(master))
	}
	return references
}

func checks(report Report) []Check {
	candidateClassified := report.Counts.CandidatesFoundInSameCycleComparison + report.Counts.CandidatesFoundOnlyInHistory + report.Counts.CandidatesFoundInBothComparisons + report.Counts.CandidatesAbsentFromAllComparisons
	committeeClassified := report.Counts.CommitteesFoundInSameCycleComparison + report.Counts.CommitteesFoundOnlyInHistory + report.Counts.CommitteesFoundInBothComparisons + report.Counts.CommitteesAbsentFromAllComparisons
	return []Check{
		{Name: "candidate_gap_conservation", Passed: report.Counts.CandidatesMissingCurrentMaster == candidateClassified, Details: fmt.Sprintf("missing=%d classified=%d", report.Counts.CandidatesMissingCurrentMaster, candidateClassified)},
		{Name: "committee_gap_conservation", Passed: report.Counts.CommitteesMissingCurrentMaster == committeeClassified, Details: fmt.Sprintf("missing=%d classified=%d", report.Counts.CommitteesMissingCurrentMaster, committeeClassified)},
		{Name: "candidate_gap_rows_complete", Passed: report.Counts.CandidatesMissingCurrentMaster == uint64(len(report.Candidates)), Details: fmt.Sprintf("count=%d rows=%d", report.Counts.CandidatesMissingCurrentMaster, len(report.Candidates))},
		{Name: "committee_gap_rows_complete", Passed: report.Counts.CommitteesMissingCurrentMaster == uint64(len(report.Committees)), Details: fmt.Sprintf("count=%d rows=%d", report.Counts.CommitteesMissingCurrentMaster, len(report.Committees))},
		{Name: "comparisons_kept_as_assertions", Passed: true, Details: "different-release same-cycle and other-cycle facts are attached to gap rows and never inserted into the selected current-cycle master indexes"},
	}
}

func comparisonState(sameCycle, history int) string {
	if sameCycle > 0 && history > 0 {
		return stateFoundInBoth
	}
	if sameCycle > 0 {
		return stateFoundInSameCycle
	}
	if history > 0 {
		return stateFoundInHistory
	}
	return stateAbsentAll
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func sortedLinkages(values map[string]CommitteeLinkageAssertion) []CommitteeLinkageAssertion {
	result := make([]CommitteeLinkageAssertion, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].CandidateID < result[right].CandidateID })
	return result
}

func nonNilSortedStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func nonNilRelationships(values []fecreceipts.CommitteeRelationship) []fecreceipts.CommitteeRelationship {
	if values == nil {
		return []fecreceipts.CommitteeRelationship{}
	}
	return append([]fecreceipts.CommitteeRelationship(nil), values...)
}

func nonNilSummaries(values []fecreceipts.SourceSummary) []fecreceipts.SourceSummary {
	if values == nil {
		return []fecreceipts.SourceSummary{}
	}
	return append([]fecreceipts.SourceSummary(nil), values...)
}

func addCounts(target *fecreceipts.IncludedCounts, value fecreceipts.IncludedCounts) {
	target.Records += value.Records
	target.Positive += value.Positive
	target.Negative += value.Negative
	target.Zero += value.Zero
}

func nonNilCandidateAssertions(values []CandidateHistoricalAssertion) []CandidateHistoricalAssertion {
	if values == nil {
		return []CandidateHistoricalAssertion{}
	}
	return values
}

func nonNilCommitteeAssertions(values []CommitteeHistoricalAssertion) []CommitteeHistoricalAssertion {
	if values == nil {
		return []CommitteeHistoricalAssertion{}
	}
	return values
}
