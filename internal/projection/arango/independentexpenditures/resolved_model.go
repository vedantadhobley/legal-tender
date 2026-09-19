package independentexpenditures

import (
	"context"
	"fmt"
	"math/big"
	"path/filepath"

	feccandidate "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type resolvedLoadedInputs struct {
	aggregate        feccandidate.AggregateManifest
	aggregateDigest  string
	resolution       feccandidate.Manifest
	resolutionDigest string
	candidates       fecoccurrence.ClassicFactManifest
	candidatesDigest string
	committees       fecoccurrence.ClassicFactManifest
	committeesDigest string
}

func loadResolvedInputs(ctx context.Context, input ResolvedInput) (resolvedLoadedInputs, error) {
	if input.ReadinessBundlePath != "" {
		bundle, _, resolved, err := feccandidate.LoadResolvedProjectionBundle(ctx, input.StorageRoot, input.ReadinessBundlePath)
		if err != nil {
			return resolvedLoadedInputs{}, fmt.Errorf("load resolved projection readiness bundle: %w", err)
		}
		if bundle.Cycle != input.Cycle {
			return resolvedLoadedInputs{}, fmt.Errorf("resolved projection bundle belongs to cycle %s, expected %s", bundle.Cycle, input.Cycle)
		}
		input.AggregateManifestPath = resolved.AggregateManifestPath
		input.ResolutionManifestPath = resolved.ResolutionManifestPath
		input.CandidateManifestPath = resolved.CandidateManifestPath
		input.CommitteeManifestPath = resolved.CommitteeManifestPath
	}
	aggregate, aggregateDigest, err := feccandidate.LoadPublishedAggregateManifest(ctx, input.StorageRoot, input.AggregateManifestPath)
	if err != nil {
		return resolvedLoadedInputs{}, fmt.Errorf("load resolved independent expenditures: %w", err)
	}
	if input.ResolutionManifestPath == "" {
		input.ResolutionManifestPath = filepath.Join(
			input.StorageRoot, "calculations", "fec", "independent-expenditure-candidate-resolution", "manifests",
			aggregate.InputResolution.CalculationSetID+".json",
		)
	}
	resolution, resolutionDigest, err := feccandidate.LoadPublishedManifest(ctx, input.StorageRoot, input.ResolutionManifestPath)
	if err != nil {
		return resolvedLoadedInputs{}, fmt.Errorf("load candidate-resolution ancestry: %w", err)
	}
	if resolution.CalculationSetID != aggregate.InputResolution.CalculationSetID ||
		resolutionDigest != aggregate.InputResolution.ManifestSHA256 ||
		resolution.Decisions.CompressedSHA256 != aggregate.InputResolution.DecisionsSHA256 {
		return resolvedLoadedInputs{}, fmt.Errorf("resolved calculation does not match candidate-resolution ancestry")
	}
	candidates, candidatesDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(input.StorageRoot, input.CandidateManifestPath, "candidate-master")
	if err != nil {
		return resolvedLoadedInputs{}, fmt.Errorf("load candidate master facts: %w", err)
	}
	committees, committeesDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(input.StorageRoot, input.CommitteeManifestPath, "committee-master")
	if err != nil {
		return resolvedLoadedInputs{}, fmt.Errorf("load committee master facts: %w", err)
	}
	if candidates.FactSetID != resolution.InputCandidateFactSet.FactSetID || candidatesDigest != resolution.InputCandidateFactSet.ManifestSHA256 {
		return resolvedLoadedInputs{}, fmt.Errorf("candidate master differs from the exact candidate-resolution input")
	}
	for dataset, manifest := range map[string]fecoccurrence.ClassicFactManifest{
		"candidate-master": candidates, "committee-master": committees,
	} {
		if manifest.Cycle != aggregate.Cycle || manifest.SourceReleaseID != aggregate.SourceReleaseID {
			return resolvedLoadedInputs{}, fmt.Errorf("%s facts do not share the aggregate cycle and source release", dataset)
		}
	}
	if input.Cycle != "" && aggregate.Cycle != input.Cycle {
		return resolvedLoadedInputs{}, fmt.Errorf("resolved independent expenditures belong to cycle %s, expected %s", aggregate.Cycle, input.Cycle)
	}
	return resolvedLoadedInputs{
		aggregate: aggregate, aggregateDigest: aggregateDigest,
		resolution: resolution, resolutionDigest: resolutionDigest,
		candidates: candidates, candidatesDigest: candidatesDigest,
		committees: committees, committeesDigest: committeesDigest,
	}, nil
}

func buildResolvedProjection(ctx context.Context, storageRoot string, inputs resolvedLoadedInputs) (resolvedProjection, error) {
	references := InputReferences{
		SourceReleaseID:  inputs.aggregate.SourceReleaseID,
		CalculationSetID: inputs.aggregate.CalculationSetID, CalculationManifestSHA256: inputs.aggregateDigest,
		ScheduleEFactSetID:      inputs.resolution.InputCalculation.ScheduleEFactSetID,
		ScheduleEManifestSHA256: inputs.resolution.InputCalculation.ScheduleEManifestSHA256,
		CandidateFactSetID:      inputs.candidates.FactSetID, CandidateManifestSHA256: inputs.candidatesDigest,
		CommitteeFactSetID: inputs.committees.FactSetID, CommitteeManifestSHA256: inputs.committeesDigest,
		CandidateResolutionCalculationSetID: inputs.resolution.CalculationSetID,
		CandidateResolutionManifestSHA256:   inputs.resolutionDigest,
		CandidateResolutionDecisionsSHA256:  inputs.resolution.Decisions.CompressedSHA256,
	}
	projectionID := digestParts(
		ResolvedProjectionVersion, inputs.aggregate.Cycle,
		references.CalculationSetID, references.CalculationManifestSHA256,
		references.CandidateResolutionCalculationSetID, references.CandidateResolutionManifestSHA256,
		references.CandidateResolutionDecisionsSHA256,
		references.ScheduleEFactSetID, references.ScheduleEManifestSHA256,
		references.CandidateFactSetID, references.CandidateManifestSHA256,
		references.CommitteeFactSetID, references.CommitteeManifestSHA256,
	)
	result := resolvedProjection{
		ID: projectionID, Database: resolvedDatabaseName(inputs.aggregate.Cycle, projectionID),
		Cycle: inputs.aggregate.Cycle, Inputs: references,
		Coverage: CandidateResolutionCoverage{
			SourceDecisions:         inputs.aggregate.Counts.SourceDecisions,
			ProjectableDecisions:    inputs.aggregate.Counts.ProjectableDecisions,
			UnprojectableDecisions:  inputs.aggregate.Counts.UnprojectableDecisions,
			ProjectableMinorUnits:   inputs.aggregate.Amounts.ProjectableMinorUnits,
			UnprojectableMinorUnits: inputs.aggregate.Amounts.UnprojectableMinorUnits,
		},
	}

	candidateMasters, err := resolvedCandidateMasters(ctx, storageRoot, inputs.candidates, result.Cycle)
	if err != nil {
		return resolvedProjection{}, err
	}
	committeeMasters, err := resolvedCommitteeMasters(ctx, storageRoot, inputs.committees, result.Cycle)
	if err != nil {
		return resolvedProjection{}, err
	}

	entities := make(map[string]entityDocument)
	edges := make(map[string]resolvedExpenditureEdge)
	candidateDegrees := make(map[string]int)
	spenderDegrees := make(map[string]int)
	var attributed, support, opposition big.Int
	var confirmedAmount, resolvedAmount, unverifiedAmount big.Int
	var expenditureCount, confirmedCount, resolvedCount, unverifiedCount uint64
	reader, err := storageartifact.Open[feccandidate.AggregateResult](ctx, storageRoot, inputs.aggregate.Results)
	if err != nil {
		return resolvedProjection{}, fmt.Errorf("open resolved independent-expenditure results: %w", err)
	}
	for {
		calculationResult, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return resolvedProjection{}, fmt.Errorf("read resolved independent-expenditure result: %w", readErr)
		}
		if !ok {
			break
		}
		if err := feccandidate.ValidateAggregateResult(calculationResult); err != nil {
			reader.Abort()
			return resolvedProjection{}, err
		}
		if calculationResult.CalculationSetID != inputs.aggregate.CalculationSetID || calculationResult.Cycle != inputs.aggregate.Cycle {
			reader.Abort()
			return resolvedProjection{}, fmt.Errorf("resolved independent-expenditure result does not match its manifest")
		}
		if err := validFECID("spender committee", calculationResult.SpenderCommitteeID); err != nil {
			reader.Abort()
			return resolvedProjection{}, err
		}
		if err := validFECID("candidate", calculationResult.CandidateID); err != nil {
			reader.Abort()
			return resolvedProjection{}, err
		}
		amount, _ := new(big.Int).SetString(calculationResult.SignedAmountMinorUnits, 10)
		attributed.Add(&attributed, amount)
		if calculationResult.SupportOppose == "S" {
			support.Add(&support, amount)
			result.Counts.SupportEdges++
		} else {
			opposition.Add(&opposition, amount)
			result.Counts.OppositionEdges++
		}
		expenditureCount += calculationResult.ExpenditureCount
		confirmedCount += calculationResult.ResolutionCounts.Confirmed
		resolvedCount += calculationResult.ResolutionCounts.Resolved
		unverifiedCount += calculationResult.ResolutionCounts.Unverified
		addExact(&confirmedAmount, calculationResult.ResolutionAmounts.ConfirmedMinorUnits)
		addExact(&resolvedAmount, calculationResult.ResolutionAmounts.ResolvedMinorUnits)
		addExact(&unverifiedAmount, calculationResult.ResolutionAmounts.UnverifiedMinorUnits)

		candidateDocumentKey := candidateKey(calculationResult.CandidateID)
		if _, exists := entities[candidateDocumentKey]; !exists {
			if master, present := candidateMasters[candidateDocumentKey]; present {
				entities[candidateDocumentKey] = master
			} else {
				entities[candidateDocumentKey] = missingEntity("candidate", calculationResult.CandidateID, result.Cycle)
				result.Missing.Candidates++
			}
		}
		spenderDocumentKey := committeeKey(calculationResult.SpenderCommitteeID)
		if _, exists := entities[spenderDocumentKey]; !exists {
			if master, present := committeeMasters[spenderDocumentKey]; present {
				entities[spenderDocumentKey] = master
			} else {
				entities[spenderDocumentKey] = missingEntity("committee", calculationResult.SpenderCommitteeID, result.Cycle)
				result.Missing.Spenders++
			}
		}
		relationType := "independent_expenditure_support"
		if calculationResult.SupportOppose == "O" {
			relationType = "independent_expenditure_oppose"
		}
		edge := resolvedExpenditureEdge{
			Key:  "ie_resolved_" + calculationResult.ResultID[:40],
			From: entitiesCollection + "/" + spenderDocumentKey, To: entitiesCollection + "/" + candidateDocumentKey,
			SchemaVersion: ResolvedEdgeSchemaVersion, RelationType: relationType, Cycle: result.Cycle,
			SupportOppose: calculationResult.SupportOppose, AmountMinorUnits: calculationResult.SignedAmountMinorUnits,
			ExpenditureCount: calculationResult.ExpenditureCount, PositiveCount: calculationResult.PositiveCount,
			NegativeCount: calculationResult.NegativeCount, ZeroCount: calculationResult.ZeroCount,
			ResolutionCounts: EdgeResolutionCounts{
				Confirmed: calculationResult.ResolutionCounts.Confirmed, Resolved: calculationResult.ResolutionCounts.Resolved,
				Unverified: calculationResult.ResolutionCounts.Unverified,
			},
			ResolutionAmounts: EdgeResolutionAmounts{
				ConfirmedMinorUnits:  calculationResult.ResolutionAmounts.ConfirmedMinorUnits,
				ResolvedMinorUnits:   calculationResult.ResolutionAmounts.ResolvedMinorUnits,
				UnverifiedMinorUnits: calculationResult.ResolutionAmounts.UnverifiedMinorUnits,
			},
			ResultID: calculationResult.ResultID, CalculationSetID: inputs.aggregate.CalculationSetID,
			CandidateResolutionCalculationSetID: inputs.resolution.CalculationSetID,
			ScheduleEFactSetID:                  inputs.resolution.InputCalculation.ScheduleEFactSetID,
			SourceReleaseID:                     inputs.aggregate.SourceReleaseID,
		}
		edge.DocumentDigest = documentDigest(edge)
		if _, exists := edges[edge.Key]; exists {
			reader.Abort()
			return resolvedProjection{}, fmt.Errorf("duplicate resolved independent-expenditure result %s", calculationResult.ResultID)
		}
		edges[edge.Key] = edge
		candidateDegrees[calculationResult.CandidateID]++
		spenderDegrees[calculationResult.SpenderCommitteeID]++
	}
	if err := reader.Close(); err != nil {
		return resolvedProjection{}, fmt.Errorf("close resolved independent-expenditure result reader: %w", err)
	}
	if attributed.String() != inputs.aggregate.Amounts.ProjectableMinorUnits || uint64(len(edges)) != inputs.aggregate.Counts.ResultGroups ||
		expenditureCount != inputs.aggregate.Counts.ProjectableDecisions || confirmedCount != inputs.aggregate.Counts.Confirmed ||
		resolvedCount != inputs.aggregate.Counts.Resolved || unverifiedCount != inputs.aggregate.Counts.Unverified ||
		confirmedAmount.String() != inputs.aggregate.Amounts.ConfirmedMinorUnits || resolvedAmount.String() != inputs.aggregate.Amounts.ResolvedMinorUnits ||
		unverifiedAmount.String() != inputs.aggregate.Amounts.UnverifiedMinorUnits {
		return resolvedProjection{}, fmt.Errorf("resolved projection does not conserve calculation results and quality components")
	}

	result.Entities = sortedMapValues(entities, func(document entityDocument) string { return document.Key })
	result.Edges = sortedMapValues(edges, func(edge resolvedExpenditureEdge) string { return edge.Key })
	for _, document := range result.Entities {
		if document.EntityType == "candidate" {
			result.Counts.Candidates++
		} else if document.EntityType == "committee" {
			result.Counts.Spenders++
		} else {
			return resolvedProjection{}, fmt.Errorf("unsupported projected entity type %q", document.EntityType)
		}
	}
	result.Counts.Entities = uint64(len(result.Entities))
	result.Counts.Edges = uint64(len(result.Edges))
	if result.Counts.SupportEdges+result.Counts.OppositionEdges != result.Counts.Edges {
		return resolvedProjection{}, fmt.Errorf("resolved support and opposition edge counts are not conserved")
	}
	result.Amounts = ProjectionAmounts{
		AttributedMinorUnits: attributed.String(), SupportMinorUnits: support.String(), OppositionMinorUnits: opposition.String(),
	}
	result.RepresentativeCandidate = representative(candidateDegrees)
	result.RepresentativeSpender = representative(spenderDegrees)
	if result.RepresentativeCandidate == "" || result.RepresentativeSpender == "" {
		return resolvedProjection{}, fmt.Errorf("no representative resolved graph endpoints were projected")
	}
	coverage := result.Coverage
	result.Metadata = projectionMetadata{
		Key: result.ID, SchemaVersion: ResolvedMetadataSchema, ProjectionVersion: ResolvedProjectionVersion,
		ProjectionID: result.ID, Cycle: result.Cycle, Graph: GraphName, Inputs: result.Inputs,
		Counts: result.Counts, Amounts: result.Amounts, Missing: result.Missing, Coverage: &coverage,
	}
	result.Metadata.DocumentDigest = documentDigest(result.Metadata)
	return result, nil
}

func resolvedCandidateMasters(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest, cycle string) (map[string]entityDocument, error) {
	masters := make(map[string]entityDocument)
	err := streamClassicFacts(ctx, storageRoot, manifest, func(fact fecoccurrence.ClassicFact) error {
		if fact.State != "valid" {
			return nil
		}
		fields, err := decodeTypedFields[fecoccurrence.CandidateTypedFields](fact.TypedFields)
		if err != nil {
			return fmt.Errorf("candidate fact %s: %w", fact.FactID, err)
		}
		if err := validFECID("candidate", fields.CandidateID); err != nil {
			return err
		}
		factID, factSetID := fact.FactID, manifest.FactSetID
		document := entityDocument{
			Key: candidateKey(fields.CandidateID), SchemaVersion: EntitySchemaVersion,
			EntityType: "candidate", EntityID: fields.CandidateID, Cycle: cycle,
			SourceState: "present", SourceFactID: &factID, SourceFactSetID: &factSetID,
			Name: fields.Name, PartyAffiliation: fields.PartyAffiliation, Office: fields.Office,
			OfficeState: fields.OfficeState, OfficeDistrict: fields.OfficeDistrict,
		}
		document.DocumentDigest = documentDigest(document)
		return addEntity(masters, document)
	})
	return masters, err
}

func resolvedCommitteeMasters(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest, cycle string) (map[string]entityDocument, error) {
	masters := make(map[string]entityDocument)
	err := streamClassicFacts(ctx, storageRoot, manifest, func(fact fecoccurrence.ClassicFact) error {
		if fact.State != "valid" {
			return nil
		}
		fields, err := decodeTypedFields[fecoccurrence.CommitteeTypedFields](fact.TypedFields)
		if err != nil {
			return fmt.Errorf("committee fact %s: %w", fact.FactID, err)
		}
		if err := validFECID("committee", fields.CommitteeID); err != nil {
			return err
		}
		factID, factSetID := fact.FactID, manifest.FactSetID
		document := entityDocument{
			Key: committeeKey(fields.CommitteeID), SchemaVersion: EntitySchemaVersion,
			EntityType: "committee", EntityID: fields.CommitteeID, Cycle: cycle,
			SourceState: "present", SourceFactID: &factID, SourceFactSetID: &factSetID,
			Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
			DesignationCode: fields.DesignationCode, CommitteeTypeCode: fields.CommitteeTypeCode,
			OrganizationTypeCode: fields.OrganizationTypeCode, ConnectedOrganization: fields.ConnectedOrganization,
		}
		document.DocumentDigest = documentDigest(document)
		return addEntity(masters, document)
	})
	return masters, err
}

func addExact(target *big.Int, text string) {
	value, _ := new(big.Int).SetString(text, 10)
	target.Add(target, value)
}

func resolvedDatabaseName(cycle, projectionID string) string {
	return "lt_ie_probe_resolved_" + cycle + "_" + projectionID[:16]
}
