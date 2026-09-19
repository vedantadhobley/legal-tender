package committeeflows

import (
	"context"
	"fmt"
	"math/big"

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecidentity "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeidentity"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type loadedInputsV2 struct {
	bundle            fecidentity.ProjectionBundleManifest
	bundleDigest      string
	baseBundle        fecflows.ProjectionBundleManifest
	calculation       fecflows.Manifest
	calculationDigest string
	committees        fecoccurrence.ClassicFactManifest
	committeesDigest  string
	identity          fecidentity.Manifest
	identityDigest    string
}

func loadInputsV2(ctx context.Context, input InputV2) (loadedInputsV2, error) {
	bundle, bundleDigest, resolved, err := fecidentity.LoadProjectionBundle(ctx, input.StorageRoot, input.ReadinessBundlePath)
	if err != nil {
		return loadedInputsV2{}, fmt.Errorf("load identity-aware receiver-flow projection bundle: %w", err)
	}
	if bundle.Cycle != input.Cycle {
		return loadedInputsV2{}, fmt.Errorf("receiver-flow v2 bundle belongs to cycle %s, expected %s", bundle.Cycle, input.Cycle)
	}
	baseBundle, _, _, err := fecflows.LoadProjectionBundle(ctx, input.StorageRoot, resolved.BaseBundlePath)
	if err != nil {
		return loadedInputsV2{}, fmt.Errorf("load base receiver-flow bundle: %w", err)
	}
	calculation, calculationDigest, err := fecflows.LoadPublishedManifest(ctx, input.StorageRoot, resolved.BaseFlowInputs.CalculationManifestPath)
	if err != nil {
		return loadedInputsV2{}, fmt.Errorf("load receiver-flow calculation: %w", err)
	}
	committees, committeesDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(input.StorageRoot, resolved.BaseFlowInputs.CommitteeManifestPath, "committee-master")
	if err != nil {
		return loadedInputsV2{}, fmt.Errorf("load selected committee master: %w", err)
	}
	identity, identityDigest, err := fecidentity.LoadPublishedManifest(ctx, input.StorageRoot, resolved.IdentityCalculationPath)
	if err != nil {
		return loadedInputsV2{}, fmt.Errorf("load committee-identity coverage: %w", err)
	}
	if baseBundle.BundleID != bundle.BaseFlowBundle.BundleID || calculation.Cycle != bundle.Cycle ||
		committees.Cycle != bundle.Cycle || identity.Cycle != bundle.Cycle ||
		calculation.SourceReleaseID != bundle.SourceReleaseID || committees.SourceReleaseID != bundle.SourceReleaseID ||
		identity.SourceReleaseID != bundle.SourceReleaseID || identityDigest != bundle.IdentityCoverage.ManifestSHA256 {
		return loadedInputsV2{}, fmt.Errorf("receiver-flow v2 inputs do not share exact bundle ancestry")
	}
	return loadedInputsV2{
		bundle: bundle, bundleDigest: bundleDigest, baseBundle: baseBundle,
		calculation: calculation, calculationDigest: calculationDigest,
		committees: committees, committeesDigest: committeesDigest,
		identity: identity, identityDigest: identityDigest,
	}, nil
}

func buildProjectionV2(ctx context.Context, storageRoot string, inputs loadedInputsV2) (projectionV2, error) {
	references := InputReferencesV2{
		SourceReleaseID:   inputs.bundle.SourceReleaseID,
		ReadinessBundleID: inputs.bundle.BundleID, ReadinessBundleSHA256: inputs.bundleDigest,
		BaseFlowBundleID: inputs.baseBundle.BundleID, BaseFlowBundleSHA256: inputs.bundle.BaseFlowBundle.ManifestSHA256,
		CalculationSetID: inputs.calculation.CalculationSetID, CalculationManifestSHA256: inputs.calculationDigest,
		ScheduleAFactSetID:      inputs.calculation.InputFactSet.FactSetID,
		ScheduleAManifestSHA256: inputs.calculation.InputFactSet.ManifestSHA256,
		CommitteeFactSetID:      inputs.committees.FactSetID, CommitteeManifestSHA256: inputs.committeesDigest,
		IdentityCalculationSetID: inputs.identity.CalculationSetID, IdentityManifestSHA256: inputs.identityDigest,
		IdentityDecisionsSHA256: inputs.identity.Decisions.CompressedSHA256,
	}
	projectionID := digestParts(
		ProjectionVersionV2, inputs.bundle.Cycle, references.ReadinessBundleID,
		references.ReadinessBundleSHA256, references.BaseFlowBundleID,
		references.BaseFlowBundleSHA256, references.CalculationSetID,
		references.CalculationManifestSHA256, references.ScheduleAFactSetID,
		references.ScheduleAManifestSHA256, references.CommitteeFactSetID,
		references.CommitteeManifestSHA256, references.IdentityCalculationSetID,
		references.IdentityManifestSHA256, references.IdentityDecisionsSHA256,
	)
	result := projectionV2{
		ID: projectionID, Database: databaseNameV2(inputs.bundle.Cycle, projectionID),
		Cycle: inputs.bundle.Cycle, Inputs: references,
	}

	masters := make(map[string]entityDocumentV2)
	if err := streamClassicFacts(ctx, storageRoot, inputs.committees, func(fact fecoccurrence.ClassicFact) error {
		if fact.State != "valid" {
			return nil
		}
		fields, err := decodeTypedFields[fecoccurrence.CommitteeTypedFields](fact.TypedFields)
		if err != nil {
			return fmt.Errorf("committee fact %s: %w", fact.FactID, err)
		}
		if err := validCommitteeID(fields.CommitteeID); err != nil {
			return err
		}
		factID, factSetID := fact.FactID, inputs.committees.FactSetID
		document := entityDocumentV2{
			Key: committeeKey(fields.CommitteeID), SchemaVersion: EntitySchemaVersionV2,
			EntityType: "committee", EntityID: fields.CommitteeID, Cycle: result.Cycle,
			SourceState: "current_cycle_master", TerminalIdentityEligible: true,
			SourceFactID: &factID, SourceFactSetID: &factSetID,
			Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
			DesignationCode: fields.DesignationCode, CommitteeTypeCode: fields.CommitteeTypeCode,
			OrganizationTypeCode: fields.OrganizationTypeCode, ConnectedOrganization: fields.ConnectedOrganization,
			SameCycleAssertions:  []fecflowmastergaps.CommitteeHistoricalAssertion{},
			HistoricalAssertions: []fecflowmastergaps.CommitteeHistoricalAssertion{},
		}
		document.DocumentDigest = documentDigest(document)
		if previous, exists := masters[document.Key]; exists && previous.DocumentDigest != document.DocumentDigest {
			return fmt.Errorf("conflicting committee master facts for %s", fields.CommitteeID)
		}
		masters[document.Key] = document
		return nil
	}); err != nil {
		return projectionV2{}, err
	}

	decisions, err := fecidentity.ReadDecisions(ctx, storageRoot, inputs.identity)
	if err != nil {
		return projectionV2{}, err
	}
	decisionByCommittee := make(map[string]fecidentity.Decision, len(decisions))
	for _, decision := range decisions {
		if _, exists := masters[committeeKey(decision.CommitteeID)]; exists {
			return projectionV2{}, fmt.Errorf("committee %s has both a current master and a gap decision", decision.CommitteeID)
		}
		if _, duplicate := decisionByCommittee[decision.CommitteeID]; duplicate {
			return projectionV2{}, fmt.Errorf("duplicate identity decision for %s", decision.CommitteeID)
		}
		decisionByCommittee[decision.CommitteeID] = decision
	}

	entities := make(map[string]entityDocumentV2)
	edges := make(map[string]flowEdge)
	usedDecisions := make(map[string]struct{}, len(decisions))
	adjacencySets := make(map[string]map[string]struct{})
	var total, registered, inKind, transfer, refund big.Int
	reader, err := storageartifact.Open[fecflows.Result](ctx, storageRoot, inputs.calculation.Results)
	if err != nil {
		return projectionV2{}, fmt.Errorf("open receiver-flow results: %w", err)
	}
	for {
		calculationResult, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return projectionV2{}, fmt.Errorf("read receiver-flow result: %w", readErr)
		}
		if !ok {
			break
		}
		if err := validateCalculationResult(calculationResult, inputs.calculation); err != nil {
			reader.Abort()
			return projectionV2{}, err
		}
		amount, _ := new(big.Int).SetString(calculationResult.SignedAmountMinorUnits, 10)
		total.Add(&total, amount)
		switch calculationResult.ReceiptRole {
		case fecflows.RoleRegisteredFilerContribution:
			registered.Add(&registered, amount)
			result.Counts.RegisteredFilerContribution++
		case fecflows.RoleRegisteredFilerInKind:
			inKind.Add(&inKind, amount)
			result.Counts.InKindContribution++
		case fecflows.RoleAffiliatedTransferIn:
			transfer.Add(&transfer, amount)
			result.Counts.AffiliatedTransferIn++
		case fecflows.RoleRefundOrRepaymentReceived:
			refund.Add(&refund, amount)
			result.Counts.RefundRepaymentReceived++
		default:
			reader.Abort()
			return projectionV2{}, fmt.Errorf("unsupported receiver-flow role %q", calculationResult.ReceiptRole)
		}

		for _, committeeID := range []string{calculationResult.SourceCommitteeID, calculationResult.RecipientCommitteeID} {
			key := committeeKey(committeeID)
			if _, exists := entities[key]; exists {
				continue
			}
			if master, exists := masters[key]; exists {
				entities[key] = master
				result.Counts.CurrentCycleMasters++
				result.Counts.TerminalIdentityEligible++
				continue
			}
			decision, exists := decisionByCommittee[committeeID]
			if !exists {
				reader.Abort()
				return projectionV2{}, fmt.Errorf("committee %s has neither a current master nor an identity decision", committeeID)
			}
			entities[key] = decisionEntityV2(decision, result.Cycle)
			usedDecisions[committeeID] = struct{}{}
			result.Counts.TerminalIdentityIneligible++
			switch decision.State {
			case fecidentity.StateHistoricalRegistration:
				result.Counts.HistoricalRegistrations++
			case fecidentity.StateAlternateReleaseRegistration:
				result.Counts.AlternateReleaseRegistrations++
			case fecidentity.StateUnresolvedReportedID:
				result.Counts.UnresolvedReportedIDs++
			default:
				reader.Abort()
				return projectionV2{}, fmt.Errorf("unsupported identity state %q", decision.State)
			}
		}

		edge := flowEdge{
			Key:           edgeKey(calculationResult.ResultID),
			From:          entitiesCollection + "/" + committeeKey(calculationResult.SourceCommitteeID),
			To:            entitiesCollection + "/" + committeeKey(calculationResult.RecipientCommitteeID),
			SchemaVersion: EdgeSchemaVersion, RelationType: "receiver_reported_committee_flow",
			Cycle: result.Cycle, ReceiptRole: calculationResult.ReceiptRole,
			AmountMinorUnits: calculationResult.SignedAmountMinorUnits,
			ReceiptCount:     calculationResult.ReceiptCount, PositiveCount: calculationResult.PositiveCount,
			NegativeCount: calculationResult.NegativeCount, ZeroCount: calculationResult.ZeroCount,
			ResultID: calculationResult.ResultID, CalculationSetID: inputs.calculation.CalculationSetID,
			ScheduleAFactSetID: inputs.calculation.InputFactSet.FactSetID,
			SourceReleaseID:    inputs.calculation.SourceReleaseID,
		}
		edge.DocumentDigest = documentDigest(edge)
		if previous, exists := edges[edge.Key]; exists {
			reader.Abort()
			if previous.DocumentDigest != edge.DocumentDigest {
				return projectionV2{}, fmt.Errorf("conflicting receiver-flow edge %s", edge.Key)
			}
			return projectionV2{}, fmt.Errorf("duplicate receiver-flow result %s", calculationResult.ResultID)
		}
		edges[edge.Key] = edge
		if adjacencySets[calculationResult.SourceCommitteeID] == nil {
			adjacencySets[calculationResult.SourceCommitteeID] = make(map[string]struct{})
		}
		adjacencySets[calculationResult.SourceCommitteeID][calculationResult.RecipientCommitteeID] = struct{}{}
		if adjacencySets[calculationResult.RecipientCommitteeID] == nil {
			adjacencySets[calculationResult.RecipientCommitteeID] = make(map[string]struct{})
		}
	}
	if err := reader.Close(); err != nil {
		return projectionV2{}, fmt.Errorf("close receiver-flow result reader: %w", err)
	}

	expected, ok := new(big.Int).SetString(inputs.calculation.Amounts.IncludedMinorUnits, 10)
	if !ok || expected.Cmp(&total) != 0 || uint64(len(edges)) != inputs.calculation.ResultCounts.ResultGroups {
		return projectionV2{}, fmt.Errorf("receiver-flow v2 projection does not conserve calculation results")
	}
	if len(usedDecisions) != len(decisionByCommittee) {
		return projectionV2{}, fmt.Errorf("identity decisions contain %d committees not referenced by the flow graph", len(decisionByCommittee)-len(usedDecisions))
	}
	result.Entities = sortedMapValues(entities, func(document entityDocumentV2) string { return document.Key })
	result.Edges = sortedMapValues(edges, func(edge flowEdge) string { return edge.Key })
	result.Counts.Entities = uint64(len(result.Entities))
	result.Counts.Edges = uint64(len(result.Edges))
	if result.Counts.Entities != result.Counts.CurrentCycleMasters+result.Counts.HistoricalRegistrations+
		result.Counts.AlternateReleaseRegistrations+result.Counts.UnresolvedReportedIDs ||
		result.Counts.Entities != result.Counts.TerminalIdentityEligible+result.Counts.TerminalIdentityIneligible ||
		result.Counts.Edges != result.Counts.RegisteredFilerContribution+result.Counts.InKindContribution+
			result.Counts.AffiliatedTransferIn+result.Counts.RefundRepaymentReceived {
		return projectionV2{}, fmt.Errorf("receiver-flow v2 projection counts do not conserve")
	}
	if result.Counts.Entities != inputs.bundle.Counts.ReferencedCommittees ||
		result.Counts.CurrentCycleMasters != inputs.bundle.Counts.CurrentCycleMasters ||
		result.Counts.HistoricalRegistrations != inputs.bundle.Counts.HistoricalRegistrations ||
		result.Counts.AlternateReleaseRegistrations != inputs.bundle.Counts.AlternateReleaseRegistrations ||
		result.Counts.UnresolvedReportedIDs != inputs.bundle.Counts.UnresolvedReportedIDs {
		return projectionV2{}, fmt.Errorf("receiver-flow v2 identity counts differ from readiness bundle")
	}
	result.Amounts = ProjectionAmounts{
		TotalMinorUnits: total.String(), RegisteredFilerContributionMinorUnits: registered.String(),
		InKindContributionMinorUnits: inKind.String(), AffiliatedTransferInMinorUnits: transfer.String(),
		RefundRepaymentReceivedMinorUnits: refund.String(),
	}
	adjacency := sortedAdjacency(adjacencySets)
	topology, source, target, cycleNode := analyzeTopology(adjacency)
	if source == "" || target == "" || topology.RepresentativePathHops < 1 {
		return projectionV2{}, fmt.Errorf("receiver-flow v2 graph has no representative path")
	}
	result.Topology = topology
	result.RepresentativeSource, result.RepresentativeTarget, result.RepresentativeCycle = source, target, cycleNode
	result.Metadata = projectionMetadataV2{
		Key: result.ID, SchemaVersion: MetadataSchemaV2, ProjectionVersion: ProjectionVersionV2,
		ProjectionID: result.ID, Cycle: result.Cycle, Graph: GraphName, Inputs: result.Inputs,
		Counts: result.Counts, Amounts: result.Amounts, Topology: result.Topology,
	}
	result.Metadata.DocumentDigest = documentDigest(result.Metadata)
	return result, nil
}

func decisionEntityV2(decision fecidentity.Decision, cycle string) entityDocumentV2 {
	decisionID, calculationSetID := decision.DecisionID, decision.CalculationSetID
	document := entityDocumentV2{
		Key: committeeKey(decision.CommitteeID), SchemaVersion: EntitySchemaVersionV2,
		EntityType: "committee", EntityID: decision.CommitteeID, Cycle: cycle,
		SourceState: decision.State, TerminalIdentityEligible: false,
		IdentityDecisionID: &decisionID, IdentityCalculationSetID: &calculationSetID,
		Name: "", SameCycleAssertions: append([]fecflowmastergaps.CommitteeHistoricalAssertion(nil), decision.SameCycleAssertions...),
		HistoricalAssertions: append([]fecflowmastergaps.CommitteeHistoricalAssertion(nil), decision.HistoricalAssertions...),
	}
	document.DocumentDigest = documentDigest(document)
	return document
}

func databaseNameV2(cycle, projectionID string) string {
	return "lt_flow_probe_v2_" + cycle + "_" + projectionID[:16]
}
