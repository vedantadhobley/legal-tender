package candidatereceipts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	fecreceipts "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

var fecIDPattern = regexp.MustCompile(`^[A-Z0-9]+$`)

type loadedInputs struct {
	bundle            fecreceipts.FactBundleManifest
	bundleDigest      string
	calculation       fecreceipts.CompactManifest
	calculationDigest string
	candidates        fecoccurrence.ClassicFactManifest
	candidatesDigest  string
	committees        fecoccurrence.ClassicFactManifest
	committeesDigest  string
}

func loadInputs(ctx context.Context, input Input) (loadedInputs, error) {
	bundle, bundleDigest, err := fecreceipts.LoadPublishedFactBundle(input.StorageRoot, input.FactBundleManifestPath)
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load receipt fact bundle: %w", err)
	}
	calculation, calculationDigest, err := fecreceipts.LoadPublishedCompactManifest(ctx, input.StorageRoot, input.CalculationManifestPath)
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load receipt calculation: %w", err)
	}
	if !fecreceipts.CompactManifestUsesBundle(calculation, bundle) {
		return loadedInputs{}, fmt.Errorf("receipt calculation does not use the selected fact bundle")
	}
	candidates, candidatesDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(input.StorageRoot, input.CandidateManifestPath, "candidate-master")
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load candidate master facts: %w", err)
	}
	committees, committeesDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(input.StorageRoot, input.CommitteeManifestPath, "committee-master")
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load committee master facts: %w", err)
	}
	for dataset, manifest := range map[string]fecoccurrence.ClassicFactManifest{
		"candidate-master": candidates,
		"committee-master": committees,
	} {
		if manifest.Cycle != bundle.Cycle || manifest.SourceReleaseID != bundle.SourceReleaseID {
			return loadedInputs{}, fmt.Errorf("%s facts do not share the receipt bundle cycle and source release", dataset)
		}
	}
	if input.Cycle != "" && bundle.Cycle != input.Cycle {
		return loadedInputs{}, fmt.Errorf("receipt fact bundle belongs to cycle %s, expected %s", bundle.Cycle, input.Cycle)
	}
	return loadedInputs{
		bundle: bundle, bundleDigest: bundleDigest,
		calculation: calculation, calculationDigest: calculationDigest,
		candidates: candidates, candidatesDigest: candidatesDigest,
		committees: committees, committeesDigest: committeesDigest,
	}, nil
}

func buildProjection(ctx context.Context, storageRoot string, inputs loadedInputs) (projection, error) {
	references := InputReferences{
		SourceReleaseID: inputs.bundle.SourceReleaseID,
		FactBundleID:    inputs.bundle.BundleID, FactBundleManifestSHA256: inputs.bundleDigest,
		CalculationSetID: inputs.calculation.CalculationSetID, CalculationManifestSHA256: inputs.calculationDigest,
		CandidateFactSetID: inputs.candidates.FactSetID, CandidateManifestSHA256: inputs.candidatesDigest,
		CommitteeFactSetID: inputs.committees.FactSetID, CommitteeManifestSHA256: inputs.committeesDigest,
	}
	projectionID := digestParts(
		ProjectionVersion, inputs.bundle.Cycle,
		references.FactBundleID, references.FactBundleManifestSHA256,
		references.CalculationSetID, references.CalculationManifestSHA256,
		references.CandidateFactSetID, references.CandidateManifestSHA256,
		references.CommitteeFactSetID, references.CommitteeManifestSHA256,
	)
	result := projection{
		ID: projectionID, Database: databaseName(inputs.bundle.Cycle, projectionID),
		Cycle: inputs.bundle.Cycle, Inputs: references,
	}

	entities := make(map[string]entityDocument)
	if err := streamClassicFacts(ctx, storageRoot, inputs.candidates, func(fact fecoccurrence.ClassicFact) error {
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
		factID, factSetID := fact.FactID, inputs.candidates.FactSetID
		document := entityDocument{
			Key: candidateKey(fields.CandidateID), SchemaVersion: EntitySchemaVersion,
			EntityType: "candidate", EntityID: fields.CandidateID, Cycle: result.Cycle,
			SourceState: "present", SourceFactID: &factID, SourceFactSetID: &factSetID,
			Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
			Office: fields.Office, OfficeState: fields.OfficeState, OfficeDistrict: fields.OfficeDistrict,
		}
		document.DocumentDigest = documentDigest(document)
		return addEntity(entities, document)
	}); err != nil {
		return projection{}, err
	}
	if err := streamClassicFacts(ctx, storageRoot, inputs.committees, func(fact fecoccurrence.ClassicFact) error {
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
		factID, factSetID := fact.FactID, inputs.committees.FactSetID
		document := entityDocument{
			Key: committeeKey(fields.CommitteeID), SchemaVersion: EntitySchemaVersion,
			EntityType: "committee", EntityID: fields.CommitteeID, Cycle: result.Cycle,
			SourceState: "present", SourceFactID: &factID, SourceFactSetID: &factSetID,
			Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
			DesignationCode: fields.DesignationCode, CommitteeTypeCode: fields.CommitteeTypeCode,
			OrganizationTypeCode: fields.OrganizationTypeCode, ConnectedOrganization: fields.ConnectedOrganization,
		}
		document.DocumentDigest = documentDigest(document)
		return addEntity(entities, document)
	}); err != nil {
		return projection{}, err
	}

	relationships := make(map[string]relationshipEdge)
	receiptComponents := make(map[string]receiptComponentEdge)
	representativeCount := -1
	reader, err := storageartifact.Open[fecreceipts.Result](ctx, storageRoot, inputs.calculation.Results)
	if err != nil {
		return projection{}, fmt.Errorf("open compact calculation results: %w", err)
	}
	for {
		calculationResult, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return projection{}, fmt.Errorf("read compact calculation results: %w", readErr)
		}
		if !ok {
			break
		}
		if calculationResult.Cycle != result.Cycle {
			reader.Abort()
			return projection{}, fmt.Errorf("candidate %s result belongs to cycle %s", calculationResult.CandidateID, calculationResult.Cycle)
		}
		if err := validFECID("candidate", calculationResult.CandidateID); err != nil {
			reader.Abort()
			return projection{}, err
		}
		candidateDocumentKey := candidateKey(calculationResult.CandidateID)
		if _, exists := entities[candidateDocumentKey]; !exists {
			placeholder := missingEntity("candidate", calculationResult.CandidateID, result.Cycle)
			entities[placeholder.Key] = placeholder
			result.Missing.Candidates++
		}
		candidateResult := resultDocument{
			Key: candidateDocumentKey, SchemaVersion: ResultDocumentSchema,
			Cycle: result.Cycle, CandidateID: calculationResult.CandidateID,
			CalculationSetID: inputs.calculation.CalculationSetID,
			SourceReleaseID:  inputs.calculation.SourceReleaseID, Result: calculationResult,
		}
		candidateResult.DocumentDigest = documentDigest(candidateResult)
		result.Results = append(result.Results, candidateResult)
		for _, relationship := range calculationResult.CommitteeRelationships {
			if err := validFECID("committee", relationship.CommitteeID); err != nil {
				reader.Abort()
				return projection{}, err
			}
			committeeDocumentKey := committeeKey(relationship.CommitteeID)
			if _, exists := entities[committeeDocumentKey]; !exists {
				placeholder := missingEntity("committee", relationship.CommitteeID, result.Cycle)
				entities[placeholder.Key] = placeholder
				result.Missing.Committees++
			}
			edge := relationshipEdge{
				Key:           relationshipKey(result.Cycle, calculationResult.CandidateID, relationship.CommitteeID),
				From:          entitiesCollection + "/" + committeeDocumentKey,
				To:            entitiesCollection + "/" + candidateDocumentKey,
				SchemaVersion: EdgeSchemaVersion, RelationType: "candidate_committee_relationship",
				Cycle: result.Cycle, RelationshipState: relationship.State,
				DesignationCodes:  nonNilStrings(relationship.DesignationCodes),
				SupportingFactIDs: nonNilStrings(relationship.SupportingFactIDs),
				CalculationSetID:  inputs.calculation.CalculationSetID,
			}
			edge.DocumentDigest = documentDigest(edge)
			if previous, exists := relationships[edge.Key]; exists && previous.DocumentDigest != edge.DocumentDigest {
				reader.Abort()
				return projection{}, fmt.Errorf("conflicting relationship edge %s", edge.Key)
			}
			relationships[edge.Key] = edge
		}
		for _, subtotal := range calculationResult.CommitteeSubtotals {
			if err := validFECID("committee", subtotal.CommitteeID); err != nil {
				reader.Abort()
				return projection{}, err
			}
			committeeDocumentKey := committeeKey(subtotal.CommitteeID)
			if _, exists := entities[committeeDocumentKey]; !exists {
				placeholder := missingEntity("committee", subtotal.CommitteeID, result.Cycle)
				entities[placeholder.Key] = placeholder
				result.Missing.Committees++
			}
			edge := receiptComponentEdge{
				Key:           receiptKey(result.Cycle, calculationResult.CandidateID, subtotal.CommitteeID),
				From:          entitiesCollection + "/" + committeeDocumentKey,
				To:            entitiesCollection + "/" + candidateDocumentKey,
				SchemaVersion: EdgeSchemaVersion, RelationType: "fec_itemized_individual_receipts",
				Cycle: result.Cycle, AmountMinorUnits: subtotal.AmountMinorUnits,
				IncludedRecords:    subtotal.IncludedRecords.Records,
				PositiveRecords:    subtotal.IncludedRecords.Positive,
				NegativeRecords:    subtotal.IncludedRecords.Negative,
				ZeroRecords:        subtotal.IncludedRecords.Zero,
				CalculationSetID:   inputs.calculation.CalculationSetID,
				CandidateResultKey: resultsCollection + "/" + candidateResult.Key,
			}
			edge.DocumentDigest = documentDigest(edge)
			if previous, exists := receiptComponents[edge.Key]; exists && previous.DocumentDigest != edge.DocumentDigest {
				reader.Abort()
				return projection{}, fmt.Errorf("conflicting receipt component edge %s", edge.Key)
			}
			receiptComponents[edge.Key] = edge
		}
		if len(calculationResult.CommitteeSubtotals) > representativeCount ||
			(len(calculationResult.CommitteeSubtotals) == representativeCount && calculationResult.CandidateID < result.RepresentativeCandidate) {
			representativeCount = len(calculationResult.CommitteeSubtotals)
			result.RepresentativeCandidate = calculationResult.CandidateID
		}
	}
	if err := reader.Close(); err != nil {
		return projection{}, fmt.Errorf("close compact calculation result reader: %w", err)
	}

	result.Entities = sortedMapValues(entities, func(document entityDocument) string { return document.Key })
	result.Relationships = sortedMapValues(relationships, func(edge relationshipEdge) string { return edge.Key })
	result.ReceiptComponents = sortedMapValues(receiptComponents, func(edge receiptComponentEdge) string { return edge.Key })
	sort.Slice(result.Results, func(left, right int) bool { return result.Results[left].Key < result.Results[right].Key })
	for _, entity := range result.Entities {
		switch entity.EntityType {
		case "candidate":
			result.Counts.Candidates++
		case "committee":
			result.Counts.Committees++
		}
	}
	result.Counts.Entities = uint64(len(result.Entities))
	result.Counts.CandidateResults = uint64(len(result.Results))
	result.Counts.CandidateCommitteeRelationships = uint64(len(result.Relationships))
	result.Counts.ReceiptComponents = uint64(len(result.ReceiptComponents))
	result.Metadata = projectionMetadata{
		Key: result.ID, SchemaVersion: MetadataSchema, ProjectionVersion: ProjectionVersion,
		ProjectionID: result.ID, Cycle: result.Cycle, Graph: GraphName,
		Inputs: result.Inputs, Counts: result.Counts,
	}
	result.Metadata.DocumentDigest = documentDigest(result.Metadata)
	return result, nil
}

func streamClassicFacts(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest, consume func(fecoccurrence.ClassicFact) error) error {
	descriptor := storageartifact.Descriptor{
		RecordCount:        manifest.Facts.RecordCount,
		UncompressedBytes:  manifest.Facts.UncompressedBytes,
		UncompressedSHA256: manifest.Facts.UncompressedSHA256,
		CompressedBytes:    manifest.Facts.CompressedBytes,
		CompressedSHA256:   manifest.Facts.CompressedSHA256,
		Compression:        manifest.Facts.Compression,
		StorageKey:         manifest.Facts.StorageKey,
	}
	reader, err := storageartifact.Open[fecoccurrence.ClassicFact](ctx, storageRoot, descriptor)
	if err != nil {
		return fmt.Errorf("open %s facts: %w", manifest.Dataset, err)
	}
	for {
		fact, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return fmt.Errorf("read %s facts: %w", manifest.Dataset, readErr)
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
		return fmt.Errorf("close %s fact reader: %w", manifest.Dataset, err)
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

func addEntity(entities map[string]entityDocument, document entityDocument) error {
	if previous, exists := entities[document.Key]; exists && previous.DocumentDigest != document.DocumentDigest {
		return fmt.Errorf("conflicting entity document %s", document.Key)
	}
	entities[document.Key] = document
	return nil
}

func missingEntity(entityType, entityID, cycle string) entityDocument {
	document := entityDocument{
		Key: entityType + "_" + entityID, SchemaVersion: EntitySchemaVersion,
		EntityType: entityType, EntityID: entityID, Cycle: cycle,
		SourceState: "missing_master_fact", Name: entityID,
	}
	document.DocumentDigest = documentDigest(document)
	return document
}

func candidateKey(candidateID string) string { return "candidate_" + candidateID }
func committeeKey(committeeID string) string { return "committee_" + committeeID }

func relationshipKey(cycle, candidateID, committeeID string) string {
	return "relationship_" + digestParts(cycle, candidateID, committeeID)
}

func receiptKey(cycle, candidateID, committeeID string) string {
	return "receipt_" + digestParts(cycle, candidateID, committeeID)
}

func databaseName(cycle, projectionID string) string {
	return "lt_probe_" + cycle + "_" + projectionID[:16]
}

func validFECID(kind, value string) error {
	if !fecIDPattern.MatchString(value) {
		return fmt.Errorf("%s ID %q is invalid", kind, value)
	}
	return nil
}

func documentDigest(value any) string {
	content, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

func digestParts(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func sortedMapValues[T any](values map[string]T, key func(T) string) []T {
	result := make([]T, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return strings.Compare(key(result[left]), key(result[right])) < 0 })
	return result
}
