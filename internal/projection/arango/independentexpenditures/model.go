package independentexpenditures

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"

	feceffective "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

var fecIDPattern = regexp.MustCompile(`^[A-Z0-9]+$`)

type loadedInputs struct {
	calculation       feceffective.Manifest
	calculationDigest string
	candidates        fecoccurrence.ClassicFactManifest
	candidatesDigest  string
	committees        fecoccurrence.ClassicFactManifest
	committeesDigest  string
}

func loadInputs(ctx context.Context, input Input) (loadedInputs, error) {
	if input.ReadinessBundlePath != "" {
		bundle, _, resolved, err := feceffective.LoadProjectionBundle(ctx, input.StorageRoot, input.ReadinessBundlePath)
		if err != nil {
			return loadedInputs{}, fmt.Errorf("load independent-expenditure projection readiness bundle: %w", err)
		}
		if bundle.Cycle != input.Cycle {
			return loadedInputs{}, fmt.Errorf("projection readiness bundle belongs to cycle %s, expected %s", bundle.Cycle, input.Cycle)
		}
		input.CalculationManifestPath = resolved.CalculationManifestPath
		input.CandidateManifestPath = resolved.CandidateManifestPath
		input.CommitteeManifestPath = resolved.CommitteeManifestPath
	}
	calculation, calculationDigest, err := feceffective.LoadPublishedManifest(ctx, input.StorageRoot, input.CalculationManifestPath)
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load effective independent-expenditure calculation: %w", err)
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
		if manifest.Cycle != calculation.Cycle || manifest.SourceReleaseID != calculation.SourceReleaseID {
			return loadedInputs{}, fmt.Errorf("%s facts do not share the calculation cycle and source release", dataset)
		}
	}
	if input.Cycle != "" && calculation.Cycle != input.Cycle {
		return loadedInputs{}, fmt.Errorf("independent-expenditure calculation belongs to cycle %s, expected %s", calculation.Cycle, input.Cycle)
	}
	return loadedInputs{
		calculation: calculation, calculationDigest: calculationDigest,
		candidates: candidates, candidatesDigest: candidatesDigest,
		committees: committees, committeesDigest: committeesDigest,
	}, nil
}

func buildProjection(ctx context.Context, storageRoot string, inputs loadedInputs) (projection, error) {
	references := InputReferences{
		SourceReleaseID:  inputs.calculation.SourceReleaseID,
		CalculationSetID: inputs.calculation.CalculationSetID, CalculationManifestSHA256: inputs.calculationDigest,
		ScheduleEFactSetID: inputs.calculation.InputFactSet.FactSetID, ScheduleEManifestSHA256: inputs.calculation.InputFactSet.ManifestSHA256,
		CandidateFactSetID: inputs.candidates.FactSetID, CandidateManifestSHA256: inputs.candidatesDigest,
		CommitteeFactSetID: inputs.committees.FactSetID, CommitteeManifestSHA256: inputs.committeesDigest,
	}
	projectionID := digestParts(
		ProjectionVersion, inputs.calculation.Cycle,
		references.CalculationSetID, references.CalculationManifestSHA256,
		references.ScheduleEFactSetID, references.ScheduleEManifestSHA256,
		references.CandidateFactSetID, references.CandidateManifestSHA256,
		references.CommitteeFactSetID, references.CommitteeManifestSHA256,
	)
	result := projection{
		ID: projectionID, Database: databaseName(inputs.calculation.Cycle, projectionID),
		Cycle: inputs.calculation.Cycle, Inputs: references,
	}

	candidateMasters := make(map[string]entityDocument)
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
		return addEntity(candidateMasters, document)
	}); err != nil {
		return projection{}, err
	}

	committeeMasters := make(map[string]entityDocument)
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
		return addEntity(committeeMasters, document)
	}); err != nil {
		return projection{}, err
	}

	entities := make(map[string]entityDocument)
	edges := make(map[string]expenditureEdge)
	candidateDegrees := make(map[string]int)
	spenderDegrees := make(map[string]int)
	var attributed, support, opposition big.Int
	reader, err := storageartifact.Open[feceffective.Result](ctx, storageRoot, inputs.calculation.Results)
	if err != nil {
		return projection{}, fmt.Errorf("open independent-expenditure results: %w", err)
	}
	for {
		calculationResult, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return projection{}, fmt.Errorf("read independent-expenditure result: %w", readErr)
		}
		if !ok {
			break
		}
		if err := validateCalculationResult(calculationResult, inputs.calculation); err != nil {
			reader.Abort()
			return projection{}, err
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
		edge := expenditureEdge{
			Key:           edgeKey(calculationResult.ResultID),
			From:          entitiesCollection + "/" + spenderDocumentKey,
			To:            entitiesCollection + "/" + candidateDocumentKey,
			SchemaVersion: EdgeSchemaVersion, RelationType: relationType,
			Cycle: result.Cycle, SupportOppose: calculationResult.SupportOppose,
			AmountMinorUnits:            calculationResult.SignedAmountMinorUnits,
			ExpenditureCount:            calculationResult.ExpenditureCount,
			PositiveCount:               calculationResult.PositiveCount,
			NegativeCount:               calculationResult.NegativeCount,
			ZeroCount:                   calculationResult.ZeroCount,
			MissingExpenditureTypeCount: calculationResult.MissingExpenditureTypeCount,
			ResultID:                    calculationResult.ResultID,
			CalculationSetID:            inputs.calculation.CalculationSetID,
			ScheduleEFactSetID:          inputs.calculation.InputFactSet.FactSetID,
			SourceReleaseID:             inputs.calculation.SourceReleaseID,
		}
		edge.DocumentDigest = documentDigest(edge)
		if previous, exists := edges[edge.Key]; exists {
			reader.Abort()
			if previous.DocumentDigest != edge.DocumentDigest {
				return projection{}, fmt.Errorf("conflicting independent-expenditure edge %s", edge.Key)
			}
			return projection{}, fmt.Errorf("duplicate independent-expenditure result %s", calculationResult.ResultID)
		}
		edges[edge.Key] = edge
		candidateDegrees[calculationResult.CandidateID]++
		spenderDegrees[calculationResult.SpenderCommitteeID]++
	}
	if err := reader.Close(); err != nil {
		return projection{}, fmt.Errorf("close independent-expenditure result reader: %w", err)
	}

	expectedAttributed, ok := new(big.Int).SetString(inputs.calculation.Amounts.AttributedMinorUnits, 10)
	if !ok || expectedAttributed.Cmp(&attributed) != 0 || uint64(len(edges)) != inputs.calculation.RouteCounts.ResultGroups {
		return projection{}, fmt.Errorf("independent-expenditure projection does not conserve calculation results")
	}
	result.Entities = sortedMapValues(entities, func(document entityDocument) string { return document.Key })
	result.Edges = sortedMapValues(edges, func(edge expenditureEdge) string { return edge.Key })
	for _, document := range result.Entities {
		switch document.EntityType {
		case "candidate":
			result.Counts.Candidates++
		case "committee":
			result.Counts.Spenders++
		default:
			return projection{}, fmt.Errorf("unsupported projected entity type %q", document.EntityType)
		}
	}
	result.Counts.Entities = uint64(len(result.Entities))
	result.Counts.Edges = uint64(len(result.Edges))
	if result.Counts.SupportEdges+result.Counts.OppositionEdges != result.Counts.Edges {
		return projection{}, fmt.Errorf("support and opposition edge counts are not conserved")
	}
	result.Amounts = ProjectionAmounts{
		AttributedMinorUnits: attributed.String(), SupportMinorUnits: support.String(), OppositionMinorUnits: opposition.String(),
	}
	result.RepresentativeCandidate = representative(candidateDegrees)
	result.RepresentativeSpender = representative(spenderDegrees)
	if result.RepresentativeCandidate == "" || result.RepresentativeSpender == "" {
		return projection{}, fmt.Errorf("no representative graph endpoints were projected")
	}
	result.Metadata = projectionMetadata{
		Key: result.ID, SchemaVersion: MetadataSchema, ProjectionVersion: ProjectionVersion,
		ProjectionID: result.ID, Cycle: result.Cycle, Graph: GraphName,
		Inputs: result.Inputs, Counts: result.Counts, Amounts: result.Amounts, Missing: result.Missing,
	}
	result.Metadata.DocumentDigest = documentDigest(result.Metadata)
	return result, nil
}

func validateCalculationResult(result feceffective.Result, manifest feceffective.Manifest) error {
	if result.SchemaVersion != feceffective.ResultSchemaVersion || result.CalculationSetID != manifest.CalculationSetID || result.Cycle != manifest.Cycle {
		return fmt.Errorf("independent-expenditure result identity does not match its calculation")
	}
	if !validDigest(result.ResultID) {
		return fmt.Errorf("independent-expenditure result ID is invalid")
	}
	if err := validFECID("spender committee", result.SpenderCommitteeID); err != nil {
		return err
	}
	if err := validFECID("candidate", result.CandidateID); err != nil {
		return err
	}
	if result.SupportOppose != "S" && result.SupportOppose != "O" {
		return fmt.Errorf("independent-expenditure stance %q is invalid", result.SupportOppose)
	}
	if result.ExpenditureCount == 0 || result.ExpenditureCount != result.PositiveCount+result.NegativeCount+result.ZeroCount {
		return fmt.Errorf("independent-expenditure result counts are not conserved")
	}
	amount, ok := new(big.Int).SetString(result.SignedAmountMinorUnits, 10)
	if !ok || amount.String() != result.SignedAmountMinorUnits {
		return fmt.Errorf("independent-expenditure result amount is not canonical exact cents")
	}
	return nil
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

func addEntity(entities map[string]entityDocument, document entityDocument) error {
	if previous, exists := entities[document.Key]; exists && previous.DocumentDigest != document.DocumentDigest {
		return fmt.Errorf("conflicting master facts for %s", document.Key)
	}
	entities[document.Key] = document
	return nil
}

func missingEntity(entityType, entityID, cycle string) entityDocument {
	document := entityDocument{
		Key: entityType + "_" + entityID, SchemaVersion: EntitySchemaVersion,
		EntityType: entityType, EntityID: entityID, Cycle: cycle,
		SourceState: "missing_master_fact", Name: "",
	}
	if entityType == "committee" {
		document.Key = committeeKey(entityID)
	}
	document.DocumentDigest = documentDigest(document)
	return document
}

func candidateKey(candidateID string) string { return "candidate_" + candidateID }
func committeeKey(committeeID string) string { return "committee_" + committeeID }
func edgeKey(resultID string) string         { return "ie_" + resultID[:40] }

func databaseName(cycle, projectionID string) string {
	return "lt_ie_probe_" + cycle + "_" + projectionID[:16]
}

func validFECID(kind, value string) error {
	if value == "" || !fecIDPattern.MatchString(value) {
		return fmt.Errorf("%s ID %q is invalid", kind, value)
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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

func sortedMapValues[T any](values map[string]T, key func(T) string) []T {
	result := make([]T, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return key(result[left]) < key(result[right]) })
	return result
}

func representative(degrees map[string]int) string {
	selected := ""
	selectedCount := -1
	for id, count := range degrees {
		if count > selectedCount || (count == selectedCount && (selected == "" || id < selected)) {
			selected = id
			selectedCount = count
		}
	}
	return selected
}
