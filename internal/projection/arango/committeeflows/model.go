package committeeflows

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

	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

var committeeIDPattern = regexp.MustCompile(`^C[0-9]{8}$`)

type loadedInputs struct {
	bundle            fecflows.ProjectionBundleManifest
	bundleDigest      string
	calculation       fecflows.Manifest
	calculationDigest string
	committees        fecoccurrence.ClassicFactManifest
	committeesDigest  string
}

func loadInputs(ctx context.Context, input Input) (loadedInputs, error) {
	bundle, bundleDigest, resolved, err := fecflows.LoadProjectionBundle(ctx, input.StorageRoot, input.ReadinessBundlePath)
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load receiver-flow projection readiness bundle: %w", err)
	}
	if bundle.Cycle != input.Cycle {
		return loadedInputs{}, fmt.Errorf("receiver-flow projection bundle belongs to cycle %s, expected %s", bundle.Cycle, input.Cycle)
	}
	calculation, calculationDigest, err := fecflows.LoadPublishedManifest(ctx, input.StorageRoot, resolved.CalculationManifestPath)
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load receiver-flow calculation: %w", err)
	}
	committees, committeesDigest, err := fecoccurrence.LoadPublishedClassicFactManifest(input.StorageRoot, resolved.CommitteeManifestPath, "committee-master")
	if err != nil {
		return loadedInputs{}, fmt.Errorf("load committee-master facts: %w", err)
	}
	if calculation.Cycle != bundle.Cycle || calculation.SourceReleaseID != bundle.SourceReleaseID ||
		committees.Cycle != bundle.Cycle || committees.SourceReleaseID != bundle.SourceReleaseID {
		return loadedInputs{}, fmt.Errorf("receiver-flow projection inputs do not share the bundle cycle and source release")
	}
	return loadedInputs{
		bundle: bundle, bundleDigest: bundleDigest,
		calculation: calculation, calculationDigest: calculationDigest,
		committees: committees, committeesDigest: committeesDigest,
	}, nil
}

func buildProjection(ctx context.Context, storageRoot string, inputs loadedInputs) (projection, error) {
	references := InputReferences{
		SourceReleaseID:   inputs.bundle.SourceReleaseID,
		ReadinessBundleID: inputs.bundle.BundleID, ReadinessBundleSHA256: inputs.bundleDigest,
		CalculationSetID: inputs.calculation.CalculationSetID, CalculationManifestSHA256: inputs.calculationDigest,
		ScheduleAFactSetID:      inputs.calculation.InputFactSet.FactSetID,
		ScheduleAManifestSHA256: inputs.calculation.InputFactSet.ManifestSHA256,
		CommitteeFactSetID:      inputs.committees.FactSetID, CommitteeManifestSHA256: inputs.committeesDigest,
	}
	projectionID := digestParts(
		ProjectionVersion, inputs.bundle.Cycle, references.ReadinessBundleID,
		references.ReadinessBundleSHA256, references.CalculationSetID,
		references.CalculationManifestSHA256, references.ScheduleAFactSetID,
		references.ScheduleAManifestSHA256, references.CommitteeFactSetID,
		references.CommitteeManifestSHA256,
	)
	result := projection{
		ID: projectionID, Database: databaseName(inputs.bundle.Cycle, projectionID),
		Cycle: inputs.bundle.Cycle, Inputs: references,
	}

	masters := make(map[string]entityDocument)
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
		document := entityDocument{
			Key: committeeKey(fields.CommitteeID), SchemaVersion: EntitySchemaVersion,
			EntityType: "committee", EntityID: fields.CommitteeID, Cycle: result.Cycle,
			SourceState: "present", SourceFactID: &factID, SourceFactSetID: &factSetID,
			Name: fields.Name, PartyAffiliation: fields.PartyAffiliation,
			DesignationCode: fields.DesignationCode, CommitteeTypeCode: fields.CommitteeTypeCode,
			OrganizationTypeCode: fields.OrganizationTypeCode, ConnectedOrganization: fields.ConnectedOrganization,
		}
		document.DocumentDigest = documentDigest(document)
		if previous, exists := masters[document.Key]; exists && previous.DocumentDigest != document.DocumentDigest {
			return fmt.Errorf("conflicting committee master facts for %s", fields.CommitteeID)
		}
		masters[document.Key] = document
		return nil
	}); err != nil {
		return projection{}, err
	}

	entities := make(map[string]entityDocument)
	edges := make(map[string]flowEdge)
	adjacencySets := make(map[string]map[string]struct{})
	var total, registered, inKind, transfer, refund big.Int
	reader, err := storageartifact.Open[fecflows.Result](ctx, storageRoot, inputs.calculation.Results)
	if err != nil {
		return projection{}, fmt.Errorf("open receiver-flow results: %w", err)
	}
	for {
		calculationResult, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return projection{}, fmt.Errorf("read receiver-flow result: %w", readErr)
		}
		if !ok {
			break
		}
		if err := validateCalculationResult(calculationResult, inputs.calculation); err != nil {
			reader.Abort()
			return projection{}, err
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
			return projection{}, fmt.Errorf("unsupported receiver-flow role %q", calculationResult.ReceiptRole)
		}

		for _, committeeID := range []string{calculationResult.SourceCommitteeID, calculationResult.RecipientCommitteeID} {
			key := committeeKey(committeeID)
			if _, exists := entities[key]; exists {
				continue
			}
			if master, exists := masters[key]; exists {
				entities[key] = master
				result.Counts.PresentCommitteeMasters++
			} else {
				entities[key] = missingEntity(committeeID, result.Cycle)
				result.Missing.Committees++
				result.Counts.MissingCommitteeMasters++
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
				return projection{}, fmt.Errorf("conflicting receiver-flow edge %s", edge.Key)
			}
			return projection{}, fmt.Errorf("duplicate receiver-flow result %s", calculationResult.ResultID)
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
		return projection{}, fmt.Errorf("close receiver-flow result reader: %w", err)
	}

	expected, ok := new(big.Int).SetString(inputs.calculation.Amounts.IncludedMinorUnits, 10)
	if !ok || expected.Cmp(&total) != 0 || uint64(len(edges)) != inputs.calculation.ResultCounts.ResultGroups {
		return projection{}, fmt.Errorf("receiver-flow projection does not conserve calculation results")
	}
	result.Entities = sortedMapValues(entities, func(document entityDocument) string { return document.Key })
	result.Edges = sortedMapValues(edges, func(edge flowEdge) string { return edge.Key })
	result.Counts.Entities = uint64(len(result.Entities))
	result.Counts.Edges = uint64(len(result.Edges))
	if result.Counts.Entities != result.Counts.PresentCommitteeMasters+result.Counts.MissingCommitteeMasters ||
		result.Counts.Edges != result.Counts.RegisteredFilerContribution+result.Counts.InKindContribution+
			result.Counts.AffiliatedTransferIn+result.Counts.RefundRepaymentReceived {
		return projection{}, fmt.Errorf("receiver-flow projection counts do not conserve")
	}
	result.Amounts = ProjectionAmounts{
		TotalMinorUnits: total.String(), RegisteredFilerContributionMinorUnits: registered.String(),
		InKindContributionMinorUnits: inKind.String(), AffiliatedTransferInMinorUnits: transfer.String(),
		RefundRepaymentReceivedMinorUnits: refund.String(),
	}
	adjacency := sortedAdjacency(adjacencySets)
	topology, source, target, cycleNode := analyzeTopology(adjacency)
	if source == "" || target == "" || topology.RepresentativePathHops < 1 {
		return projection{}, fmt.Errorf("receiver-flow graph has no representative path")
	}
	result.Topology = topology
	result.RepresentativeSource, result.RepresentativeTarget, result.RepresentativeCycle = source, target, cycleNode
	result.Metadata = projectionMetadata{
		Key: result.ID, SchemaVersion: MetadataSchema, ProjectionVersion: ProjectionVersion,
		ProjectionID: result.ID, Cycle: result.Cycle, Graph: GraphName, Inputs: result.Inputs,
		Counts: result.Counts, Amounts: result.Amounts, Topology: result.Topology, Missing: result.Missing,
	}
	result.Metadata.DocumentDigest = documentDigest(result.Metadata)
	return result, nil
}

func validateCalculationResult(result fecflows.Result, manifest fecflows.Manifest) error {
	if result.SchemaVersion != fecflows.ResultSchemaVersion || result.CalculationSetID != manifest.CalculationSetID || result.Cycle != manifest.Cycle || !validDigest(result.ResultID) {
		return fmt.Errorf("receiver-flow result identity does not match its calculation")
	}
	if err := validCommitteeID(result.SourceCommitteeID); err != nil {
		return err
	}
	if err := validCommitteeID(result.RecipientCommitteeID); err != nil {
		return err
	}
	if result.ReceiptCount == 0 || result.ReceiptCount != result.PositiveCount+result.NegativeCount+result.ZeroCount {
		return fmt.Errorf("receiver-flow result counts are not conserved")
	}
	amount, ok := new(big.Int).SetString(result.SignedAmountMinorUnits, 10)
	if !ok || amount.String() != result.SignedAmountMinorUnits {
		return fmt.Errorf("receiver-flow result amount is not canonical exact cents")
	}
	return nil
}

func streamClassicFacts(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest, consume func(fecoccurrence.ClassicFact) error) error {
	reader, err := storageartifact.Open[fecoccurrence.ClassicFact](ctx, storageRoot, classicDescriptor(manifest.Facts))
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

func classicDescriptor(artifact fecoccurrence.Artifact) storageartifact.Descriptor {
	return storageartifact.Descriptor{
		RecordCount: artifact.RecordCount, UncompressedBytes: artifact.UncompressedBytes,
		UncompressedSHA256: artifact.UncompressedSHA256, CompressedBytes: artifact.CompressedBytes,
		CompressedSHA256: artifact.CompressedSHA256, Compression: artifact.Compression, StorageKey: artifact.StorageKey,
	}
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

func sortedAdjacency(sets map[string]map[string]struct{}) map[string][]string {
	result := make(map[string][]string, len(sets))
	for source, targets := range sets {
		for target := range targets {
			result[source] = append(result[source], target)
		}
		sort.Strings(result[source])
	}
	return result
}

func missingEntity(committeeID, cycle string) entityDocument {
	document := entityDocument{
		Key: committeeKey(committeeID), SchemaVersion: EntitySchemaVersion,
		EntityType: "committee", EntityID: committeeID, Cycle: cycle,
		SourceState: "missing_master_fact", Name: "",
	}
	document.DocumentDigest = documentDigest(document)
	return document
}

func committeeKey(committeeID string) string { return "committee_" + committeeID }
func edgeKey(resultID string) string         { return "flow_" + resultID[:40] }

func databaseName(cycle, projectionID string) string {
	return "lt_flow_probe_" + cycle + "_" + projectionID[:16]
}

func validCommitteeID(value string) error {
	if !committeeIDPattern.MatchString(value) {
		return fmt.Errorf("committee ID %q is invalid", value)
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

func analyzeTopology(adjacency map[string][]string) (TopologyMetrics, string, string, string) {
	nodes := make([]string, 0, len(adjacency))
	for node := range adjacency {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	metrics := TopologyMetrics{WeakComponents: weakComponentCount(nodes, adjacency)}
	components := strongComponents(nodes, adjacency)
	metrics.StrongComponents = uint64(len(components))
	cycleNode := ""
	for _, component := range components {
		if len(component) < 2 {
			continue
		}
		metrics.CyclicStrongComponents++
		metrics.CommitteesInCycles += uint64(len(component))
		if cycleNode == "" {
			cycleNode, metrics.RepresentativeCycleHops = findCycle(component, adjacency)
		}
	}
	source, target, hops := representativePath(nodes, adjacency)
	metrics.RepresentativePathHops = hops
	return metrics, source, target, cycleNode
}

func weakComponentCount(nodes []string, adjacency map[string][]string) uint64 {
	undirected := make(map[string][]string, len(nodes))
	for source, targets := range adjacency {
		for _, target := range targets {
			undirected[source] = append(undirected[source], target)
			undirected[target] = append(undirected[target], source)
		}
	}
	seen := make(map[string]struct{}, len(nodes))
	var count uint64
	for _, start := range nodes {
		if _, exists := seen[start]; exists {
			continue
		}
		count++
		seen[start] = struct{}{}
		queue := []string{start}
		for len(queue) != 0 {
			current := queue[0]
			queue = queue[1:]
			for _, next := range undirected[current] {
				if _, exists := seen[next]; !exists {
					seen[next] = struct{}{}
					queue = append(queue, next)
				}
			}
		}
	}
	return count
}

func strongComponents(nodes []string, adjacency map[string][]string) [][]string {
	index := 0
	indices := make(map[string]int, len(nodes))
	low := make(map[string]int, len(nodes))
	onStack := make(map[string]bool, len(nodes))
	stack := make([]string, 0, len(nodes))
	components := make([][]string, 0)
	var visit func(string)
	visit = func(node string) {
		index++
		indices[node], low[node] = index, index
		stack = append(stack, node)
		onStack[node] = true
		for _, next := range adjacency[node] {
			if indices[next] == 0 {
				visit(next)
				if low[next] < low[node] {
					low[node] = low[next]
				}
			} else if onStack[next] && indices[next] < low[node] {
				low[node] = indices[next]
			}
		}
		if low[node] != indices[node] {
			return
		}
		component := []string{}
		for {
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[last] = false
			component = append(component, last)
			if last == node {
				break
			}
		}
		sort.Strings(component)
		components = append(components, component)
	}
	for _, node := range nodes {
		if indices[node] == 0 {
			visit(node)
		}
	}
	return components
}

func findCycle(component []string, adjacency map[string][]string) (string, int) {
	allowed := make(map[string]struct{}, len(component))
	for _, node := range component {
		allowed[node] = struct{}{}
	}
	color := make(map[string]uint8, len(component))
	parent := make(map[string]string, len(component))
	cycleStart, cycleHops := "", 0
	var visit func(string) bool
	visit = func(node string) bool {
		color[node] = 1
		for _, next := range adjacency[node] {
			if _, exists := allowed[next]; !exists {
				continue
			}
			if color[next] == 0 {
				parent[next] = node
				if visit(next) {
					return true
				}
			} else if color[next] == 1 {
				cycleStart, cycleHops = next, 1
				for current := node; current != next; current = parent[current] {
					cycleHops++
				}
				return true
			}
		}
		color[node] = 2
		return false
	}
	for _, node := range component {
		if color[node] == 0 && visit(node) {
			return cycleStart, cycleHops
		}
	}
	return "", 0
}

func representativePath(nodes []string, adjacency map[string][]string) (string, string, int) {
	candidates := append([]string(nil), nodes...)
	sort.Slice(candidates, func(left, right int) bool {
		if len(adjacency[candidates[left]]) != len(adjacency[candidates[right]]) {
			return len(adjacency[candidates[left]]) > len(adjacency[candidates[right]])
		}
		return candidates[left] < candidates[right]
	})
	if len(candidates) > 64 {
		candidates = candidates[:64]
	}
	bestSource, bestTarget, bestHops := "", "", 0
	for _, source := range candidates {
		distance := map[string]int{source: 0}
		queue := []string{source}
		for len(queue) != 0 {
			current := queue[0]
			queue = queue[1:]
			if distance[current] >= 8 {
				continue
			}
			for _, next := range adjacency[current] {
				if _, exists := distance[next]; exists {
					continue
				}
				distance[next] = distance[current] + 1
				queue = append(queue, next)
				if distance[next] > bestHops ||
					(distance[next] == bestHops && (bestSource == "" || source < bestSource || source == bestSource && next < bestTarget)) {
					bestSource, bestTarget, bestHops = source, next, distance[next]
				}
			}
		}
		if bestHops == 8 {
			break
		}
	}
	return bestSource, bestTarget, bestHops
}
