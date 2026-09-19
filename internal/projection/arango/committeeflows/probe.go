package committeeflows

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

// Run builds or reuses one isolated content-addressed receiver-flow graph and
// verifies its counts, exact amounts, topology, and representative queries.
func Run(ctx context.Context, input Input, options Options) (Result, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if input.StorageRoot == "" || input.Cycle == "" || input.ReadinessBundlePath == "" {
		return Result{}, fmt.Errorf("storage root, cycle, and receiver-flow projection bundle are required")
	}
	if !fecrelease.ValidAcquisitionRunID(input.RunID) {
		return Result{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if input.BatchSize == 0 {
		input.BatchSize = 5_000
	}
	if input.BatchSize < 1 || input.BatchSize > 50_000 {
		return Result{}, fmt.Errorf("batch size must be between 1 and 50000")
	}
	if input.QueryRepetitions == 0 {
		input.QueryRepetitions = 10
	}
	if input.QueryRepetitions < 1 || input.QueryRepetitions > 100 {
		return Result{}, fmt.Errorf("query repetitions must be between 1 and 100")
	}
	client, err := newArangoClient(input.Endpoint, input.Username, input.Password)
	if err != nil {
		return Result{}, err
	}
	progress := func(message string) {
		if options.Progress != nil {
			options.Progress(message)
		}
	}

	progress("validating immutable receiver-flow projection bundle")
	loaded, err := loadInputs(ctx, input)
	if err != nil {
		return Result{}, err
	}
	progress("building deterministic receiver-reported committee-flow graph documents")
	model, err := buildProjection(ctx, input.StorageRoot, loaded)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	progress("ensuring isolated ArangoDB flow probe database " + model.Database)
	if err := client.ensureDatabase(ctx, model.Database); err != nil {
		return Result{}, err
	}
	if err := ensureSchema(ctx, client, model.Database); err != nil {
		return Result{}, err
	}
	existing, complete, err := client.metadata(ctx, model.Database, model.ID)
	if err != nil {
		return Result{}, fmt.Errorf("read projection metadata: %w", err)
	}
	if complete {
		if !reflect.DeepEqual(existing, model.Metadata) {
			return Result{}, fmt.Errorf("existing projection metadata does not match projection %s", model.ID)
		}
		progress("reusing completed content-addressed receiver-flow projection")
	} else {
		progress("importing referenced committee entities")
		if err := importBatches(ctx, client, model.Database, entitiesCollection, model.Entities, input.BatchSize); err != nil {
			return Result{}, err
		}
		progress("importing receiver-reported committee-flow edges")
		if err := importBatches(ctx, client, model.Database, edgesCollection, model.Edges, input.BatchSize); err != nil {
			return Result{}, err
		}
	}

	observedCounts, err := readCounts(ctx, client, model.Database)
	if err != nil {
		return Result{}, err
	}
	if observedCounts != model.Counts {
		return Result{}, fmt.Errorf("ArangoDB receiver-flow counts differ: got %+v want %+v", observedCounts, model.Counts)
	}
	observedAmounts, err := readAmounts(ctx, client, model.Database)
	if err != nil {
		return Result{}, err
	}
	if observedAmounts != model.Amounts {
		return Result{}, fmt.Errorf("ArangoDB receiver-flow amounts differ: got %+v want %+v", observedAmounts, model.Amounts)
	}
	if !complete {
		if _, err := client.importDocuments(ctx, model.Database, metadataCollection, []projectionMetadata{model.Metadata}); err != nil {
			return Result{}, fmt.Errorf("publish projection metadata: %w", err)
		}
	}
	storage, err := readStorageMetrics(ctx, client, model.Database, observedCounts)
	if err != nil {
		return Result{}, err
	}
	if len(storage.Collections) != 3 || storage.CombinedBytes == 0 {
		return Result{}, fmt.Errorf("ArangoDB returned incomplete receiver-flow storage figures")
	}

	progress("measuring representative neighborhood, path, shortest-path, and cycle queries")
	queries, err := benchmarkQueries(ctx, client, model, input.QueryRepetitions)
	if err != nil {
		return Result{}, err
	}
	expectedQueries := 3
	if model.Topology.CyclicStrongComponents != 0 {
		expectedQueries = 4
	}
	state := "ready"
	if model.Missing.Committees != 0 {
		state = "partial"
	}
	checks := []Check{
		{ID: "input_bundle", Passed: true, Severity: "block", Detail: "the graph consumes one verified immutable projection-readiness bundle"},
		{ID: "calculation_lineage", Passed: true, Severity: "block", Detail: "every edge binds the exact receiver-flow calculation and Schedule A fact set"},
		{ID: "content_addressed_database", Passed: true, Severity: "block", Detail: "the isolated database name derives from all projection inputs and model version"},
		{ID: "count_conservation", Passed: true, Severity: "block", Detail: "ArangoDB entity, edge, and role counts equal the deterministic model"},
		{ID: "signed_amount_conservation", Passed: true, Severity: "block", Detail: "exact signed edge cents read from ArangoDB equal the calculation and every role subtotal"},
		{ID: "topology_analysis", Passed: true, Severity: "block", Detail: "weak components, strong components, cyclic components, and representative endpoints were derived from the complete projected graph"},
		{ID: "master_fact_coverage", Passed: state == "ready", Severity: "warn", Detail: fmt.Sprintf("missing committee masters=%d", model.Missing.Committees)},
		{ID: "storage_figures", Passed: true, Severity: "block", Detail: "document and index figures were read for every projection collection"},
		{ID: "query_execution", Passed: len(queries) == expectedQueries, Severity: "block", Detail: "neighborhood, ranked-path, shortest-path, and available cycle queries completed repeatedly"},
	}
	return Result{
		SchemaVersion: ResultSchemaVersion, ProjectionVersion: ProjectionVersion,
		ProjectionID: model.ID, State: state, Cycle: model.Cycle,
		Database: model.Database, Graph: GraphName, RunID: input.RunID,
		ObservedAt: options.Clock().UTC(), Inputs: model.Inputs,
		ExpectedCounts: model.Counts, ObservedCounts: observedCounts,
		ExpectedAmounts: model.Amounts, ObservedAmounts: observedAmounts,
		Topology: model.Topology, ReusedProjection: complete,
		MissingMasterFacts: model.Missing, Storage: storage,
		RepresentativeSource: model.RepresentativeSource,
		RepresentativeTarget: model.RepresentativeTarget,
		RepresentativeCycle:  model.RepresentativeCycle,
		Queries:              queries, Checks: checks,
	}, nil
}

func ensureSchema(ctx context.Context, client *arangoClient, database string) error {
	for _, collection := range []struct {
		name           string
		collectionType int
	}{
		{entitiesCollection, 2}, {edgesCollection, 3}, {metadataCollection, 2},
	} {
		if err := client.ensureCollection(ctx, database, collection.name, collection.collectionType); err != nil {
			return err
		}
	}
	for _, index := range []struct {
		collection string
		name       string
		fields     []string
	}{
		{entitiesCollection, "committee_identity", []string{"cycle", "entity_id"}},
		{edgesCollection, "flow_role", []string{"cycle", "receipt_role"}},
		{edgesCollection, "flow_result", []string{"result_id"}},
		{metadataCollection, "projection_identity", []string{"projection_id"}},
	} {
		if err := client.ensurePersistentIndex(ctx, database, index.collection, index.name, index.fields); err != nil {
			return err
		}
	}
	return client.ensureGraph(ctx, database)
}

func importBatches[T any](ctx context.Context, client *arangoClient, database, collection string, documents []T, batchSize int) error {
	for start := 0; start < len(documents); start += batchSize {
		end := start + batchSize
		if end > len(documents) {
			end = len(documents)
		}
		if _, err := client.importDocuments(ctx, database, collection, documents[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func readCounts(ctx context.Context, client *arangoClient, database string) (ProjectionCounts, error) {
	var result ProjectionCounts
	for _, collection := range []struct {
		name string
		set  func(uint64)
	}{
		{entitiesCollection, func(value uint64) { result.Entities = value }},
		{edgesCollection, func(value uint64) { result.Edges = value }},
	} {
		rows, err := client.query(ctx, database, "RETURN LENGTH(@@collection)", map[string]any{"@collection": collection.name})
		if err != nil {
			return ProjectionCounts{}, fmt.Errorf("count %s: %w", collection.name, err)
		}
		if len(rows) != 1 {
			return ProjectionCounts{}, fmt.Errorf("count %s returned %d rows", collection.name, len(rows))
		}
		var count uint64
		if err := json.Unmarshal(rows[0], &count); err != nil {
			return ProjectionCounts{}, err
		}
		collection.set(count)
	}
	entityRows, err := client.query(ctx, database, `
FOR entity IN @@entities
  COLLECT source_state = entity.source_state WITH COUNT INTO count
  RETURN {source_state, count}
`, map[string]any{"@entities": entitiesCollection})
	if err != nil {
		return ProjectionCounts{}, fmt.Errorf("count committee source states: %w", err)
	}
	for _, row := range entityRows {
		var item struct {
			SourceState string `json:"source_state"`
			Count       uint64 `json:"count"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionCounts{}, err
		}
		switch item.SourceState {
		case "present":
			result.PresentCommitteeMasters = item.Count
		case "missing_master_fact":
			result.MissingCommitteeMasters = item.Count
		default:
			return ProjectionCounts{}, fmt.Errorf("unexpected committee source state %q", item.SourceState)
		}
	}
	roleRows, err := client.query(ctx, database, `
FOR edge IN @@edges
  COLLECT role = edge.receipt_role WITH COUNT INTO count
  RETURN {role, count}
`, map[string]any{"@edges": edgesCollection})
	if err != nil {
		return ProjectionCounts{}, fmt.Errorf("count receiver-flow roles: %w", err)
	}
	for _, row := range roleRows {
		var item struct {
			Role  string `json:"role"`
			Count uint64 `json:"count"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionCounts{}, err
		}
		switch item.Role {
		case "registered_filer_contribution":
			result.RegisteredFilerContribution = item.Count
		case "registered_filer_in_kind_contribution":
			result.InKindContribution = item.Count
		case "affiliated_transfer_in":
			result.AffiliatedTransferIn = item.Count
		case "refund_or_repayment_received":
			result.RefundRepaymentReceived = item.Count
		default:
			return ProjectionCounts{}, fmt.Errorf("unexpected receiver-flow role %q", item.Role)
		}
	}
	return result, nil
}

func readAmounts(ctx context.Context, client *arangoClient, database string) (ProjectionAmounts, error) {
	rows, err := client.query(ctx, database, `
FOR edge IN @@edges
  RETURN {role: edge.receipt_role, amount_minor_units: edge.amount_minor_units}
`, map[string]any{"@edges": edgesCollection})
	if err != nil {
		return ProjectionAmounts{}, fmt.Errorf("read receiver-flow edge amounts: %w", err)
	}
	var total, registered, inKind, transfer, refund big.Int
	for _, row := range rows {
		var item struct {
			Role             string `json:"role"`
			AmountMinorUnits string `json:"amount_minor_units"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionAmounts{}, err
		}
		amount, ok := new(big.Int).SetString(item.AmountMinorUnits, 10)
		if !ok || amount.String() != item.AmountMinorUnits {
			return ProjectionAmounts{}, fmt.Errorf("ArangoDB edge contains invalid exact cents %q", item.AmountMinorUnits)
		}
		total.Add(&total, amount)
		switch item.Role {
		case "registered_filer_contribution":
			registered.Add(&registered, amount)
		case "registered_filer_in_kind_contribution":
			inKind.Add(&inKind, amount)
		case "affiliated_transfer_in":
			transfer.Add(&transfer, amount)
		case "refund_or_repayment_received":
			refund.Add(&refund, amount)
		default:
			return ProjectionAmounts{}, fmt.Errorf("ArangoDB edge contains invalid role %q", item.Role)
		}
	}
	return ProjectionAmounts{
		TotalMinorUnits: total.String(), RegisteredFilerContributionMinorUnits: registered.String(),
		InKindContributionMinorUnits: inKind.String(), AffiliatedTransferInMinorUnits: transfer.String(),
		RefundRepaymentReceivedMinorUnits: refund.String(),
	}, nil
}

func readStorageMetrics(ctx context.Context, client *arangoClient, database string, counts ProjectionCounts) (StorageMetrics, error) {
	collections := []struct {
		name  string
		count uint64
	}{
		{entitiesCollection, counts.Entities}, {edgesCollection, counts.Edges}, {metadataCollection, 1},
	}
	result := StorageMetrics{Collections: make([]CollectionMetrics, 0, len(collections))}
	for _, collection := range collections {
		figures, err := client.collectionFigures(ctx, database, collection.name)
		if err != nil {
			return StorageMetrics{}, fmt.Errorf("read %s storage figures: %w", collection.name, err)
		}
		metric := CollectionMetrics{
			Name: collection.name, Documents: collection.count,
			DocumentBytes: figures.Figures.DocumentsSize, IndexCount: figures.Figures.Indexes.Count,
			IndexBytes: figures.Figures.Indexes.Size, CacheBytes: figures.Figures.CacheSize,
		}
		result.Collections = append(result.Collections, metric)
		result.DocumentBytes += metric.DocumentBytes
		result.IndexBytes += metric.IndexBytes
	}
	result.CombinedBytes = result.DocumentBytes + result.IndexBytes
	return result, nil
}

func benchmarkQueries(ctx context.Context, client *arangoClient, model projection, repetitions int) ([]QueryMetric, error) {
	queries := []struct {
		id       string
		query    string
		bindVars map[string]any
	}{
		{
			id: "committee_direction_agnostic_neighborhood",
			query: `
FOR vertex, edge, path IN 1..4 ANY @start GRAPH @graph
  OPTIONS {uniqueVertices: "path", uniqueEdges: "path"}
  LIMIT 25
  RETURN {
    committee_id: vertex.entity_id,
    receipt_role: edge.receipt_role,
    amount_minor_units: edge.amount_minor_units,
    path_vertices: path.vertices[*]._key,
    path_edges: path.edges[*]._key
  }
`,
			bindVars: map[string]any{"start": entitiesCollection + "/" + committeeKey(model.RepresentativeSource), "graph": GraphName},
		},
		{
			id: "ranked_paths_between_committees",
			query: `
FOR path IN OUTBOUND K_SHORTEST_PATHS @start TO @target GRAPH @graph
  LIMIT 25
  RETURN {path_vertices: path.vertices[*]._key, path_edges: path.edges[*]._key}
`,
			bindVars: map[string]any{
				"start":  entitiesCollection + "/" + committeeKey(model.RepresentativeSource),
				"target": entitiesCollection + "/" + committeeKey(model.RepresentativeTarget), "graph": GraphName,
			},
		},
		{
			id: "directed_shortest_path_between_committees",
			query: `
FOR vertex, edge IN OUTBOUND SHORTEST_PATH @start TO @target GRAPH @graph
  RETURN {committee_id: vertex.entity_id, edge_key: edge == null ? null : edge._key}
`,
			bindVars: map[string]any{
				"start":  entitiesCollection + "/" + committeeKey(model.RepresentativeSource),
				"target": entitiesCollection + "/" + committeeKey(model.RepresentativeTarget), "graph": GraphName,
			},
		},
	}
	if model.RepresentativeCycle != "" {
		queries = append(queries, struct {
			id       string
			query    string
			bindVars map[string]any
		}{
			id: "directed_cycles_from_committee",
			query: fmt.Sprintf(`
FOR vertex, edge, path IN 1..%d OUTBOUND @start GRAPH @graph
  OPTIONS {uniqueVertices: "none", uniqueEdges: "path"}
  FILTER vertex._id == @start
  LIMIT 25
  RETURN {path_vertices: path.vertices[*]._key, path_edges: path.edges[*]._key}
`, model.Topology.RepresentativeCycleHops),
			bindVars: map[string]any{
				"start": entitiesCollection + "/" + committeeKey(model.RepresentativeCycle), "graph": GraphName,
			},
		})
	}
	metrics := make([]QueryMetric, 0, len(queries))
	for _, selected := range queries {
		if _, err := client.query(ctx, model.Database, selected.query, selected.bindVars); err != nil {
			return nil, fmt.Errorf("warm %s: %w", selected.id, err)
		}
		durations := make([]int64, 0, repetitions)
		resultRows := -1
		for repetition := 0; repetition < repetitions; repetition++ {
			started := time.Now()
			rows, err := client.query(ctx, model.Database, selected.query, selected.bindVars)
			elapsed := time.Since(started).Microseconds()
			if err != nil {
				return nil, fmt.Errorf("benchmark %s: %w", selected.id, err)
			}
			if resultRows == -1 {
				resultRows = len(rows)
			} else if len(rows) != resultRows {
				return nil, fmt.Errorf("query %s returned unstable row counts", selected.id)
			}
			durations = append(durations, elapsed)
		}
		if resultRows == 0 {
			return nil, fmt.Errorf("query %s returned no rows", selected.id)
		}
		sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
		p95Index, err := percentileIndex(len(durations), 95)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, QueryMetric{
			ID: selected.id, Repetitions: repetitions, ResultRows: resultRows,
			MinimumMicros: durations[0], MedianMicros: durations[(len(durations)-1)/2],
			P95Micros: durations[p95Index], MaximumMicros: durations[len(durations)-1],
		})
	}
	return metrics, nil
}

func percentileIndex(length, percentile int) (int, error) {
	if length < 1 || percentile < 1 || percentile > 100 {
		return 0, fmt.Errorf("invalid percentile input length=%s percentile=%s", strconv.Itoa(length), strconv.Itoa(percentile))
	}
	index := (length*percentile + 99) / 100
	return index - 1, nil
}
