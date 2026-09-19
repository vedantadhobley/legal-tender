package candidatereceipts

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

// Run builds or reuses one content-addressed, isolated ArangoDB database and
// measures the candidate-receipt queries that justify the projection.
func Run(ctx context.Context, input Input, options Options) (Result, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if input.StorageRoot == "" || input.Cycle == "" || input.FactBundleManifestPath == "" ||
		input.CalculationManifestPath == "" || input.CandidateManifestPath == "" || input.CommitteeManifestPath == "" {
		return Result{}, fmt.Errorf("storage root, cycle, fact bundle, calculation, candidate facts, and committee facts are required")
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

	progress("validating immutable projection inputs")
	loaded, err := loadInputs(ctx, input)
	if err != nil {
		return Result{}, err
	}
	progress("building deterministic candidate-receipt projection documents")
	model, err := buildProjection(ctx, input.StorageRoot, loaded)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	progress("ensuring isolated ArangoDB probe database " + model.Database)
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
		progress("reusing completed content-addressed projection")
	} else {
		progress("importing candidate and committee entities")
		if err := importBatches(ctx, client, model.Database, entitiesCollection, model.Entities, input.BatchSize); err != nil {
			return Result{}, err
		}
		progress("importing candidate calculation results")
		if err := importBatches(ctx, client, model.Database, resultsCollection, model.Results, input.BatchSize); err != nil {
			return Result{}, err
		}
		progress("importing candidate-committee relationship edges")
		if err := importBatches(ctx, client, model.Database, relationshipsCollection, model.Relationships, input.BatchSize); err != nil {
			return Result{}, err
		}
		progress("importing calculated receipt-component edges")
		if err := importBatches(ctx, client, model.Database, receiptsCollection, model.ReceiptComponents, input.BatchSize); err != nil {
			return Result{}, err
		}
	}

	observedCounts, err := readCounts(ctx, client, model.Database)
	if err != nil {
		return Result{}, err
	}
	if observedCounts != model.Counts {
		return Result{}, fmt.Errorf("ArangoDB projection counts differ: got %+v want %+v", observedCounts, model.Counts)
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
	if len(storage.Collections) != 5 || storage.CombinedBytes == 0 {
		return Result{}, fmt.Errorf("ArangoDB returned incomplete projection storage figures")
	}

	progress("measuring representative graph queries")
	queries, err := benchmarkQueries(ctx, client, model.Database, model.RepresentativeCandidate, input.QueryRepetitions)
	if err != nil {
		return Result{}, err
	}
	state := "ready"
	if model.Missing.Candidates != 0 || model.Missing.Committees != 0 {
		state = "partial"
	}
	checks := []Check{
		{ID: "input_coherence", Passed: true, Severity: "block", Detail: "calculation, fact bundle, and master facts share one exact cycle and source release"},
		{ID: "content_addressed_database", Passed: true, Severity: "block", Detail: "the isolated database name derives from projection inputs and model version"},
		{ID: "count_conservation", Passed: true, Severity: "block", Detail: "ArangoDB collection counts equal the deterministic projection counts"},
		{ID: "master_fact_coverage", Passed: state == "ready", Severity: "warn", Detail: fmt.Sprintf("missing candidate masters=%d committee masters=%d", model.Missing.Candidates, model.Missing.Committees)},
		{ID: "storage_figures", Passed: true, Severity: "block", Detail: "document and index figures were read for every projection collection"},
		{ID: "query_execution", Passed: len(queries) == 2, Severity: "block", Detail: "point result and inbound graph-neighborhood queries completed repeatedly"},
	}
	return Result{
		SchemaVersion: ResultSchemaVersion, ProjectionVersion: ProjectionVersion,
		ProjectionID: model.ID, State: state, Cycle: model.Cycle,
		Database: model.Database, Graph: GraphName, RunID: input.RunID,
		ObservedAt: options.Clock().UTC(), Inputs: model.Inputs,
		ExpectedCounts: model.Counts, ObservedCounts: observedCounts,
		ReusedProjection:        complete,
		MissingMasterFacts:      model.Missing,
		Storage:                 storage,
		RepresentativeCandidate: model.RepresentativeCandidate,
		Queries:                 queries, Checks: checks,
	}, nil
}

func readStorageMetrics(ctx context.Context, client *arangoClient, database string, counts ProjectionCounts) (StorageMetrics, error) {
	collections := []struct {
		name  string
		count uint64
	}{
		{entitiesCollection, counts.Entities},
		{resultsCollection, counts.CandidateResults},
		{relationshipsCollection, counts.CandidateCommitteeRelationships},
		{receiptsCollection, counts.ReceiptComponents},
		{metadataCollection, 1},
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

func ensureSchema(ctx context.Context, client *arangoClient, database string) error {
	for _, collection := range []struct {
		name           string
		collectionType int
	}{
		{entitiesCollection, 2}, {resultsCollection, 2},
		{relationshipsCollection, 3}, {receiptsCollection, 3},
		{metadataCollection, 2},
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
		{entitiesCollection, "entity_identity", []string{"cycle", "entity_type", "entity_id"}},
		{resultsCollection, "candidate_result_identity", []string{"cycle", "candidate_id"}},
		{relationshipsCollection, "relationship_state", []string{"cycle", "relationship_state"}},
		{receiptsCollection, "receipt_component_type", []string{"cycle", "relation_type"}},
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
	collections := []struct {
		name string
		set  func(*ProjectionCounts, uint64)
	}{
		{entitiesCollection, func(counts *ProjectionCounts, value uint64) { counts.Entities = value }},
		{resultsCollection, func(counts *ProjectionCounts, value uint64) { counts.CandidateResults = value }},
		{relationshipsCollection, func(counts *ProjectionCounts, value uint64) { counts.CandidateCommitteeRelationships = value }},
		{receiptsCollection, func(counts *ProjectionCounts, value uint64) { counts.ReceiptComponents = value }},
	}
	var result ProjectionCounts
	for _, collection := range collections {
		rows, err := client.query(ctx, database, "RETURN LENGTH(@@collection)", map[string]any{"@collection": collection.name})
		if err != nil {
			return ProjectionCounts{}, fmt.Errorf("count %s: %w", collection.name, err)
		}
		if len(rows) != 1 {
			return ProjectionCounts{}, fmt.Errorf("count %s returned %d rows", collection.name, len(rows))
		}
		var count uint64
		if err := json.Unmarshal(rows[0], &count); err != nil {
			return ProjectionCounts{}, fmt.Errorf("decode %s count: %w", collection.name, err)
		}
		collection.set(&result, count)
	}
	rows, err := client.query(ctx, database, `
FOR entity IN @@entities
  COLLECT entity_type = entity.entity_type WITH COUNT INTO count
  RETURN {entity_type, count}
`, map[string]any{"@entities": entitiesCollection})
	if err != nil {
		return ProjectionCounts{}, fmt.Errorf("count entity types: %w", err)
	}
	for _, row := range rows {
		var item struct {
			EntityType string `json:"entity_type"`
			Count      uint64 `json:"count"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionCounts{}, err
		}
		switch item.EntityType {
		case "candidate":
			result.Candidates = item.Count
		case "committee":
			result.Committees = item.Count
		default:
			return ProjectionCounts{}, fmt.Errorf("unexpected entity type %q", item.EntityType)
		}
	}
	return result, nil
}

func benchmarkQueries(ctx context.Context, client *arangoClient, database, candidateID string, repetitions int) ([]QueryMetric, error) {
	if candidateID == "" {
		return nil, fmt.Errorf("no representative candidate was projected")
	}
	queries := []struct {
		id       string
		query    string
		bindVars map[string]any
	}{
		{
			id:       "candidate_result_by_id",
			query:    "RETURN DOCUMENT(@document_id)",
			bindVars: map[string]any{"document_id": resultsCollection + "/" + candidateKey(candidateID)},
		},
		{
			id: "candidate_inbound_receipt_neighborhood",
			query: `
FOR vertex, edge, path IN 1..1 INBOUND @start GRAPH @graph
  FILTER edge.relation_type == @relation_type
  LIMIT 25
  RETURN {
    committee_id: vertex.entity_id,
    amount_minor_units: edge.amount_minor_units,
    path_vertices: path.vertices[*]._key
  }
`,
			bindVars: map[string]any{
				"start": entitiesCollection + "/" + candidateKey(candidateID),
				"graph": GraphName, "relation_type": "fec_itemized_individual_receipts",
			},
		},
	}
	metrics := make([]QueryMetric, 0, len(queries))
	for _, selected := range queries {
		if _, err := client.query(ctx, database, selected.query, selected.bindVars); err != nil {
			return nil, fmt.Errorf("warm %s: %w", selected.id, err)
		}
		durations := make([]int64, 0, repetitions)
		resultRows := -1
		for repetition := 0; repetition < repetitions; repetition++ {
			started := time.Now()
			rows, err := client.query(ctx, database, selected.query, selected.bindVars)
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
