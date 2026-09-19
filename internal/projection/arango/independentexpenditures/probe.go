package independentexpenditures

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

// Run builds or reuses one content-addressed, isolated ArangoDB database and
// measures the outside-spending queries that justify the projection.
func Run(ctx context.Context, input Input, options Options) (Result, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if input.StorageRoot == "" || input.Cycle == "" {
		return Result{}, fmt.Errorf("storage root and cycle are required")
	}
	directInputs := input.CalculationManifestPath != "" || input.CandidateManifestPath != "" || input.CommitteeManifestPath != ""
	if input.ReadinessBundlePath != "" && directInputs {
		return Result{}, fmt.Errorf("projection readiness bundle cannot be combined with direct manifest paths")
	}
	if input.ReadinessBundlePath == "" &&
		(input.CalculationManifestPath == "" || input.CandidateManifestPath == "" || input.CommitteeManifestPath == "") {
		return Result{}, fmt.Errorf("projection readiness bundle or all three direct manifest paths are required")
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

	progress("validating immutable outside-spending projection inputs")
	loaded, err := loadInputs(ctx, input)
	if err != nil {
		return Result{}, err
	}
	progress("building deterministic independent-expenditure graph documents")
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
		progress("reusing completed content-addressed outside-spending projection")
	} else {
		progress("importing referenced candidate and spender entities")
		if err := importBatches(ctx, client, model.Database, entitiesCollection, model.Entities, input.BatchSize); err != nil {
			return Result{}, err
		}
		progress("importing support and opposition edges")
		if err := importBatches(ctx, client, model.Database, edgesCollection, model.Edges, input.BatchSize); err != nil {
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
	observedAmounts, err := readAmounts(ctx, client, model.Database)
	if err != nil {
		return Result{}, err
	}
	if observedAmounts != model.Amounts {
		return Result{}, fmt.Errorf("ArangoDB projection amounts differ: got %+v want %+v", observedAmounts, model.Amounts)
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
		return Result{}, fmt.Errorf("ArangoDB returned incomplete projection storage figures")
	}

	progress("measuring representative outside-spending graph queries")
	queries, err := benchmarkQueries(ctx, client, model.Database, model.RepresentativeCandidate, model.RepresentativeSpender, input.QueryRepetitions)
	if err != nil {
		return Result{}, err
	}
	state := "ready"
	if model.Missing.Candidates != 0 || model.Missing.Spenders != 0 {
		state = "partial"
	}
	checks := []Check{
		{ID: "input_coherence", Passed: true, Severity: "block", Detail: "calculation and master facts share one exact cycle and source release"},
		{ID: "calculation_lineage", Passed: true, Severity: "block", Detail: "the projection binds the exact effective calculation and Schedule E fact-set identities"},
		{ID: "content_addressed_database", Passed: true, Severity: "block", Detail: "the isolated database name derives from all projection inputs and the model version"},
		{ID: "count_conservation", Passed: true, Severity: "block", Detail: "ArangoDB entity, edge, and stance counts equal the deterministic model"},
		{ID: "signed_amount_conservation", Passed: true, Severity: "block", Detail: "exact signed edge cents read from ArangoDB equal the attributed calculation amount"},
		{ID: "master_fact_coverage", Passed: state == "ready", Severity: "warn", Detail: fmt.Sprintf("missing candidate masters=%d spender masters=%d", model.Missing.Candidates, model.Missing.Spenders)},
		{ID: "storage_figures", Passed: true, Severity: "block", Detail: "document and index figures were read for every projection collection"},
		{ID: "query_execution", Passed: len(queries) == 3, Severity: "block", Detail: "candidate inbound paths, spender outbound paths, and stance summaries completed repeatedly"},
	}
	return Result{
		SchemaVersion: ResultSchemaVersion, ProjectionVersion: ProjectionVersion,
		ProjectionID: model.ID, State: state, Cycle: model.Cycle,
		Database: model.Database, Graph: GraphName, RunID: input.RunID,
		ObservedAt: options.Clock().UTC(), Inputs: model.Inputs,
		ExpectedCounts: model.Counts, ObservedCounts: observedCounts,
		ExpectedAmounts: model.Amounts, ObservedAmounts: observedAmounts,
		ReusedProjection: complete, MissingMasterFacts: model.Missing,
		Storage: storage, RepresentativeCandidate: model.RepresentativeCandidate,
		RepresentativeSpender: model.RepresentativeSpender,
		Queries:               queries, Checks: checks,
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
		{entitiesCollection, "entity_identity", []string{"cycle", "entity_type", "entity_id"}},
		{edgesCollection, "outside_spending_stance", []string{"cycle", "support_oppose"}},
		{edgesCollection, "outside_spending_result", []string{"result_id"}},
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
			return ProjectionCounts{}, fmt.Errorf("decode %s count: %w", collection.name, err)
		}
		collection.set(count)
	}
	entityRows, err := client.query(ctx, database, `
FOR entity IN @@entities
  COLLECT entity_type = entity.entity_type WITH COUNT INTO count
  RETURN {entity_type, count}
`, map[string]any{"@entities": entitiesCollection})
	if err != nil {
		return ProjectionCounts{}, fmt.Errorf("count entity types: %w", err)
	}
	for _, row := range entityRows {
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
			result.Spenders = item.Count
		default:
			return ProjectionCounts{}, fmt.Errorf("unexpected entity type %q", item.EntityType)
		}
	}
	stanceRows, err := client.query(ctx, database, `
FOR edge IN @@edges
  COLLECT stance = edge.support_oppose WITH COUNT INTO count
  RETURN {stance, count}
`, map[string]any{"@edges": edgesCollection})
	if err != nil {
		return ProjectionCounts{}, fmt.Errorf("count edge stances: %w", err)
	}
	for _, row := range stanceRows {
		var item struct {
			Stance string `json:"stance"`
			Count  uint64 `json:"count"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionCounts{}, err
		}
		switch item.Stance {
		case "S":
			result.SupportEdges = item.Count
		case "O":
			result.OppositionEdges = item.Count
		default:
			return ProjectionCounts{}, fmt.Errorf("unexpected edge stance %q", item.Stance)
		}
	}
	return result, nil
}

func readAmounts(ctx context.Context, client *arangoClient, database string) (ProjectionAmounts, error) {
	rows, err := client.query(ctx, database, `
FOR edge IN @@edges
  RETURN {stance: edge.support_oppose, amount_minor_units: edge.amount_minor_units}
`, map[string]any{"@edges": edgesCollection})
	if err != nil {
		return ProjectionAmounts{}, fmt.Errorf("read edge amounts: %w", err)
	}
	var attributed, support, opposition big.Int
	for _, row := range rows {
		var item struct {
			Stance           string `json:"stance"`
			AmountMinorUnits string `json:"amount_minor_units"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionAmounts{}, err
		}
		amount, ok := new(big.Int).SetString(item.AmountMinorUnits, 10)
		if !ok || amount.String() != item.AmountMinorUnits {
			return ProjectionAmounts{}, fmt.Errorf("ArangoDB edge contains invalid exact cents %q", item.AmountMinorUnits)
		}
		attributed.Add(&attributed, amount)
		switch item.Stance {
		case "S":
			support.Add(&support, amount)
		case "O":
			opposition.Add(&opposition, amount)
		default:
			return ProjectionAmounts{}, fmt.Errorf("ArangoDB edge contains invalid stance %q", item.Stance)
		}
	}
	return ProjectionAmounts{
		AttributedMinorUnits: attributed.String(), SupportMinorUnits: support.String(), OppositionMinorUnits: opposition.String(),
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

func benchmarkQueries(ctx context.Context, client *arangoClient, database, candidateID, spenderID string, repetitions int) ([]QueryMetric, error) {
	queries := []struct {
		id       string
		query    string
		bindVars map[string]any
	}{
		{
			id: "candidate_inbound_outside_spending_paths",
			query: `
FOR vertex, edge, path IN 1..1 INBOUND @start GRAPH @graph
  LIMIT 25
  RETURN {
    spender_committee_id: vertex.entity_id,
    support_oppose: edge.support_oppose,
    amount_minor_units: edge.amount_minor_units,
    path_vertices: path.vertices[*]._key,
    path_edges: path.edges[*]._key
  }
`,
			bindVars: map[string]any{"start": entitiesCollection + "/" + candidateKey(candidateID), "graph": GraphName},
		},
		{
			id: "spender_outbound_candidate_paths",
			query: `
FOR vertex, edge, path IN 1..1 OUTBOUND @start GRAPH @graph
  LIMIT 25
  RETURN {
    candidate_id: vertex.entity_id,
    support_oppose: edge.support_oppose,
    amount_minor_units: edge.amount_minor_units,
    path_vertices: path.vertices[*]._key,
    path_edges: path.edges[*]._key
  }
`,
			bindVars: map[string]any{"start": entitiesCollection + "/" + committeeKey(spenderID), "graph": GraphName},
		},
		{
			id: "candidate_stance_summary",
			query: `
FOR edge IN @@edges
  FILTER edge._to == @candidate
  COLLECT stance = edge.support_oppose WITH COUNT INTO edge_count
  SORT stance
  RETURN {stance, edge_count}
`,
			bindVars: map[string]any{"@edges": edgesCollection, "candidate": entitiesCollection + "/" + candidateKey(candidateID)},
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
