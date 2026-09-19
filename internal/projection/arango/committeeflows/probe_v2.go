package committeeflows

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

// RunV2 builds or reuses an isolated identity-aware receiver-flow graph. The
// shipped v1 database is never opened for writes.
func RunV2(ctx context.Context, input InputV2, options Options) (ResultV2, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if input.StorageRoot == "" || input.Cycle == "" || input.ReadinessBundlePath == "" {
		return ResultV2{}, fmt.Errorf("storage root, cycle, and v2 receiver-flow bundle are required")
	}
	if !fecrelease.ValidAcquisitionRunID(input.RunID) {
		return ResultV2{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if input.BatchSize == 0 {
		input.BatchSize = 5_000
	}
	if input.BatchSize < 1 || input.BatchSize > 50_000 {
		return ResultV2{}, fmt.Errorf("batch size must be between 1 and 50000")
	}
	if input.QueryRepetitions == 0 {
		input.QueryRepetitions = 10
	}
	if input.QueryRepetitions < 1 || input.QueryRepetitions > 100 {
		return ResultV2{}, fmt.Errorf("query repetitions must be between 1 and 100")
	}
	client, err := newArangoClient(input.Endpoint, input.Username, input.Password)
	if err != nil {
		return ResultV2{}, err
	}
	progress := func(message string) {
		if options.Progress != nil {
			options.Progress(message)
		}
	}

	progress("validating immutable identity-aware receiver-flow bundle")
	loaded, err := loadInputsV2(ctx, input)
	if err != nil {
		return ResultV2{}, err
	}
	progress("building deterministic identity-aware receiver-flow documents")
	model, err := buildProjectionV2(ctx, input.StorageRoot, loaded)
	if err != nil {
		return ResultV2{}, err
	}
	if err := ctx.Err(); err != nil {
		return ResultV2{}, err
	}

	progress("ensuring isolated ArangoDB v2 flow database " + model.Database)
	if err := client.ensureDatabase(ctx, model.Database); err != nil {
		return ResultV2{}, err
	}
	if err := ensureSchema(ctx, client, model.Database); err != nil {
		return ResultV2{}, err
	}
	existing, complete, err := client.metadataV2(ctx, model.Database, model.ID)
	if err != nil {
		return ResultV2{}, fmt.Errorf("read v2 projection metadata: %w", err)
	}
	if complete {
		if !reflect.DeepEqual(existing, model.Metadata) {
			return ResultV2{}, fmt.Errorf("existing v2 projection metadata does not match projection %s", model.ID)
		}
		progress("reusing completed content-addressed identity-aware projection")
	} else {
		progress("importing committee entities with explicit identity states")
		if err := importBatches(ctx, client, model.Database, entitiesCollection, model.Entities, input.BatchSize); err != nil {
			return ResultV2{}, err
		}
		progress("importing receiver-reported committee-flow edges")
		if err := importBatches(ctx, client, model.Database, edgesCollection, model.Edges, input.BatchSize); err != nil {
			return ResultV2{}, err
		}
	}

	observedCounts, err := readCountsV2(ctx, client, model.Database)
	if err != nil {
		return ResultV2{}, err
	}
	if observedCounts != model.Counts {
		return ResultV2{}, fmt.Errorf("ArangoDB v2 receiver-flow counts differ: got %+v want %+v", observedCounts, model.Counts)
	}
	observedAmounts, err := readAmounts(ctx, client, model.Database)
	if err != nil {
		return ResultV2{}, err
	}
	if observedAmounts != model.Amounts {
		return ResultV2{}, fmt.Errorf("ArangoDB v2 receiver-flow amounts differ: got %+v want %+v", observedAmounts, model.Amounts)
	}
	if !complete {
		if _, err := client.importDocuments(ctx, model.Database, metadataCollection, []projectionMetadataV2{model.Metadata}); err != nil {
			return ResultV2{}, fmt.Errorf("publish v2 projection metadata: %w", err)
		}
	}
	storage, err := readStorageMetrics(ctx, client, model.Database, ProjectionCounts{Entities: observedCounts.Entities, Edges: observedCounts.Edges})
	if err != nil {
		return ResultV2{}, err
	}
	if len(storage.Collections) != 3 || storage.CombinedBytes == 0 {
		return ResultV2{}, fmt.Errorf("ArangoDB returned incomplete v2 receiver-flow storage figures")
	}

	progress("measuring representative neighborhood, path, shortest-path, and cycle queries")
	queryModel := projection{
		Database: model.Database, Topology: model.Topology,
		RepresentativeSource: model.RepresentativeSource,
		RepresentativeTarget: model.RepresentativeTarget,
		RepresentativeCycle:  model.RepresentativeCycle,
	}
	queries, err := benchmarkQueries(ctx, client, queryModel, input.QueryRepetitions)
	if err != nil {
		return ResultV2{}, err
	}
	expectedQueries := 3
	if model.Topology.CyclicStrongComponents != 0 {
		expectedQueries = 4
	}
	state := "ready"
	if model.Counts.UnresolvedReportedIDs != 0 {
		state = "partial"
	}
	checks := []Check{
		{ID: "input_bundle", Passed: true, Severity: "block", Detail: "the graph consumes one verified immutable v2 bundle over the shipped v1 flow boundary"},
		{ID: "calculation_lineage", Passed: true, Severity: "block", Detail: "every edge retains the exact receiver-flow calculation and Schedule A fact set"},
		{ID: "identity_lineage", Passed: true, Severity: "block", Detail: "every non-current identity state binds one exact immutable coverage decision"},
		{ID: "content_addressed_database", Passed: true, Severity: "block", Detail: "the isolated v2 database name derives from all flow, master, identity, and model versions"},
		{ID: "count_conservation", Passed: true, Severity: "block", Detail: "ArangoDB entity, identity-state, eligibility, edge, and role counts equal the deterministic model"},
		{ID: "signed_amount_conservation", Passed: true, Severity: "block", Detail: "exact signed edge cents read from ArangoDB equal the calculation and every role subtotal"},
		{ID: "terminal_identity_guard", Passed: model.Counts.TerminalIdentityIneligible == model.Counts.HistoricalRegistrations+model.Counts.AlternateReleaseRegistrations+model.Counts.UnresolvedReportedIDs, Severity: "block", Detail: "historical, alternate-release, and unresolved identities cannot stop terminal-source traversal"},
		{ID: "unresolved_identity_coverage", Passed: state == "ready", Severity: "warn", Detail: fmt.Sprintf("unresolved reported committee IDs=%d", model.Counts.UnresolvedReportedIDs)},
		{ID: "topology_analysis", Passed: true, Severity: "block", Detail: "weak components, strong components, cyclic components, and representative endpoints were derived from the complete projected graph"},
		{ID: "storage_figures", Passed: true, Severity: "block", Detail: "document and index figures were read for every v2 projection collection"},
		{ID: "query_execution", Passed: len(queries) == expectedQueries, Severity: "block", Detail: "neighborhood, ranked-path, shortest-path, and available cycle queries completed repeatedly"},
	}
	return ResultV2{
		SchemaVersion: ResultSchemaVersionV2, ProjectionVersion: ProjectionVersionV2,
		ProjectionID: model.ID, State: state, Cycle: model.Cycle,
		Database: model.Database, Graph: GraphName, RunID: input.RunID,
		ObservedAt: options.Clock().UTC(), Inputs: model.Inputs,
		ExpectedCounts: model.Counts, ObservedCounts: observedCounts,
		ExpectedAmounts: model.Amounts, ObservedAmounts: observedAmounts,
		Topology: model.Topology, ReusedProjection: complete, Storage: storage,
		RepresentativeSource: model.RepresentativeSource,
		RepresentativeTarget: model.RepresentativeTarget,
		RepresentativeCycle:  model.RepresentativeCycle,
		Queries:              queries, Checks: checks,
	}, nil
}

func readCountsV2(ctx context.Context, client *arangoClient, database string) (ProjectionCountsV2, error) {
	var result ProjectionCountsV2
	for _, collection := range []struct {
		name string
		set  func(uint64)
	}{
		{entitiesCollection, func(value uint64) { result.Entities = value }},
		{edgesCollection, func(value uint64) { result.Edges = value }},
	} {
		rows, err := client.query(ctx, database, "RETURN LENGTH(@@collection)", map[string]any{"@collection": collection.name})
		if err != nil {
			return ProjectionCountsV2{}, fmt.Errorf("count %s: %w", collection.name, err)
		}
		if len(rows) != 1 {
			return ProjectionCountsV2{}, fmt.Errorf("count %s returned %d rows", collection.name, len(rows))
		}
		var count uint64
		if err := json.Unmarshal(rows[0], &count); err != nil {
			return ProjectionCountsV2{}, err
		}
		collection.set(count)
	}
	entityRows, err := client.query(ctx, database, `
FOR entity IN @@entities
  COLLECT source_state = entity.source_state, eligible = entity.terminal_identity_eligible WITH COUNT INTO count
  RETURN {source_state, eligible, count}
`, map[string]any{"@entities": entitiesCollection})
	if err != nil {
		return ProjectionCountsV2{}, fmt.Errorf("count committee identity states: %w", err)
	}
	for _, row := range entityRows {
		var item struct {
			SourceState string `json:"source_state"`
			Eligible    bool   `json:"eligible"`
			Count       uint64 `json:"count"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionCountsV2{}, err
		}
		switch item.SourceState {
		case "current_cycle_master":
			if !item.Eligible {
				return ProjectionCountsV2{}, fmt.Errorf("current-cycle master is terminal-identity-ineligible")
			}
			result.CurrentCycleMasters = item.Count
			result.TerminalIdentityEligible += item.Count
		case "historical_registration":
			result.HistoricalRegistrations = item.Count
		case "alternate_release_registration":
			result.AlternateReleaseRegistrations = item.Count
		case "unresolved_reported_id":
			result.UnresolvedReportedIDs = item.Count
		default:
			return ProjectionCountsV2{}, fmt.Errorf("unexpected committee identity state %q", item.SourceState)
		}
		if item.SourceState != "current_cycle_master" {
			if item.Eligible {
				return ProjectionCountsV2{}, fmt.Errorf("committee state %s is incorrectly terminal-identity-eligible", item.SourceState)
			}
			result.TerminalIdentityIneligible += item.Count
		}
	}
	roleRows, err := client.query(ctx, database, `
FOR edge IN @@edges
  COLLECT role = edge.receipt_role WITH COUNT INTO count
  RETURN {role, count}
`, map[string]any{"@edges": edgesCollection})
	if err != nil {
		return ProjectionCountsV2{}, fmt.Errorf("count receiver-flow roles: %w", err)
	}
	for _, row := range roleRows {
		var item struct {
			Role  string `json:"role"`
			Count uint64 `json:"count"`
		}
		if err := json.Unmarshal(row, &item); err != nil {
			return ProjectionCountsV2{}, err
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
			return ProjectionCountsV2{}, fmt.Errorf("unexpected receiver-flow role %q", item.Role)
		}
	}
	return result, nil
}
