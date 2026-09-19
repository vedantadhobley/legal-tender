package independentexpenditures

import (
	"context"
	"fmt"
	"reflect"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

// RunResolved builds or reuses a content-addressed ArangoDB projection from
// resolved candidate groups. It never mutates or relabels the v1 reported-ID
// probe database.
func RunResolved(ctx context.Context, input ResolvedInput, options Options) (Result, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if input.StorageRoot == "" || input.Cycle == "" {
		return Result{}, fmt.Errorf("storage root and cycle are required")
	}
	directInputs := input.AggregateManifestPath != "" || input.ResolutionManifestPath != "" ||
		input.CandidateManifestPath != "" || input.CommitteeManifestPath != ""
	if input.ReadinessBundlePath != "" && directInputs {
		return Result{}, fmt.Errorf("resolved projection readiness bundle cannot be combined with direct manifest paths")
	}
	if input.ReadinessBundlePath == "" &&
		(input.AggregateManifestPath == "" || input.CandidateManifestPath == "" || input.CommitteeManifestPath == "") {
		return Result{}, fmt.Errorf("resolved projection readiness bundle or aggregate, candidate, and committee manifests are required")
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

	progress("validating immutable resolved outside-spending projection inputs")
	loaded, err := loadResolvedInputs(ctx, input)
	if err != nil {
		return Result{}, err
	}
	progress("building deterministic resolved independent-expenditure graph documents")
	model, err := buildResolvedProjection(ctx, input.StorageRoot, loaded)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	progress("ensuring isolated ArangoDB resolved probe database " + model.Database)
	if err := client.ensureDatabase(ctx, model.Database); err != nil {
		return Result{}, err
	}
	if err := ensureSchema(ctx, client, model.Database); err != nil {
		return Result{}, err
	}
	existing, complete, err := client.metadata(ctx, model.Database, model.ID)
	if err != nil {
		return Result{}, fmt.Errorf("read resolved projection metadata: %w", err)
	}
	if complete {
		if !reflect.DeepEqual(existing, model.Metadata) {
			return Result{}, fmt.Errorf("existing resolved projection metadata does not match projection %s", model.ID)
		}
		progress("reusing completed content-addressed resolved outside-spending projection")
	} else {
		progress("importing resolved candidate and spender entities")
		if err := importBatches(ctx, client, model.Database, entitiesCollection, model.Entities, input.BatchSize); err != nil {
			return Result{}, err
		}
		progress("importing resolved support and opposition edges")
		if err := importBatches(ctx, client, model.Database, edgesCollection, model.Edges, input.BatchSize); err != nil {
			return Result{}, err
		}
	}

	observedCounts, err := readCounts(ctx, client, model.Database)
	if err != nil {
		return Result{}, err
	}
	if observedCounts != model.Counts {
		return Result{}, fmt.Errorf("ArangoDB resolved projection counts differ: got %+v want %+v", observedCounts, model.Counts)
	}
	observedAmounts, err := readAmounts(ctx, client, model.Database)
	if err != nil {
		return Result{}, err
	}
	if observedAmounts != model.Amounts {
		return Result{}, fmt.Errorf("ArangoDB resolved projection amounts differ: got %+v want %+v", observedAmounts, model.Amounts)
	}
	if !complete {
		if _, err := client.importDocuments(ctx, model.Database, metadataCollection, []projectionMetadata{model.Metadata}); err != nil {
			return Result{}, fmt.Errorf("publish resolved projection metadata: %w", err)
		}
	}
	storage, err := readStorageMetrics(ctx, client, model.Database, observedCounts)
	if err != nil {
		return Result{}, err
	}
	if len(storage.Collections) != 3 || storage.CombinedBytes == 0 {
		return Result{}, fmt.Errorf("ArangoDB returned incomplete resolved projection storage figures")
	}

	progress("measuring representative resolved outside-spending graph queries")
	queries, err := benchmarkQueries(ctx, client, model.Database, model.RepresentativeCandidate, model.RepresentativeSpender, input.QueryRepetitions)
	if err != nil {
		return Result{}, err
	}
	state := "ready"
	if model.Missing.Candidates != 0 || model.Missing.Spenders != 0 {
		state = "partial"
	}
	coverage := model.Coverage
	checks := []Check{
		{ID: "input_coherence", Passed: true, Severity: "block", Detail: "resolved calculation, candidate resolution, and master facts share one exact cycle and source release"},
		{ID: "resolution_lineage", Passed: true, Severity: "block", Detail: "the projection binds the exact resolved calculation, dense candidate decisions, and Schedule E ancestry"},
		{ID: "candidate_identity", Passed: model.Missing.Candidates == 0, Severity: "block", Detail: fmt.Sprintf("resolved candidate IDs missing same-release masters=%d", model.Missing.Candidates)},
		{ID: "unprojectable_coverage", Passed: coverage.SourceDecisions == coverage.ProjectableDecisions+coverage.UnprojectableDecisions, Severity: "block", Detail: fmt.Sprintf("kept %d candidate-unresolved decisions outside graph edges", coverage.UnprojectableDecisions)},
		{ID: "content_addressed_database", Passed: true, Severity: "block", Detail: "a new isolated database identity derives from the resolved model and all exact inputs"},
		{ID: "count_conservation", Passed: true, Severity: "block", Detail: "ArangoDB entity, edge, and stance counts equal the deterministic resolved model"},
		{ID: "signed_amount_conservation", Passed: true, Severity: "block", Detail: "exact signed edge cents read from ArangoDB equal the projectable resolved amount"},
		{ID: "master_fact_coverage", Passed: state == "ready", Severity: "warn", Detail: fmt.Sprintf("missing candidate masters=%d spender masters=%d", model.Missing.Candidates, model.Missing.Spenders)},
		{ID: "storage_figures", Passed: true, Severity: "block", Detail: "document and index figures were read for every resolved projection collection"},
		{ID: "query_execution", Passed: len(queries) == 3, Severity: "block", Detail: "candidate inbound paths, spender outbound paths, and stance summaries completed repeatedly"},
	}
	return Result{
		SchemaVersion: ResolvedResultSchemaVersion, ProjectionVersion: ResolvedProjectionVersion,
		ProjectionID: model.ID, State: state, Cycle: model.Cycle, Database: model.Database,
		Graph: GraphName, RunID: input.RunID, ObservedAt: options.Clock().UTC(), Inputs: model.Inputs,
		ExpectedCounts: model.Counts, ObservedCounts: observedCounts,
		ExpectedAmounts: model.Amounts, ObservedAmounts: observedAmounts,
		ReusedProjection: complete, MissingMasterFacts: model.Missing, Storage: storage,
		RepresentativeCandidate: model.RepresentativeCandidate, RepresentativeSpender: model.RepresentativeSpender,
		Queries: queries, Checks: checks, CandidateResolutionCoverage: &coverage,
	}, nil
}
