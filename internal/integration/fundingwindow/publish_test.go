package fundingwindow_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	resolve "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	fc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	ie "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	conduits "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	outside "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// Only reachable in the disposable Compose network. Never accept an operator
// endpoint or credentials: failure injection must not target a retained graph.
const fixtureEndpoint = "http://window-arango:8529"

func executableSHA(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	check(t, err)
	b, err := os.ReadFile(path)
	check(t, err)
	return digest(b)
}

type publication struct {
	input      gen.WindowInput
	generation gen.Result
	a, b       string
}

func publishCycle(t *testing.T, ctx context.Context, root, cycle string, s fixtureSources) publication {
	t.Helper()
	fixtureBuild := executableSHA(t)
	t.Log("publishing synthetic source/fact/calculation/graph chain", cycle)
	o := publicationOptions(root)
	path := func(base, id string) string { return filepath.Join(root, base, "manifests", id+".json") }
	masters := map[classic.Dataset]string{}
	for _, dataset := range []classic.Dataset{classic.CandidateMaster, classic.CommitteeMaster, classic.CandidateCommitteeLinkage} {
		m, err := occ.PublishClassic(ctx, s.manifest, s.sha, string(dataset), cycle, "window-classic", o)
		check(t, err)
		om := path("evidence/fec/classic/"+string(dataset), m.OccurrenceSetID)
		f, err := occ.PublishClassicFacts(ctx, s.manifest, s.sha, om, "window-classic-facts", o)
		check(t, err)
		masters[dataset] = path("facts/fec/classic/"+string(dataset), f.FactSetID)
	}
	ao, err := occ.Publish(ctx, s.manifest, s.sha, cycle, "window-a-occurrences", o)
	check(t, err)
	af, err := occ.PublishScheduleAColumnarFacts(ctx, s.manifest, s.sha, path("evidence/fec/schedule-a", ao.OccurrenceSetID), "window-a-facts", o)
	check(t, err)
	a := path("facts/fec/schedule-a/columnar", af.FactSetID)
	bf, err := occ.PublishScheduleBColumnarFacts(ctx, s.manifest, s.sha, cycle, "window-b-facts", occ.ScheduleBColumnarOptions{StorageRoot: root, PGRestorePath: s.restores[cycle], Clock: o.Clock, RowsPerShard: 2, RowsPerRowGroup: 1, FreeFloorBytes: 1, WorkingMarginBytes: 1})
	check(t, err)
	b := path("facts/fec/schedule-b/columnar", bf.FactSetID)
	eo, err := occ.PublishScheduleEOccurrences(ctx, s.manifest, s.sha, cycle, "window-e-occurrences", o)
	check(t, err)
	ef, err := occ.PublishScheduleEFacts(ctx, s.manifest, s.sha, path("evidence/fec/schedule-e", eo.OccurrenceSetID), "window-e-facts", o)
	check(t, err)
	e := path("facts/fec/schedule-e", ef.FactSetID)
	calc, err := fc.Publish(ctx, fc.Options{StorageRoot: root, ScheduleA: a, ScheduleB: b, Release: s.path, Cycle: cycle, Workers: 1})
	check(t, err)
	if calc.A.Selected.Rows != 1 || calc.B.Selected.Rows != 1 {
		t.Fatalf("fixture failed A/B policy membership: A=%+v B=%+v", calc.A, calc.B)
	}
	flowBundle, err := fc.PublishBundle(ctx, fc.BundleOptions{StorageRoot: root, Calculation: path(fc.PublicationBase, calc.CalculationSetID), Committee: masters[classic.CommitteeMaster], Cycle: cycle})
	check(t, err)
	flowPath := path(fc.BundleBase, flowBundle.BundleID)
	_, err = flow.Run(ctx, flow.Options{StorageRoot: root, Bundle: flowPath, Cycle: cycle, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", BatchSize: 100})
	check(t, err)

	effective, err := ie.Publish(ctx, ie.PublishInput{ScheduleEFactManifestPath: e}, "window-effective", ie.PublishOptions{StorageRoot: root, Clock: o.Clock})
	check(t, err)
	resolution, err := resolve.Publish(ctx, resolve.PublishInput{EffectiveManifestPath: path("calculations/fec/effective-independent-expenditures", effective.CalculationSetID), CandidateManifestPath: masters[classic.CandidateMaster]}, "window-resolution", resolve.PublishOptions{StorageRoot: root, Clock: o.Clock})
	check(t, err)
	aggregate, err := resolve.PublishAggregate(ctx, resolve.AggregatePublishInput{CandidateResolutionManifestPath: path("calculations/fec/independent-expenditure-candidate-resolution", resolution.CalculationSetID)}, "window-aggregate", resolve.AggregatePublishOptions{StorageRoot: root, Clock: o.Clock})
	check(t, err)
	eBundle, err := resolve.PublishResolvedProjectionBundle(ctx, resolve.ResolvedProjectionBundleInput{AggregateManifestPath: path("calculations/fec/resolved-independent-expenditures", aggregate.CalculationSetID), CandidateManifestPath: masters[classic.CandidateMaster], CommitteeManifestPath: masters[classic.CommitteeMaster]}, "window-outside-bundle", resolve.ResolvedProjectionBundleOptions{StorageRoot: root, ExpectedCycle: cycle, Clock: o.Clock})
	check(t, err)
	ePath := path("bundles/fec/resolved-independent-expenditure-projection", eBundle.BundleID)
	_, err = outside.RunResolved(ctx, outside.ResolvedInput{StorageRoot: root, Cycle: cycle, ReadinessBundlePath: ePath, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", RunID: "window-outside", BatchSize: 100, QueryRepetitions: 1}, outside.Options{Clock: o.Clock})
	check(t, err)

	dir := filepath.Join(root, "fixture-publications", cycle)
	check(t, os.MkdirAll(dir, 0750))
	refDir, topologyDir, partDir, conduitDir := filepath.Join(dir, "references"), filepath.Join(dir, "topology"), filepath.Join(dir, "participants"), filepath.Join(dir, "conduits")
	reference, err := refs.Run(ctx, refs.Options{StorageRoot: root, Manifest: a, Cycle: cycle, OutputDirectory: refDir, BuildSHA256: fixtureBuild, RunRows: 100, FanIn: 2, FilterBytes: 1024, ScanWorkers: 1, Workers: 1, MaxWorkspaceBytes: 16 << 20})
	check(t, err)
	topology, err := refs.RunTopology(ctx, refs.TopologyOptions{ReferenceManifest: filepath.Join(refDir, "manifest.json"), ExpectedReferenceID: reference.CalculationID, OutputDirectory: topologyDir, BuildSHA256: fixtureBuild, RunRows: 100, FanIn: 2, MaxWorkspaceBytes: 16 << 20})
	check(t, err)
	p, err := participants.Run(ctx, participants.Options{StorageRoot: root, Manifest: a, Cycle: cycle, OutputDirectory: partDir, BuildSHA256: fixtureBuild, Workers: 1, MaxOutputBytes: 16 << 20})
	check(t, err)
	c, err := conduits.Run(ctx, conduits.Options{Participants: filepath.Join(partDir, "manifest.json"), ParticipantID: p.CalculationID, Topology: filepath.Join(topologyDir, "manifest.json"), TopologyID: topology.CalculationID, OutputDirectory: conduitDir, BuildSHA256: fixtureBuild, Workers: 1, RunRows: 100, FanIn: 2, MaxWorkspaceBytes: 16 << 20})
	check(t, err)
	ro := receipt.Options{StorageRoot: root, Participants: filepath.Join(partDir, "manifest.json"), ParticipantID: p.CalculationID, Conduits: filepath.Join(conduitDir, "manifest.json"), ConduitID: c.CalculationID,
		Facts: a, Committees: masters[classic.CommitteeMaster], Candidates: masters[classic.CandidateMaster], Linkages: masters[classic.CandidateCommitteeLinkage], Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", BuildSHA256: fixtureBuild,
		LockDirectory: filepath.Join(dir, "locks"), Workers: 1, BatchSize: 100, FullCycle: true, Layout: receipt.CompactLayout, PublicationDirectory: filepath.Join(dir, "graph"), ArangoDataDirectory: "/arango-data", ReserveFreeBytes: 1, MaxFilesystemGrowthBytes: 256 << 20, MaxEncodedBytes: 16 << 20}
	graph, err := receipt.Run(ctx, ro)
	check(t, err)
	manifest := graph.Publication.Manifest
	bytes, err := os.ReadFile(manifest)
	check(t, err)
	flowBytes, err := os.ReadFile(flowPath)
	check(t, err)
	eBytes, err := os.ReadFile(ePath)
	check(t, err)
	g, err := gen.Verify(ctx, gen.Options{Receipt: receipt.Options{StorageRoot: root, Participants: ro.Participants, ParticipantID: p.CalculationID, Conduits: ro.Conduits, ConduitID: c.CalculationID, Facts: a, Committees: ro.Committees, Candidates: ro.Candidates, Linkages: ro.Linkages, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture"}, ReceiptManifest: manifest, ReceiptSHA256: digest(bytes), FlowBundle: flowPath, FlowBundleSHA256: digest(flowBytes), OutsideBundle: ePath, OutsideBundleSHA256: digest(eBytes), BuildSHA256: fixtureBuild})
	check(t, err)
	gp := filepath.Join(dir, "generation.json")
	sha := writeJSON(t, gp, g)
	return publication{gen.WindowInput{Generation: gp, GenerationSHA256: sha, GraphManifest: manifest, Participants: ro.Participants, Conduits: ro.Conduits}, g, a, b}
}
