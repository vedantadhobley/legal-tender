package fundingwindow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func windowSharedInput(t *testing.T, base publication, out gen.SharedGenerationResult, calculation string) gen.WindowInput {
	t.Helper()
	b, err := json.Marshal(out.Generation)
	check(t, err)
	path := filepath.Join(filepath.Dir(calculation), "generation.json")
	check(t, os.WriteFile(path, b, 0600))
	i := base.input
	i.Generation, i.GenerationSHA256 = path, digest(b)
	i.Shared = &gen.SharedReadOptions{BaseGeneration: base.input.Generation, GraphManifest: out.Publication.Manifest, Conduits: calculation}
	return i
}

func publishWindowShared(t *testing.T, ctx context.Context, root string, base publication) (gen.SharedGenerationResult, string) {
	t.Helper()
	build := executableSHA(t)
	prior, err := c.Load(base.input.Conduits, base.generation.Receipts.Inputs.Conduits.ID)
	check(t, err)
	top := filepath.Join(filepath.Dir(filepath.Dir(base.input.Conduits)), "topology", "manifest.json")
	dir := filepath.Join(filepath.Dir(top), "..", "shared")
	v, err := c.Run(ctx, c.Options{Participants: base.input.Participants, ParticipantID: prior.ParticipantID, Topology: top, TopologyID: prior.TopologyID, GroupBaseline: base.input.Conduits, GroupBaselineID: prior.CalculationID, OutputDirectory: dir, BuildSHA256: build, Workers: 2, RunRows: 2, FanIn: 2, MaxWorkspaceBytes: 16 << 20})
	check(t, err)
	op := gen.ReadOptions{Generation: base.input.Generation, GenerationSHA256: base.input.GenerationSHA256, StorageRoot: root, GraphManifest: base.input.GraphManifest, Participants: base.input.Participants, Conduits: base.input.Conduits, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", BuildSHA256: build}
	r, err := gen.OpenReader(ctx, op)
	check(t, err)
	path := filepath.Join(dir, "manifest.json")
	out, err := r.PublishShared(ctx, receipt.SharedOptions{Calculation: path, CalculationID: v.CalculationID, BuildSHA256: build, PublicationDirectory: filepath.Join(root, "shared-graph"), LockDirectory: filepath.Join(root, "locks"), ArangoDataDirectory: "/arango-data", Workers: 2, BatchSize: 1, ReserveFreeBytes: 1, MaxFilesystemGrowthBytes: 256 << 20, MaxEncodedBytes: 16 << 20})
	check(t, err)
	return out, path
}

func testSharedWindow(t *testing.T, ctx context.Context, root string, sources fixtureSources, second publication, secondShared gen.SharedGenerationResult, calculation string) {
	t.Helper()
	first := publishCycle(t, ctx, root, "2022", sources)
	firstShared, firstCalculation := publishWindowShared(t, ctx, root, first)
	firstInput := windowSharedInput(t, first, firstShared, firstCalculation)
	secondInput := windowSharedInput(t, second, secondShared, calculation)
	o := gen.WindowOpenOptions{Inputs: []gen.WindowInput{firstInput, secondInput}, StorageRoot: root, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", BuildSHA256: executableSHA(t)}
	r, err := gen.OpenWindowReader(ctx, o)
	check(t, err)
	q := gen.WindowConnectionQuery{PathQuery: gen.PathQuery{ReceiptOrdinal: 2, EntryFamily: receipt.SharedFamily, Target: "H0CA00001", Ending: "candidate_authorization_context", MaxHops: 1, Limit: 10, Budget: 100, Ledger: flow.ScheduleA}, EntryGeneration: firstShared.Generation.GenerationID, Window: &gen.DateWindow{Start: "2022-12-31", End: "2023-01-01"}}
	var accepted gen.WindowConnectionsResult
	for _, side := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		q.Ledger = side
		out, err := r.ConnectionPaths(ctx, q)
		check(t, err)
		if len(out.Paths) != 2 || len(out.Links) != 4 || out.Entry.Selection != "included" || out.Entry.GenerationID != firstShared.Generation.GenerationID || out.FinancialEligibility || out.TerminalEligible {
			t.Fatal("shared date-window connection lost grain or source scope")
		}
		var e receipt.SharedEvidence
		check(t, json.Unmarshal(out.Entry.Entry.Item.Evidence, &e))
		if e.Decision.Ordinal != 2 || e.Decision.Related != 4 || e.OriginalDecision.State != "shared_related_record_unresolved" || e.Source.FactSetID != first.generation.Receipts.Inputs.Facts.ID || e.Related.FactSetID != e.Source.FactSetID {
			t.Fatal("shared source or old decision lost")
		}
		for _, link := range out.Links {
			if link.ID != link.GenerationID+":"+link.Topology.ID() {
				t.Fatal("outer generation qualification lost")
			}
			if link.Topology.Family == "receiver_reported_committee_observation" || link.Topology.Family == "sender_reported_committee_observation" {
				if link.GenerationID != secondShared.Generation.GenerationID {
					t.Fatal("cross-publication continuation routed to wrong graph")
				}
			}
		}
		for _, vertex := range out.Vertices {
			if vertex.Kind == "reported_contributor_appearance" {
				continue
			}
			if len(vertex.Facets) != 2 {
				t.Fatal("historical facets collapsed")
			}
			for _, facet := range vertex.Facets {
				if facet.SharedConduits == nil {
					t.Fatal("shared facet omitted")
				}
			}
		}
		accepted = out
	}
	q.Ledger = flow.ScheduleA
	for _, tc := range []struct {
		ordinal   uint64
		window    *gen.DateWindow
		selection string
		paths     bool
	}{
		{2, &gen.DateWindow{Start: "2023-01-01", End: "2023-01-01"}, "before_window", false},
		{2, &gen.DateWindow{Start: "2022-12-30", End: "2022-12-30"}, "after_window", false},
		{3, q.Window, "unknown_date_excluded", false},
		{3, nil, "undated_included", true},
	} {
		query := q
		query.ReceiptOrdinal, query.Window = tc.ordinal, tc.window
		out, err := r.ConnectionPaths(ctx, query)
		check(t, err)
		if out.Entry.Selection != tc.selection || (len(out.Paths) > 0) != tc.paths || out.Entry.Entry.Item == nil || len(out.Entry.Entry.Source) == 0 {
			t.Fatal("date disposition discarded or invented source evidence", tc.selection)
		}
	}
	// Outer identity selects the occurrence. The unchanged base ID is not an alias.
	wrong := q
	wrong.EntryGeneration = first.generation.GenerationID
	if _, err := r.ConnectionPaths(ctx, wrong); err == nil {
		t.Fatal("base ID aliased to outer generation")
	}
	old := q
	old.EntryFamily = "conduit_association"
	oldOut, err := r.ConnectionPaths(ctx, old)
	check(t, err)
	if oldOut.Entry.Entry.State != "no_qualified_conduit_association" || len(oldOut.Paths) != 0 {
		t.Fatal("old family gained shared links")
	}
	// Identical ordinals in different sources retain their own dates.
	other := q
	other.EntryGeneration, other.MaxHops = secondShared.Generation.GenerationID, 0
	other.Target, other.Ending = "C00000002", ""
	other.Window = &gen.DateWindow{Start: "2023-01-01", End: "2023-01-01"}
	otherOut, err := r.ConnectionPaths(ctx, other)
	check(t, err)
	if len(otherOut.Paths) != 1 || otherOut.Entry.Selection != "included" {
		t.Fatal("ordinal date came from another publication")
	}
	// Spending keeps its own native dates and source-member contract.
	for _, ending := range []string{"independent_support", "independent_opposition"} {
		spending := q
		spending.Ending, spending.SpendingDate = ending, "expenditure"
		out, err := r.ConnectionPaths(ctx, spending)
		check(t, err)
		if len(out.Paths) == 0 || len(out.SpendingCoverage) != 2 || out.SchemaVersion != gen.WindowSpendingConnectionsVersion {
			t.Fatal("shared entry broke source-grain spending")
		}
	}
	// Base and extension of one source cycle must fail admission before any DB call.
	bad := o
	bad.Endpoint = "http://127.0.0.1:1"
	bad.Inputs = []gen.WindowInput{first.input, firstInput}
	if _, err := gen.OpenWindowReader(ctx, bad); err == nil || !strings.Contains(err.Error(), "duplicate, overlapping") {
		t.Fatal("overlapping base/extension accepted")
	}
	bad = o
	broken := *firstInput.Shared
	broken.Conduits = first.input.Conduits
	bad.Inputs = []gen.WindowInput{firstInput}
	bad.Inputs[0].Shared = &broken
	if _, err := gen.OpenWindowReader(ctx, bad); err == nil {
		t.Fatal("foreign shared calculation accepted")
	}
	// Canonical order and owned nested extension metadata.
	o.Inputs = []gen.WindowInput{secondInput, firstInput}
	reopened, err := gen.OpenWindowReader(ctx, o)
	check(t, err)
	q.Ledger = flow.ScheduleB
	again, err := reopened.ConnectionPaths(ctx, q)
	check(t, err)
	if !reflect.DeepEqual(accepted, again) {
		t.Fatal("reversed-input fresh replay changed")
	}
	accepted.Inputs[0].Shared.Extension.Counts["reported_conduit_associations"] = 999
	clean, err := r.ConnectionPaths(ctx, q)
	check(t, err)
	if !reflect.DeepEqual(clean, again) {
		t.Fatal("output mutation changed reader metadata")
	}

	spec := gen.WindowInputSpec{Version: gen.WindowSharedInputsVersion, Inputs: o.Inputs}
	b, err := json.Marshal(spec)
	check(t, err)
	specPath := filepath.Join(root, "shared-window-inputs.json")
	check(t, os.WriteFile(specPath, b, 0600))
	args := []string{"pipeline", "fec", "inspect-funding-window-connections", "--inputs", specPath, "--expected-inputs-sha256", digest(b), "--storage-root", root, "--endpoint", fixtureEndpoint, "--username", "fixture", "--password-env", "LT_SHARED_FIXTURE_PASSWORD", "--receipt-generation", q.EntryGeneration, "--receipt-ordinal", strconv.FormatUint(q.ReceiptOrdinal, 10), "--entry-family", receipt.SharedFamily, "--ledger", string(q.Ledger), "--target", q.Target, "--ending-family", q.Ending, "--start-date", q.Window.Start, "--end-date", q.Window.End, "--max-committee-hops", "1", "--max-paths", "10", "--max-expansions", "100", "--expected-result-id", again.ResultID}
	var stdout, stderr bytes.Buffer
	if cli.Run(args, &stdout, &stderr) != 0 {
		t.Fatal(stderr.String())
	}
	var actual gen.WindowConnectionsResult
	check(t, json.Unmarshal(stdout.Bytes(), &actual))
	got, _ := json.Marshal(actual)
	want, _ := json.Marshal(again)
	if !bytes.Equal(got, want) {
		t.Fatal("shared window CLI differs")
	}
	// Even excluded entries must fail on damaged evidence, not return an empty success.
	api := "/_db/" + firstShared.Generation.Extension.Database + "/_api/document/reported_conduit_associations/" + again.Entry.Entry.Link.Key
	original := request(t, ctx, http.MethodGet, api, nil)
	request(t, ctx, http.MethodPatch, api, []byte(`{"additional_amount_minor_units":"1"}`))
	q.Window = &gen.DateWindow{Start: "2023-01-02", End: "2023-01-02"}
	if _, err := r.ConnectionPaths(ctx, q); err == nil {
		t.Fatal("excluded corrupt shared edge accepted")
	}
	request(t, ctx, http.MethodPut, api, original)
	_, err = r.ConnectionPaths(ctx, q)
	check(t, err)
}
