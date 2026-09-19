package fundingwindow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
)

func TestSyntheticSourceContract(t *testing.T) { makeSources(t, t.TempDir()) }

// This fixture exercises public publishers/readers against a disposable real
// ArangoDB. No windowFlowSource stub, resealed derived manifest or bypass hook.
func TestTwoPublicationLoader(t *testing.T) {
	if os.Getenv("LT_WINDOW_INTEGRATION") != "1" {
		t.Skip("run make test-window-integration for the isolated Arango fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	root := t.TempDir()
	fixtureBuild := executableSHA(t)
	s := makeSources(t, root)
	first := publishCycle(t, ctx, root, "2022", s)
	second := publishCycle(t, ctx, root, "2024", s)
	inputs := []gen.WindowInput{first.input, second.input}
	spec := filepath.Join(root, "window-inputs.json")
	sha := writeJSON(t, spec, gen.WindowInputSpec{Version: gen.WindowInputsVersion, Inputs: inputs})
	decoded, err := gen.ReadWindowInputs(spec, sha)
	check(t, err)
	o := gen.WindowOpenOptions{Inputs: decoded.Inputs, StorageRoot: root, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", BuildSHA256: fixtureBuild}
	r, err := gen.OpenWindowReader(ctx, o)
	check(t, err)
	query := gen.WindowPathQuery{From: "C00000001", Target: "C00000003", MaxHops: 2, Limit: 3, Budget: 100, Window: &gen.DateWindow{Start: "2022-12-31", End: "2023-01-01"}}
	witnesses := map[flow.Ledger]gen.WindowPathsResult{}
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		query.Ledger = ledger
		out, err := r.Paths(ctx, query)
		check(t, err)
		if len(out.Inputs) != 2 || len(out.Paths) != 1 || len(out.Links) != 2 || out.FinancialEligibility || out.TerminalEligible {
			t.Fatalf("lost cross-cycle route or promoted money: %+v", out)
		}
		origins := map[string]bool{}
		for _, link := range out.Links {
			origins[link.GenerationID] = true
			var evidence struct {
				Document struct {
					Fact    string `json:"fact_set_id"`
					Ordinal uint64 `json:"source_row_ordinal"`
					Date    *int32 `json:"date_days"`
				}
			}
			check(t, json.Unmarshal(link.Evidence.Document, &evidence.Document))
			fact := first.generation.CommitteeFlow.Inputs.A.FactSetID
			if link.GenerationID == second.generation.GenerationID {
				fact = second.generation.CommitteeFlow.Inputs.A.FactSetID
			}
			if ledger == flow.ScheduleB {
				fact = first.generation.CommitteeFlow.Inputs.B.FactSetID
				if link.GenerationID == second.generation.GenerationID {
					fact = second.generation.CommitteeFlow.Inputs.B.FactSetID
				}
			}
			if evidence.Document.Fact != fact || evidence.Document.Ordinal != 1 || !reflect.DeepEqual(evidence.Document.Date, link.Date) || len(link.Evidence.Evidence) == 0 {
				t.Fatal("cross-publication source routing lost")
			}
		}
		if !origins[first.generation.GenerationID] || !origins[second.generation.GenerationID] {
			t.Fatal("path does not span both actual publications")
		}
		for _, c := range out.Coverage {
			if c.Rows != 1 || c.Included != 1 || c.UnknownExcluded != 0 {
				t.Fatal("source-date census mismatch", c)
			}
		}
		for _, v := range out.Vertices {
			if v.ID == "C00000002" && (len(v.Facets) != 2 || bytes.Equal(v.Facets[0].Facet.Document, v.Facets[1].Facet.Document)) {
				t.Fatal("historical facets collapsed")
			}
		}
		witnesses[ledger] = out
	}
	// Fresh public open, reversed input order: all sources/graphs reverified.
	o.Inputs = []gen.WindowInput{second.input, first.input}
	reopened, err := gen.OpenWindowReader(ctx, o)
	check(t, err)
	for ledger, want := range witnesses {
		query.Ledger = ledger
		got, err := reopened.Paths(ctx, query)
		check(t, err)
		if !reflect.DeepEqual(got, want) {
			t.Fatal("fresh reverse-input replay changed result", ledger)
		}
	}
	// Neither partition alone has the route, even though all endpoint masters exist.
	for _, input := range inputs {
		o.Inputs = []gen.WindowInput{input}
		one, err := gen.OpenWindowReader(ctx, o)
		check(t, err)
		query.Ledger = flow.ScheduleA
		out, err := one.Paths(ctx, query)
		check(t, err)
		if len(out.Paths) != 0 {
			t.Fatal("cross-cycle witness already exists in one partition")
		}
	}
	query.Window = &gen.DateWindow{Start: "2023-01-01", End: "2023-01-01"}
	narrow, err := r.Paths(ctx, query)
	check(t, err)
	if len(narrow.Paths) != 0 {
		t.Fatal("date window ignored")
	}
	query.Window = witnesses[flow.ScheduleA].Query.Window
	t.Run("receipt_candidate_window_connections", func(t *testing.T) {
		testWindowConnections(t, ctx, r, reopened, first, second, root, spec, sha)
	})
	t.Run("source_grain_spending_windows", func(t *testing.T) {
		testSpendingWindows(t, ctx, r, reopened, first, second, root, spec, sha)
	})

	// Corrupt only disposable fixture backing, restore it after each check.
	o.Inputs = inputs
	t.Run("cli_and_expected_replay", func(t *testing.T) {
		t.Setenv("LT_WINDOW_FIXTURE_PASSWORD", "fixture")
		args := []string{"pipeline", "fec", "inspect-funding-window-paths", "--inputs", spec, "--expected-inputs-sha256", sha, "--storage-root", root,
			"--endpoint", fixtureEndpoint, "--username", "fixture", "--password-env", "LT_WINDOW_FIXTURE_PASSWORD", "--from-committee", query.From, "--target", query.Target,
			"--ledger", "schedule_a", "--start-date", query.Window.Start, "--end-date", query.Window.End, "--max-committee-hops", "2", "--max-paths", "3", "--max-expansions", "100"}
		var out, diagnostic bytes.Buffer
		if code := cli.Run(args, &out, &diagnostic); code != 0 {
			t.Fatalf("CLI failed: %d %s", code, diagnostic.String())
		}
		var actual gen.WindowPathsResult
		check(t, json.Unmarshal(out.Bytes(), &actual))
		// JSON indentation changes RawMessage byte slices during decoding, but
		// must not change the canonical serialized result or its identity.
		actualJSON, err := json.Marshal(actual)
		check(t, err)
		expectedJSON, err := json.Marshal(witnesses[flow.ScheduleA])
		check(t, err)
		if !bytes.Equal(actualJSON, expectedJSON) {
			t.Fatal("CLI differs from public Go reader")
		}
		prior := append([]byte(nil), out.Bytes()...)
		out.Reset()
		diagnostic.Reset()
		if code := cli.Run(append(args, "--expected-result-id", actual.ResultID), &out, &diagnostic); code != 0 || !bytes.Equal(prior, out.Bytes()) {
			t.Fatalf("CLI replay failed: %d %s", code, diagnostic.String())
		}
		out.Reset()
		diagnostic.Reset()
		if code := cli.Run(append(args, "--expected-result-id", digest([]byte("wrong result"))), &out, &diagnostic); code == 0 || out.Len() != 0 {
			t.Fatal("CLI emitted successful result for wrong expected identity")
		}
	})
	t.Run("foreign_receipt_locator", func(t *testing.T) {
		bad := o
		bad.Inputs = append([]gen.WindowInput(nil), inputs...)
		bad.Inputs[1].GraphManifest = inputs[0].GraphManifest
		if got, err := gen.OpenWindowReader(ctx, bad); err == nil || got != nil {
			t.Fatal("accepted foreign second receipt manifest")
		}
	})
	t.Run("second_source_corruption", func(t *testing.T) {
		for _, a := range s.manifest.Artifacts {
			if a.SourceID == "fec:cm:2024" {
				p := filepath.Join(root, a.StorageKey)
				prior, err := os.ReadFile(p)
				check(t, err)
				check(t, os.WriteFile(p, []byte("corrupt fixture archive"), 0600))
				defer func() { check(t, os.WriteFile(p, prior, 0600)) }()
				if got, err := gen.OpenWindowReader(ctx, o); err == nil || got != nil {
					t.Fatal("accepted corrupted second publication source")
				}
				return
			}
		}
		t.Fatal("source witness absent")
	})
	var db string
	for _, f := range second.generation.Families {
		if f.Kind == "receiver_reported_committee_observation" {
			db = f.Database
		}
	}
	if db == "" {
		t.Fatal("fixture graph absent")
	}
	t.Run("second_graph_corruption", func(t *testing.T) {
		var key string
		for _, link := range witnesses[flow.ScheduleA].Links {
			if link.GenerationID == second.generation.GenerationID {
				key = link.Topology.Key
			}
		}
		api := "/_db/" + db + "/_api/document/receiver_reported_observations/" + key
		prior := request(t, ctx, "GET", api, nil)
		request(t, ctx, "PATCH", api, []byte(`{"signed_amount_minor_units":"999"}`))
		defer request(t, ctx, "PUT", api, prior)
		if got, err := gen.OpenWindowReader(ctx, o); err == nil || got != nil {
			t.Fatal("accepted corrupted second graph")
		}
	})
	t.Run("second_completion_changes_after_open", func(t *testing.T) {
		api := "/_db/" + db + "/_api/document/projection_metadata/" + second.generation.CommitteeFlow.ProjectionID
		prior := request(t, ctx, "GET", api, nil)
		request(t, ctx, "PATCH", api, []byte(`{"state":"fixture-invalid"}`))
		defer request(t, ctx, "PUT", api, prior)
		if out, err := r.Paths(ctx, query); err == nil || out.ResultID != "" {
			t.Fatal("accepted changed second completion")
		}
		connection := gen.WindowConnectionQuery{PathQuery: gen.PathQuery{From: query.From, Target: "H0CA00001", Ending: "candidate_authorization_context", Ledger: flow.ScheduleA, MaxHops: 2, Limit: 3, Budget: 100}, Window: query.Window}
		if out, err := r.ConnectionPaths(ctx, connection); err == nil || out.ResultID != "" {
			t.Fatal("connection accepted changed second completion")
		}
		if got, err := gen.OpenWindowReader(ctx, o); err == nil || got != nil {
			t.Fatal("opened incomplete second graph")
		}
	})
	// Restored fixture must pass again; failure probes cannot bless broken backing.
	_, err = gen.OpenWindowReader(ctx, o)
	check(t, err)
}

func request(t *testing.T, ctx context.Context, method, api string, body []byte) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, fixtureEndpoint+api, bytes.NewReader(body))
	check(t, err)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	check(t, err)
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		t.Fatalf("fixture Arango %s: HTTP %d", method, res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	check(t, err)
	return b
}
