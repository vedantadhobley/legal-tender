package fundingwindow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
)

func testWindowConnections(t *testing.T, ctx context.Context, r, reopened *gen.WindowReader, first, second publication, root, spec, sha string) {
	t.Helper()
	query := gen.WindowConnectionQuery{PathQuery: gen.PathQuery{ReceiptOrdinal: 1, EntryFamily: "reported_receipt", Target: "H0CA00001", Ending: "candidate_authorization_context", MaxHops: 1, Limit: 10, Budget: 100},
		EntryGeneration: first.generation.GenerationID, Window: &gen.DateWindow{Start: "2022-12-31", End: "2023-01-01"}}
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		query.Ledger = ledger
		out, err := r.ConnectionPaths(ctx, query)
		check(t, err)
		if len(out.Paths) != 2 || len(out.Links) != 4 || out.Entry == nil || out.Entry.GenerationID != first.generation.GenerationID || out.Entry.Selection != "included" || out.FinancialEligibility || out.TerminalEligible {
			t.Fatal("lost receipt/cross-cycle/candidate evidence", out)
		}
		date := int32(time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / 86400)
		if out.Entry.Date == nil || *out.Entry.Date != date {
			t.Fatal("receipt date was not source date")
		}
		var source struct {
			FactSet string `json:"fact_set_id"`
			Source  struct {
				Ordinal uint64 `json:"source_row_ordinal"`
			} `json:"source"`
		}
		check(t, json.Unmarshal(out.Entry.Entry.Source, &source))
		if source.FactSet != first.generation.CommitteeFlow.Inputs.A.FactSetID || source.Source.Ordinal != 1 {
			t.Fatal("receipt routed through wrong source")
		}
		auth := []gen.WindowConnectionLink{}
		for _, link := range out.Links {
			if link.Topology.Family == "candidate_authorization_context" {
				auth = append(auth, link)
				if link.Date != nil || link.TemporalBasis != "source_publication_context_day_level_validity_unknown" {
					t.Fatal("invented authorization date")
				}
			}
		}
		if len(auth) != 2 || auth[0].Topology.Key != auth[1].Topology.Key || auth[0].GenerationID == auth[1].GenerationID || auth[0].ID == auth[1].ID || bytes.Equal(auth[0].Evidence.Evidence, auth[1].Evidence.Evidence) {
			t.Fatal("same endpoint authorization assertions were merged")
		}
		for _, c := range out.Contexts {
			if c.Links != 1 {
				t.Fatal("candidate context census mismatch")
			}
		}
		for _, path := range out.Paths {
			if len(path.Links) != 3 {
				t.Fatal("entry/committee/ending topology lost")
			}
		}
		again, err := reopened.ConnectionPaths(ctx, query)
		check(t, err)
		if !reflect.DeepEqual(out, again) {
			t.Fatal("connection input-order/fresh-open replay changed")
		}
	}
	query.Ledger = flow.ScheduleA
	t.Run("dated_receipt_entry_and_qualified_ordinal", func(t *testing.T) {
		q := query
		q.Window = &gen.DateWindow{Start: "2023-01-01", End: "2023-01-01"}
		out, err := r.ConnectionPaths(ctx, q)
		check(t, err)
		if len(out.Paths) != 0 || out.Entry.Selection != "before_window" || out.Search.State != "receipt_entry_excluded_by_date_window" || len(out.Entry.Entry.Source) == 0 {
			t.Fatal("excluded source evidence lost", out)
		}
		q.EntryGeneration, q.MaxHops = second.generation.GenerationID, 0
		out, err = r.ConnectionPaths(ctx, q)
		check(t, err)
		if len(out.Paths) != 2 || out.Entry.Selection != "included" || out.Entry.Entry.Link.To != "C00000003" {
			t.Fatal("same ordinal was not routed to second source")
		}
		q.EntryFamily = "conduit_association"
		out, err = r.ConnectionPaths(ctx, q)
		check(t, err)
		if len(out.Paths) != 0 || out.Entry.Entry.State != "no_qualified_conduit_association" || out.Search.State != "start_relationship_not_available" {
			t.Fatal("invented conduit association")
		}
		q.EntryGeneration = digest([]byte("foreign generation"))
		if out, err := r.ConnectionPaths(ctx, q); err == nil || out.ResultID != "" {
			t.Fatal("accepted foreign receipt scope")
		}
		q.EntryGeneration, q.ReceiptOrdinal = first.generation.GenerationID, 2
		if out, err := r.ConnectionPaths(ctx, q); err == nil || out.ResultID != "" {
			t.Fatal("accepted nonexistent receipt occurrence")
		}
	})
	t.Run("committee_start_and_receipt_to_committee", func(t *testing.T) {
		q := query
		q.EntryGeneration, q.ReceiptOrdinal, q.EntryFamily = "", 0, ""
		q.From, q.MaxHops = "C00000001", 2
		out, err := r.ConnectionPaths(ctx, q)
		check(t, err)
		if out.Entry != nil || len(out.Paths) != 2 || len(out.Paths[0].Links) != 3 {
			t.Fatal("committee-to-candidate route lost")
		}
		q = query
		q.Target, q.Ending = "C00000003", ""
		out, err = r.ConnectionPaths(ctx, q)
		check(t, err)
		if len(out.Paths) != 1 || len(out.Paths[0].Links) != 2 || len(out.Contexts) != 0 {
			t.Fatal("receipt-to-committee route lost")
		}
	})
	t.Run("connection_cli_replay", func(t *testing.T) {
		t.Setenv("LT_WINDOW_FIXTURE_PASSWORD", "fixture")
		args := []string{"pipeline", "fec", "inspect-funding-window-connections", "--inputs", spec, "--expected-inputs-sha256", sha, "--storage-root", root,
			"--endpoint", fixtureEndpoint, "--username", "fixture", "--password-env", "LT_WINDOW_FIXTURE_PASSWORD", "--receipt-generation", query.EntryGeneration, "--receipt-ordinal", "1", "--entry-family", query.EntryFamily, "--target", query.Target,
			"--ending-family", query.Ending, "--ledger", "schedule_a", "--start-date", query.Window.Start, "--end-date", query.Window.End, "--max-committee-hops", "1", "--max-paths", "10", "--max-expansions", "100"}
		want, err := r.ConnectionPaths(ctx, query)
		check(t, err)
		var out, diagnostic bytes.Buffer
		if code := cli.Run(args, &out, &diagnostic); code != 0 {
			t.Fatalf("connection CLI failed: %d %s", code, diagnostic.String())
		}
		var got gen.WindowConnectionsResult
		check(t, json.Unmarshal(out.Bytes(), &got))
		wantJSON, err := json.Marshal(want)
		check(t, err)
		gotJSON, err := json.Marshal(got)
		check(t, err)
		if !bytes.Equal(wantJSON, gotJSON) {
			t.Fatal("connection CLI differs from public Go reader")
		}
		prior := append([]byte(nil), out.Bytes()...)
		out.Reset()
		diagnostic.Reset()
		if code := cli.Run(append(args, "--expected-result-id", want.ResultID), &out, &diagnostic); code != 0 || !bytes.Equal(prior, out.Bytes()) {
			t.Fatalf("connection CLI replay failed: %d %s", code, diagnostic.String())
		}
		out.Reset()
		diagnostic.Reset()
		if code := cli.Run(append(args, "--expected-result-id", digest([]byte("wrong result"))), &out, &diagnostic); code == 0 || out.Len() != 0 {
			t.Fatal("connection CLI emitted success for wrong identity")
		}
	})
}
