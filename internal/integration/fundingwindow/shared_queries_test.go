package fundingwindow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func testSharedQueries(t *testing.T, ctx context.Context, op gen.ReadOptions, publication gen.SharedGenerationResult, calculation string) {
	t.Helper()
	base, err := gen.OpenReader(ctx, op)
	check(t, err)
	shared := gen.SharedReadOptions{BaseGeneration: op.Generation, GraphManifest: publication.Publication.Manifest, Conduits: calculation}
	if _, err := gen.OpenQueryReader(ctx, op, shared); err == nil {
		t.Fatal("base reader ignored shared locators")
	}
	b, err := json.Marshal(publication.Generation)
	check(t, err)
	op.Generation = filepath.Join(t.TempDir(), "shared-generation.json")
	check(t, os.WriteFile(op.Generation, b, 0600))
	op.GenerationSHA256 = digest(b)
	if _, err := gen.OpenReader(ctx, op); err == nil {
		t.Fatal("unwired consumer accepted extended generation")
	}
	if _, err := gen.OpenQueryReader(ctx, op, gen.SharedReadOptions{}); err == nil {
		t.Fatal("missing locators accepted")
	}
	bad := shared
	bad.Conduits = op.Conduits
	if _, err := gen.OpenQueryReader(ctx, op, bad); err == nil {
		t.Fatal("old calculation accepted as extension")
	}
	r, err := gen.OpenQueryReader(ctx, op, shared)
	check(t, err)
	q := gen.Query{Entity: "C00000002", Family: receipt.SharedFamily, Limit: 1}
	if _, err := base.Neighborhood(ctx, q); err == nil {
		t.Fatal("base generation claimed shared family")
	}
	n, err := r.Neighborhood(ctx, q)
	check(t, err)
	page := n.Pages[len(n.Pages)-1]
	if len(n.Pages) != len(publication.Generation.Base.Families)+1 || len(page.Items) != 1 || !page.HasMore || page.NextCursor == "" || n.GenerationID != publication.Generation.GenerationID || n.GenerationSHA256 != op.GenerationSHA256 || page.Family.Database != publication.Generation.Extension.Database || n.FinancialEligibility || n.TerminalEligible {
		t.Fatal("wrong shared page, scope or money semantics")
	}
	for _, prior := range publication.Generation.Base.Families {
		if prior.Kind == "conduit_association" && (prior.OverlapGroup != page.Family.OverlapGroup || prior.AmountMeaning != page.Family.AmountMeaning || prior.Ledger != page.Family.Ledger || prior.Grain != page.Family.Grain) {
			t.Fatal("shared family changed the common receipt evidence vocabulary")
		}
	}
	var evidence receipt.SharedEvidence
	check(t, json.Unmarshal(page.Items[0].Evidence, &evidence))
	if evidence.OriginalDecision.State != "shared_related_record_unresolved" || evidence.OriginalDecision.ConduitID != nil || evidence.Decision.State != "reported_shared_earmark_memo_association" || evidence.Decision.Ordinal != evidence.OriginalDecision.Ordinal || evidence.Decision.Related != evidence.Group.Related || evidence.Group.SharedRows != 2 || evidence.Calculation != publication.Generation.Extension.Calculation || evidence.BaseProjection != publication.Generation.Extension.Base || len(page.Items[0].Evidence) < 1000 {
		t.Fatal("source/group/prior-decision evidence lost")
	}
	q.Cursor = page.NextCursor
	next, err := r.Neighborhood(ctx, q)
	check(t, err)
	np := next.Pages[len(next.Pages)-1]
	if np.HasMore || len(np.Items) != 1 || np.Items[0].Key <= page.Items[0].Key {
		t.Fatal("shared continuation skipped or repeated a row")
	}
	for _, invalid := range []gen.Query{
		{Entity: "C00000003", Family: receipt.SharedFamily, Limit: 1, Cursor: q.Cursor},
		{Entity: q.Entity, Family: "conduit_association", Limit: 1, Cursor: q.Cursor},
	} {
		if _, err := r.Neighborhood(ctx, invalid); err == nil {
			t.Fatal("foreign cursor accepted")
		}
	}
	pq := gen.PathQuery{ReceiptOrdinal: evidence.Decision.Ordinal, EntryFamily: receipt.SharedFamily, Ledger: flow.ScheduleA, Target: q.Entity, MaxHops: 0, Limit: 1, Budget: 100}
	if _, err := base.Paths(ctx, pq); err == nil {
		t.Fatal("base generation claimed shared entry")
	}
	path, err := r.Paths(ctx, pq)
	check(t, err)
	if len(path.Paths) != 1 || len(path.Links) != 1 || path.Links[0].Family.Database != publication.Generation.Extension.Database || !reflect.DeepEqual(path.Entry.Item, &page.Items[0]) {
		t.Fatal("shared path and neighborhood evidence diverged")
	}
	// The old family stays unavailable for this row; the receipt stays unchanged.
	pq.EntryFamily = "conduit_association"
	old, err := r.Paths(ctx, pq)
	check(t, err)
	if old.Entry.State != "no_qualified_conduit_association" || len(old.Paths) != 0 {
		t.Fatal("old decision overwritten")
	}
	pq.EntryFamily, pq.Target = "reported_receipt", "C00000003"
	reported, err := r.Paths(ctx, pq)
	check(t, err)
	oldReported, err := base.Paths(ctx, pq)
	check(t, err)
	if !reflect.DeepEqual(reported.Entry, oldReported.Entry) {
		t.Fatal("original receipt changed")
	}
	// A real committee hop after the shared entry, on each separate ledger.
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		pq.EntryFamily, pq.Ledger, pq.MaxHops = receipt.SharedFamily, ledger, 2
		connected, err := r.Paths(ctx, pq)
		check(t, err)
		if len(connected.Paths) == 0 || len(connected.Links) < 2 {
			t.Fatal("shared entry did not compose with committee ledger", ledger)
		}
	}
	for _, ending := range []string{"candidate_authorization_context", "independent_support", "independent_opposition"} {
		pq.Target, pq.Ending, pq.Ledger = "H0CA00001", ending, flow.ScheduleA
		connected, err := r.Paths(ctx, pq)
		check(t, err)
		if len(connected.Paths) == 0 || connected.Links[0].Family.Kind != receipt.SharedFamily || connected.Links[len(connected.Links)-1].Family.Kind != ending {
			t.Fatal("shared entry did not compose with explicit candidate ending", ending)
		}
	}
	// Fresh reader yields the same typed response.
	again, err := gen.OpenQueryReader(ctx, op, shared)
	check(t, err)
	q.Cursor = ""
	replay, err := again.Neighborhood(ctx, q)
	check(t, err)
	if !reflect.DeepEqual(n, replay) {
		t.Fatal("fresh query replay changed")
	}
	args := []string{"pipeline", "fec", "inspect-funding-neighborhood", "--generation", op.Generation, "--expected-generation-sha256", op.GenerationSHA256, "--storage-root", op.StorageRoot, "--graph-manifest", op.GraphManifest, "--participants", op.Participants, "--conduits", op.Conduits, "--base-generation", shared.BaseGeneration, "--shared-graph-manifest", shared.GraphManifest, "--shared-conduits", shared.Conduits, "--endpoint", fixtureEndpoint, "--username", "fixture", "--password-env", "LT_SHARED_FIXTURE_PASSWORD", "--entity", q.Entity, "--family", q.Family, "--limit", "1"}
	var stdout, stderr bytes.Buffer
	if cli.Run(args, &stdout, &stderr) != 0 {
		t.Fatal(stderr.String())
	}
	var actual gen.Neighborhood
	check(t, json.Unmarshal(stdout.Bytes(), &actual))
	wantJSON, err := json.Marshal(n)
	check(t, err)
	actualJSON, err := json.Marshal(actual)
	check(t, err)
	if !bytes.Equal(actualJSON, wantJSON) {
		t.Fatal("shared CLI response differs")
	}
	// A corrupted lookahead must fail even though it would not be returned.
	api := "/_db/" + publication.Generation.Extension.Database + "/_api/document/reported_conduit_associations/" + np.Items[0].Key
	original := request(t, ctx, http.MethodGet, api, nil)
	request(t, ctx, http.MethodPatch, api, []byte(`{"additional_amount_minor_units":"1"}`))
	if _, err := r.Neighborhood(ctx, q); err == nil {
		t.Fatal("corrupt shared lookahead accepted")
	}
	request(t, ctx, http.MethodPut, api, original)
	// Corruption of the retained old disposition cannot be hidden by the overlay.
	api = "/_db/" + publication.Generation.Base.Receipts.Database + "/_api/document/contributor_appearances/" + page.Items[0].Key
	original = request(t, ctx, http.MethodGet, api, nil)
	request(t, ctx, http.MethodPatch, api, []byte(`{"identity_resolved":true}`))
	if _, err := r.Neighborhood(ctx, q); err == nil {
		t.Fatal("corrupt original disposition accepted")
	}
	request(t, ctx, http.MethodPut, api, original)
	_, err = r.Neighborhood(ctx, q)
	check(t, err)
	gate, err := r.ValidateSharedQueries(ctx, "", nil)
	check(t, err)
	if gate.State != "verified_selected_shared_queries" || gate.CommitteePathState != "verified" || gate.Continuation == nil {
		t.Fatal("shared automatic gate omitted fixture coverage")
	}
	_, err = again.ValidateSharedQueries(ctx, gate.GateID, nil)
	check(t, err)
	if _, err := r.ValidateSharedQueries(ctx, digest([]byte("wrong gate")), nil); err == nil {
		t.Fatal("foreign gate identity accepted")
	}
}
