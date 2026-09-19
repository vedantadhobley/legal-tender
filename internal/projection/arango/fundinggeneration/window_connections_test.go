package fundinggeneration

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

type windowReceiptStub struct {
	entry receipt.DatedPathEntry
	links []graphread.Link
	name  string
	err   error
}

func (s *windowReceiptStub) DatedPathEntry(_ context.Context, family string, ordinal uint64) (receipt.DatedPathEntry, error) {
	if s.err != nil {
		return receipt.DatedPathEntry{}, s.err
	}
	if ordinal != 1 {
		return receipt.DatedPathEntry{}, errors.New("fixture occurrence absent")
	}
	b, _ := json.Marshal(s.entry)
	var out receipt.DatedPathEntry
	_ = json.Unmarshal(b, &out)
	if out.Entry.Link != nil {
		out.Entry.Link.Family = family
	}
	return out, nil
}
func (s *windowReceiptStub) AuthorizedLinks() []graphread.Link { return s.links }
func (s *windowReceiptStub) AuthorizationEvidence(_ context.Context, key string) (graphread.Item, error) {
	b, _ := json.Marshal(s.name)
	return graphread.Item{Key: key, Evidence: b, Document: b}, s.err
}
func (s *windowReceiptStub) Entity(_ context.Context, _ string) (graphread.Facet, error) {
	b, _ := json.Marshal(s.name)
	return graphread.Facet{State: "present", Document: b}, s.err
}

func connectionFixtureReader(t *testing.T) (*WindowReader, WindowConnectionQuery) {
	t.Helper()
	r := windowFixture(t, flow.ScheduleA)
	day, _, err := (DateWindow{"2022-12-30", "2022-12-30"}).bounds()
	if err != nil {
		t.Fatal(err)
	}
	for i := range r.partitions {
		p := &r.partitions[i]
		key := valueID("appearance-" + p.publication.GenerationID)
		p.receipts = &windowReceiptStub{name: p.publication.Generation.Cycle,
			entry: receipt.DatedPathEntry{Date: &day, Entry: receipt.PathEntry{State: "available", Link: &graphread.Link{Family: "reported_receipt", Key: key, From: key, To: "C00000001"}, Item: &graphread.Item{Key: key, Evidence: json.RawMessage(`{"fixture":true}`)}, Source: json.RawMessage(`{"fixture":true}`)}},
			links: []graphread.Link{{Family: "candidate_authorization_context", Key: valueID("same-endpoint-pair"), From: "C00000003", To: "H0CA00001"}},
		}
	}
	q := WindowConnectionQuery{PathQuery: PathQuery{ReceiptOrdinal: 1, EntryFamily: "reported_receipt", Ledger: flow.ScheduleA, Target: "H0CA00001", Ending: "candidate_authorization_context", MaxHops: 2, Limit: 10, Budget: 100}, EntryGeneration: r.partitions[0].publication.GenerationID, Window: &DateWindow{"2022-12-30", "2023-01-02"}}
	return r, q
}

func TestWindowConnectionQualificationReplayAndOwnership(t *testing.T) {
	r, q := connectionFixtureReader(t)
	out, err := r.ConnectionPaths(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Paths) != 4 || out.Entry.Selection != "included" || len(out.Contexts) != 2 || out.FinancialEligibility || out.TerminalEligible {
		t.Fatal("lost evidence or promoted money", out)
	}
	for _, p := range out.Paths {
		if len(p.Links) != 4 {
			t.Fatal("lost entry/chain/ending")
		}
	}
	for _, p := range r.partitions {
		seenKeys := map[string]bool{}
		for _, key := range p.source.(*windowStub).evidenceKeys {
			if seenKeys[key] {
				t.Fatal("source occurrence reread for each authorization variant")
			}
			seenKeys[key] = true
		}
	}
	seen := map[string]bool{}
	for _, link := range out.Links {
		if seen[link.ID] || link.ID != connectionLinkID(link.GenerationID, link.Topology) {
			t.Fatal("link not publication-qualified")
		}
		seen[link.ID] = true
		if link.Topology.Family == "candidate_authorization_context" && (link.Date != nil || link.TemporalBasis != authorizationTimeBasis) {
			t.Fatal("invented authorization date")
		}
	}
	again, err := r.ConnectionPaths(context.Background(), q)
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("unstable replay", err)
	}
	out.Inputs[0].Generation.Receipts.Counts["fixture"] = 999
	*out.Entry.Date = 0
	out.Entry.Entry.Link.To = "C99999999"
	q.Window.Start = "2023-01-01"
	if again.Query.Window.Start != "2022-12-30" || out.Query.Window.Start != "2022-12-30" || r.partitions[0].publication.Generation.Receipts.Counts["fixture"] != 1 {
		t.Fatal("result aliases query or source")
	}
	q.Window.Start = "2022-12-30"
	fresh, err := r.ConnectionPaths(context.Background(), q)
	if err != nil || !reflect.DeepEqual(fresh, again) {
		t.Fatal("returned entry mutates future results", err)
	}
}

func TestWindowConnectionDateAndUnavailableStates(t *testing.T) {
	for _, family := range []string{"reported_receipt", "conduit_association"} {
		for _, tc := range []struct {
			date      *int32
			window    *DateWindow
			selection string
			paths     bool
		}{
			{nil, nil, "undated_included", true},
			{nil, &DateWindow{"2022-12-30", "2023-01-02"}, "unknown_date_excluded", false},
			{dayPtr(19000), &DateWindow{"2022-12-30", "2023-01-02"}, "before_window", false},
			{dayPtr(20000), &DateWindow{"2022-12-30", "2023-01-02"}, "after_window", false},
		} {
			r, q := connectionFixtureReader(t)
			q.EntryFamily, q.Window = family, tc.window
			r.partitions[0].receipts.(*windowReceiptStub).entry.Date = tc.date
			out, err := r.ConnectionPaths(context.Background(), q)
			if err != nil || out.Entry.Selection != tc.selection || (len(out.Paths) > 0) != tc.paths || len(out.Entry.Entry.Source) == 0 {
				t.Fatal("date disposition/evidence changed", tc, err)
			}
			if !tc.paths && out.Search.State != "receipt_entry_excluded_by_date_window" {
				t.Fatal(out.Search)
			}
		}
	}
	r, q := connectionFixtureReader(t)
	s := r.partitions[0].receipts.(*windowReceiptStub)
	s.entry.Entry = receipt.PathEntry{State: "reported_recipient_unresolved", Source: json.RawMessage(`{"retained":true}`)}
	out, err := r.ConnectionPaths(context.Background(), q)
	if err != nil || out.Search.State != "start_relationship_not_available" || out.Entry.Entry.State != "reported_recipient_unresolved" || len(out.Paths) != 0 {
		t.Fatal("unresolved recipient promoted", err)
	}
}

func dayPtr(day int32) *int32 { return &day }

func TestWindowConnectionValidationAndFailClosedSources(t *testing.T) {
	_, valid := connectionFixtureReader(t)
	for _, mutate := range []func(*WindowConnectionQuery){
		func(q *WindowConnectionQuery) { q.EntryGeneration = "" },
		func(q *WindowConnectionQuery) { q.EntryGeneration = "2024" },
		func(q *WindowConnectionQuery) { q.From = "C00000001" },
		func(q *WindowConnectionQuery) { q.ReceiptOrdinal = 0 },
		func(q *WindowConnectionQuery) { q.EntryFamily = "invented" },
		func(q *WindowConnectionQuery) { q.Ending = "independent_support" },
		func(q *WindowConnectionQuery) { q.Ending = "independent_opposition" },
		func(q *WindowConnectionQuery) { q.Window = &DateWindow{"2023-01-01", "2022-12-31"} },
		func(q *WindowConnectionQuery) { q.Ledger = "both" },
		func(q *WindowConnectionQuery) { q.Budget = 0 },
	} {
		q := valid
		mutate(&q)
		if q.Validate() == nil {
			t.Fatal("accepted invalid connection scope", q)
		}
	}
	for _, mutate := range []func(*WindowReader, *WindowConnectionQuery){
		func(_ *WindowReader, q *WindowConnectionQuery) { q.EntryGeneration = valueID("foreign") },
		func(r *WindowReader, _ *WindowConnectionQuery) { r.partitions[1].receipts = nil },
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[0].receipts.(*windowReceiptStub).err = errors.New("source corrupt")
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			s := r.partitions[1].receipts.(*windowReceiptStub)
			s.links = append(s.links, s.links[0])
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[1].receipts.(*windowReceiptStub).links[0].Family = "independent_support"
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[0].receipts.(*windowReceiptStub).entry.Entry.Item.Key = valueID("wrong")
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			calls := 0
			r.partitions[1].verify = func(context.Context) error {
				calls++
				if calls > 1 {
					return errors.New("completion changed")
				}
				return nil
			}
		},
	} {
		r, q := connectionFixtureReader(t)
		mutate(r, &q)
		if out, err := r.ConnectionPaths(context.Background(), q); err == nil || out.ResultID != "" {
			t.Fatal("accepted unverified evidence")
		}
	}
	r, q := connectionFixtureReader(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out, err := r.ConnectionPaths(ctx, q); !errors.Is(err, context.Canceled) || out.ResultID != "" {
		t.Fatal("ignored cancellation")
	}
}
