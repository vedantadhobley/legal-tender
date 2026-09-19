package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	fc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Explicit opt-in only. This exercises the public connection reader against
// pinned publications; it neither downloads source bytes nor modifies graphs.
func TestWindowConnectionLiveGate(t *testing.T) {
	if os.Getenv("LT_WINDOW_INPUTS") == "" {
		t.Skip("retained window connection gate not requested")
	}
	ctx := context.Background()
	spec, err := ReadWindowInputs(os.Getenv("LT_WINDOW_INPUTS"), os.Getenv("LT_WINDOW_INPUTS_SHA256"))
	liveCheck(t, err)
	exe, err := os.Executable()
	liveCheck(t, err)
	f, err := os.Open(exe)
	liveCheck(t, err)
	h := sha256.New()
	_, err = io.Copy(h, f)
	liveCheck(t, err)
	liveCheck(t, f.Close())
	root := os.Getenv("LT_WINDOW_STORAGE_ROOT")
	r, err := OpenWindowReader(ctx, WindowOpenOptions{Inputs: spec.Inputs, StorageRoot: root, Endpoint: os.Getenv("LT_WINDOW_ENDPOINT"), Username: "root", Password: os.Getenv("ARANGO_PASSWORD"), BuildSHA256: hex.EncodeToString(h.Sum(nil)), Progress: func(s string) { t.Log(s) }})
	liveCheck(t, err)
	counts := loadConnectionCensus(t, ctx, r, root)
	gate := runWindowConnectionGate(t, ctx, r, counts)
	if expected := os.Getenv("LT_WINDOW_EXPECTED_GATE"); expected != "" && expected != gate.GateID {
		t.Fatal("fresh connection gate identity differs")
	}
	b, err := json.MarshalIndent(gate, "", "  ")
	liveCheck(t, err)
	out, err := os.OpenFile(os.Getenv("LT_WINDOW_OUTPUT"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	liveCheck(t, err)
	_, err = out.Write(append(b, '\n'))
	liveCheck(t, err)
	liveCheck(t, out.Close())
}

type connectionCensus struct {
	days    map[int32]uint64
	unknown uint64
}
type connectionCounts map[string]map[flow.Ledger]connectionCensus
type windowConnectionCase struct {
	Kind   string                   `json:"kind"`
	State  string                   `json:"state"`
	Result *WindowConnectionsResult `json:"result"`
}
type windowConnectionGate struct {
	Version     string                 `json:"schema_version"`
	GateID      string                 `json:"gate_id"`
	BuildSHA256 string                 `json:"consumer_executable_sha256"`
	Scope       string                 `json:"scope"`
	Cases       []windowConnectionCase `json:"cases"`
}

func liveCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Complete independent compact-artifact decoding, not windowTopology or its
// dated accessor. Counts remain separate by publication and ledger.
func loadConnectionCensus(t *testing.T, ctx context.Context, r *WindowReader, root string) connectionCounts {
	t.Helper()
	counts := connectionCounts{}
	for _, p := range r.partitions {
		g := p.publication.Generation
		calculation, _, err := fc.LoadPublished(ctx, root, filepath.Join(root, fc.PublicationBase, "manifests", g.CommitteeFlow.CalculationID+".json"))
		liveCheck(t, err)
		counts[p.publication.GenerationID] = map[flow.Ledger]connectionCensus{}
		for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
			d := calculation.A.Observations
			if ledger == flow.ScheduleB {
				d = calculation.B.Observations
			}
			reader, err := artifact.Open[fc.Observation](ctx, filepath.Join(root, fc.PublicationBase), d)
			liveCheck(t, err)
			c := connectionCensus{days: map[int32]uint64{}}
			for {
				row, ok, err := reader.Next()
				if err != nil {
					reader.Abort()
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if row.Date == nil {
					c.unknown++
				} else {
					c.days[*row.Date]++
				}
			}
			liveCheck(t, reader.Close())
			counts[p.publication.GenerationID][ledger] = c
		}
	}
	return counts
}

type connectionWitnessSource interface {
	Page(context.Context, string, string, string, int) (graphread.Page, error)
	ConduitWitnessID(context.Context) (string, error)
}

func runWindowConnectionGate(t *testing.T, ctx context.Context, r *WindowReader, counts connectionCounts) windowConnectionGate {
	t.Helper()
	gate := windowConnectionGate{Version: "legal-tender.funding-window-connection-live-gate.v1", BuildSHA256: r.build, Scope: "complete_selected_ledger_date_census_and_data_selected_connections_not_all_paths_or_terminal_sources", Cases: []windowConnectionCase{}}
	run := func(kind string, q WindowConnectionQuery, requirePath bool) WindowConnectionsResult {
		t.Log("checking", kind)
		out, err := r.ConnectionPaths(ctx, q)
		liveCheck(t, err)
		if requirePath && len(out.Paths) == 0 {
			t.Fatal("selected connection not returned", kind, out.Search)
		}
		liveCheck(t, checkConnectionGateResult(out, counts))
		gate.Cases = append(gate.Cases, windowConnectionCase{Kind: kind, State: "verified", Result: &out})
		return out
	}
	absent := func(kind, state string) {
		gate.Cases = append(gate.Cases, windowConnectionCase{Kind: kind, State: state})
	}
	endings := []graphread.Link{}
	for _, p := range r.partitions {
		endings = append(endings, p.receipts.AuthorizedLinks()...)
	}
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		// Exclude undated committee observations only for positive witness
		// selection. The actual query still scans and reports the complete ledger.
		known := pathTopology{}
		for _, p := range r.partitions {
			liveCheck(t, p.source.VisitDatedLinks(ctx, ledger, func(row flow.DatedLink) error {
				if row.Date != nil {
					return known.add(row.Link)
				}
				return nil
			}))
		}
		liveCheck(t, known.order())
		from, target, ok := selectPathWitness(known, endings)
		if !ok {
			t.Fatal("no dated two-observation candidate witness in supplied ledger", ledger)
		}
		q := WindowConnectionQuery{PathQuery: PathQuery{From: from, Target: target, Ledger: ledger, Ending: "candidate_authorization_context", MaxHops: 2, Limit: 2, Budget: 100000}}
		baseline := run(string(ledger)+"_committee_candidate_all_dates", q, true)
		q.Window = connectionWitnessWindow(t, baseline)
		run(string(ledger)+"_committee_candidate_bounded", q, true)
		bounded := q
		bounded.MaxHops = 0
		stopped := run(string(ledger)+"_hop_bound", bounded, false)
		if len(stopped.Paths) != 0 || stopped.Search.HopFrontiers == 0 {
			t.Fatal("hop frontier witness lost")
		}
		// Receipt selection is bounded: one source-backed first page per input
		// at the selected origin. No entity name, cycle or source ordinal policy.
		found := false
		for _, p := range r.partitions {
			s, ok := p.receipts.(connectionWitnessSource)
			if !ok {
				t.Fatal("verified receipt witness reader required")
			}
			page, err := s.Page(ctx, "reported_receipt", from, "", 1)
			liveCheck(t, err)
			if len(page.Items) == 0 {
				continue
			}
			var doc struct {
				Ordinal uint64 `json:"source_row_ordinal"`
			}
			liveCheck(t, json.Unmarshal(page.Items[0].Document, &doc))
			if doc.Ordinal == 0 {
				t.Fatal("receipt witness lacks exact source locator")
			}
			entryQ := q
			entryQ.From, entryQ.ReceiptOrdinal, entryQ.EntryFamily, entryQ.EntryGeneration, entryQ.Window = "", doc.Ordinal, "reported_receipt", p.publication.GenerationID, nil
			entry := run(string(ledger)+"_receipt_candidate_all_dates", entryQ, true)
			if entry.Entry.Entry.Link.To != from {
				t.Fatal("receipt witness changed recipient")
			}
			entryQ.Window = connectionWitnessWindow(t, entry)
			selected := run(string(ledger)+"_receipt_candidate_bounded", entryQ, entry.Entry.Date != nil)
			if entry.Entry.Date == nil {
				if selected.Entry.Selection != "unknown_date_excluded" || len(selected.Paths) != 0 {
					t.Fatal("unknown receipt date became dated")
				}
			} else {
				next := time.Unix(int64(*entry.Entry.Date)*86400, 0).UTC().AddDate(0, 0, 1).Format(time.DateOnly)
				entryQ.Window = &DateWindow{Start: next, End: next}
				excluded := run(string(ledger)+"_receipt_following_day", entryQ, false)
				if excluded.Entry.Selection != "before_window" || excluded.Search.State != "receipt_entry_excluded_by_date_window" || len(excluded.Paths) != 0 {
					t.Fatal("date-excluded receipt entered topology")
				}
			}
			found = true
			break
		}
		if !found {
			absent(string(ledger)+"_receipt_candidate", "no_receipt_at_selected_origin_in_supplied_inputs")
		}
	}
	// A conduit association can be verified without asserting that it is another
	// payment or has a downstream candidate route. Zero committee hops is explicit.
	foundConduit := false
	for _, p := range r.partitions {
		s := p.receipts.(connectionWitnessSource)
		id, err := s.ConduitWitnessID(ctx)
		liveCheck(t, err)
		if id == "" {
			continue
		}
		page, err := s.Page(ctx, "conduit_association", id, "", 1)
		liveCheck(t, err)
		if len(page.Items) == 0 {
			t.Fatal("published conduit witness missing from graph")
		}
		var doc struct {
			Decision struct {
				Ordinal uint64 `json:"source_row_ordinal"`
			} `json:"decision"`
		}
		liveCheck(t, json.Unmarshal(page.Items[0].Document, &doc))
		if doc.Decision.Ordinal == 0 {
			t.Fatal("conduit witness lacks source locator")
		}
		q := WindowConnectionQuery{PathQuery: PathQuery{ReceiptOrdinal: doc.Decision.Ordinal, EntryFamily: "conduit_association", Target: id, Ledger: flow.ScheduleA, MaxHops: 0, Limit: 1, Budget: 100}, EntryGeneration: p.publication.GenerationID}
		out := run("conduit_entry_all_dates", q, true)
		if out.Entry.Date != nil {
			day := time.Unix(int64(*out.Entry.Date)*86400, 0).UTC()
			q.Window = &DateWindow{Start: day.Format(time.DateOnly), End: day.Format(time.DateOnly)}
			run("conduit_entry_witness_day", q, true)
			next := day.AddDate(0, 0, 1).Format(time.DateOnly)
			q.Window = &DateWindow{Start: next, End: next}
			excluded := run("conduit_entry_following_day", q, false)
			if len(excluded.Paths) != 0 || excluded.Entry.Selection != "before_window" {
				t.Fatal("conduit date ignored")
			}
		} else {
			absent("conduit_entry_witness_day", "selected_conduit_receipt_has_unknown_date")
		}
		foundConduit = true
		break
	}
	if !foundConduit {
		absent("conduit_entry", "no_qualified_conduit_in_supplied_publications")
	}
	liveCheck(t, r.verify(ctx))
	gate.GateID = valueID(gate)
	return gate
}

func connectionWitnessWindow(t *testing.T, out WindowConnectionsResult) *DateWindow {
	t.Helper()
	var first, last string
	for _, link := range out.Links {
		if link.Topology.Family == "candidate_authorization_context" {
			continue
		}
		if link.Date == nil {
			continue
		} // Unknown receipts are tested as exclusions.
		day := time.Unix(int64(*link.Date)*86400, 0).UTC().Format(time.DateOnly)
		if first == "" || day < first {
			first = day
		}
		if last == "" || day > last {
			last = day
		}
	}
	if first == "" {
		t.Fatal("selected route has no known observation dates")
	}
	return &DateWindow{Start: first, End: last}
}

// Keep this checker independent of receiptDateSelection and windowTopology.
func independentDateSelection(date *int32, w *DateWindow) string {
	if date == nil {
		if w == nil {
			return "undated_included"
		}
		return "unknown_date_excluded"
	}
	if w == nil {
		return "included"
	}
	day := time.Unix(int64(*date)*86400, 0).UTC().Format(time.DateOnly)
	if day < w.Start {
		return "before_window"
	}
	if day > w.End {
		return "after_window"
	}
	return "included"
}

// Used by the real gate and failure-injection fixtures in the companion file.
func sameDate(a, b *int32) bool { return reflect.DeepEqual(a, b) }
