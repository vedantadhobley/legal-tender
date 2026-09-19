package fundinggeneration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

// Explicit opt-in, read-only, automatic witness selection. All real source and
// graph checks remain active; no fixture endpoint or writer is used here.
func TestSharedWindowLiveGate(t *testing.T) {
	if os.Getenv("LT_SHARED_WINDOW_INPUTS") == "" {
		t.Skip("retained shared window gate not requested")
	}
	ctx := context.Background()
	spec, err := ReadWindowInputs(os.Getenv("LT_SHARED_WINDOW_INPUTS"), os.Getenv("LT_SHARED_WINDOW_INPUTS_SHA256"))
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
	r, err := OpenWindowReader(ctx, WindowOpenOptions{Inputs: spec.Inputs, StorageRoot: root, Endpoint: os.Getenv("LT_WINDOW_ENDPOINT"), Username: "root", Password: os.Getenv("ARANGO_PASSWORD"), BuildSHA256: hex.EncodeToString(h.Sum(nil)), Progress: func(s string) { t.Log(time.Now().UTC().Format(time.RFC3339), s) }})
	liveCheck(t, err)
	counts := loadConnectionCensus(t, ctx, r, root)
	gate := runSharedWindowGate(t, ctx, r, counts)
	if expected := os.Getenv("LT_WINDOW_EXPECTED_GATE"); expected != "" && expected != gate.GateID {
		t.Fatal("fresh shared window gate differs")
	}
	b, err := json.MarshalIndent(gate, "", "  ")
	liveCheck(t, err)
	out, err := os.OpenFile(os.Getenv("LT_WINDOW_OUTPUT"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	liveCheck(t, err)
	_, err = out.Write(append(b, '\n'))
	liveCheck(t, err)
	liveCheck(t, out.Close())
}

type sharedWindowWitness interface {
	Page(context.Context, string, string, int) (graphread.Page, error)
	WitnessID() string
}

func runSharedWindowGate(t *testing.T, ctx context.Context, r *WindowReader, counts connectionCounts) windowConnectionGate {
	t.Helper()
	out := windowConnectionGate{Version: "legal-tender.shared-conduit-window-gate.v1", BuildSHA256: r.build, Scope: "complete_selected_ledger_date_census_and_selected_shared_entries_not_all_receipt_dates_or_paths", Cases: []windowConnectionCase{}}
	run := func(kind string, q WindowConnectionQuery, wantPaths bool) WindowConnectionsResult {
		t.Log(time.Now().UTC().Format(time.RFC3339), "checking", kind)
		v, err := r.ConnectionPaths(ctx, q)
		liveCheck(t, err)
		liveCheck(t, checkSharedWindowResult(v, counts))
		if (len(v.Paths) > 0) != wantPaths {
			t.Fatal("unexpected shared path availability", kind)
		}
		out.Cases = append(out.Cases, windowConnectionCase{Kind: kind, State: "verified", Result: &v})
		return v
	}
	for _, p := range r.partitions {
		if p.shared == nil {
			continue
		}
		s, ok := p.shared.(sharedWindowWitness)
		if !ok {
			t.Fatal("verified shared witness reader required")
		}
		id := s.WitnessID()
		if id == "" {
			out.Cases = append(out.Cases, windowConnectionCase{Kind: p.publication.GenerationID, State: "no_shared_associations"})
			continue
		}
		page, err := s.Page(ctx, id, "", 1)
		liveCheck(t, err)
		if len(page.Items) != 1 {
			t.Fatal("shared witness missing")
		}
		var evidence receipt.SharedEvidence
		liveCheck(t, json.Unmarshal(page.Items[0].Evidence, &evidence))
		q := WindowConnectionQuery{PathQuery: PathQuery{ReceiptOrdinal: evidence.Decision.Ordinal, EntryFamily: receipt.SharedFamily, Ledger: flow.ScheduleA, Target: id, MaxHops: 0, Limit: 1, Budget: 100000}, EntryGeneration: p.publication.GenerationID}
		base := run("shared_all_dates_a", q, true)
		if base.Entry.Date != nil {
			day := time.Unix(int64(*base.Entry.Date)*86400, 0).UTC()
			for _, tc := range []struct {
				name    string
				offset  int
				present bool
			}{{"shared_inclusive_day", 0, true}, {"shared_before_window", 1, false}, {"shared_after_window", -1, false}} {
				date := day.AddDate(0, 0, tc.offset).Format(time.DateOnly)
				q.Window = &DateWindow{Start: date, End: date}
				run(tc.name, q, tc.present)
			}
		} else {
			// A declared fixture window is only an unknown-date exclusion probe.
			q.Window = &DateWindow{Start: "2000-01-01", End: "2000-01-01"}
			run("shared_unknown_date_excluded", q, false)
		}
		q.Window, q.Ledger = nil, flow.ScheduleB
		run("shared_all_dates_b", q, true)
		q.EntryFamily = "conduit_association"
		old := run("original_conduit_remains_unavailable", q, false)
		if old.Entry.Entry.State != "no_qualified_conduit_association" {
			t.Fatal("original conduit disposition changed")
		}
		q.EntryFamily, q.Ledger, q.MaxHops = receipt.SharedFamily, flow.ScheduleA, 1
		var first *flow.DatedLink
		for _, other := range r.partitions {
			liveCheck(t, other.source.VisitDatedLinks(ctx, flow.ScheduleA, func(row flow.DatedLink) error {
				if row.Link.From == id && row.Link.To != id && row.Date != nil && (first == nil || row.Link.ID() < first.Link.ID()) {
					copy := row
					first = &copy
				}
				return nil
			}))
		}
		if first != nil && base.Entry.Date != nil {
			lo, hi := min(*first.Date, *base.Entry.Date), max(*first.Date, *base.Entry.Date)
			q.Target = first.Link.To
			q.Window = &DateWindow{Start: time.Unix(int64(lo)*86400, 0).UTC().Format(time.DateOnly), End: time.Unix(int64(hi)*86400, 0).UTC().Format(time.DateOnly)}
			run("shared_dated_committee_continuation", q, true)
		} else {
			out.Cases = append(out.Cases, windowConnectionCase{Kind: "shared_dated_committee_continuation", State: "no_matching_selection_population"})
		}
	}
	if len(out.Cases) == 0 {
		t.Fatal("shared gate requires extended input")
	}
	out.GateID = valueID(out)
	return out
}

func checkSharedWindowResult(out WindowConnectionsResult, counts connectionCounts) error {
	if out.SchemaVersion != WindowConnectionsVersion || out.Policy != WindowConnectionsPolicy {
		return fmt.Errorf("shared window changed date policy")
	}
	return checkConnectionGateCommon(out, counts, func(link WindowConnectionLink) error {
		entry := out.Entry
		if link.Topology.Family != receipt.SharedFamily || entry == nil || entry.Entry.Link == nil || entry.Entry.Item == nil || entry.GenerationID != link.GenerationID || *entry.Entry.Link != link.Topology || !sameDate(entry.Date, link.Date) || link.TemporalBasis != "underlying_receipt_reported_date" {
			return fmt.Errorf("shared entry lost date/source routing")
		}
		var e receipt.SharedEvidence
		if err := json.Unmarshal(link.Evidence.Evidence, &e); err != nil {
			return err
		}
		b, _ := json.Marshal(e.Source)
		source, err := graphread.Canonical(entry.Entry.Source)
		if err != nil {
			return err
		}
		expected, err := graphread.Canonical(b)
		if err != nil {
			return err
		}
		if !bytes.Equal(source, expected) || e.Decision.Ordinal != out.Query.ReceiptOrdinal || e.OriginalDecision.Ordinal != e.Decision.Ordinal || e.OriginalDecision.Related != e.Decision.Related || e.OriginalDecision.AmountComparison != e.Decision.AmountComparison || e.OriginalDecision.State != "shared_related_record_unresolved" || e.OriginalDecision.ConduitID != nil || e.Decision.State != "reported_shared_earmark_memo_association" || e.Group.Related != e.Decision.Related || e.Decision.ConduitID == nil || *e.Decision.ConduitID != link.Topology.To || e.Group.Decision.ConduitID == nil || *e.Group.Decision.ConduitID != *e.Decision.ConduitID {
			return fmt.Errorf("shared original/new evidence differs")
		}
		var doc struct {
			Amount   string `json:"additional_amount_minor_units"`
			Eligible bool   `json:"financial_eligibility"`
		}
		if json.Unmarshal(link.Evidence.Document, &doc) != nil || doc.Amount != "0" || doc.Eligible {
			return fmt.Errorf("shared link acquired money")
		}
		for _, input := range out.Inputs {
			if input.GenerationID == link.GenerationID {
				if input.Shared == nil || input.Shared.GenerationID != input.GenerationID || e.BaseProjection != input.Generation.Receipts.Projection || e.Calculation != input.Shared.Extension.Calculation || e.Source.FactSetID != input.Generation.Receipts.Inputs.Facts.ID || e.Related.Source.Ordinal != e.Decision.Related {
					return fmt.Errorf("shared physical/source binding differs")
				}
				return nil
			}
		}
		return fmt.Errorf("shared source generation absent")
	})
}
