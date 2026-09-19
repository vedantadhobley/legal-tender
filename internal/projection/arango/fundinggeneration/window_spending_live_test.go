package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

type spendingGateCase struct {
	Kind        string                   `json:"kind"`
	State       string                   `json:"state"`
	WitnessFact string                   `json:"witness_fact_id,omitempty"`
	Result      *WindowConnectionsResult `json:"result,omitempty"`
}
type spendingLiveGate struct {
	Version string                  `json:"schema_version"`
	GateID  string                  `json:"gate_id"`
	Build   string                  `json:"consumer_executable_sha256"`
	Scope   string                  `json:"scope"`
	Census  []spendingCensusSummary `json:"source_census"`
	Cases   []spendingGateCase      `json:"cases"`
}

func TestWindowSpendingLiveGate(t *testing.T) {
	if os.Getenv("LT_WINDOW_INPUTS") == "" {
		t.Skip("retained spending gate not requested")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
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
	t.Log("decoding complete independent A/B and Schedule E census")
	counts := loadConnectionCensus(t, ctx, r, root)
	census := loadSpendingCensus(t, ctx, r, root)
	gate := runSpendingGate(t, ctx, r, counts, census)
	if expected := os.Getenv("LT_WINDOW_EXPECTED_GATE"); expected != "" && expected != gate.GateID {
		t.Fatal("fresh spending gate identity differs")
	}
	b, err := json.MarshalIndent(gate, "", "  ")
	liveCheck(t, err)
	out, err := os.OpenFile(os.Getenv("LT_WINDOW_OUTPUT"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	liveCheck(t, err)
	_, err = out.Write(append(b, '\n'))
	liveCheck(t, err)
	liveCheck(t, out.Close())
}

func sortedSpendingFacts(c spendingGateCensus) []spendingReference {
	keys := make([]string, 0, len(c.Facts))
	for k := range c.Facts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]spendingReference, 0, len(keys))
	for _, k := range keys {
		out = append(out, c.Facts[k])
	}
	return out
}

func spendingQuery(ref spendingReference, basis string) WindowConnectionQuery {
	return WindowConnectionQuery{PathQuery: PathQuery{From: ref.Member.Link.From, Target: ref.Member.Link.To, Ledger: flow.ScheduleA,
		Ending: ref.Member.Parent.Family, MaxHops: 0, Limit: 3, Budget: 100000}, SpendingDate: basis}
}
func spendingDayWindow(day int32) *DateWindow {
	s := time.Unix(int64(day)*86400, 0).UTC().Format(time.DateOnly)
	return &DateWindow{Start: s, End: s}
}

// Select from complete supplied membership. Never select a real identity,
// source ordinal, cycle or amount via a literal exception in the gate.
func selectSpendingDateWitness(c spendingGateCensus, stance, basis string) (spendingReference, bool) {
	days := map[string]map[int32]bool{}
	for _, ref := range c.Facts {
		m := ref.Member
		day := spendingDate(m, basis)
		if m.Link == nil || m.Stance != stance || day == nil {
			continue
		}
		k := spendingGroupKey(ref.Generation, m.Link.From, m.Link.To, m.Stance)
		if days[k] == nil {
			days[k] = map[int32]bool{}
		}
		days[k][*day] = true
	}
	for _, ref := range sortedSpendingFacts(c) {
		m := ref.Member
		if m.Link != nil && m.Stance == stance && spendingDate(m, basis) != nil && len(days[spendingGroupKey(ref.Generation, m.Link.From, m.Link.To, m.Stance)]) > 1 {
			return ref, true
		}
	}
	return spendingReference{}, false
}

func runSpendingGate(t *testing.T, ctx context.Context, r *WindowReader, counts connectionCounts, census spendingGateCensus) spendingLiveGate {
	t.Helper()
	gate := spendingLiveGate{Version: "legal-tender.funding-window-spending-live-gate.v1", Build: r.build,
		Scope: "complete_source_and_date_census_with_bounded_source_member_connections_not_all_paths_or_terminal_attribution", Census: census.Summaries, Cases: []spendingGateCase{}}
	run := func(kind string, q WindowConnectionQuery, ref spendingReference, requirePath bool) WindowConnectionsResult {
		started := time.Now()
		t.Log("checking", kind)
		out, err := r.ConnectionPaths(ctx, q)
		liveCheck(t, err)
		liveCheck(t, checkSpendingGateResult(out, counts, census))
		if requirePath && len(out.Paths) == 0 {
			t.Fatal("selected spending witness not returned", kind)
		}
		gate.Cases = append(gate.Cases, spendingGateCase{Kind: kind, State: "verified", WitnessFact: ref.Member.FactID, Result: &out})
		t.Logf("verified %s in %s", kind, time.Since(started))
		return out
	}
	for _, stance := range []string{"S", "O"} {
		for _, basis := range []string{"expenditure", "dissemination"} {
			ref, ok := selectSpendingDateWitness(census, stance, basis)
			if !ok {
				t.Fatal("no multi-date source group for required spending witness", stance, basis)
			}
			q := spendingQuery(ref, basis)
			prefix := q.Ending + "_" + basis
			run(prefix+"_all_dates", q, ref, true)
			day := *spendingDate(ref.Member, basis)
			q.Window = spendingDayWindow(day)
			run(prefix+"_source_day", q, ref, true)
			q.Window = spendingDayWindow(day + 1)
			run(prefix+"_following_day", q, ref, false)
		}
	}
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
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
		endings := []graphread.Link{}
		for _, ref := range census.Facts {
			if ref.Member.Link != nil && ref.Member.Stance == "S" && ref.Member.ExpenditureDate != nil {
				endings = append(endings, *ref.Member.Link)
			}
		}
		from, target, ok := selectPathWitness(known, endings)
		if !ok {
			t.Fatal("no two-observation upstream spending witness", ledger)
		}
		q := WindowConnectionQuery{PathQuery: PathQuery{From: from, Target: target, Ledger: ledger, Ending: "independent_support", MaxHops: 2, Limit: 1, Budget: 100000}, SpendingDate: "expenditure"}
		baseline := run(string(ledger)+"_upstream_all_dates", q, spendingReference{}, true)
		q.Window = connectionWitnessWindow(t, baseline)
		run(string(ledger)+"_upstream_bounded", q, spendingReference{}, true)
		found := false
		for _, p := range r.partitions {
			s, ok := p.receipts.(connectionWitnessSource)
			if !ok {
				t.Fatal("source-backed receipt witness reader required")
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
				t.Fatal("receipt witness lacks source locator")
			}
			q.From, q.EntryGeneration, q.EntryFamily, q.ReceiptOrdinal, q.Window = "", p.publication.GenerationID, "reported_receipt", doc.Ordinal, nil
			entry := run(string(ledger)+"_receipt_upstream_all_dates", q, spendingReference{}, true)
			q.Window = connectionWitnessWindow(t, entry)
			run(string(ledger)+"_receipt_upstream_bounded", q, spendingReference{}, entry.Entry.Date != nil)
			found = true
			break
		}
		if !found {
			gate.Cases = append(gate.Cases, spendingGateCase{Kind: string(ledger) + "_receipt_upstream", State: "no_receipt_at_selected_origin_in_supplied_inputs"})
		}
	}
	// Additional source shapes are explicit selection coverage. A missing
	// shape is recorded, not silently passed or supplied by synthetic real data.
	for _, feature := range []string{"unknown_expenditure", "unknown_dissemination", "different_native_dates", "negative_amount", "zero_amount", "resolved_candidate", "unverified_candidate"} {
		ref, basis, ok := selectSpendingFeature(census, feature)
		if !ok {
			gate.Cases = append(gate.Cases, spendingGateCase{Kind: feature, State: "no_returnable_witness_in_complete_supplied_membership"})
			continue
		}
		q := spendingQuery(ref, basis)
		q.Limit = 10
		if day := spendingDate(ref.Member, basis); day != nil {
			q.Window = spendingDayWindow(*day)
		}
		out := run(feature, q, ref, true)
		found := false
		for _, l := range out.Links {
			if l.Topology.Key == ref.Member.FactID {
				found = true
			}
		}
		if !found {
			t.Fatal("feature witness omitted", feature)
		}
		if feature == "unknown_expenditure" || feature == "unknown_dissemination" {
			known, ok := selectSpendingDateWitness(census, ref.Member.Stance, basis)
			if !ok {
				t.Fatal("known-day exclusion witness absent")
			}
			q.Window = spendingDayWindow(*spendingDate(known.Member, basis))
			out := run(feature+"_bounded_exclusion", q, ref, false)
			for _, l := range out.Links {
				if l.Topology.Key == ref.Member.FactID {
					t.Fatal("unknown source date filled")
				}
			}
		}
		if feature == "different_native_dates" {
			q.SpendingDate = "dissemination"
			out := run(feature+"_other_basis", q, ref, false)
			for _, l := range out.Links {
				if l.Topology.Key == ref.Member.FactID {
					t.Fatal("alternate native date ignored")
				}
			}
		}
	}
	gate.GateID = valueID(gate)
	return gate
}

func selectSpendingFeature(c spendingGateCensus, feature string) (spendingReference, string, bool) {
	basis := "expenditure"
	if feature == "unknown_dissemination" {
		basis = "dissemination"
	}
	// Rank within each native-day group, or whole endpoint group for an
	// unknown date. Only first ten members are returnable by the public API.
	ranks := map[string]int{}
	for _, ref := range sortedSpendingFacts(c) {
		m := ref.Member
		if m.Link == nil {
			continue
		}
		k := m.Link.From + ":" + m.Link.To + ":" + m.Stance
		if feature != "unknown_expenditure" && feature != "unknown_dissemination" {
			if day := spendingDate(m, basis); day != nil {
				k += ":" + time.Unix(int64(*day)*86400, 0).UTC().Format(time.DateOnly)
			} else {
				k += ":unknown"
			}
		}
		ranks[k]++
		if ranks[k] > 10 {
			continue
		}
		match := feature == "unknown_expenditure" && m.ExpenditureDate == nil || feature == "unknown_dissemination" && m.DisseminationDate == nil ||
			feature == "different_native_dates" && m.ExpenditureDate != nil && m.DisseminationDate != nil && *m.ExpenditureDate != *m.DisseminationDate ||
			feature == "negative_amount" && m.Amount != nil && len(*m.Amount) > 0 && (*m.Amount)[0] == '-' ||
			feature == "zero_amount" && m.Amount != nil && *m.Amount == "0" || feature == "resolved_candidate" && m.ResolutionState == "resolved" || feature == "unverified_candidate" && m.ResolutionState == "unverified"
		if match {
			return ref, basis, true
		}
	}
	return spendingReference{}, basis, false
}
