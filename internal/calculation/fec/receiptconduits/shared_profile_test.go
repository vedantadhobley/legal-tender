package receiptconduits

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
)

func sharedFixture() ([]participants.Row, map[uint64]refs.Endpoint) {
	var rows []participants.Row
	eps := map[uint64]refs.Endpoint{}
	for _, shape := range []string{"same", "different", "unknown", "mixed", "none", "partial", "unsafe_original", "unsafe_peer"} {
		start := len(rows)
		children := 2
		if shape == "partial" {
			children++
		}
		peer := uint64(start + children + 1)
		for i := 0; i <= children; i++ {
			r := participants.Row{Ordinal: int64(len(rows) + 1), Recipient: ptr("C00000001"), Entity: ptr("IND"), ReceiptType: ptr("15E"), Amount: ptr(int64(50)), AmountState: "reported_value", Component: "fixture", SourceRoute: "fixture", ConduitState: "fixture", EarmarkState: "fixture"}
			if i == children {
				r.Memo, r.Entity, r.Amount = true, ptr("PAC"), ptr(int64(100))
				r.Contributor, r.CleanContributor = ptr("C00000002"), ptr("C00000002")
				eps[peer] = refs.Endpoint{Ordinal: peer, Peers: uint64(children), Incoming: uint64(children)}
			} else {
				ordinal := uint64(r.Ordinal)
				eps[ordinal] = refs.Endpoint{Ordinal: ordinal, Peers: 1, OnlyPeer: peer, Outgoing: 1}
				if i == 2 {
					r.Memo = true
				}
			}
			rows = append(rows, r)
		}
		switch shape {
		case "different":
			rows[start].Amount = ptr(int64(-50))
		case "unknown":
			rows[start].Amount = nil
			rows[start].AmountState = "source_null"
		case "mixed":
			rows[start].Entity = ptr("ORG")
		case "none":
			rows[peer-1].Memo = false
			rows[peer-1].ReceiptType = ptr("15")
		case "unsafe_original":
			e := eps[uint64(start+1)]
			e.UnsafeReasons = 1
			eps[e.Ordinal] = e
		case "unsafe_peer":
			e := eps[peer]
			e.UnsafeReasons = 2
			eps[peer] = e
		}
	}
	return rows, eps
}

func TestSharedProfileCompleteMembershipAndLayoutReplay(t *testing.T) {
	rows, eps := sharedFixture()
	var previousProfile, previousValues string
	for _, layout := range [][4]int{{5, 1, 3, 2}, {7, 4, 5, 3}, {1, 8, 100, 8}} {
		w, dir := setup(t, context.Background(), rows, eps, layout[0], layout[1], layout[2], layout[3], 64<<20)
		w.o.profile = newSharedProfileCollector()
		baseline, err := w.calculate(dir)
		if err != nil {
			t.Fatal(err)
		}
		r, err := w.o.profile.finish(baseline)
		if err != nil {
			t.Fatal(err)
		}
		if r.SharedRows != 13 || r.SharedGroups != 7 || r.Roles[policy.RolesCompatible] != 10 || r.Roles["original_contributor_role_unresolved"] != 1 || r.Roles["related_role_unresolved"] != 2 {
			t.Fatalf("wrong complete profile: %+v", r)
		}
		if r.AssociationChanges != 0 || r.AdditionalAmount != "0" || r.TerminalEligible || baseline.Qualified != 0 {
			t.Fatal("diagnostic changed eligibility", r)
		}
		if previousProfile != "" && (r.ProfileID != previousProfile || baseline.Decisions.ValuesSHA256 != previousValues) {
			t.Fatal("layout-dependent output")
		}
		previousProfile, previousValues = r.ProfileID, baseline.Decisions.ValuesSHA256
		var groupRows, groups uint64
		for _, c := range r.Classes {
			groupRows += c.Rows
			groups += c.Groups
			witness := c.Witness
			if c.OriginalAmounts.Known+c.OriginalAmounts.Missing != c.Rows || c.RelatedAmounts.Known+c.RelatedAmounts.Missing != c.Groups {
				t.Fatal("amount population mismatch", c)
			}
			if c.Coverage == "complete_safe_earmark_leaf_coverage" && witness.OtherPeers != 0 {
				t.Fatal("partial group marked complete")
			}
			if c.AmountComparison == "different_reported_amount" && c.Coverage == "complete_safe_earmark_leaf_coverage" {
				if c.OriginalAmounts.Negative != 1 || c.OriginalAmounts.Sum != "0" {
					t.Fatal("signed sum changed", c)
				}
			}
			for _, example := range witness.Examples {
				row := rows[example.Occurrence.Ordinal-1]
				if !reflect.DeepEqual(example.Occurrence, profileOccurrence(row, eps[uint64(row.Ordinal)])) {
					t.Fatal("witness evidence differs")
				}
			}
		}
		if groupRows != r.SharedRows || groups != r.SharedGroups {
			t.Fatal("group conservation")
		}
		baseline.States[sharedRejection]++
		if _, err := w.o.profile.finish(baseline); err == nil {
			t.Fatal("incomplete profile accepted")
		}
	}
}

func TestSharedProfileDoesNotChangeAssociationValues(t *testing.T) {
	rows, eps := fixture()
	var prior Result
	for _, enabled := range []bool{false, true} {
		w, dir := setup(t, context.Background(), rows, eps, 7, 4, 3, 2, 64<<20)
		if enabled {
			w.o.profile = newSharedProfileCollector()
		}
		r, err := w.calculate(dir)
		if err != nil {
			t.Fatal(err)
		}
		if enabled && (r.Decisions.ValuesSHA256 != prior.Decisions.ValuesSHA256 || !reflect.DeepEqual(r.States, prior.States)) {
			t.Fatal("profile changed decisions")
		}
		prior = r
	}
}

func TestProfileAmountsDoNotOverflowOrHideNull(t *testing.T) {
	var a, b amountTally
	a.add(ptr(int64(math.MaxInt64)))
	b.add(ptr(int64(math.MaxInt64)))
	b.add(ptr(int64(-1)))
	b.add(nil)
	b.add(ptr(int64(0)))
	a.merge(&b)
	got := a.result()
	if got.Sum != "18446744073709551613" || got.Known != 4 || got.Missing != 1 || got.Positive != 2 || got.Negative != 1 || got.Zero != 1 {
		t.Fatal(got)
	}
}

func TestProfileOwnsWitnessesAndRejectsInvalidCoverage(t *testing.T) {
	rows, eps := sharedFixture()
	p := newSharedProfileCollector()
	var g sharedGroup
	g.observe(rows[0], eps[1], rows[2])
	g.observe(rows[1], eps[2], rows[2])
	if err := p.add(rows[2], eps[3], 2, g); err != nil {
		t.Fatal(err)
	}
	r, err := p.finish(Result{States: map[string]uint64{sharedRejection: 2}})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(r)
	*rows[0].Entity = "MUTATED"
	*rows[2].Contributor = "MUTATED"
	*rows[2].Amount = -999
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("reader buffer changed retained witnesses")
	}
	for _, e := range []refs.Endpoint{{Peers: 1}, {Peers: 2, UnsafeReasons: 1}, {Peers: 2}} {
		if err := p.add(rows[2], e, 3, g); err == nil {
			t.Fatal("invalid coverage accepted")
		}
	}
}
