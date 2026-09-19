package receiptgraph

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

func TestGateCandidateSelectionUsesOnlyPinnedBoundaryMembership(t *testing.T) {
	links := []authorization{
		{From: entities + "/C00000003", To: entities + "/H0ZZ00001", State: "authorized"},
		{From: entities + "/C00000002", To: entities + "/H0ZZ00003", State: "authorized"},
		{From: entities + "/C00000001", To: entities + "/H0ZZ00002", State: "authorized"},
	}
	recipients := map[string]bool{"C00000001": true, "C00000002": true}
	want, err := selectGateCandidate(links, recipients)
	if err != nil || want.Candidate != "H0ZZ00002" {
		t.Fatal(want, err)
	}
	links[0], links[2] = links[2], links[0]
	again, err := selectGateCandidate(links, recipients)
	if err != nil || again != want {
		t.Fatal("input order affected selection", again, err)
	}
	for _, bad := range [][]authorization{
		nil,
		append(append([]authorization{}, links...), links[0]),
		{{From: "C00000001", To: entities + "/H0ZZ00001", State: "authorized"}},
		{{From: entities + "/C00000001", To: entities + "/H0ZZ00001", State: "authorized", FinancialEligibility: true}},
		{{From: entities + "/C00000001", To: entities + "/H0ZZ00001", State: "unresolved"}},
	} {
		if _, err := selectGateCandidate(bad, recipients); err == nil {
			t.Fatal("accepted invalid or empty selection")
		}
	}
	if _, err := selectGateCandidate(links, nil); err == nil {
		t.Fatal("missing boundary accepted")
	}
}

func selectorFixture(t *testing.T) *connectionSelector {
	t.Helper()
	s, err := newConnectionSelector([]upstream.Node{
		{CommitteeID: "C00000001", Authorized: true},
		{CommitteeID: "C00000002", Hops: 1, CyclicComponent: ptr("cycle")},
	}, map[string]entity{"C00000001": {Key: "C00000001"}, "C00000003": {Key: "C00000003"}})
	if err != nil {
		t.Fatal(err)
	}
	rows := []p.Row{
		{Ordinal: 1, Recipient: ptr("C00000001"), Amount: ptr(int64(10))},
		{Ordinal: 2, Recipient: ptr("C00000002"), Amount: ptr(int64(-10))},
		{Ordinal: 3, Recipient: ptr("C00000003"), Amount: ptr(int64(0)), Memo: true},
		{Ordinal: 4},
		{Ordinal: 5, Recipient: ptr("C00000001"), Amount: ptr(int64(20))},
	}
	for i, row := range rows {
		var d *c.Decision
		if i == 1 {
			d = &c.Decision{Ordinal: 2, ConduitID: ptr("C00000004")}
		}
		if i == 2 {
			d = &c.Decision{Ordinal: 3}
		}
		if err := s.observe(row, d); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestConnectionCensusConservesAndRetainsAllCaseStates(t *testing.T) {
	s := selectorFixture(t)
	if err := s.validate(5); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.counts, []uint64{2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}) {
		t.Fatal(s.counts)
	}
	if s.first[0].Row.Ordinal != 1 || s.first[1].Row.Ordinal != 2 {
		t.Fatal("witness is not first ordinal")
	}
	if s.validate(6) == nil {
		t.Fatal("incomplete census accepted")
	}
	if s.observe(p.Row{Ordinal: 7}, nil) == nil {
		t.Fatal("gap accepted")
	}
	if s.observe(p.Row{Ordinal: 5}, nil) == nil {
		t.Fatal("duplicate accepted")
	}
	s, _ = newConnectionSelector(nil, nil)
	if s.observe(p.Row{Ordinal: 1}, &c.Decision{Ordinal: 2}) == nil {
		t.Fatal("foreign decision accepted")
	}
}

func TestConnectionSelectorOwnsBorrowedWitnesses(t *testing.T) {
	s, _ := newConnectionSelector(nil, nil)
	id, amount := "C00000001", int64(10)
	d := c.Decision{Ordinal: 1, ConduitID: &id}
	if err := s.observe(p.Row{Ordinal: 1, Recipient: &id, Amount: &amount}, &d); err != nil {
		t.Fatal(err)
	}
	id, amount, d.Ordinal = "C00000009", 99, 9
	w := s.first[2]
	if *w.Row.Recipient != "C00000001" || *w.Row.Amount != 10 || w.Decision.Ordinal != 1 || *w.Decision.ConduitID != "C00000001" {
		t.Fatal("borrowed source data retained")
	}
}

func selectedConnectionFixture(s *connectionSelector, row p.Row) Connection {
	v := Connection{ConnectionID: "fixture", Source: p.Inspection{Participant: row}, State: "unresolved_reported_recipient"}
	if row.Recipient != nil {
		v.State = "no_path_in_selected_receiver_cohort_not_terminal"
		if n, ok := s.nodes[*row.Recipient]; ok {
			v.State = "connected_observation_path_not_attributed_money"
			v.Authorization = json.RawMessage(`{}`)
			v.CommitteePath = make([]json.RawMessage, n.Hops)
		}
	}
	return v
}

func TestConnectionGateReplaysUniqueWitnessesAndFailsClosed(t *testing.T) {
	s := selectorFixture(t)
	for _, mode := range []string{"ok", "changed", "wrong_source", "financial", "terminal", "error", "cancelled", "wrong_path"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			calls := map[int64]int{}
			out, err := runConnectionCases(ctx, s, func(_ context.Context, row p.Row, _ *c.Decision) (Connection, error) {
				calls[row.Ordinal]++
				v := selectedConnectionFixture(s, row)
				switch mode {
				case "changed":
					if calls[row.Ordinal] == 2 {
						v.State = "changed"
					}
				case "wrong_source":
					v.Source.Participant.Ordinal++
				case "wrong_path":
					v.State = "unresolved_reported_recipient"
				case "financial":
					v.FinancialEligibility = true
				case "terminal":
					v.TerminalEligible = true
				case "error":
					return Connection{}, errors.New("source mismatch")
				}
				return v, nil
			})
			if mode != "ok" {
				if err == nil {
					t.Fatal("accepted", mode)
				}
				return
			}
			if err != nil || len(out.Connections) != 4 || len(out.Cases) != len(connectionCaseNames) {
				t.Fatal(out, err)
			}
			for _, n := range calls {
				if n != 2 {
					t.Fatal("did not replay exactly once per unique witness", calls)
				}
			}
			for _, c := range out.Cases {
				if c.State != "verified_and_replayed" || c.Ordinal == nil || c.ConnectionID == nil {
					t.Fatal(c)
				}
			}
		})
	}
}

func TestConnectionGateAbsentCategoriesAreExplicit(t *testing.T) {
	s, _ := newConnectionSelector(nil, nil)
	if err := s.observe(p.Row{Ordinal: 1}, nil); err != nil {
		t.Fatal(err)
	}
	out, err := runConnectionCases(context.Background(), s, func(_ context.Context, row p.Row, _ *c.Decision) (Connection, error) {
		return selectedConnectionFixture(s, row), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range out.Cases {
		if i == 3 || i == 11 {
			continue
		}
		if c.Population != 0 || c.Ordinal != nil || c.ConnectionID != nil || c.State != "not_present_in_complete_candidate_cycle_scope" {
			t.Fatal(c)
		}
	}
	for _, o := range []ConnectionGateOptions{
		{ConnectionOptions: ConnectionOptions{Candidate: "H0ZZ00001"}},
		{ConnectionOptions: ConnectionOptions{Ordinal: 1}},
		{ExpectedGateID: "invalid"},
	} {
		if _, err := ValidateConnections(context.Background(), o); err == nil {
			t.Fatal("manual scope accepted")
		}
	}
}
