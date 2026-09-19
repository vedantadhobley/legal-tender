package fundinggeneration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
)

type spendingStub struct {
	members     []ie.DatedMember
	err         error
	badReadback bool
	reads       int
}

func (s *spendingStub) VisitDatedMembers(_ context.Context, visit func(ie.DatedMember) error) error {
	for _, m := range s.members {
		if err := visit(m); err != nil {
			return err
		}
	}
	return s.err
}
func (s *spendingStub) DatedMemberEvidence(_ context.Context, want []ie.DatedMember) (map[string]graphread.Item, error) {
	s.reads++
	out := map[string]graphread.Item{}
	for _, m := range want {
		b, _ := json.Marshal(m)
		out[m.FactID] = graphread.Item{Key: m.FactID, Document: b, Evidence: b}
	}
	if s.badReadback {
		return nil, nil
	}
	return out, s.err
}
func (s *spendingStub) Entity(context.Context, string) (graphread.Facet, error) {
	return graphread.Facet{State: "fixture"}, s.err
}

func spendingFixture(t *testing.T) (*WindowReader, WindowConnectionQuery) {
	r, q := connectionFixtureReader(t)
	q.From, q.ReceiptOrdinal, q.EntryFamily, q.EntryGeneration = "C00000003", 0, "", ""
	q.Ending, q.SpendingDate, q.MaxHops = "independent_support", "expenditure", 0
	q.Window = &DateWindow{Start: "2023-01-01", End: "2023-01-01"}
	date, _, err := q.Window.bounds()
	if err != nil {
		t.Fatal(err)
	}
	amount := "725"
	for i := range r.partitions {
		p := &r.partitions[i]
		p.publication.Generation.OutsideSpending.Inputs.ScheduleEFactSetID = valueID("E-facts-" + p.publication.GenerationID)
		id := valueID("member-" + p.publication.GenerationID)
		m := ie.DatedMember{FactID: id, DecisionID: valueID("decision-" + id), EffectiveState: "included", ResolutionState: "confirmed", Stance: "S", Amount: &amount, ExpenditureDate: &date,
			Link:   &graphread.Link{Family: "independent_support_observation", Key: id, From: q.From, To: q.Target},
			Parent: &graphread.Link{Family: q.Ending, Key: valueID("parent-" + id), From: q.From, To: q.Target}}
		p.outside = &spendingStub{members: []ie.DatedMember{m}}
	}
	return r, q
}

func TestSpendingWindowExplicitBasisAndBatchReadback(t *testing.T) {
	r, q := spendingFixture(t)
	out, err := r.ConnectionPaths(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Paths) != 2 || len(out.SpendingCoverage) != 2 {
		t.Fatal("lost publications")
	}
	for _, p := range r.partitions {
		if p.outside.(*spendingStub).reads != 1 {
			t.Fatal("source evidence was not batched")
		}
	}
	q.SpendingDate = "dissemination"
	out, err = r.ConnectionPaths(context.Background(), q)
	if err != nil || len(out.Paths) != 0 {
		t.Fatal("filled missing dissemination date", err)
	}
	for _, c := range out.SpendingCoverage {
		if c.Buckets[0].Selection != "unknown_date_excluded" {
			t.Fatal("unknown date dropped")
		}
	}
	q.Window = nil
	out, err = r.ConnectionPaths(context.Background(), q)
	if err != nil || len(out.Paths) != 2 {
		t.Fatal("unbounded unknown members dropped", err)
	}
	for _, link := range out.Links {
		if link.Date != nil {
			t.Fatal("invented date")
		}
	}
	for _, basis := range []string{"", "cycle", "either", "EXpenditure"} {
		q.SpendingDate = basis
		if q.Validate() == nil {
			t.Fatal("accepted ambiguous date basis", basis)
		}
	}
}

func TestSpendingWindowFailsClosed(t *testing.T) {
	for _, mutate := range []func(*WindowReader, *WindowConnectionQuery){
		func(r *WindowReader, _ *WindowConnectionQuery) { r.partitions[1].outside = nil },
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[1].outside.(*spendingStub).err = errors.New("corrupt source after callbacks")
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[1].outside.(*spendingStub).badReadback = true
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[1].publication.Generation.OutsideSpending.Inputs.ScheduleEFactSetID = r.partitions[0].publication.Generation.OutsideSpending.Inputs.ScheduleEFactSetID
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[1].outside.(*spendingStub).members = r.partitions[0].outside.(*spendingStub).members
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			s := "7.25"
			r.partitions[0].outside.(*spendingStub).members[0].Amount = &s
		},
		func(r *WindowReader, _ *WindowConnectionQuery) {
			r.partitions[0].outside.(*spendingStub).members[0].Parent = nil
		},
		func(_ *WindowReader, q *WindowConnectionQuery) { q.Ending = "candidate_authorization_context" },
	} {
		r, q := spendingFixture(t)
		mutate(r, &q)
		if out, err := r.ConnectionPaths(context.Background(), q); err == nil || out.ResultID != "" {
			t.Fatal("accepted inconsistent spending source")
		}
	}
}

func TestExistingWindowConnectionJSONRemainsV1(t *testing.T) {
	r, q := connectionFixtureReader(t)
	out, err := r.ConnectionPaths(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if out.SchemaVersion != WindowConnectionsVersion || out.Policy != WindowConnectionsPolicy ||
		strings.Contains(string(b), "spending_date_field") || strings.Contains(string(b), "spending_source_coverage") || strings.Contains(string(b), "outside_spending_facet") {
		t.Fatal("additive spending fields changed old contract")
	}
}
