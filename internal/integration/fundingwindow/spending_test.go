package fundingwindow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func testSpendingWindows(t *testing.T, ctx context.Context, r, reopened *gen.WindowReader, first, second publication, root, spec, sha string) {
	t.Helper()
	base := gen.WindowConnectionQuery{PathQuery: gen.PathQuery{From: "C00000002", Target: "H0CA00001", Ledger: flow.ScheduleA,
		Ending: "independent_support", MaxHops: 0, Limit: 10, Budget: 100}, SpendingDate: "expenditure",
		Window: &gen.DateWindow{Start: "2023-01-01", End: "2023-01-01"}}
	for _, tc := range []struct {
		name, basis, ending, day string
		suffixes                 []string
		amount                   string
	}{
		{"expenditure_day", "expenditure", "independent_support", "2023-01-01", []string{"100", "106", "107", "110", "111"}, "1900"},
		{"other_expenditure_day", "expenditure", "independent_support", "2023-01-02", []string{"101"}, "1200"},
		{"dissemination_day", "dissemination", "independent_support", "2023-01-01", []string{"101", "105", "106", "107", "110", "111"}, "2675"},
		{"other_dissemination_day", "dissemination", "independent_support", "2023-01-02", []string{"100"}, "725"},
		{"opposition", "expenditure", "independent_opposition", "2023-01-01", []string{"102"}, "200"},
		{"all_supplied_dates", "expenditure", "independent_support", "", []string{"100", "101", "105", "106", "107", "110", "111"}, "3400"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := base
			q.SpendingDate, q.Ending = tc.basis, tc.ending
			q.Window = nil
			if tc.day != "" {
				q.Window = &gen.DateWindow{Start: tc.day, End: tc.day}
			}
			out, err := r.ConnectionPaths(ctx, q)
			check(t, err)
			if out.SchemaVersion != gen.WindowSpendingConnectionsVersion || out.Policy != gen.WindowSpendingConnectionsPolicy ||
				len(out.Paths) != len(tc.suffixes) || len(out.Links) != len(tc.suffixes) || len(out.Contexts) != 0 ||
				len(out.SpendingCoverage) != 2 || out.FinancialEligibility || out.TerminalEligible {
				t.Fatalf("wrong source-grain result: %+v", out)
			}
			want := map[string]bool{}
			for _, s := range tc.suffixes {
				want["2024"+s] = true
			}
			var sum int64
			for _, link := range out.Links {
				var m ie.DatedMember
				var e ie.MemberEvidence
				check(t, json.Unmarshal(link.Evidence.Document, &m))
				check(t, json.Unmarshal(link.Evidence.Evidence, &e))
				key := e.Source.TypedFields.Filing.SubmissionID
				if !want[key] {
					t.Fatal("wrong source occurrence", key)
				}
				delete(want, key)
				if m.Link == nil || *m.Link != link.Topology || link.Topology.Family != q.Ending+"_observation" ||
					link.Topology.Key != e.Source.FactID || e.Decision.FactID != e.Source.FactID || e.Decision.DecisionID != m.DecisionID ||
					m.Amount == nil || *m.Amount != e.Decision.AmountMinorUnits || m.Parent.Key != e.Parent.Key ||
					link.GenerationID != second.generation.GenerationID || e.Source.Cycle != "2024" {
					t.Fatal("lost exact source/decision/parent/generation identity")
				}
				var amount int64
				check(t, json.Unmarshal([]byte(*m.Amount), &amount))
				sum += amount
				var parent struct {
					Amount string `json:"amount_minor_units"`
					Count  uint64 `json:"expenditure_count"`
				}
				check(t, json.Unmarshal(e.Parent.Document, &parent))
				parentAmount, count := "3400", uint64(7)
				if q.Ending == "independent_opposition" {
					parentAmount, count = "200", 1
				}
				if parent.Amount != parentAmount || parent.Count != count {
					t.Fatal("parent aggregate was window-filtered or lost")
				}
				date := e.Source.TypedFields.Expenditure.ExpenditureOn
				if q.SpendingDate == "dissemination" {
					date = e.Source.TypedFields.Expenditure.DisseminatedOn
				}
				if date == nil {
					if link.Date != nil || q.Window != nil {
						t.Fatal("unknown date filled or bounded selection admitted")
					}
				} else {
					d, err := time.Parse("2006-01-02", *date)
					check(t, err)
					if link.Date == nil || *link.Date != int32(d.Unix()/86400) {
						t.Fatal("wrong date field")
					}
				}
				if link.TemporalBasis != "schedule_e_"+q.SpendingDate+"_reported_date" {
					t.Fatal("unidentified date basis")
				}
				if strings.HasSuffix(key, "110") && e.Decision.State != "resolved" {
					t.Fatal("lost candidate resolution")
				}
				if strings.HasSuffix(key, "111") && e.Decision.State != "unverified" {
					t.Fatal("promoted candidate confidence")
				}
			}
			var expected int64
			check(t, json.Unmarshal([]byte(tc.amount), &expected))
			if sum != expected || len(want) != 0 {
				t.Fatal("member money not conserved")
			}
			for _, c := range out.SpendingCoverage {
				var rows, memo, unknownAmount, unrouted, unresolved uint64
				for _, b := range c.Buckets {
					rows += b.Rows
					if b.EffectiveState == "excluded_memo" {
						memo += b.Rows
						if b.Amount != "9900" || b.Projectable {
							t.Fatal("memo policy changed")
						}
					}
					if b.EffectiveState == "unresolved_amount" {
						unknownAmount += b.Rows
						if b.KnownAmountRows != 0 || b.Projectable {
							t.Fatal("invented amount")
						}
					}
					if len(b.RouteReasons) > 0 {
						unrouted += b.Rows
						if b.Amount != "400" || b.Projectable {
							t.Fatal("lost route exception")
						}
					}
					if b.ResolutionState == "unresolved" {
						unresolved += b.Rows
						if b.Amount != "600" || b.Projectable {
							t.Fatal("lost candidate exception")
						}
					}
				}
				if c.SourceFacts != 12 || rows != 12 || memo != 1 || unknownAmount != 1 || unrouted != 1 || unresolved != 1 {
					t.Fatal("source coverage dropped exceptions", c)
				}
			}
			for _, v := range out.Vertices {
				for _, f := range v.Facets {
					if f.OutsideSpending == nil {
						t.Fatal("outside facet omitted")
					}
				}
			}
			again, err := reopened.ConnectionPaths(ctx, q)
			check(t, err)
			if !reflect.DeepEqual(out, again) {
				t.Fatal("fresh reversed publication replay changed")
			}
		})
	}
	t.Run("cross_publication_receipt_to_spending", func(t *testing.T) {
		q := base
		q.From, q.EntryFamily, q.ReceiptOrdinal, q.EntryGeneration = "", "reported_receipt", 1, first.generation.GenerationID
		q.Window = &gen.DateWindow{Start: "2022-12-31", End: "2023-01-01"}
		for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
			q.Ledger = ledger
			out, err := r.ConnectionPaths(ctx, q)
			check(t, err)
			if len(out.Paths) != 5 || out.Entry.GenerationID != first.generation.GenerationID {
				t.Fatal("lost cross-publication receipt entry")
			}
			for _, p := range out.Paths {
				if len(p.Links) != 2 {
					t.Fatal("unexpected path grain")
				}
			}
		}
	})
	t.Run("spending_cli_fresh_replay", func(t *testing.T) {
		t.Setenv("LT_WINDOW_FIXTURE_PASSWORD", "fixture")
		args := []string{"pipeline", "fec", "inspect-funding-window-connections", "--inputs", spec, "--expected-inputs-sha256", sha,
			"--storage-root", root, "--endpoint", fixtureEndpoint, "--username", "fixture", "--password-env", "LT_WINDOW_FIXTURE_PASSWORD",
			"--from-committee", base.From, "--target", base.Target, "--ledger", "schedule_a", "--ending-family", base.Ending,
			"--spending-date-field", base.SpendingDate, "--start-date", base.Window.Start, "--end-date", base.Window.End,
			"--max-committee-hops", "0", "--max-paths", "10", "--max-expansions", "100"}
		want, err := r.ConnectionPaths(ctx, base)
		check(t, err)
		var out, diagnostic bytes.Buffer
		if code := cli.Run(append(args, "--expected-result-id", want.ResultID), &out, &diagnostic); code != 0 {
			t.Fatal(code, diagnostic.String())
		}
		var got gen.WindowConnectionsResult
		check(t, json.Unmarshal(out.Bytes(), &got))
		b, err := json.Marshal(got)
		check(t, err)
		w, err := json.Marshal(want)
		check(t, err)
		if !bytes.Equal(b, w) {
			t.Fatal("CLI source-member result differs")
		}
		out.Reset()
		diagnostic.Reset()
		if code := cli.Run(append(args, "--expected-result-id", digest([]byte("wrong"))), &out, &diagnostic); code == 0 || out.Len() != 0 {
			t.Fatal("CLI emitted unverified result")
		}
	})
	t.Run("pinned_spending_source_corruption", func(t *testing.T) {
		id := second.generation.OutsideSpending.Inputs.ScheduleEFactSetID
		m, _, err := occ.LoadPublishedScheduleEFactManifest(ctx, root, filepath.Join(root, "facts/fec/schedule-e/manifests", id+".json"))
		check(t, err)
		p := filepath.Join(root, m.Facts.StorageKey)
		original, err := os.ReadFile(p)
		check(t, err)
		defer func() { check(t, os.WriteFile(p, original, 0600)) }()
		check(t, os.WriteFile(p, append(append([]byte{}, original...), byte(1)), 0600))
		q := base
		// Even an empty date window must verify excluded source backing.
		q.Window = &gen.DateWindow{Start: "1900-01-01", End: "1900-01-01"}
		if out, err := r.ConnectionPaths(ctx, q); err == nil || out.ResultID != "" {
			t.Fatal("corrupt excluded spending source accepted")
		}
	})
}
