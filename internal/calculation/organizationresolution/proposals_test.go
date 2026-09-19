package organizationresolution

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestExpandedNameRules(t *testing.T) {
	for _, tc := range []struct{ query, name, rule string }{
		{"  Example, Corp. ", "EXAMPLE CORP", "normalized_exact"},
		{"Local 42 Health Workers", "Local42 Health Workers", "digit_letter_boundaries"},
		{"AB12CD", "AB 12 CD", "digit_letter_boundaries"},
		{"Route 7B", "Route7 B", "digit_letter_boundaries"},
		{"Café2", "Café 2", "digit_letter_boundaries"},
		{"Cafe\u03012", "Cafe\u0301 2", "digit_letter_boundaries"},
		{"Example Corporation", "Example Corp.", "legal_suffix_variant"},
		{"Example Incorporated", "Example Inc", "legal_suffix_variant"},
		{"Example Ltd", "Example Limited", "legal_suffix_variant"},
		{"Example LLC", "Example", "legal_suffix_variant"},
		{"Example Corporation", "Example", "legal_suffix_variant"},
		{"A Corporation", "A", "legal_suffix_variant"}, // No arbitrary length threshold.
		{"Route 7B Corp", "Route7 B", "digit_letter_boundaries_and_legal_suffix_variant"},
	} {
		t.Run(tc.query+"/"+tc.name, func(t *testing.T) {
			m, ok := matchName(tc.query, tc.name)
			if !ok || m.Rule != tc.rule || m.Query.Comparison != m.Candidate.Comparison || m.Query.Normalized != Normalize(tc.query) || m.Candidate.Normalized != Normalize(tc.name) {
				t.Fatalf("missing or unexplained match: %+v", m)
			}
			reverse, ok := matchName(tc.name, tc.query)
			if !ok || reverse.Rule != m.Rule || reverse.Query != m.Candidate || reverse.Candidate != m.Query {
				t.Fatal("asymmetric name rule", reverse)
			}
		})
	}
	m, _ := matchName("Route7 B Corporation", "Route 7B")
	if m.Query.RemovedLegalSuffix != "CORPORATION" || m.Candidate.RemovedLegalSuffix != "" || !m.Query.DigitLetterBoundaries || !m.Candidate.DigitLetterBoundaries || m.Query.Comparison != "ROUTE 7 B" {
		t.Fatal("transformations not recorded", m)
	}
}

func TestExpandedNameNonEquivalences(t *testing.T) {
	for _, pair := range [][2]string{
		{"", ""}, {"!!!", "!!!"}, {"AB C", "A BC"}, {"AB12", "A B12"},
		{"Café", "Cafe"}, {"Example", "Exampel"}, {"Example Bank", "Bank Example"},
		{"Example Holdings", "Example"}, {"Example Group", "Example"},
		{"Example Bank Corp", "Example Corp"}, {"Example Foundation", "Example"},
		{"Example PAC", "Example"}, {"Example Company", "Example"},
		{"Example LLC", "Example Corp"}, {"Example Ltd", "Example Inc"},
		{"Example Corp", "Example Incorporated"},
		{"ExampleInc", "Example"}, {"Example L.L.C.", "Example"},
		{"Example Inc Corp", "Example"}, {"Corporation Example", "Example"},
		{"Corp", "Corporation"}, {"Corp", "Inc"}, {"123 Corp", "123"},
		{"INC", ""}, {"Example North", "Example South"},
	} {
		if m, ok := matchName(pair[0], pair[1]); ok {
			t.Fatal("unsupported name match", pair, m)
		}
	}
}

func TestExpandedPolicyPreservesGuardsBaselineAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, state, proposed string
		change                func(*wikimedia.Observation)
	}{
		{"suffix", "single_name_candidate_in_search_window", "Q100", func(*wikimedia.Observation) {}},
		{"human", "no_name_candidate", "", func(o *wikimedia.Observation) {
			e := o.Entities["Q100"]
			e.Claims["P31"] = json.RawMessage(`[{"rank":"normal","mainsnak":{"property":"P31","snaktype":"value","datavalue":{"type":"wikibase-entityid","value":{"id":"Q5"}}}}]`)
			o.Entities[e.ID] = e
		}},
		{"disambiguation", "no_name_candidate", "", func(o *wikimedia.Observation) { o.Pages[0].Props["disambiguation"] = "" }},
		{"missing", "candidate_evidence_incomplete", "", func(o *wikimedia.Observation) {
			o.Entities["Q100"] = wikimedia.Entity{ID: "Q100", Missing: json.RawMessage(`true`)}
		}},
		{"absent_metadata", "candidate_evidence_incomplete", "", func(o *wikimedia.Observation) { delete(o.Entities, "Q100") }},
		{"no_qid", "candidate_evidence_incomplete", "", func(o *wikimedia.Observation) { o.Pages = append(o.Pages, wikimedia.Page{ID: 2, Title: "Example"}) }},
		{"source_error", "source_incomplete", "", func(o *wikimedia.Observation) { o.Issue = "http_status_not_ok" }},
		{"no_hits", "no_search_results", "", func(o *wikimedia.Observation) { o.Pages = nil; o.Entities = nil }},
		{"misspelling", "no_name_candidate", "", func(o *wikimedia.Observation) { o.Query.Text = "Exampel" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := observation(t)
			o.Query.Text = "Example"
			tc.change(&o)
			input := wikimedia.Replay{CaptureSHA256: wikimedia.Hash([]byte("capture")), Observations: []wikimedia.Observation{o}}
			before, _ := json.Marshal(input)
			build := wikimedia.Hash([]byte("build"))
			base := Resolve(input, build)
			for _, policy := range []string{"", Policy} {
				r, err := ResolveWithPolicy(input, build, policy)
				if err != nil || !reflect.DeepEqual(r, base) {
					t.Fatal("v1/default changed", err)
				}
			}
			r, err := ResolveWithPolicy(input, build, ExpandedPolicy)
			if err != nil {
				t.Fatal(err)
			}
			d := r.Decisions[0]
			if d.State != tc.state || d.ProposedQID != tc.proposed || r.Policy != ExpandedPolicy || r.Complete != base.Complete {
				t.Fatal("incorrect v2 decision", d)
			}
			wantBase := &ProposalBaseline{Policy: Policy, State: base.Decisions[0].State, ProposedQID: base.Decisions[0].ProposedQID}
			if !reflect.DeepEqual(d.Baseline, wantBase) || !reflect.DeepEqual(d.Evidence, o) {
				t.Fatal("baseline or source evidence changed")
			}
			if r.IdentityResolved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
				t.Fatal("name proposal approved a relationship")
			}
			after, _ := json.Marshal(input)
			if string(before) != string(after) {
				t.Fatal("input mutated")
			}
			again, err := ResolveWithPolicy(input, build, ExpandedPolicy)
			if err != nil || !reflect.DeepEqual(r, again) {
				t.Fatal("nondeterministic replay", err)
			}
		})
	}
	if _, err := ResolveWithPolicy(wikimedia.Replay{}, "", "latest"); err == nil {
		t.Fatal("unknown policy accepted")
	}
}

func TestExpandedRivalBlocksExactWinnerAndMatchesAreNotVotes(t *testing.T) {
	o := observation(t)
	o.Query.Text = "Example"
	// This candidate has an exact alias, a broader label and a broader title.
	e := o.Entities["Q100"]
	e.Aliases["en"] = []wikimedia.Term{{Language: "en", Value: "Example"}, {Language: "en", Value: "Example"}, {Language: "en", Value: "Example Corp"}}
	o.Entities[e.ID] = e
	replay := wikimedia.Replay{Observations: []wikimedia.Observation{o}}
	r, _ := ResolveWithPolicy(replay, "build", ExpandedPolicy)
	d := r.Decisions[0]
	if d.ProposedQID != "Q100" || len(d.Candidates) != 1 || len(d.Candidates[0].Matches) != 4 || len(d.Candidates[0].ExactNames) != 1 {
		t.Fatal("name observations counted as separate candidates", d)
	}
	// A different QID matches only after suffix removal. Do not silently retain
	// the exact v1 winner when v2 exposes a rival in the same search window.
	e = wikimedia.Entity{ID: "Q200", Type: "item", Labels: map[string]wikimedia.Term{"en": {Value: "Example Incorporated"}}}
	o.Entities[e.ID] = e
	o.Pages = append(o.Pages, wikimedia.Page{ID: 2, Title: "Example Incorporated", Props: map[string]string{"wikibase_item": e.ID}})
	replay.Observations[0] = o
	r, _ = ResolveWithPolicy(replay, "build", ExpandedPolicy)
	d = r.Decisions[0]
	if d.State != "ambiguous_name_candidates" || d.ProposedQID != "" || d.Baseline.ProposedQID != "Q100" || len(d.Candidates) != 2 || len(d.Candidates[1].Matches) != 2 {
		t.Fatal("rival suppressed", d)
	}
	// Match explanations have stable ordering even if aliases/pages are reordered.
	want := d.Candidates
	slices.Reverse(o.Pages)
	e = o.Entities["Q100"]
	slices.Reverse(e.Aliases["en"])
	o.Entities[e.ID] = e
	replay.Observations[0] = o
	r, _ = ResolveWithPolicy(replay, "build", ExpandedPolicy)
	if !reflect.DeepEqual(want, r.Decisions[0].Candidates) {
		t.Fatal("order-dependent candidates")
	}
}
