package organizationresolution

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func observation(t *testing.T) wikimedia.Observation {
	t.Helper()
	b, err := os.ReadFile("../../source/wikimedia/testdata/search.json")
	if err != nil {
		t.Fatal(err)
	}
	p, err := wikimedia.ParseSearch(b)
	if err != nil {
		t.Fatal(err)
	}
	s, err := os.ReadFile("../../source/wikimedia/testdata/entities.json")
	if err != nil {
		t.Fatal(err)
	}
	e, err := wikimedia.ParseEntities(s, wikimedia.PageIDs(p))
	if err != nil {
		t.Fatal(err)
	}
	return wikimedia.Observation{Query: wikimedia.Query{Text: "EXAMPLE CORP", References: []wikimedia.Reference{{FactSetID: "fixture", ManifestSHA256: wikimedia.Hash([]byte("source")), FactID: "fact", Field: "CONNECTED_ORG_NM"}}}, SearchSHA256: wikimedia.Hash(b), EntitiesSHA256: wikimedia.Hash(s), Pages: p, Entities: e}
}

func TestNormalizationDoesNotExpandIdentity(t *testing.T) {
	if Normalize("  Example, Corp. ") != "EXAMPLE CORP" {
		t.Fatal("format normalization")
	}
	for _, p := range [][2]string{{"ACME", "ACME INC"}, {"ACME", "ACNE"}, {"A B", "AB"}, {"ABC Holdings", "Holdings ABC"}, {"Café", "Cafe"}} {
		if Normalize(p[0]) == Normalize(p[1]) {
			t.Fatal("unsupported identity equivalence", p)
		}
	}
}

func TestNameProposalsNeverBecomeIdentityOrMoney(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		change      func(*wikimedia.Observation)
		proposed    string
	}{
		{"exact_alias", "single_exact_name_candidate_in_search_window", func(*wikimedia.Observation) {}, "Q100"},
		{"typo", "no_exact_name_candidate", func(o *wikimedia.Observation) { o.Query.Text = "Exampel Corp" }, ""},
		{"suffix", "no_exact_name_candidate", func(o *wikimedia.Observation) { o.Query.Text = "Example" }, ""},
		{"ambiguous", "ambiguous_exact_name_candidates", func(o *wikimedia.Observation) {
			e := o.Entities["Q100"]
			e.ID = "Q200"
			o.Entities[e.ID] = e
			p := o.Pages[0]
			p.ID = 2
			p.Index = 2
			p.Props = map[string]string{"wikibase_item": "Q200"}
			o.Pages = append(o.Pages, p)
		}, ""},
		{"human", "no_exact_name_candidate", func(o *wikimedia.Observation) {
			e := o.Entities["Q100"]
			e.Claims["P31"] = json.RawMessage(`[{"rank":"normal","mainsnak":{"property":"P31","snaktype":"value","datavalue":{"type":"wikibase-entityid","value":{"id":"Q5"}}}}]`)
			o.Entities[e.ID] = e
		}, ""},
		{"disambiguation", "no_exact_name_candidate", func(o *wikimedia.Observation) { o.Pages[0].Props["disambiguation"] = "" }, ""},
		{"missing", "candidate_evidence_incomplete", func(o *wikimedia.Observation) {
			o.Entities["Q100"] = wikimedia.Entity{ID: "Q100", Missing: json.RawMessage(`true`)}
		}, ""},
		{"no_qid", "candidate_evidence_incomplete", func(o *wikimedia.Observation) {
			p := o.Pages[0]
			p.ID = 2
			p.Index = 2
			p.Props = nil
			o.Pages = append(o.Pages, p)
		}, ""},
		{"error", "source_incomplete", func(o *wikimedia.Observation) { o.Issue = "maxlag" }, ""},
		{"no_hits", "no_search_results", func(o *wikimedia.Observation) { o.Pages = nil; o.Entities = nil }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := observation(t)
			tc.change(&o)
			input := wikimedia.Replay{CaptureSHA256: wikimedia.Hash([]byte("capture")), Observations: []wikimedia.Observation{o}}
			r := Resolve(input, wikimedia.Hash([]byte("build")))
			if r.Decisions[0].State != tc.state || r.Decisions[0].ProposedQID != tc.proposed {
				t.Fatalf("wrong decision: %+v", r.Decisions[0])
			}
			if r.IdentityResolved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
				t.Fatal("proposal became financial/identity authority")
			}
			if !reflect.DeepEqual(r.Decisions[0].Evidence, o) {
				t.Fatal("evidence lost")
			}
			if !reflect.DeepEqual(r, Resolve(input, r.BuildSHA256)) {
				t.Fatal("nondeterministic replay")
			}
			if (o.Issue == "") != r.Complete {
				t.Fatal("capture usability lost")
			}
		})
	}
}

func TestDeprecatedHumanClaimIsNotCurrentType(t *testing.T) {
	o := observation(t)
	e := o.Entities["Q100"]
	e.Claims["P31"] = json.RawMessage(`[{"rank":"deprecated","mainsnak":{"property":"P31","snaktype":"value","datavalue":{"type":"wikibase-entityid","value":{"id":"Q5"}}}}]`)
	if human(e) {
		t.Fatal("deprecated claim treated as type")
	}
}
