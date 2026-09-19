package personaffiliation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func TestProseCitationRetainedOccurrenceAudit(t *testing.T) {
	// Expected failures in retained research evidence, never runtime matching rules.
	nonunique := map[string]map[string]string{
		"nvidia-huang":              {"m2": "ambiguous", "m3": "ambiguous", "m11": "not_found"},
		"salesforce-benioff":        {"m3": "ambiguous"},
		"fresh-targeted-correction": {"m5": "not_found"},
	}
	counts := map[string]int{}
	missingGeneration := 0
	for _, c := range proseInterpretationCases(t) {
		t.Run(c.ID, func(t *testing.T) {
			cat, err := newCitationCatalog(c)
			if err != nil {
				t.Fatal(err)
			}
			view, _ := json.Marshal(cat.Entries)
			plain, _ := json.Marshal(c.Entries)
			t.Logf("source entries JSON=%d bytes; token catalog JSON=%d bytes", len(plain), len(view))
			raw, err := os.ReadFile(filepath.Join(fixtureDir, interpretationModelFixture, c.ID+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var record modelRecord
			if err := strictjson.Decode(raw, &record); err != nil || !reflect.DeepEqual(c, record.Case) {
				t.Fatal("retained source case differs", err)
			}
			content, err := modelContent(record.Response)
			if err != nil {
				if c.ID != "apple-cook" || record.Failure != err.Error() || len(record.EvidenceAttachment) != 0 {
					t.Fatal("generation failure changed or partially salvaged", err)
				}
				missingGeneration++
				return
			}
			// The original strict validation still yields exactly the saved outcome.
			_, originalErr := inspectModelInterpretations(c, content)
			if (originalErr == nil) != (record.CitationCheck == "literal_citations_valid_semantics_unassessed") || (originalErr != nil && originalErr.Error() != record.Failure) {
				t.Fatal("original rejection/pass changed", originalErr)
			}
			var answer modelInterpretations
			if err := strictjson.Decode(content, &answer); err != nil {
				t.Fatal(err)
			}
			foundExceptions := 0
			for _, mention := range answer.Mentions {
				options, err := cat.literalOptions(mention.Entry, mention.Text)
				if err != nil {
					t.Fatal(err)
				}
				want := "unique"
				if state, exists := nonunique[c.ID][mention.ID]; exists {
					want = state
					foundExceptions++
				}
				if options.State != want {
					t.Fatal("literal diagnosis changed", mention.ID, options.State, want)
				}
				counts[options.State]++
				for _, option := range options.Matches {
					if option.Range == nil {
						t.Fatal("retained exact mention is unexpectedly inside a token", mention.ID)
					}
					quote, err := cat.selectRange(*option.Range)
					if err != nil || quote != option.Evidence {
						t.Fatal("range altered the saved literal text", err)
					}
				}
				if want == "ambiguous" && len(options.Matches) != 2 {
					t.Fatal("lost repeated occurrence")
				}
			}
			if foundExceptions != len(nonunique[c.ID]) {
				t.Fatal("expected diagnosis omitted")
			}
		})
	}
	if missingGeneration != 1 || counts["ambiguous"] != 3 || counts["not_found"] != 2 {
		t.Fatal("retained failure coverage changed", missingGeneration, counts)
	}
	t.Logf("literal-query diagnostics only: counts=%v; incomplete generations=%d; no model answer repaired", counts, missingGeneration)
}

func TestProseCitationRetainedEntryGrounding(t *testing.T) {
	for _, c := range proseInterpretationCases(t) {
		if c.ID != "fresh-bound-pronoun" && c.ID != "fresh-targeted-correction" {
			continue
		}
		t.Run(c.ID, func(t *testing.T) {
			cat, err := newCitationCatalog(c)
			if err != nil {
				t.Fatal(err)
			}
			// Independently reviewed selections over retained source entries, not
			// a conversion or repaired version of the earlier model response.
			annotated := mentionInterpretationInput{Origin: "reviewed_fixture", Links: []contextProposal{}}
			if c.ID == "fresh-bound-pronoun" {
				annotated.Mentions = []mentionProposal{
					{ID: "person", Entry: 0, Text: "Elena Marín"}, {ID: "organization", Entry: 0, Text: "Willow Instruments"},
					{ID: "pronoun", Entry: 1, Text: "She"},
				}
				annotated.Interpretations = []interpretationProposal{{ID: "ctoproposal", Clause: "pronoun", Kind: "role", Normalized: "chief technology officer", Role: "executive", Subjects: []string{"person"}, Organizations: []string{"organization"}, Evidence: []string{"pronoun", "person", "organization"}, Binding: "coreference", Status: "asserted"}}
			} else {
				annotated.Mentions = []mentionProposal{
					{ID: "person", Entry: 0, Text: "Daniel Moss"}, {ID: "company", Entry: 0, Text: "Briar Systems"},
					{ID: "ceoclause", Entry: 0, Text: c.Entries[0].Text}, {ID: "directorclause", Entry: 1, Text: c.Entries[1].Text},
					{ID: "foundation", Entry: 1, Text: "Lake Foundation"}, {ID: "correction", Entry: 2, Text: c.Entries[2].Text},
				}
				annotated.Interpretations = []interpretationProposal{
					{ID: "ceoproposal", Clause: "ceoclause", Kind: "role", Normalized: "CEO", Role: "executive", Subjects: []string{"person"}, Organizations: []string{"company"}, Evidence: []string{"ceoclause", "person", "company"}, Binding: "direct", Status: "asserted"},
					{ID: "directorproposal", Clause: "directorclause", Kind: "role", Normalized: "director", Role: "board_director", Subjects: []string{"person"}, Organizations: []string{"foundation"}, Evidence: []string{"directorclause", "person", "foundation"}, Binding: "coreference", Status: "asserted"},
					{ID: "notice", Clause: "correction", Kind: "correction", Normalized: "withdrawal", Subjects: []string{}, Organizations: []string{}, Evidence: []string{"correction"}, Binding: "unresolved", Status: "asserted"},
				}
				annotated.Links = []contextProposal{{From: "notice", To: "ceoproposal", Kind: "retracts"}}
			}
			selections := reviewedCitationSelections(t, cat, annotated)
			raw, _ := json.Marshal(selections)
			out, err := inspectCitationSelections(cat, raw, "reviewed_fixture")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(out.Review.Context, c.Entries) || out.Review.IdentityApproved || out.Review.GraphPublicationApproved || out.Review.FinancialAttribution {
				t.Fatal("context or approvals changed")
			}
			if c.ID == "fresh-bound-pronoun" {
				if !reflect.DeepEqual(out.Contexts[0].Entries, c.Entries[:2]) || out.Review.Mentions[2].Evidence.Matched.Text != "She" || out.Review.Interpretations[0].State != "unverified_role_interpretation" {
					t.Fatal("short span erased the full role/date context or gained approval")
				}
			} else if out.Review.Mentions[3].Evidence.Matched.Text != c.Entries[1].Text || out.Review.Interpretations[0].State != "context_blocked" || out.Review.Interpretations[1].State != "unverified_role_interpretation" || !reflect.DeepEqual(out.Contexts[2].Entries, c.Entries[2:3]) {
				t.Fatal("source-owned director text or correction boundary changed")
			}
		})
	}
}
