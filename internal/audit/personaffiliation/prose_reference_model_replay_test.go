package personaffiliation

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func TestProseReferenceModelRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, referenceModelFixture)
	read := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	compact := func(raw []byte) []byte {
		t.Helper()
		var b bytes.Buffer
		if err := json.Compact(&b, raw); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	var review struct {
		Profile  modelProfile
		Metadata map[string]string
		Cases    []struct {
			ID, SHA256, Selection, Binding, Roles, ContextTime string
			Valid                                              bool
		}
	}
	if err := strictjson.Decode(read("review.json"), &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) {
		t.Fatal("trial profile changed")
	}
	for _, name := range []string{"models.json", "completed.json", "inputs.json"} {
		if hash(read(name)) != review.Metadata[name] {
			t.Fatal("trial metadata or pre-run expectations changed", name)
		}
	}
	if err := checkModelProfile(read("models.json"), review.Profile); err != nil {
		t.Fatal(err)
	}
	var completed struct {
		Model                    string
		Profile                  modelProfile
		CaseCount                int    `json:"case_count"`
		PromptSHA                string `json:"prompt_sha256"`
		ObservedAt               string `json:"observed_at"`
		IdentityApproved         bool   `json:"identity_approved"`
		GraphPublicationApproved bool   `json:"graph_publication_approved"`
		FinancialAttribution     bool   `json:"financial_attribution"`
	}
	if err := strictjson.Decode(read("completed.json"), &completed); err != nil {
		t.Fatal(err)
	}
	cases := proseReferenceModelCases(t)
	if completed.Model != review.Profile.Model || completed.Profile != review.Profile || completed.PromptSHA != hash([]byte(proseReferenceModelPrompt)) || completed.CaseCount != len(cases) || len(review.Cases) != len(cases) || completed.IdentityApproved || completed.GraphPublicationApproved || completed.FinancialAttribution {
		t.Fatal("completion marker or task changed")
	}
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			raw := read(want.ID + ".json")
			if hash(raw) != want.SHA256 || want.Selection == "" || want.Binding == "" || want.Roles == "" || want.ContextTime == "" {
				t.Fatal("capture changed or semantic review missing")
			}
			var r modelRecord
			if err := strictjson.Decode(raw, &r); err != nil {
				t.Fatal(err)
			}
			request, err := proseReferenceModelRequest(cases[i], review.Profile)
			if err != nil || !bytes.Equal(compact(r.Request), request) || !reflect.DeepEqual(r.Case, cases[i]) || want.ID != r.Case.ID {
				t.Fatal("source, schema or prompt changed", err)
			}
			if r.HTTPStatus != 200 || len(r.Response) == 0 {
				if want.Valid || r.CitationCheck != "not_checked" || r.Failure == "" || len(r.EvidenceAttachment) != 0 {
					t.Fatal("transport failure yielded evidence")
				}
				return
			}
			content, err := modelContent(r.Response)
			var derived json.RawMessage
			if err == nil {
				// Selection-only diagnostics include rejected answers, but never
				// supply a repaired/partial interpretation report.
				var proposal derivedReferenceInput
				if strictjson.Decode(content, &proposal) == nil {
					cat, catalogErr := newCitationCatalog(cases[i])
					if catalogErr != nil {
						t.Fatal(catalogErr)
					}
					for _, s := range proposal.Selections {
						if s.Entry == nil || s.First == nil || s.Last == nil {
							t.Logf("selection-only diagnostic %s: missing coordinates", s.ID)
							continue
						}
						quote, rangeErr := cat.selectRange(citationRange{Entry: *s.Entry, First: *s.First, Last: *s.Last})
						t.Logf("selection-only diagnostic %s: %q (error=%v)", s.ID, quote.Matched.Text, rangeErr)
					}
				}
				derived, err = inspectReferenceModelAnswer(cases[i], content)
			}
			if (err == nil) != want.Valid {
				t.Fatal("reference outcome changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("rejection repaired or changed")
				}
				return
			}
			if r.CitationCheck != "literal_citations_valid_semantics_unassessed" || r.Failure != "" || !bytes.Equal(derived, compact(r.EvidenceAttachment)) {
				t.Fatal("derived report changed")
			}
			var out derivedReferenceReport
			var supplied derivedReferenceInput
			if err := strictjson.Decode(derived, &out); err != nil {
				t.Fatal(err)
			}
			if err := strictjson.Decode(content, &supplied); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(out.Proposal, supplied) || out.Review.Review.Origin != "model_proposal" || out.Review.Review.Source != r.Case.Source || !reflect.DeepEqual(out.Review.Review.Context, r.Case.Entries) || out.Review.Review.IdentityApproved || out.Review.Review.GraphPublicationApproved || out.Review.Review.FinancialAttribution {
				t.Fatal("supplied proposal, provenance, context or approval changed")
			}
		})
	}
}
