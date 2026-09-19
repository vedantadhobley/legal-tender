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

func TestProseCitationModelRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, citationModelFixture)
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
			ID, SHA256, Selection, Binding, Classification, Context, NameTime string
			Valid                                                             bool
		}
	}
	if err := strictjson.Decode(read("review.json"), &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) {
		t.Fatal("trial profile changed")
	}
	for name, digest := range review.Metadata {
		if hash(read(name)) != digest {
			t.Fatal("trial metadata changed", name)
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
	cases := proseInterpretationCases(t)
	if completed.Profile != review.Profile || completed.Model != review.Profile.Model || completed.PromptSHA != hash([]byte(proseCitationModelPrompt)) || completed.CaseCount != len(cases) || len(review.Cases) != len(cases) || completed.IdentityApproved || completed.GraphPublicationApproved || completed.FinancialAttribution {
		t.Fatal("completion marker or task changed")
	}
	ranges := 0
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			raw := read(want.ID + ".json")
			if hash(raw) != want.SHA256 || want.Selection == "" || want.Binding == "" || want.Classification == "" || want.Context == "" || want.NameTime == "" {
				t.Fatal("retained capture or separate review missing")
			}
			var r modelRecord
			if err := strictjson.Decode(raw, &r); err != nil {
				t.Fatal(err)
			}
			request, err := proseCitationModelRequest(cases[i], review.Profile)
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
				// Diagnostic only, including rejected answers. Never publish these
				// individual ranges as a salvaged interpretation report.
				var proposal citationSelectionInput
				if decodeErr := strictjson.Decode(content, &proposal); decodeErr == nil {
					cat, catalogErr := newCitationCatalog(cases[i])
					if catalogErr != nil {
						t.Fatal(catalogErr)
					}
					for _, s := range proposal.Selections {
						if s.Entry == nil || s.First == nil || s.Last == nil {
							t.Fatal("retained run had complete range coordinates")
						}
						quote, rangeErr := cat.selectRange(citationRange{Entry: *s.Entry, First: *s.First, Last: *s.Last})
						if rangeErr != nil {
							t.Fatal("retained run had valid token ranges", rangeErr)
						}
						ranges++
						t.Logf("selection-only diagnostic %s: %q (error=%v)", s.ID, quote.Matched.Text, rangeErr)
					}
				}
				derived, err = inspectCitationModelAnswer(cases[i], content)
			}
			if (err == nil) != want.Valid {
				t.Fatal("range/reference outcome changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("rejection was changed or repaired")
				}
				return
			}
			if r.CitationCheck != "literal_citations_valid_semantics_unassessed" || r.Failure != "" || !bytes.Equal(derived, compact(r.EvidenceAttachment)) {
				t.Fatal("derived report changed")
			}
			var report citationSelectionReport
			if err := strictjson.Decode(derived, &report); err != nil {
				t.Fatal(err)
			}
			if report.Review.Origin != "model_proposal" || report.Review.Source != r.Case.Source || !reflect.DeepEqual(report.Review.Context, r.Case.Entries) || report.Review.IdentityApproved || report.Review.GraphPublicationApproved || report.Review.FinancialAttribution {
				t.Fatal("provenance, source context or approval changed")
			}
		})
	}
	if ranges != 56 {
		t.Fatal("retained selection count changed", ranges)
	}
}
