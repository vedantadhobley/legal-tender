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

func TestProseNameModelRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, nameModelFixture)
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
			ID, SHA256, Review string
			Valid              bool
		}
	}
	if err := strictjson.Decode(read("review.json"), &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) {
		t.Fatal("trial profile changed")
	}
	for _, name := range []string{"models.json", "completed.json", "inputs.json", "expected.json"} {
		if hash(read(name)) != review.Metadata[name] {
			t.Fatal("metadata or pre-run expectations changed", name)
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
	cases := proseNameModelCases(t)
	expected := proseNameExpectations(t, cases)
	if completed.Model != review.Profile.Model || completed.Profile != review.Profile || completed.PromptSHA != hash([]byte(proseNameModelPrompt)) || completed.CaseCount != len(cases) || len(review.Cases) != len(cases) || completed.IdentityApproved || completed.GraphPublicationApproved || completed.FinancialAttribution {
		t.Fatal("completion marker or task changed")
	}
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			raw := read(want.ID + ".json")
			if hash(raw) != want.SHA256 || want.Review == "" {
				t.Fatal("capture changed or review missing")
			}
			var r modelRecord
			if err := strictjson.Decode(raw, &r); err != nil {
				t.Fatal(err)
			}
			request, err := proseNameModelRequest(cases[i], review.Profile)
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
				derived, err = inspectNameModelAnswer(cases[i], content)
			}
			if (err == nil) != want.Valid {
				t.Fatal("structural outcome changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("rejection changed or repaired")
				}
				return // No partial scoring of invalid answers.
			}
			if r.CitationCheck != "literal_citations_valid_semantics_unassessed" || r.Failure != "" || !bytes.Equal(derived, compact(r.EvidenceAttachment)) {
				t.Fatal("derived report changed")
			}
			var out nameSelectionReport
			var in nameSelectionInput
			if err := strictjson.Decode(derived, &out); err != nil {
				t.Fatal(err)
			}
			if err := strictjson.Decode(content, &in); err != nil {
				t.Fatal(err)
			}
			e := out.Citations.Review
			if !reflect.DeepEqual(out.Supplied, in.Names) || e.Source != r.Case.Source || !reflect.DeepEqual(e.Context, r.Case.Entries) || e.Origin != "model_proposal" || len(e.Interpretations) != 0 || len(e.Links) != 0 || e.IdentityApproved || e.GraphPublicationApproved || e.FinancialAttribution {
				t.Fatal("source, supplied names or no-acceptance boundary changed")
			}
			score, err := json.Marshal(scoreNameSelections(expected[i].Names, out))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s exact-name score: %s", expected[i].Group, score)
		})
	}
}
