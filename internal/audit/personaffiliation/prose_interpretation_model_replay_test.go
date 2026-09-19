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

func TestProseInterpretationModelRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, interpretationModelFixture)
	read := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	var review struct {
		Profile  modelProfile
		Metadata map[string]string
		Cases    []struct {
			ID, SHA256, Binding, Classification, Context, NameTime string
			LiteralValid                                           bool `json:"literal_valid"`
		}
	}
	if err := strictjson.Decode(read("review.json"), &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) {
		t.Fatal("research profile changed")
	}
	for name, digest := range review.Metadata {
		if hash(read(name)) != digest {
			t.Fatal("retained trial metadata changed", name)
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
	if len(review.Cases) != len(cases) || completed.CaseCount != len(cases) || completed.Model != review.Profile.Model || completed.Profile != review.Profile || completed.PromptSHA != hash([]byte(proseInterpretationPrompt)) || completed.IdentityApproved || completed.GraphPublicationApproved || completed.FinancialAttribution {
		t.Fatal("completion marker/task boundary changed")
	}
	compact := func(raw []byte) []byte {
		t.Helper()
		var b bytes.Buffer
		if err := json.Compact(&b, raw); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			raw := read(want.ID + ".json")
			if hash(raw) != want.SHA256 || want.Binding == "" || want.Classification == "" || want.Context == "" || want.NameTime == "" {
				t.Fatal("capture or dimension-specific review missing")
			}
			var r modelRecord
			if err := strictjson.Decode(raw, &r); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cases[i], r.Case) || want.ID != r.Case.ID || !bytes.Equal(compact(r.Request), proseInterpretationRequest(cases[i], review.Profile)) {
				t.Fatal("source selection, prompt or schema changed")
			}
			if r.HTTPStatus != 200 || len(r.Response) == 0 {
				if want.LiteralValid || r.CitationCheck != "not_checked" || r.Failure == "" || len(r.EvidenceAttachment) != 0 {
					t.Fatal("transport failure produced usable evidence")
				}
				return // Retained transport errors cannot be replayed as an inference.
			}
			content, err := modelContent(r.Response)
			var derived json.RawMessage
			if err == nil {
				derived, err = inspectModelInterpretations(cases[i], content)
			}
			if (err == nil) != want.LiteralValid {
				t.Fatal("literal/reference outcome changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("whole-answer rejection changed")
				}
				return
			}
			if r.CitationCheck != "literal_citations_valid_semantics_unassessed" || r.Failure != "" || !bytes.Equal(derived, compact(r.EvidenceAttachment)) {
				t.Fatal("derived report changed")
			}
			var report mentionInterpretationReport
			if err := strictjson.Decode(derived, &report); err != nil {
				t.Fatal(err)
			}
			if report.Origin != "model_proposal" || report.Source != r.Case.Source || !reflect.DeepEqual(report.Context, r.Case.Entries) || report.IdentityApproved || report.GraphPublicationApproved || report.FinancialAttribution {
				t.Fatal("source, origin or approval boundary changed")
			}
		})
	}
}
