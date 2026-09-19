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

func TestProseBindingModelRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, bindingModelFixture)
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
			ID, SHA256, NameSHA256, Binding, Roles, Context, Abstention string
			Valid                                                       bool
		}
	}
	if err := strictjson.Decode(read("review.json"), &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) {
		t.Fatal("trial profile changed")
	}
	for _, name := range []string{"inputs.json", "cases.json", "models.json", "completed.json", "seed-names/models.json", "seed-names/completed.json"} {
		if hash(read(name)) != review.Metadata[name] {
			t.Fatal("trial metadata changed", name)
		}
	}
	checkCompleted := func(prefix, prompt string, count int) {
		t.Helper()
		if err := checkModelProfile(read(prefix+"models.json"), review.Profile); err != nil {
			t.Fatal(err)
		}
		var done struct {
			Model                    string
			Profile                  modelProfile
			CaseCount                int    `json:"case_count"`
			PromptSHA                string `json:"prompt_sha256"`
			ObservedAt               string `json:"observed_at"`
			IdentityApproved         bool   `json:"identity_approved"`
			GraphPublicationApproved bool   `json:"graph_publication_approved"`
			FinancialAttribution     bool   `json:"financial_attribution"`
		}
		if err := strictjson.Decode(read(prefix+"completed.json"), &done); err != nil {
			t.Fatal(err)
		}
		if done.Model != review.Profile.Model || done.Profile != review.Profile || done.PromptSHA != hash([]byte(prompt)) || done.CaseCount != count || done.IdentityApproved || done.GraphPublicationApproved || done.FinancialAttribution {
			t.Fatal("completion marker changed", prefix)
		}
	}
	seeds := proseBindingSeeds(t)
	checkCompleted("", proseBindingModelPrompt, len(seeds))
	checkCompleted("seed-names/", proseNameModelPrompt, len(proseInputCases(t, bindingModelFixture)))
	if len(review.Cases) != len(seeds) {
		t.Fatal("missing case reviews")
	}
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			s := seeds[i]
			raw := read(want.ID + ".json")
			if hash(raw) != want.SHA256 || s.SHA256 != want.NameSHA256 || want.Binding == "" || want.Roles == "" || want.Context == "" || want.Abstention == "" {
				t.Fatal("capture, name seed or semantic review changed")
			}
			var r modelRecord
			if err := strictjson.Decode(raw, &r); err != nil {
				t.Fatal(err)
			}
			request, err := proseBindingModelRequest(s.Case, s.Names, review.Profile)
			if err != nil || !bytes.Equal(request, compact(r.Request)) || !reflect.DeepEqual(r.Case, s.Case) || want.ID != s.Case.ID {
				t.Fatal("binding task or inputs changed", err)
			}
			if r.HTTPStatus != 200 || len(r.Response) == 0 {
				if want.Valid || r.CitationCheck != "not_checked" || r.Failure == "" || len(r.EvidenceAttachment) != 0 {
					t.Fatal("transport failure yielded evidence")
				}
				return
			}
			content, err := modelContent(r.Response)
			var out bindingSelectionReport
			if err == nil {
				out, err = inspectBindingSelections(s.Case, s.Names, content, "model_proposal")
			}
			if (err == nil) != want.Valid {
				t.Fatal("structural outcome changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("rejection changed or repaired")
				}
				return
			}
			encoded, err := json.Marshal(out)
			if err != nil || !bytes.Equal(encoded, compact(r.EvidenceAttachment)) || r.Failure != "" || r.CitationCheck != "literal_citations_valid_semantics_unassessed" {
				t.Fatal("derived report changed", err)
			}
			var proposal derivedReferenceInput
			if err := strictjson.Decode(content, &proposal); err != nil {
				t.Fatal(err)
			}
			names, candidates, err := bindingCandidates(s.Case, s.Names, "model_proposal")
			e := out.Derived.Review.Review
			if err != nil || !reflect.DeepEqual(out.Names, names) || !reflect.DeepEqual(out.Candidates, candidates) || !reflect.DeepEqual(out.Proposal, proposal) || e.Source != s.Case.Source || !reflect.DeepEqual(e.Context, s.Case.Entries) || e.Origin != "model_proposal" || e.IdentityApproved || e.GraphPublicationApproved || e.FinancialAttribution {
				t.Fatal("upstream names, proposal, context or approval changed", err)
			}
			for _, p := range e.Interpretations {
				t.Logf("unverified interpretation: %s %s %s %s subjects=%v organizations=%v state=%s", p.Proposal.ID, p.Proposal.Kind, p.Proposal.Normalized, p.Proposal.Binding, p.Proposal.Subjects, p.Proposal.Organizations, p.State)
			}
		})
	}
}
