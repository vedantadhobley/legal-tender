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

func TestProseGroundingRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, groundingFixture)
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
		Profile      modelProfile
		InputsSHA256 string
		Cases        []struct {
			ID, SHA256, NameSHA256, Findings string
			Valid                            bool
		}
	}
	if err := strictjson.Decode(read("review.json"), &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) || review.InputsSHA256 != hash(read("inputs.json")) {
		t.Fatal("trial controls or inputs changed")
	}
	cases := proseInputCases(t, groundingFixture)
	if len(cases) != len(review.Cases) {
		t.Fatal("missing semantic reviews")
	}
	for _, stage := range []struct{ prefix, prompt string }{{"", proseGroundingPrompt}, {"names/", proseNameModelPrompt}} {
		if err := checkModelProfile(read(stage.prefix+"models.json"), review.Profile); err != nil {
			t.Fatal(err)
		}
		var done struct {
			Model      string
			Profile    modelProfile
			Count      int    `json:"case_count"`
			Prompt     string `json:"prompt_sha256"`
			ObservedAt string `json:"observed_at"`
			Identity   bool   `json:"identity_approved"`
			Graph      bool   `json:"graph_publication_approved"`
			Money      bool   `json:"financial_attribution"`
		}
		if err := strictjson.Decode(read(stage.prefix+"completed.json"), &done); err != nil {
			t.Fatal(err)
		}
		if done.Model != review.Profile.Model || done.Profile != review.Profile || done.Count != len(cases) || done.Prompt != hash([]byte(stage.prompt)) || done.Identity || done.Graph || done.Money {
			t.Fatal("completion evidence changed")
		}
	}
	for i, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			want := review.Cases[i]
			raw := read(c.ID + ".json")
			if want.ID != c.ID || want.Findings == "" || hash(raw) != want.SHA256 || hash(read("names/"+c.ID+".json")) != want.NameSHA256 {
				t.Fatal("retained capture or review changed")
			}
			names := groundingNameInput(t, c)
			var r modelRecord
			if err := strictjson.Decode(raw, &r); err != nil {
				t.Fatal(err)
			}
			request, err := proseGroundingRequest(c, names, review.Profile)
			if err != nil || !bytes.Equal(request, compact(r.Request)) || !reflect.DeepEqual(r.Case, c) {
				t.Fatal("request or source changed", err)
			}
			if r.HTTPStatus != 200 || len(r.Response) == 0 {
				if want.Valid || r.CitationCheck != "not_checked" || r.Failure == "" || len(r.EvidenceAttachment) != 0 {
					t.Fatal("transport failure supplied evidence")
				}
				return
			}
			content, err := modelContent(r.Response)
			var out groundedRoleReport
			if err == nil {
				out, err = inspectGroundedRoles(c, names, content, "model_proposal")
			}
			if (err == nil) != want.Valid {
				t.Fatal("structural outcome changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("rejection repaired or changed")
				}
				return
			}
			encoded, err := json.Marshal(out)
			if err != nil || !bytes.Equal(encoded, compact(r.EvidenceAttachment)) || r.Failure != "" || r.CitationCheck != "literal_citations_valid_semantics_unassessed" {
				t.Fatal("evidence changed", err)
			}
			var supplied groundedRoleInput
			if err := strictjson.Decode(content, &supplied); err != nil {
				t.Fatal(err)
			}
			first, candidates, err := bindingCandidates(c, names, "model_proposal")
			e := out.Review.Derived.Review.Review
			if err != nil || !reflect.DeepEqual(out.Proposal, supplied) || !reflect.DeepEqual(out.Review.Names, first) || !reflect.DeepEqual(out.Review.Candidates, candidates) || !reflect.DeepEqual(e.Context, c.Entries) || e.Source != c.Source || e.Origin != "model_proposal" || e.IdentityApproved || e.GraphPublicationApproved || e.FinancialAttribution {
				t.Fatal("proposals, names, context or approvals changed", err)
			}
		})
	}
}
