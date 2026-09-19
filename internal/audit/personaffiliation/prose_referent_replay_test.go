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

// Captured outputs are replayed, not repaired or replaced by review annotations.
func TestProseReferentRetainedReplay(t *testing.T) {
	seeds, _ := proseReferentSeeds(t)
	type reviewedCase struct {
		ID         string
		SHA256     string
		Structural bool `json:"structural_valid"`
		Semantic   bool `json:"semantic_checks_met"`
		Usable     bool
		Findings   string
	}
	type reviewedModel struct {
		Directory       string
		Profile         modelProfile
		CompletedSHA256 string `json:"completed_sha256"`
		HTTP200         int    `json:"http_200"`
		Structural      int    `json:"structural_valid"`
		Semantic        int    `json:"semantic_checks_met"`
		Usable          int
		Elapsed         int64 `json:"elapsed_ms"`
		Prompt          int   `json:"prompt_tokens"`
		Cached          int   `json:"cached_prompt_tokens"`
		Completion      int   `json:"completion_tokens"`
		Cases           []reviewedCase
	}
	var review struct {
		Method      string
		CasesSHA256 string `json:"cases_sha256"`
		PromptSHA   string `json:"prompt_sha256"`
		Timeout     int    `json:"request_timeout_seconds"`
		Models      []reviewedModel
		Identity    bool `json:"identity_approved"`
		Graph       bool `json:"graph_publication_approved"`
		Money       bool `json:"financial_attribution"`
	}
	root := filepath.Join(fixtureDir, referentModelFixture)
	reviewRaw, err := os.ReadFile(filepath.Join(root, "review.json"))
	if err != nil || strictjson.Decode(reviewRaw, &review) != nil {
		t.Fatal("invalid review", err)
	}
	casesRaw, err := os.ReadFile(filepath.Join(root, "cases.json"))
	if err != nil || review.Method != "manual-review-of-fixed-referent-trial.v1" || review.CasesSHA256 != hash(casesRaw) || review.PromptSHA != hash([]byte(proseReferentPrompt)) || review.Timeout != 900 || len(review.Models) != 2 || review.Identity || review.Graph || review.Money {
		t.Fatal("review boundary changed", err)
	}
	compact := func(raw []byte) []byte {
		t.Helper()
		var b bytes.Buffer
		if err := json.Compact(&b, raw); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	for _, model := range review.Models {
		t.Run(model.Profile.Model, func(t *testing.T) {
			dir := filepath.Join(root, model.Directory)
			read := func(name string) []byte {
				t.Helper()
				raw, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			if err := checkModelProfile(read("models.json"), model.Profile); err != nil {
				t.Fatal(err)
			}
			var done struct {
				Model      string
				Profile    modelProfile
				Count      int    `json:"case_count"`
				Timeout    int    `json:"request_timeout_seconds"`
				Prompt     string `json:"prompt_sha256"`
				ObservedAt string `json:"observed_at"`
				Identity   bool   `json:"identity_approved"`
				Graph      bool   `json:"graph_publication_approved"`
				Money      bool   `json:"financial_attribution"`
			}
			completed := read("completed.json")
			if err := strictjson.Decode(completed, &done); err != nil {
				t.Fatal(err)
			}
			if hash(completed) != model.CompletedSHA256 || done.Model != model.Profile.Model || done.Profile != model.Profile || done.Count != len(seeds) || done.Timeout != review.Timeout || done.Prompt != review.PromptSHA || done.ObservedAt == "" || done.Identity || done.Graph || done.Money || len(model.Cases) != len(seeds) {
				t.Fatal("completion marker, review cases or controls changed")
			}
			http200, structural, semantic, usable := 0, 0, 0, 0
			var elapsed int64
			prompt, cached, completion := 0, 0, 0
			for i, seed := range seeds {
				want := model.Cases[i]
				t.Run(seed.Case.ID, func(t *testing.T) {
					if want.ID != seed.Case.ID || want.Findings == "" || want.Usable != (want.Structural && want.Semantic) {
						t.Fatal("invalid manual review")
					}
					capture := read(seed.Case.ID + ".json")
					if hash(capture) != want.SHA256 {
						t.Fatal("retained capture changed")
					}
					var r modelRecord
					if err := strictjson.Decode(capture, &r); err != nil {
						t.Fatal(err)
					}
					request, err := proseReferentRequest(seed.Case, seed.Names, model.Profile)
					if err != nil || !bytes.Equal(request, compact(r.Request)) || !reflect.DeepEqual(r.Case, seed.Case) || r.ElapsedMS <= 0 {
						t.Fatal("request, original names or source changed", err)
					}
					elapsed += r.ElapsedMS
					if r.HTTPStatus != 200 || len(r.Response) == 0 {
						if r.CitationCheck != "not_checked" || r.Failure == "" || len(r.EvidenceAttachment) != 0 {
							t.Fatal("transport failure supplied evidence")
						}
						return
					}
					http200++
					var response struct {
						Usage struct {
							Prompt     int `json:"prompt_tokens"`
							Completion int `json:"completion_tokens"`
							Details    struct {
								Cached int `json:"cached_tokens"`
							} `json:"prompt_tokens_details"`
						}
					}
					if err := json.Unmarshal(r.Response, &response); err != nil {
						t.Fatal(err)
					}
					prompt += response.Usage.Prompt
					cached += response.Usage.Details.Cached
					completion += response.Usage.Completion
					content, err := modelContent(r.Response)
					var report json.RawMessage
					if err == nil {
						report, err = inspectReferentModelAnswer(seed.Case, seed.Names, content)
					}
					if (err == nil) != want.Structural {
						t.Fatal("reviewed structural outcome changed", err)
					}
					if err != nil {
						if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
							t.Fatal("rejection changed or salvaged")
						}
						return
					}
					if !bytes.Equal(report, compact(r.EvidenceAttachment)) || r.Failure != "" || r.CitationCheck != "literal_citations_valid_semantics_unassessed" {
						t.Fatal("retained evidence changed")
					}
					structural++
					var out referentReport
					var supplied referentInput
					if err := strictjson.Decode(report, &out); err != nil {
						t.Fatal(err)
					}
					if err := strictjson.Decode(content, &supplied); err != nil {
						t.Fatal(err)
					}
					review := out.Derived.Review.Derived.Review.Review
					if !reflect.DeepEqual(out.Proposal, supplied) || !reflect.DeepEqual(review.Context, seed.Case.Entries) || review.Source != seed.Case.Source || review.Origin != "model_proposal" || out.AssessmentOrigin != "model_proposal" || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
						t.Fatal("source, provenance, proposal or approval changed")
					}
				})
				if want.Semantic {
					semantic++
				}
				if want.Usable {
					usable++
				}
			}
			if http200 != model.HTTP200 || structural != model.Structural || semantic != model.Semantic || usable != model.Usable || elapsed != model.Elapsed || prompt != model.Prompt || cached != model.Cached || completion != model.Completion {
				t.Fatal("review summary does not match retained captures")
			}
		})
	}
}
