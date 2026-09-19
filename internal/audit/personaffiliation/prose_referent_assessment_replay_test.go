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

func TestProseReferentAssessmentRetainedReplay(t *testing.T) {
	seeds, _ := proseReferentAssessmentSeeds(t)
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
		RealUsable      int   `json:"real_source_usable"`
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
	root := filepath.Join(fixtureDir, referentAssessmentFixture)
	readRoot := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	reviewRaw, casesRaw := readRoot("review.json"), readRoot("cases.json")
	if err := strictjson.Decode(reviewRaw, &review); err != nil || review.Method != "manual-review-of-grounding-bound-assessment.v1" || review.CasesSHA256 != hash(casesRaw) || review.PromptSHA != hash([]byte(proseReferentAssessmentPrompt)) || review.Timeout != 900 || len(review.Models) != 2 || review.Identity || review.Graph || review.Money {
		t.Fatal("assessment review boundary changed", err)
	}
	compact := func(raw []byte) []byte {
		t.Helper()
		var out bytes.Buffer
		if err := json.Compact(&out, raw); err != nil {
			t.Fatal(err)
		}
		return out.Bytes()
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
			var completed struct {
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
			completedRaw := read("completed.json")
			if err := strictjson.Decode(completedRaw, &completed); err != nil || hash(completedRaw) != model.CompletedSHA256 || completed.Model != model.Profile.Model || completed.Profile != model.Profile || completed.Count != len(seeds) || completed.Timeout != review.Timeout || completed.Prompt != review.PromptSHA || completed.ObservedAt == "" || completed.Identity || completed.Graph || completed.Money || len(model.Cases) != len(seeds) {
				t.Fatal("completion marker, profile or review cases changed", err)
			}
			http200, structural, semantic, usable, realUsable := 0, 0, 0, 0, 0
			var elapsed int64
			prompt, cached, completion := 0, 0, 0
			for i, seed := range seeds {
				want := model.Cases[i]
				t.Run(seed.Case.ID, func(t *testing.T) {
					if want.ID != seed.Case.ID || want.Findings == "" || want.Usable != (want.Structural && want.Semantic) {
						t.Fatal("invalid assessment review")
					}
					capture := read(seed.Case.ID + ".json")
					if hash(capture) != want.SHA256 {
						t.Fatal("assessment capture changed")
					}
					var record modelRecord
					if err := strictjson.Decode(capture, &record); err != nil || !reflect.DeepEqual(record.Case, seed.Case) || record.ElapsedMS <= 0 {
						t.Fatal("invalid assessment record", err)
					}
					request, err := proseReferentAssessmentRequest(seed, model.Profile)
					if err != nil || !bytes.Equal(request, compact(record.Request)) {
						t.Fatal("assessment request, source or grounding changed", err)
					}
					elapsed += record.ElapsedMS
					if record.HTTPStatus != 200 || len(record.Response) == 0 {
						if record.CitationCheck != "not_checked" || record.Failure == "" || len(record.EvidenceAttachment) != 0 {
							t.Fatal("transport failure supplied assessment")
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
					if err := json.Unmarshal(record.Response, &response); err != nil {
						t.Fatal(err)
					}
					prompt += response.Usage.Prompt
					cached += response.Usage.Details.Cached
					completion += response.Usage.Completion
					content, err := modelContent(record.Response)
					var report json.RawMessage
					if err == nil {
						report, err = inspectReferentAssessmentModelAnswer(seed, content)
					}
					if (err == nil) != want.Structural {
						t.Fatal("reviewed assessment structure changed", err)
					}
					if err != nil {
						if record.CitationCheck != "rejected" || record.Failure != err.Error() || len(record.EvidenceAttachment) != 0 {
							t.Fatal("rejected assessment changed or was salvaged")
						}
						return
					}
					if !bytes.Equal(report, compact(record.EvidenceAttachment)) || record.Failure != "" || record.CitationCheck != "literal_citations_valid_semantics_unassessed" {
						t.Fatal("retained assessment evidence changed")
					}
					structural++
					var out referentAssessmentReport
					if err := strictjson.Decode(report, &out); err != nil {
						t.Fatal(err)
					}
					grounding := out.Joined.Proposal.Grounding
					var wantGrounding groundedRoleInput
					if err := strictjson.Decode(seed.Grounding, &wantGrounding); err != nil {
						t.Fatal(err)
					}
					review := out.Joined.Derived.Review.Derived.Review.Review
					if out.GroundingSHA256 != hash(seed.Grounding) || out.GroundingOrigin != seed.GroundingOrigin || !reflect.DeepEqual(grounding, wantGrounding) || out.Joined.AssessmentOrigin != "model_proposal" || !reflect.DeepEqual(review.Context, seed.Case.Entries) || review.Source != seed.Case.Source || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
						t.Fatal("grounding, source, provenance or approval changed")
					}
				})
				if want.Semantic {
					semantic++
				}
				if want.Usable {
					usable++
					if i < 3 {
						realUsable++
					}
				}
			}
			if http200 != model.HTTP200 || structural != model.Structural || semantic != model.Semantic || usable != model.Usable || realUsable != model.RealUsable || elapsed != model.Elapsed || prompt != model.Prompt || cached != model.Cached || completion != model.Completion {
				t.Fatal("assessment review summary does not match captures")
			}
		})
	}
}
