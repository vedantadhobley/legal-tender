package personaffiliation

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestProseAttachmentRetainedReplay(t *testing.T) {
	dir := filepath.Join(fixtureDir, attachmentFixture)
	raw, err := os.ReadFile(filepath.Join(dir, "review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var review struct {
		Profile      modelProfile
		Metadata     map[string]string
		LiteralValid int `json:"literal_valid"`
		Cases        []struct {
			ID, SHA256, Review string
			LiteralValid       bool `json:"literal_valid"`
			Roles, Aliases     int
		}
		IdentityApproved         bool `json:"identity_approved"`
		GraphPublicationApproved bool `json:"graph_publication_approved"`
		FinancialAttribution     bool `json:"financial_attribution"`
	}
	if err := json.Unmarshal(raw, &review); err != nil {
		t.Fatal(err)
	}
	if review.Profile != (modelProfile{"gpt-oss-120b", "medium", 4096}) || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
		t.Fatal("trial scope/acceptance changed")
	}
	for name, pin := range review.Metadata {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || hash(raw) != pin {
			t.Fatal("metadata changed", name, err)
		}
	}
	models, err := os.ReadFile(filepath.Join(dir, "models.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkModelProfile(models, review.Profile); err != nil {
		t.Fatal(err)
	}
	cases := proseAttachmentCases(t)
	if len(review.Cases) != len(cases) {
		t.Fatal("scope changed")
	}
	valid := 0
	for i, want := range review.Cases {
		t.Run(want.ID, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, want.ID+".json"))
			if err != nil || hash(raw) != want.SHA256 {
				t.Fatal("capture changed", err)
			}
			var r modelRecord
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			if r.Case.ID != want.ID || !reflect.DeepEqual(cases[i], r.Case) || want.Review == "" {
				t.Fatal("input/review changed")
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, r.Request); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(compact.Bytes(), proseAttachmentRequest(cases[i], review.Profile)) {
				t.Fatal("task or controls changed")
			}
			if r.HTTPStatus != 200 {
				t.Fatal("retained run was not HTTP 200")
			}
			content, err := modelContent(r.Response)
			var attached json.RawMessage
			var answer entryAnswer
			if err == nil {
				if decodeErr := json.Unmarshal(content, &answer); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				attached, err = attachModelEvidence(r.Case, content)
			}
			if (err == nil) != want.LiteralValid || len(answer.Roles) != want.Roles || len(answer.Aliases) != want.Aliases {
				t.Fatal("literal result or counts changed", err)
			}
			if err != nil {
				if r.CitationCheck != "rejected" || r.Failure != err.Error() || len(r.EvidenceAttachment) != 0 {
					t.Fatal("rejection changed or partially salvaged")
				}
			} else {
				valid++
				if r.CitationCheck != "literal_citations_valid_semantics_unassessed" || r.Failure != "" {
					t.Fatal("validation changed")
				}
				compact.Reset()
				if err := json.Compact(&compact, r.EvidenceAttachment); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(attached, compact.Bytes()) {
					t.Fatal("attached original evidence changed")
				}
				var output attachedEvidence
				if err := json.Unmarshal(attached, &output); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(output.Context, r.Case.Entries) || output.Source != r.Case.Source || output.IdentityApproved || output.GraphPublicationApproved || output.FinancialAttribution {
					t.Fatal("context or approval changed")
				}
			}
			t.Logf("literal_valid=%t; %s", want.LiteralValid, want.Review)
		})
	}
	if valid != review.LiteralValid {
		t.Fatal("literal-valid total changed", valid)
	}
}
