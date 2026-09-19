package personaffiliation

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

// This test locks observed output, including misses. A pass does NOT mean the
// grammar is accurate enough for use as an assertion or identity extractor.
// These manually reviewed witnesses never enter production matching code.
func TestProseOutOfDevelopmentEvaluation(t *testing.T) {
	dir := "../../../tests/fixtures/person-affiliation/prose-evaluation-v1"
	rawReview, err := os.ReadFile(filepath.Join(dir, "review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var review struct {
		Policy  string `json:"policy"`
		Sources []struct {
			ID, File                       string
			Source                         companypage.Source
			Bytes, Entries, Roles, Aliases int
			TextBlocks                     int `json:"text_blocks"`
			Witnesses                      []struct {
				ID, Person, Role, Organization, Outcome, Reason string
				Evidence                                        []struct {
					Entry int
					Span  companypage.Span
					Text  string
				}
			}
		}
		RetrievalFailures []struct {
			ID, Outcome string
			Attempts    []struct {
				CurlExit      int `json:"curl_exit"`
				HTTPStatus    int `json:"http_status"`
				ReceivedBytes int `json:"received_bytes"`
			}
		} `json:"retrieval_failures"`
		IdentityApproved         bool `json:"identity_approved"`
		GraphPublicationApproved bool `json:"graph_publication_approved"`
		FinancialAttribution     bool `json:"financial_attribution"`
	}
	if err := json.Unmarshal(rawReview, &review); err != nil {
		t.Fatal(err)
	}
	if review.Policy != ProsePolicy || len(review.Sources) != 5 || len(review.RetrievalFailures) != 1 || review.IdentityApproved || review.GraphPublicationApproved || review.FinancialAttribution {
		t.Fatal("evaluation scope or acceptance boundary changed")
	}
	var found, missed int
	for _, s := range review.Sources {
		t.Run(s.ID, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, s.File))
			if err != nil || len(raw) != s.Bytes {
				t.Fatal("retained body changed", err)
			}
			e, err := companypage.Extract(context.Background(), raw, s.Source)
			if err != nil || len(e.Entries) != s.Entries {
				t.Fatal("source binding or lexical output changed", err)
			}
			r, err := ProposeProse(context.Background(), e)
			if err != nil {
				t.Fatal(err)
			}
			checkProseCitations(t, e, r)
			if len(r.Entries) != s.TextBlocks || len(r.Roles) != s.Roles || len(r.Aliases) != s.Aliases {
				t.Fatal("measured candidate census changed", len(r.Entries), len(r.Roles), len(r.Aliases))
			}
			reviewedCandidates := map[int]bool{}
			for _, w := range s.Witnesses {
				if w.ID == "" || w.Reason == "" || len(w.Evidence) == 0 {
					t.Fatal("incomplete review")
				}
				entries := map[int]bool{}
				var texts []string
				for _, ref := range w.Evidence {
					if ref.Entry < 0 || ref.Entry >= len(e.Entries) {
						t.Fatal("review entry outside source")
					}
					entry := e.Entries[ref.Entry]
					if entry.Span != ref.Span || ref.Span.Start < 0 || ref.Span.End > len(raw) || ref.Span.End <= ref.Span.Start || wikimedia.Hash(raw[ref.Span.Start:ref.Span.End]) != ref.Span.SHA256 || ref.Text == "" || !strings.Contains(entry.Text, ref.Text) {
						t.Fatal("review witness lost its exact original citation", w.ID)
					}
					entries[ref.Entry] = true
					texts = append(texts, entry.Text)
				}
				// This verifies reviewer labels occur in the cited material; it is
				// not a production rule for joining those strings into a relation.
				for _, field := range []string{w.Person, w.Role, w.Organization} {
					if field == "" || !strings.Contains(strings.Join(texts, "\n"), field) {
						t.Fatal("review label unsupported by cited entries", w.ID, field)
					}
				}
				matches := 0
				for i, candidate := range r.Roles {
					if entries[candidate.Evidence.Entry] && candidate.Person.Text == w.Person && candidate.Role.Text == w.Role && candidate.Organization.Text == w.Organization {
						if reviewedCandidates[i] || candidate.Predicate.Text != "is" || candidate.TimeWording != nil || candidate.TimeState != "role_validity_unknown" {
							t.Fatal("duplicate review or changed candidate meaning")
						}
						reviewedCandidates[i] = true
						matches++
					}
				}
				outcome := "missed_reviewed_role"
				if matches > 0 {
					outcome = "syntax_candidate_found"
					found++
				} else {
					missed++
				}
				if outcome != w.Outcome || matches > 1 {
					t.Fatal("reviewed result changed", w.ID, outcome)
				}
				t.Logf("%s: %s; %s", w.ID, outcome, w.Reason)
			}
			if len(reviewedCandidates) != len(r.Roles) {
				t.Fatal("unreviewed or misleading candidate appeared")
			}
			again, err := ProposeProse(context.Background(), e)
			if err != nil || !reflect.DeepEqual(r, again) {
				t.Fatal("offline replay changed", err)
			}
		})
	}
	for _, failure := range review.RetrievalFailures {
		if failure.ID != "amd-su" || failure.Outcome != "not_evaluated_no_retained_body" || len(failure.Attempts) != 2 {
			t.Fatal("retrieval failure dropped or relabeled")
		}
		for _, attempt := range failure.Attempts {
			if attempt.CurlExit == 0 || attempt.HTTPStatus != 0 || attempt.ReceivedBytes != 0 {
				t.Fatal("failed request became a successful empty source")
			}
		}
		if _, err := os.Stat(filepath.Join(dir, "amd-su.html")); !os.IsNotExist(err) {
			t.Fatal("unreviewed substitute body added for failed acquisition", err)
		}
	}
	if found != 1 || missed != 5 {
		t.Fatal("witness coverage changed", found, missed)
	}
	t.Logf("selected witnesses: %d found, %d missed; retrieval failures excluded; not population recall", found, missed)
}
