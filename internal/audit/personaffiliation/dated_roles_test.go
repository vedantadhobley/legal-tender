package personaffiliation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

// These are reviewed test annotations, not runtime rules, automatic prose-role
// extraction or a donor whitelist. Only the unchanged reader and evaluators run.
type datedRoleReview struct {
	ID                           string            `json:"id"`
	File                         string            `json:"file"`
	URL                          string            `json:"url"`
	RequestedURL                 string            `json:"requested_url"`
	ObservedOn                   string            `json:"observed_on"`
	HTTPStatus                   int               `json:"http_status"`
	Bytes                        int               `json:"bytes"`
	SHA256                       string            `json:"sha256"`
	Person                       string            `json:"reviewed_person_name"`
	Organization                 string            `json:"reviewed_organization_name"`
	RoleText                     string            `json:"role_text"`
	SubjectText                  string            `json:"subject_text"`
	DocumentDateText             string            `json:"document_date_text"`
	AsOf                         string            `json:"reviewed_as_of"`
	DateBasis                    string            `json:"date_basis"`
	OtherDateTexts               []string          `json:"other_date_texts"`
	JSONLDDates                  map[string]string `json:"json_ld_dates"`
	AppearanceIDs                []string          `json:"appearance_ids"`
	WantPersonNameCorrespondence bool              `json:"want_person_name_correspondence"`
	WantEmployerRule             string            `json:"want_employer_rule"`
	WantDaysBeforeReceipt        int               `json:"want_days_before_receipt"`
}

func TestDatedFirstPartyRoleEvidence(t *testing.T) {
	dir := filepath.Join(fixtureDir, "dated-roles")
	manifest, err := os.ReadFile(filepath.Join(dir, "review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var review struct {
		Scope   string            `json:"scope"`
		Sources []datedRoleReview `json:"sources"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(manifest)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&review); err != nil || len(review.Sources) != 2 || review.Scope == "" {
		t.Fatal("invalid research review", err)
	}
	corpus, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	appearances := map[string]screen.Appearance{}
	for _, c := range corpus.Cases {
		appearances[c.ID] = c.Assessment.Appearance
	}
	seen := map[screen.Reference]bool{}
	for _, source := range review.Sources {
		t.Run(source.ID, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, source.File))
			if err != nil || len(raw) != source.Bytes || hash(raw) != source.SHA256 || source.HTTPStatus != 200 || source.DateBasis == "" {
				t.Fatal("retained source or review changed", err)
			}
			input := companypage.Source{URL: source.URL, ObservedOn: source.ObservedOn, SHA256: source.SHA256}
			evidence, err := companypage.Extract(context.Background(), raw, input)
			if err != nil || evidence.IdentityApproved || evidence.GraphPublicationApproved || evidence.FinancialAttribution {
				t.Fatal("source extraction or acceptance boundary", err)
			}
			again, err := companypage.Extract(context.Background(), raw, input)
			if err != nil || !reflect.DeepEqual(evidence, again) {
				t.Fatal("nondeterministic source replay", err)
			}
			input.ObservedOn = "2026-09-17"
			later, err := companypage.Extract(context.Background(), raw, input)
			if err != nil || !reflect.DeepEqual(evidence.Entries, later.Entries) {
				t.Fatal("capture day altered source dates/text", err)
			}
			var roleEntries []companypage.Entry
			var texts []string
			dates := map[string]bool{}
			for _, e := range evidence.Entries {
				if e.Span.Start < 0 || e.Span.End > len(raw) || e.Span.End <= e.Span.Start || hash(raw[e.Span.Start:e.Span.End]) != e.Span.SHA256 {
					t.Fatal("invalid source span")
				}
				if e.Kind == "text" || e.Kind == "heading" || e.Kind == "title" {
					texts = append(texts, e.Text)
					if strings.Contains(e.Text, source.SubjectText) && strings.Contains(e.Text, source.RoleText) {
						roleEntries = append(roleEntries, e)
					}
				}
				if e.Kind == "json_ld" && e.JSONState == "valid_json_uninterpreted" {
					var object map[string]json.RawMessage
					if json.Unmarshal([]byte(e.Raw), &object) != nil {
						continue
					}
					for key, want := range source.JSONLDDates {
						var value string
						if json.Unmarshal(object[key], &value) == nil && value == want {
							dates[key] = true
						}
					}
				}
			}
			if len(roleEntries) != 1 || len(dates) != len(source.JSONLDDates) {
				t.Fatal("reviewed role occurrence or distinct JSON-LD dates missing", len(roleEntries), dates)
			}
			joined := strings.Join(texts, "\n")
			for _, needle := range append([]string{source.DocumentDateText}, source.OtherDateTexts...) {
				if !strings.Contains(joined, needle) {
					t.Fatal("lost reviewed date context", needle)
				}
			}
			span := roleEntries[0].Span
			// The reviewer supplies this point assertion from narrative context.
			// The parser does not map a page/filing date to a role's start or end.
			claim := screen.Claim{
				Source:   screen.Reference{SHA256: source.SHA256, Locator: fmt.Sprintf("bytes[%d:%d];review=%s", span.Start, span.End, source.ID)},
				PersonID: "review:" + source.ID + "/person", OrganizationID: "review:" + source.ID + "/organization",
				PersonName: source.Person, OrganizationName: source.Organization, Role: screen.Executive, AsOf: source.AsOf,
			}
			asOf, err := time.Parse(time.DateOnly, source.AsOf)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range source.AppearanceIDs {
				a, found := appearances[id]
				if !found || seen[a.Source] {
					t.Fatal("missing or collapsed FEC occurrence")
				}
				seen[a.Source] = true
				person := org.Normalize(*a.Receipt.Name) == org.Normalize(source.Person)
				match, employer := org.MatchName(*a.Receipt.Employer, source.Organization)
				if person != source.WantPersonNameCorrespondence || !employer || match.Rule != source.WantEmployerRule {
					t.Fatal("name/employer correspondence changed", person, match.Rule)
				}
				receiptDay, err := time.Parse(time.DateOnly, *a.Receipt.ReceiptDate)
				if err != nil || int(receiptDay.Sub(asOf).Hours()/24) != source.WantDaysBeforeReceipt {
					t.Fatal("source/receipt date comparison changed", err)
				}
				result, err := screen.Assess(a, []screen.Claim{claim})
				if err != nil || len(result.Decisions) != 1 {
					t.Fatal("reviewed point assessment", err)
				}
				d := result.Decisions[0]
				if d.TimeState != "unknown_time" || d.Claim.ValidFrom != "" || d.Claim.ValidThrough != "" || d.ScreeningMatch || result.IdentityResolved || result.GraphPublicationApproved || result.FinancialAttribution {
					t.Fatal("earlier point evidence became continuous tenure or identity")
				}
				// Replay the additive evidence evaluator on the same reviewed
				// claim, without inventing an origin, alias or identity binding.
				assessment, err := screen.AssessEvidence(a, []screen.RoleObservation{{Claim: claim, Polarity: "asserted"}}, true)
				if err != nil || len(assessment.Timelines) != 1 || assessment.Timelines[0].State != "no_source_coverage_at_day" || assessment.Timelines[0].Continuity != nil || assessment.IdentityResolved || assessment.GraphPublicationApproved || assessment.FinancialAttribution {
					t.Fatal("retained point promoted by additive evaluator", err)
				}
				wantCandidates := 0
				if source.WantPersonNameCorrespondence {
					wantCandidates = 1
				}
				if len(assessment.CandidatePersonIDs) != wantCandidates || !reflect.DeepEqual(assessment.Appearance, a) {
					t.Fatal("candidate gap hidden or FEC occurrence changed")
				}
				t.Logf("%s: person-name correspondence=%t, employer rule=%s; reviewed role point is %d days before receipt; no accepted identity or continuous tenure", id, person, match.Rule, source.WantDaysBeforeReceipt)
			}
		})
	}
	if len(seen) != 3 || len(corpus.Cases) != 4 {
		t.Fatal("selected occurrence coverage changed; unsearched engineer remains unassessed")
	}
}
