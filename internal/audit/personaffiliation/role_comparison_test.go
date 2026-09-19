package personaffiliation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

// These IDs and comparisons are reviewed fixture expectations, never runtime
// identity rules. ExtractRoles does not receive the corpus or these bindings.
func TestInteractiveRoleSourceComparison(t *testing.T) {
	const pin = "5683ad0692e124dcd9d87bcdc81b7448def720f632a2e4e4b66b8aae4f490076"
	meta, err := os.ReadFile(filepath.Join(fixtureDir, "role-source-interactive.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture struct {
		RequestMode string   `json:"request_mode"`
		URL         string   `json:"source_url"`
		IDs         []string `json:"expected_ids"`
		Status      int      `json:"http_status"`
		Body        Artifact `json:"body"`
	}
	if err := json.Unmarshal(meta, &capture); err != nil {
		t.Fatal(err)
	}
	if capture.RequestMode != "interactive_maxlag_omitted" || capture.Status != 200 || capture.Body.SHA256 != pin || capture.Body.Bytes != 615656 || !slices.Equal(capture.IDs, []string{"Q1393271", "Q173395", "Q8034666"}) {
		t.Fatal("interactive observation metadata changed")
	}
	// The one-off request differs only in maxlag; production URL policy is intact.
	background, err := url.Parse(wikimedia.EntityURL(capture.IDs))
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(capture.URL)
	if err != nil || u.Query().Has("maxlag") || background.Query().Get("maxlag") != "5" {
		t.Fatal("interactive/background policy boundary changed")
	}
	query := background.Query()
	query.Del("maxlag")
	if u.Scheme != background.Scheme || u.Host != background.Host || u.Path != background.Path || !reflect.DeepEqual(u.Query(), query) {
		t.Fatal("unreviewed source request change")
	}
	raw, err := read(fixtureDir, capture.Body, maxArtifact)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := wikimedia.ExtractRoles(raw, pin, capture.IDs)
	if err != nil {
		t.Fatal(err)
	}
	again, err := wikimedia.ExtractRoles(raw, pin, []string{"Q8034666", "Q173395", "Q1393271"})
	if err != nil || !reflect.DeepEqual(evidence, again) {
		t.Fatal("unstable extraction replay", err)
	}
	if evidence.IdentityApproved || evidence.GraphPublicationApproved || evidence.FinancialAttribution {
		t.Fatal("source extraction approved identity or money")
	}
	// Independently enumerate the pinned source arrays, not just expected counts.
	var source struct {
		Entities map[string]struct {
			Claims map[string][]json.RawMessage `json:"claims"`
		} `json:"entities"`
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	originals := map[string]json.RawMessage{}
	for id, e := range source.Entities {
		for _, p := range []string{"P108", "P112", "P169", "P3320", "P127", "P1830", "P488", "P39"} {
			for i, row := range e.Claims[p] {
				originals[fmt.Sprintf("/entities/%s/claims/%s/%d", id, p, i)] = row
			}
		}
	}
	statements := map[string]wikimedia.RoleStatement{}
	roles := map[string]int{}
	for _, e := range evidence.Entities {
		for _, s := range e.Statements {
			if _, duplicate := statements[s.Locator]; duplicate || !bytes.Equal(originals[s.Locator], s.Raw) || wikimedia.Hash(s.Raw) != s.RawSHA256 {
				t.Fatal("raw occurrence conservation failed")
			}
			statements[s.Locator] = s
			roles[s.Role]++
		}
	}
	if len(statements) != 31 || len(originals) != len(statements) || !reflect.DeepEqual(roles, map[string]int{"employee": 2, "founder": 4, "executive": 6, "owner": 6, "board_director": 13}) {
		t.Fatal("selected source roles changed", roles)
	}
	historical := statements["/entities/Q173395/claims/P169/2"]
	if historical.HolderID != "wikidata:Q1393271" || historical.RelatedEntityID != "wikidata:Q173395" || historical.Role != "executive" || !historical.HolderHumanStatementObserved || historical.ReferenceCount != 1 || len(historical.Issues) != 0 || len(historical.Times) != 2 {
		t.Fatal("historical CEO source direction or context lost")
	}
	for i, expected := range []struct{ property, text string }{{"P580", "1995"}, {"P582", "2015"}} {
		time := historical.Times[i]
		if time.Property != expected.property || time.Text != expected.text || time.State != "year_precision" || time.Precision == nil || *time.Precision != 9 {
			t.Fatal("year evidence lost or promoted to day boundaries")
		}
	}
	// Employment on the person item is not the same assertion as CEO on Cisco.
	employment := statements["/entities/Q1393271/claims/P108/1"]
	if employment.Role != "employee" || employment.RelatedEntityID != historical.RelatedEntityID || employment.HolderID != historical.HolderID || len(employment.Times) != 0 || !slices.Contains(employment.Issues, "no_reference_supplied") {
		t.Fatal("undated employment promoted to dated executive role")
	}
	// Read the unchanged reviewed corpus; do not turn its local IDs into joins.
	baseline, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]bool{}
	for _, c := range baseline.Cases {
		if c.Assessment.IdentityState != "no_name_employer_candidate" || c.Assessment.IdentityResolved || c.Assessment.GraphPublicationApproved || c.Assessment.FinancialAttribution {
			t.Fatal("frozen FEC screening changed")
		}
		for _, d := range c.Assessment.Decisions {
			_, reviewID, ok := strings.Cut(d.Claim.Source.Locator, ";review=")
			if !ok {
				t.Fatal("missing annotation provenance")
			}
			if claims[reviewID] {
				continue // Two FEC appearances reuse annotations, not occurrences.
			}
			claims[reviewID] = true
			switch reviewID {
			case "cisco-historical-ceo":
				if string(d.Claim.Role) != historical.Role || d.Claim.Issue != "historical_role_end_year_only_not_supported_as_day_bound" || d.Claim.ValidThrough != "" {
					t.Fatal("historical annotation relabeled or day bound invented")
				}
				t.Log(reviewID, "reviewed role agrees with extracted executive assertion; end year 2015 preserved, identity join unapproved")
			case "cisco-emeritus":
				for _, s := range statements {
					if s.HolderID == historical.HolderID && s.RelatedEntityID == historical.RelatedEntityID && (s.Property == "P488" || s.Property == "P3320") {
						t.Fatal("snapshot title/member coverage changed; review again")
					}
				}
				t.Log(reviewID, "title not represented by selected statements in these items; not disproved")
			case "jc2-ceo", "jc2-founder", "jc2-near-name", "ridgeline-board", "ridgeline-founder":
				t.Log(reviewID, "discovery incomplete: relevant organization/alternative person not fetched; no negative affiliation conclusion")
			default:
				t.Fatal("unreviewed comparison case", reviewID)
			}
		}
	}
	if len(claims) != 7 || len(baseline.Cases) != 4 || len(baseline.Cases[3].Assessment.Decisions) != 0 {
		t.Fatal("review population or unsurveyed engineer gap changed")
	}
}
