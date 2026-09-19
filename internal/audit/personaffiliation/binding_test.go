package personaffiliation

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

// Contrast the exact FEC fields with previously reviewed company-page claims.
// This tests existing normalization on real text, not automatic prose extraction
// or a fabricated Wikidata item/role. Annotation references remain corpus-local.
func TestReviewedCompanyBindingContrast(t *testing.T) {
	corpus, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range corpus.Cases {
		correspondences := 0
		for _, d := range c.Assessment.Decisions {
			person := org.Normalize(*c.Assessment.Appearance.Receipt.Name) == org.Normalize(d.Claim.PersonName)
			match, employer := org.MatchName(*c.Assessment.Appearance.Receipt.Employer, d.Claim.OrganizationName)
			if person && employer {
				correspondences++
				if !strings.HasPrefix(d.Claim.PersonID, "review:") || d.Claim.ValidFrom != "" || d.Claim.ValidThrough != "" || d.Claim.AsOf != "" {
					t.Fatal("review claim became external identity or dated role")
				}
				t.Log(c.ID, "reviewed company claim has person-name and employer correspondence via", match.Rule, "; no role interval or accepted identity")
			}
			if i == 2 && (!employer || person) {
				t.Fatal("Ridgeline suffix or David/Dave boundary changed")
			}
		}
		want := 0
		if i < 2 {
			want = 2 // CEO and founder annotations share one source sentence.
		}
		if correspondences != want || c.Assessment.IdentityResolved || c.Assessment.IdentityState != "no_name_employer_candidate" {
			t.Fatal("normalization contrast changed the frozen baseline or case evidence")
		}
	}
}

func TestRetainedAppearanceBindingKeepsNamesakesAndMissingEmployers(t *testing.T) {
	corpus, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	appearances := map[screen.Reference]screen.Appearance{}
	for _, c := range corpus.Cases {
		a := c.Assessment.Appearance
		appearances[a.Source] = a
	}
	if len(appearances) != 4 {
		t.Fatal("source occurrences collapsed")
	}
	for _, capture := range []struct{ directory, pin string }{
		{"discovery-v1", "c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c"},
		{"discovery-v2", "5ff30d1c86bd6e4b2b7746f88628057f30d03220a6b27a6853fc8124e04a9c81"},
	} {
		t.Run(capture.directory, func(t *testing.T) {
			discovery, err := wikimedia.ReadDiscovery(filepath.Join(fixtureDir, capture.directory), capture.pin)
			if err != nil || !discovery.CaptureUsable {
				t.Fatal("discovery input unusable", err)
			}
			seen := map[screen.Reference]bool{}
			nameSets := map[screen.Reference]map[string]bool{}
			for _, observation := range discovery.Observations {
				for _, index := range observation.Search.Appearances {
					input := discovery.Plan.Appearances[index]
					ref := screen.Reference{SHA256: input.SHA256, Locator: input.Locator}
					a, found := appearances[ref]
					if !found || !reflect.DeepEqual(a.Receipt.Name, input.Name) || !reflect.DeepEqual(a.Receipt.Employer, input.Employer) {
						t.Fatal("discovery is not bound to the FEC appearance")
					}
					seen[ref] = true
					if nameSets[ref] == nil {
						nameSets[ref] = map[string]bool{}
					}
					if observation.Roles == nil {
						continue // Retained empty/failed source state, never an identity negative.
					}
					r, err := screen.AssessBinding(a, *observation.Roles)
					if err != nil || r.IdentityResolved || r.GraphPublicationApproved || r.FinancialAttribution {
						t.Fatal("binding or financial boundary", err)
					}
					if !reflect.DeepEqual(a, r.Appearance) || r.BodySHA256 != observation.Roles.BodySHA256 {
						t.Fatal("source or occurrence provenance lost")
					}
					for _, c := range r.Candidates {
						nameSets[ref][c.ID] = true // Test-only inventory, never a cross-snapshot identity merge.
						if c.EmployerState != "no_corresponding_employer_endpoint_observed" {
							t.Fatal("retained employer coverage changed; review evidence", c.ID)
						}
					}
					again, err := screen.AssessBinding(a, *observation.Roles)
					if err != nil || !reflect.DeepEqual(r, again) {
						t.Fatal("unstable binding replay", err)
					}
				}
			}
			if len(seen) != 4 {
				t.Fatal("appearance coverage lost")
			}
			for i, c := range corpus.Cases {
				ids := nameSets[c.Assessment.Appearance.Source]
				switch i {
				case 0, 1:
					if !ids["wikidata:Q1393271"] || !ids["wikidata:Q93784"] || len(ids) != 2 {
						t.Fatal("Chambers namesake evidence changed", ids)
					}
				case 2:
					if !ids["wikidata:Q5233095"] || len(ids) != 1 {
						t.Fatal("Duffield candidate coverage changed", ids)
					}
				case 3:
					if len(ids) != 0 {
						t.Fatal("engineer candidate coverage changed; not a verified negative", ids)
					}
				}
				t.Logf("%s: %d distinct name candidates across retained observations; no corresponding employer endpoint; identity unresolved", c.ID, len(ids))
			}
		})
	}
}

func TestRetainedBindingComparesCoarseTermToActualReceiptDay(t *testing.T) {
	corpus, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(fixtureDir, "wikidata-interactive-1.json"))
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := wikimedia.ExtractRoles(raw, "5683ad0692e124dcd9d87bcdc81b7448def720f632a2e4e4b66b8aae4f490076", []string{"Q1393271", "Q173395", "Q8034666"})
	if err != nil {
		t.Fatal(err)
	}
	historical := 0
	for _, c := range corpus.Cases {
		r, err := screen.AssessBinding(c.Assessment.Appearance, evidence)
		if err != nil || r.IdentityResolved || r.GraphPublicationApproved || r.FinancialAttribution {
			t.Fatal("binding boundary", err)
		}
		for _, person := range r.Candidates {
			for _, role := range person.Roles {
				s := role.Statement
				if s.Property == "P169" && s.HolderID == "wikidata:Q1393271" && s.RelatedEntityID == "wikidata:Q173395" {
					historical++
					if role.TemporalState != "after_reported_end_precision" || role.EndpointState != "employer_name_not_corresponding" || len(s.Times) != 2 || s.Times[0].Text != "1995" || s.Times[1].Text != "2015" {
						t.Fatal("historical CEO role turned into 2023 employer/role evidence")
					}
					t.Log(c.ID, *r.Appearance.Receipt.ReceiptDate, "is after reported CEO end year 2015; Cisco differs from reported JC2 employer; donor identity remains unresolved")
				}
				if s.Property == "P108" && role.TemporalState != "role_time_unknown" {
					t.Fatal("undated employment inherited another claim's dates")
				}
			}
		}
	}
	if historical != 2 {
		t.Fatal("same rule did not process both distinct Chambers occurrences")
	}
	// Neither order nor existing human review annotations steer the new binding.
	dir, updated := fixtureCopy(t)
	for i := range updated.Roles {
		updated.Roles[i].Claim.PersonName = "Unrelated review annotation"
	}
	changed, err := Run(context.Background(), dir, writeCorpus(t, dir, updated))
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(evidence.Entities)
	for i, c := range changed.Cases {
		want, err := screen.AssessBinding(corpus.Cases[i].Assessment.Appearance, evidence)
		if err != nil {
			t.Fatal(err)
		}
		got, err := screen.AssessBinding(c.Assessment.Appearance, evidence)
		if err != nil || !reflect.DeepEqual(want, got) {
			t.Fatal("review labels changed automatic comparison", err)
		}
	}
}
