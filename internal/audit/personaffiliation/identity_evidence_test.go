package personaffiliation

import (
	"context"
	"strings"
	"testing"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
)

func TestRetainedIdentityEvidenceClassification(t *testing.T) {
	corpus, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range corpus.Cases {
		a := tc.Assessment.Appearance
		reported := &screen.ReportedRoleMeaning{Source: a.Source, Raw: *a.Receipt.Occupation, Role: screen.UnknownRole, Issue: "reported_occupation_semantics_unassessed"}
		observations := make([]screen.IdentityObservation, 0, len(tc.Assessment.Decisions))
		for _, old := range tc.Assessment.Decisions {
			name := screen.PersonNameComponents{}
			roleID := ""
			originLocator := "reviewed-source:unknown"
			switch {
			case strings.HasSuffix(old.Claim.Source.Locator, "review=jc2-near-name"):
				name = screen.PersonNameComponents{First: "John", Middle: "J", Last: "Chambers"}
				roleID, originLocator = "review:head-of-growth", "reviewed-source:jc2-about"
			case strings.Contains(old.Claim.Source.Locator, "review=jc2-"):
				name = screen.PersonNameComponents{First: "John", Last: "Chambers"}
				originLocator = "reviewed-source:jc2-about"
				if strings.HasSuffix(old.Claim.Source.Locator, "review=jc2-ceo") {
					roleID = "review:chief-executive"
				} else {
					roleID = "review:founder"
				}
			case strings.Contains(old.Claim.Source.Locator, "review=cisco-"):
				name = screen.PersonNameComponents{First: "John", Last: "Chambers"}
				originLocator = "reviewed-source:jc2-biography"
			case strings.Contains(old.Claim.Source.Locator, "review=ridgeline-"):
				name = screen.PersonNameComponents{First: "Dave", Last: "Duffield"}
				originLocator = "reviewed-source:ridgeline-leadership"
			}
			origin := screen.Reference{SHA256: old.Claim.Source.SHA256, Locator: originLocator}
			observations = append(observations, screen.IdentityObservation{
				Role: screen.RoleObservation{Claim: old.Claim, RoleID: roleID, Polarity: "asserted", Origin: &origin},
				Name: name,
			})
		}
		switch tc.ID {
		case "jc2-receipt-1", "jc2-receipt-2":
			reported.Role, reported.RoleID, reported.Issue = screen.Executive, "review:chief-executive", ""
		case "ridgeline-receipt":
			reported.Role, reported.RoleID, reported.Issue = screen.Executive, "", "reported_occupation_is_broad"
		}
		result, err := screen.AssessIdentityEvidence(a, reported, observations, nil, false)
		if err != nil || result.IdentityResolved || result.GraphPublicationApproved || result.FinancialAttribution || len(result.Decisions) != len(observations) {
			t.Fatal(tc.ID, "classification or acceptance boundary", err)
		}
		switch tc.ID {
		case "jc2-receipt-1", "jc2-receipt-2":
			if result.State != "supported_candidate_with_unresolved_rivals" || len(result.Candidates) != 2 {
				t.Fatal(tc.ID, result.State, result.Candidates)
			}
			candidates := map[string]screen.IdentityCandidate{}
			for _, candidate := range result.Candidates {
				candidates[candidate.PersonID] = candidate
			}
			if candidates["review:jc2/john-chambers"].State != "supported_candidate_identity_unresolved" || candidates["review:jc2/john-j-chambers"].State != "unassessed_candidate" {
				t.Fatal(tc.ID, candidates)
			}
			var ceo, rival *screen.IdentityObservationDecision
			for i := range result.Decisions {
				d := &result.Decisions[i]
				if strings.HasSuffix(d.Observation.Role.Claim.Source.Locator, "review=jc2-ceo") {
					ceo = d
				}
				if strings.HasSuffix(d.Observation.Role.Claim.Source.Locator, "review=jc2-near-name") {
					rival = d
				}
			}
			if ceo == nil || rival == nil || ceo.RoleCorrespondence != "specific_role_correspondence" || ceo.TimeState != "unknown_time" || rival.Name.State != "compatible_missing_optional_components" || rival.RoleCorrespondence != "role_semantics_unassessed" {
				t.Fatal(tc.ID, ceo, rival)
			}
		case "ridgeline-receipt":
			if result.State != "abstain_no_candidate" || len(result.Candidates) != 0 {
				t.Fatal("David/Dave acquired an unsupported alias", result.State, result.Candidates)
			}
			for _, decision := range result.Decisions {
				if decision.Name.State != "first_name_variant_unassessed" || decision.Organization == nil || decision.CandidateState != "unresolved_name_variant_not_candidate" {
					t.Fatal("Duffield evidence gap hidden", decision)
				}
			}
		case "reported-engineer-no-discovery":
			if result.State != "abstain_no_candidate" || len(result.Candidates) != 0 || len(result.Decisions) != 0 {
				t.Fatal("unsearched employee became a classified identity", result)
			}
		}
	}
}
