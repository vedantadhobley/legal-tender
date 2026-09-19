package organizationresolution

import (
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const IssuerFilingPolicy = "organization-filed-registrant-comparison.v1"

type FilingComparison struct {
	QueryIndex int      `json:"query_index"`
	CIK        string   `json:"cik"`
	State      string   `json:"state"`
	NameFacts  []int    `json:"matching_name_fact_ordinals"`
	Blockers   []string `json:"blockers"`
}

type FiledIssuerResult struct {
	Policy                      string             `json:"policy"`
	BuildSHA256                 string             `json:"build_sha256"`
	Directory                   IssuerResult       `json:"directory"`
	Filing                      sec.FilingIdentity `json:"filing"`
	Comparisons                 []FilingComparison `json:"comparisons"`
	IdentityPublicationApproved bool               `json:"identity_publication_approved"`
	EmploymentVerified          bool               `json:"employment_verified"`
	OwnershipVerified           bool               `json:"ownership_verified"`
	FinancialAttribution        bool               `json:"financial_attribution"`
}

// CompareIssuerFiling adds first-party tagged registrant evidence. It does not
// treat two SEC resources as independent confirmations of a FEC-to-CIK binding.
// The CLI supplies freshly verified directory and filing captures, not labels.
func CompareIssuerFiling(directory IssuerResult, filing sec.FilingIdentity, build string) (FiledIssuerResult, error) {
	out := FiledIssuerResult{Policy: IssuerFilingPolicy, BuildSHA256: build, Directory: directory, Filing: filing, Comparisons: []FilingComparison{}}
	if !wikimedia.Digest(build) || directory.Policy != IssuerPolicy || directory.NamePolicy != ExpandedPolicy || filing.Reference.Validate() != nil {
		return out, fmt.Errorf("filed issuer input contract")
	}
	selected := false
	for i, d := range directory.Decisions {
		for _, candidate := range d.Candidates {
			c := FilingComparison{QueryIndex: i, CIK: candidate.CIK, State: "filing_not_inspected", NameFacts: []int{}, Blockers: append([]string(nil), d.Blockers...)}
			c.Blockers = append(c.Blockers, "directory_and_filing_do_not_independently_bind_fec_identity")
			if candidate.CIK == filing.Reference.CIK {
				selected = true
				c.State = "filing_source_unusable"
				if filing.SourceUsable {
					ids, names, usable := 0, map[string]bool{}, true
					for _, fact := range filing.Facts {
						if fact.Issue != "" {
							usable = false
							continue
						}
						if fact.Concept == "EntityCentralIndexKey" {
							ids++
							continue
						}
						if fact.Concept == "EntityRegistrantName" {
							n := Normalize(fact.Text)
							if n == "" {
								usable = false
								continue
							}
							names[n] = true
							if n == Normalize(d.Query.Text) {
								c.NameFacts = append(c.NameFacts, fact.Ordinal)
							}
						}
					}
					switch {
					case !usable || ids == 0 || len(names) == 0:
						c.State = "filing_identity_evidence_incomplete_or_conflicting"
					case len(names) > 1:
						c.State = "multiple_filed_registrant_names"
					case len(c.NameFacts) == 0:
						c.State = "filed_registrant_name_does_not_match_reported_text"
					default:
						c.State = "reported_name_matches_filed_registrant"
					}
				}
			}
			out.Comparisons = append(out.Comparisons, c)
		}
	}
	if !selected {
		return out, fmt.Errorf("filing CIK is not a candidate in the pinned directory result")
	}
	return out, nil
}
