package organizationresolution

import (
	"fmt"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
)

const RegistryNamePolicy = "registry-name-candidates.v1"

type RegistryNameResult struct {
	Policy                   string                 `json:"policy"`
	BuildSHA256              string                 `json:"build_sha256"`
	Evidence                 gleif.NameReplay       `json:"evidence"`
	Decisions                []RegistryNameDecision `json:"decisions"`
	ExhaustiveDiscovery      bool                   `json:"exhaustive_discovery"`
	IdentityApproved         bool                   `json:"identity_approved"`
	EmploymentVerified       bool                   `json:"employment_verified"`
	GraphPublicationApproved bool                   `json:"graph_publication_approved"`
	FinancialAttribution     bool                   `json:"financial_attribution"`
}
type RegistryNameDecision struct {
	Input             int                     `json:"input_index"`
	Query             *int                    `json:"query_index,omitempty"`
	State             string                  `json:"state"`
	WindowComplete    bool                    `json:"publisher_query_window_complete"`
	Candidates        []RegistryNameCandidate `json:"candidates"`
	CorrespondingLEIs []string                `json:"name_corresponding_leis"`
	Blockers          []string                `json:"identity_blockers"`
}
type RegistryNameCandidate struct {
	LEI                string                       `json:"lei"`
	Locator            string                       `json:"locator"`
	Names              []RegistryNameCorrespondence `json:"name_correspondences"`
	EntityStatus       string                       `json:"entity_status"`
	RegistrationStatus string                       `json:"registration_status"`
}
type RegistryNameCorrespondence struct {
	Match    NameMatch `json:"match"`
	Language string    `json:"language"`
	Type     string    `json:"reported_name_type"`
}

// DiscoverRegistryNames consumes verified ReadNames output. Each returned LEI is
// a registry candidate, not a chosen identity or a person-role assertion.
func DiscoverRegistryNames(r gleif.NameReplay, build string) RegistryNameResult {
	out := RegistryNameResult{Policy: RegistryNamePolicy, BuildSHA256: build, Evidence: r, Decisions: []RegistryNameDecision{}}
	for i, input := range r.Manifest.Plan.Inputs {
		d := RegistryNameDecision{Input: i, State: "no_searchable_name", Candidates: []RegistryNameCandidate{}, CorrespondingLEIs: []string{}, Blockers: []string{"reported_name_has_no_independent_identifier_binding", "transaction_time_identity_unverified", "person_role_not_established"}}
		for j, o := range r.Observations {
			if !slices.Contains(o.Query.Inputs, i) {
				continue
			}
			d.Query = &j
			if o.Issue != "" || o.Page == nil {
				d.State = "source_unusable_or_unattempted"
				break
			}
			d.WindowComplete = o.Page.WindowComplete
			if !d.WindowComplete {
				d.Blockers = append(d.Blockers, "additional_registry_results_unfetched")
			}
			d.State = "no_records_in_query_window"
			if len(o.Page.Records) > 0 {
				d.State = "no_name_correspondence_in_query_window"
			}
			for ordinal, record := range o.Page.Records {
				c := RegistryNameCandidate{LEI: record.LEI, Locator: fmt.Sprintf("/data/%d", ordinal), Names: []RegistryNameCorrespondence{}, EntityStatus: record.EntityStatus, RegistrationStatus: record.RegistrationStatus}
				add := func(source string, n gleif.Name) {
					if input.Name == nil {
						return
					}
					if m, ok := MatchName(*input.Name, n.Name); ok {
						m.Source, m.Name = source, n.Name
						c.Names = append(c.Names, RegistryNameCorrespondence{Match: m, Language: n.Language, Type: n.Type})
					}
				}
				add("legal_name", record.LegalName)
				for k, n := range record.OtherNames {
					add(fmt.Sprintf("other_names:%d", k), n)
				}
				for k, n := range record.TransliteratedNames {
					add(fmt.Sprintf("transliterated_names:%d", k), n)
				}
				if len(c.Names) > 0 {
					d.CorrespondingLEIs = append(d.CorrespondingLEIs, record.LEI)
				}
				d.Candidates = append(d.Candidates, c)
			}
			slices.Sort(d.CorrespondingLEIs)
			if len(d.CorrespondingLEIs) == 1 {
				d.State = "name_correspondence_identity_unresolved"
			}
			if len(d.CorrespondingLEIs) > 1 {
				d.State = "ambiguous_name_correspondences"
			}
			break
		}
		out.Decisions = append(out.Decisions, d)
	}
	return out
}
