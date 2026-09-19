package wikimedia

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

const RelationshipContract = "wikimedia/relationship-statements@1.0.0"

// RelationshipEndpoint is vocabulary from this response only, not a resolved
// real-world identity or a label borrowed from another snapshot.
type RelationshipEndpoint struct {
	ID       string   `json:"id"`
	State    string   `json:"state"`
	Label    string   `json:"english_label,omitempty"`
	Aliases  []string `json:"english_aliases"`
	Revision int64    `json:"revision,omitempty"`
	Modified string   `json:"modified,omitempty"`
}

// ReportedHierarchy is an explicit display orientation of one source statement.
// It does not add another assertion or identify direct/ultimate/wholly owned parents.
type ReportedHierarchy struct {
	ParentID string `json:"parent_id"`
	ChildID  string `json:"child_id"`
	Mapping  string `json:"mapping"`
}

type RelationshipMatch struct {
	Statement RoleStatement         `json:"statement"`
	Subject   RelationshipEndpoint  `json:"subject"`
	Object    *RelationshipEndpoint `json:"object,omitempty"`
	Hierarchy *ReportedHierarchy    `json:"reported_hierarchy,omitempty"`
}

type RelationshipQuery struct {
	Contract                  string               `json:"contract"`
	BodySHA256                string               `json:"body_sha256"`
	ShapeSHA256               string               `json:"shape_sha256"`
	ExpectedIDs               []string             `json:"expected_ids"`
	ObservedAt                string               `json:"observed_at,omitempty"`
	ObservationTimeBasis      string               `json:"observation_time_basis"`
	Entity                    RelationshipEndpoint `json:"entity"`
	Scope                     string               `json:"scope"`
	SelectedStatementsScanned int                  `json:"selected_statements_scanned"`
	Matches                   []RelationshipMatch  `json:"matches"`
	Limitations               []string             `json:"limitations"`
	IdentityApproved          bool                 `json:"identity_approved"`
	GraphPublicationApproved  bool                 `json:"graph_publication_approved"`
	FinancialAttribution      bool                 `json:"financial_attribution"`
}

// QueryRelationships returns incident claims in one explicit retained response.
// It does not fetch endpoints, traverse recursively, choose current claims or bind
// FEC appearances. observedAt is optional caller-supplied acquisition metadata,
// never the execution time, an entity revision time or relationship validity.
func QueryRelationships(raw []byte, pin string, expectedIDs []string, entityID, observedAt string) (RelationshipQuery, error) {
	id, qualified := strings.CutPrefix(entityID, "wikidata:")
	if !qualified || !qidPattern.MatchString(id) {
		return RelationshipQuery{}, fmt.Errorf("relationship query requires a wikidata:QID, not a name or FEC identity")
	}
	if observedAt != "" {
		if _, err := time.Parse(time.RFC3339, observedAt); err != nil {
			return RelationshipQuery{}, fmt.Errorf("invalid observation timestamp")
		}
	}
	properties := maps.Clone(roleProperties)
	properties["P355"] = propertyMeaning{"child_organization_or_unit", "not_applicable", false}
	properties["P749"] = propertyMeaning{"parent_organization_or_unit", "not_applicable", false}
	evidence, err := extractStatements(raw, pin, expectedIDs, properties, RelationshipContract)
	if err != nil {
		return RelationshipQuery{}, err
	}
	endpoints := make(map[string]RelationshipEndpoint, len(evidence.Entities))
	for _, e := range evidence.Entities {
		state := "item_loaded_type_unverified"
		if e.State == "source_entity_missing" {
			state = e.State
		}
		key := "wikidata:" + e.ID
		endpoints[key] = RelationshipEndpoint{key, state, e.Label, slices.Clone(e.Aliases), e.Revision, e.Modified}
	}
	endpoint := func(key string) RelationshipEndpoint {
		if e, ok := endpoints[key]; ok {
			return e
		}
		return RelationshipEndpoint{ID: key, State: "item_not_loaded", Aliases: []string{}}
	}
	r := RelationshipQuery{
		Contract: RelationshipContract, BodySHA256: pin, ShapeSHA256: evidence.ShapeSHA256,
		ExpectedIDs: evidence.ExpectedIDs, Entity: endpoint(entityID), ObservedAt: observedAt,
		ObservationTimeBasis: "not_supplied", Scope: "one_hop_in_supplied_response",
		Matches: []RelationshipMatch{}, Limitations: append(evidence.Limitations,
			"only_selected_properties_and_parseable_incident_endpoints_are_searchable",
			"no_matches_is_not_absence_of_real_world_relationships",
			"hierarchy_is_not_verified_legal_ownership_or_direct_ultimate_parentage",
			"inverse_assertions_and_duplicate_occurrences_are_not_merged",
			"no_current_role_or_interval_overlap_inference",
			"observation_time_is_caller_metadata_not_authenticated_by_body_pin"),
	}
	if observedAt != "" {
		r.ObservationTimeBasis = "caller_supplied"
	}
	for _, e := range evidence.Entities {
		for _, s := range e.Statements {
			r.SelectedStatementsScanned++
			if s.SubjectID != entityID && s.ObjectID != entityID {
				continue
			}
			m := RelationshipMatch{Statement: s, Subject: endpoint(s.SubjectID)}
			if s.ObjectID != "" {
				o := endpoint(s.ObjectID)
				m.Object = &o
				switch s.Property {
				case "P355":
					m.Hierarchy = &ReportedHierarchy{s.SubjectID, s.ObjectID, "P355_subject_parent_object_child"}
				case "P749":
					m.Hierarchy = &ReportedHierarchy{s.ObjectID, s.SubjectID, "P749_object_parent_subject_child"}
				}
			}
			r.Matches = append(r.Matches, m)
		}
	}
	return r, nil
}
