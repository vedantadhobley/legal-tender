package wikimedia

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const RoleContract = "wikimedia/role-statements@1.0.0"
const MaxRoleEntities = 20
const maxRoleStatements = 10000

// This map encodes publisher property meanings, never person/company IDs.
// Holder is not necessarily a person; owned objects are not necessarily companies.
type propertyMeaning struct {
	relation, role string
	reverse        bool
}

var roleProperties = map[string]propertyMeaning{
	"P108":  {"employer", "employee", false},
	"P112":  {"founder", "founder", true},
	"P169":  {"chief_executive_officer", "executive", true},
	"P3320": {"board_member", "board_director", true},
	"P127":  {"owned_by", "owner", true},
	"P1830": {"owner_of", "owner", false},
	// These are preserved but never promoted to executive/board authority.
	"P488": {"chairperson", "unknown", true},
	"P39":  {"position_held", "unknown", false},
}

type RoleStatement struct {
	Locator                      string     `json:"locator"`
	RawSHA256                    string     `json:"raw_statement_sha256"`
	Raw                          []byte     `json:"raw_statement_bytes"`
	StatementID                  string     `json:"statement_id,omitempty"`
	SubjectID                    string     `json:"subject_id"`
	Property                     string     `json:"property"`
	Rank                         string     `json:"rank,omitempty"`
	SnakType                     string     `json:"snak_type,omitempty"`
	ObjectID                     string     `json:"object_id,omitempty"`
	Relation                     string     `json:"reported_relation"`
	Role                         string     `json:"provisional_role_category"`
	HolderID                     string     `json:"reported_holder_id,omitempty"`
	RelatedEntityID              string     `json:"reported_related_entity_id,omitempty"`
	HolderHumanStatementObserved bool       `json:"holder_human_statement_observed"`
	ReferenceCount               int        `json:"reference_count"`
	Times                        []RoleTime `json:"time_qualifiers"`
	Issues                       []string   `json:"issues"`
}

type PropertyCount struct {
	Property   string `json:"property"`
	Statements int    `json:"statements"`
}

type RoleEntity struct {
	ID            string          `json:"id"`
	Revision      int64           `json:"revision"`
	Modified      string          `json:"modified"`
	Label         string          `json:"english_label"`
	Aliases       []string        `json:"english_aliases"`
	State         string          `json:"state"`
	Uninterpreted []PropertyCount `json:"uninterpreted_properties"`
	Statements    []RoleStatement `json:"statements"`
}

type RoleEvidence struct {
	Contract                 string       `json:"contract"`
	BodySHA256               string       `json:"body_sha256"`
	ShapeSHA256              string       `json:"shape_sha256"`
	ExpectedIDs              []string     `json:"expected_ids"`
	Entities                 []RoleEntity `json:"entities"`
	Limitations              []string     `json:"limitations"`
	IdentityApproved         bool         `json:"identity_approved"`
	GraphPublicationApproved bool         `json:"graph_publication_approved"`
	FinancialAttribution     bool         `json:"financial_attribution"`
}

type roleStatementValue struct {
	ID         string                       `json:"id"`
	Type       string                       `json:"type"`
	Rank       string                       `json:"rank"`
	Main       json.RawMessage              `json:"mainsnak"`
	Qualifiers map[string][]json.RawMessage `json:"qualifiers"`
	Order      []string                     `json:"qualifiers-order"`
	References []json.RawMessage            `json:"references"`
}

type roleSnak struct {
	Type     string `json:"snaktype"`
	Property string `json:"property"`
	Datatype string `json:"datatype"`
	Hash     string `json:"hash,omitempty"`
	Value    *struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	} `json:"datavalue,omitempty"`
}

// ExtractRoles reads an exact wbgetentities response. Discovery and transport
// provenance are caller responsibilities; a body checksum is not authentication.
// Every selected statement occurrence survives, including unsupported/invalid ones.
func ExtractRoles(raw []byte, pin string, expectedIDs []string) (RoleEvidence, error) {
	return extractStatements(raw, pin, expectedIDs, roleProperties, RoleContract)
}

func extractStatements(raw []byte, pin string, expectedIDs []string, properties map[string]propertyMeaning, contract string) (RoleEvidence, error) {
	if len(raw) > MaxBody || !Digest(pin) || Hash(raw) != pin || len(expectedIDs) == 0 || len(expectedIDs) > MaxRoleEntities {
		return RoleEvidence{}, fmt.Errorf("role input pin or byte/entity budget")
	}
	ids := slices.Clone(expectedIDs)
	sort.Strings(ids)
	for i, id := range ids {
		if !qidPattern.MatchString(id) || (i > 0 && id == ids[i-1]) {
			return RoleEvidence{}, fmt.Errorf("invalid or repeated role entity ID")
		}
	}
	entities, err := ParseEntities(raw, ids)
	if err != nil {
		return RoleEvidence{}, err
	}
	out := RoleEvidence{Contract: contract, BodySHA256: pin, ShapeSHA256: ShapeFingerprint(raw), ExpectedIDs: ids, Entities: []RoleEntity{}, Limitations: []string{
		"community_statements_not_independent_corroboration",
		"explicit_input_entities_not_exhaustive_person_or_inverse_discovery",
		"holder_and_related_entity_types_not_assumed_from_property_direction",
		"source_roles_not_fec_identity_or_transaction_time_authority",
		"references_retained_not_fetched_or_verified",
		"rank_is_not_temporal_validity_or_confidence",
	}}
	count := 0
	for _, id := range ids {
		e := entities[id]
		r := RoleEntity{ID: id, Revision: e.LastRevision, Modified: e.Modified, Label: e.Labels["en"].Value, Aliases: []string{}, State: "no_selected_statement_in_snapshot", Uninterpreted: []PropertyCount{}, Statements: []RoleStatement{}}
		for _, a := range e.Aliases["en"] {
			r.Aliases = append(r.Aliases, a.Value)
		}
		if e.Missing != nil {
			r.State = "source_entity_missing"
			out.Entities = append(out.Entities, r)
			continue
		}
		claimProperties := make([]string, 0, len(e.Claims))
		for p := range e.Claims {
			claimProperties = append(claimProperties, p)
		}
		sort.Strings(claimProperties)
		for _, p := range claimProperties {
			var rows []json.RawMessage
			_ = json.Unmarshal(e.Claims[p], &rows) // ParseEntities already validated arrays.
			count += len(rows)
			if count > maxRoleStatements {
				return RoleEvidence{}, fmt.Errorf("role statement budget")
			}
			mapping, selected := properties[p]
			if !selected {
				r.Uninterpreted = append(r.Uninterpreted, PropertyCount{p, len(rows)})
				continue
			}
			for n, row := range rows {
				r.Statements = append(r.Statements, extractRole(id, p, n, row, entities, mapping))
			}
		}
		duplicates := map[string]int{}
		for _, s := range r.Statements {
			if s.StatementID != "" {
				duplicates[s.StatementID]++
			}
		}
		for i := range r.Statements {
			s := &r.Statements[i]
			if duplicates[s.StatementID] > 1 {
				s.Issues = append(s.Issues, "duplicate_statement_id")
			}
			sort.Strings(s.Issues)
		}
		if len(r.Statements) > 0 {
			r.State = "selected_statements_preserved"
		}
		out.Entities = append(out.Entities, r)
	}
	return out, nil
}

func extractRole(id, property string, index int, raw json.RawMessage, entities map[string]Entity, mapping propertyMeaning) RoleStatement {
	r := RoleStatement{SubjectID: "wikidata:" + id, Property: property, Relation: mapping.relation, Role: mapping.role, Locator: fmt.Sprintf("/entities/%s/claims/%s/%d", id, property, index), RawSHA256: Hash(raw), Raw: slices.Clone(raw), Times: []RoleTime{}, Issues: []string{}}
	var s roleStatementValue
	if strictjson.Decode(raw, &s) != nil || s.Type != "statement" || !strings.HasPrefix(strings.ToUpper(s.ID), id+"$") || len(s.ID) <= len(id)+1 || !slices.Contains([]string{"normal", "preferred", "deprecated"}, s.Rank) {
		r.Issues = append(r.Issues, "unsupported_statement_shape")
		return r
	}
	r.StatementID, r.Rank, r.ReferenceCount = s.ID, s.Rank, len(s.References)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	for _, p := range []string{"qualifiers", "qualifiers-order", "references"} {
		if b, present := fields[p]; present && string(b) == "null" {
			r.Issues = append(r.Issues, "null_statement_field:"+p)
		}
	}
	order := map[string]bool{}
	for _, p := range s.Order {
		if _, found := s.Qualifiers[p]; !found || order[p] {
			r.Issues = append(r.Issues, "invalid_qualifier_order")
		}
		order[p] = true
	}
	for _, ref := range s.References {
		if !roleReferenceShape(ref) {
			r.Issues = append(r.Issues, "unsupported_reference_shape")
		}
	}
	if s.Rank == "deprecated" {
		r.Issues = append(r.Issues, "deprecated_statement")
	}
	if len(s.References) == 0 {
		r.Issues = append(r.Issues, "no_reference_supplied")
	}
	if property == "P39" || property == "P488" {
		r.Issues = append(r.Issues, "role_semantics_not_promoted")
	}
	main, issue := parseRoleSnak(s.Main, property)
	if issue != "" {
		r.Issues = append(r.Issues, issue)
	} else {
		r.SnakType = main.Type
		object, issue := itemValue(main)
		if issue != "" {
			r.Issues = append(r.Issues, issue)
		} else {
			r.ObjectID = "wikidata:" + object
			if mapping.role == "not_applicable" {
				// Hierarchy is not a personal role. Retain source endpoints without
				// labeling either one a person, legal corporation or controlling owner.
				if endpoint, found := entities[object]; !found {
					r.Issues = append(r.Issues, "object_entity_not_loaded")
				} else if endpoint.Missing != nil {
					r.Issues = append(r.Issues, "object_source_entity_missing")
				}
				if id == object {
					r.Issues = append(r.Issues, "self_relation")
				}
			} else {
				holder, related := id, object
				if mapping.reverse {
					holder, related = object, id
				}
				r.HolderID, r.RelatedEntityID = "wikidata:"+holder, "wikidata:"+related
				r.HolderHumanStatementObserved = humanStatement(entities[holder])
				if h, found := entities[holder]; !found {
					r.Issues = append(r.Issues, "holder_entity_not_loaded")
				} else if h.Missing != nil {
					r.Issues = append(r.Issues, "holder_source_entity_missing")
				} else if !r.HolderHumanStatementObserved {
					r.Issues = append(r.Issues, "holder_human_statement_not_observed")
				}
				if relatedItem, found := entities[related]; !found {
					r.Issues = append(r.Issues, "related_entity_not_loaded")
				} else if relatedItem.Missing != nil {
					r.Issues = append(r.Issues, "related_source_entity_missing")
				}
				if holder == related {
					r.Issues = append(r.Issues, "self_relation")
				}
				if humanStatement(entities[related]) {
					r.Issues = append(r.Issues, "related_entity_has_human_statement_not_corporate_binding")
				}
			}
		}
	}
	properties := make([]string, 0, len(s.Qualifiers))
	for p := range s.Qualifiers {
		properties = append(properties, p)
	}
	sort.Strings(properties)
	for _, p := range properties {
		if !propertyID(p) || len(s.Qualifiers[p]) == 0 {
			r.Issues = append(r.Issues, "invalid_qualifier_array")
			continue
		}
		if p != "P580" && p != "P582" && p != "P585" {
			r.Issues = append(r.Issues, "uninterpreted_qualifier:"+p)
			continue
		}
		if len(s.Qualifiers[p]) > 1 {
			r.Issues = append(r.Issues, "multiple_time_qualifiers:"+p)
		}
		for n, q := range s.Qualifiers[p] {
			tm := extractRoleTime(p, n, q)
			r.Times = append(r.Times, tm)
			if strings.HasPrefix(tm.State, "unsupported_") {
				r.Issues = append(r.Issues, "uninterpreted_time_qualifier:"+p)
			}
		}
	}
	return r
}

func parseRoleSnak(raw json.RawMessage, property string) (roleSnak, string) {
	var s roleSnak
	if strictjson.Decode(raw, &s) != nil || s.Property != property || s.Datatype == "" || !slices.Contains([]string{"value", "somevalue", "novalue"}, s.Type) {
		return s, "unsupported_snak_shape"
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if _, present := fields["datavalue"]; present && s.Type != "value" {
		return s, "inconsistent_snak_value"
	}
	if (s.Type == "value") != (s.Value != nil) {
		return s, "inconsistent_snak_value"
	}
	return s, ""
}

// References remain opaque evidence beyond this structural gate. In particular,
// a citation is not fetched, verified, or promoted to an independent authority.
func roleReferenceShape(raw json.RawMessage) bool {
	r, err := object(raw, "snaks")
	if err != nil {
		return false
	}
	properties, err := object(r["snaks"])
	if err != nil || len(properties) == 0 {
		return false
	}
	for p, values := range properties {
		if !propertyID(p) {
			return false
		}
		var snaks []json.RawMessage
		if json.Unmarshal(values, &snaks) != nil || len(snaks) == 0 {
			return false
		}
		for _, snak := range snaks {
			if _, err := object(snak); err != nil {
				return false
			}
		}
	}
	return true
}

func itemValue(s roleSnak) (string, string) {
	if s.Type != "value" {
		return "", "source_" + s.Type
	}
	if s.Datatype != "wikibase-item" || s.Value.Type != "wikibase-entityid" {
		return "", "unsupported_item_datatype"
	}
	var v struct {
		ID      string `json:"id"`
		Type    string `json:"entity-type"`
		Numeric *int64 `json:"numeric-id"`
	}
	if strictjson.Decode(s.Value.Value, &v) != nil || v.Type != "item" || !qidPattern.MatchString(v.ID) || (v.Numeric != nil && fmt.Sprintf("Q%d", *v.Numeric) != v.ID) {
		return "", "invalid_item_identity"
	}
	return v.ID, ""
}

func humanStatement(e Entity) bool {
	var statements []json.RawMessage
	_ = json.Unmarshal(e.Claims["P31"], &statements)
	for _, raw := range statements {
		var s roleStatementValue
		if strictjson.Decode(raw, &s) != nil || s.Type != "statement" || !strings.HasPrefix(strings.ToUpper(s.ID), e.ID+"$") || len(s.ID) <= len(e.ID)+1 || !slices.Contains([]string{"normal", "preferred"}, s.Rank) || len(s.Qualifiers) != 0 {
			continue
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if q, present := fields["qualifiers"]; present && string(q) == "null" {
			continue
		}
		main, issue := parseRoleSnak(s.Main, "P31")
		if issue != "" {
			continue
		}
		id, issue := itemValue(main)
		if issue == "" && id == "Q5" {
			return true
		}
	}
	return false
}
