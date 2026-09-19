package organizationresolution

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const CorroborationPolicy = "organization-registry-corroboration.v1"

type LEIClaim struct {
	Index       int             `json:"claim_index"`
	StatementID string          `json:"statement_id,omitempty"`
	LEI         string          `json:"lei,omitempty"`
	State       string          `json:"state"`
	Raw         json.RawMessage `json:"raw"`
}

// RegistryClaims preserves every P1278 statement. Qualifiers are not treated as
// current validity, and deprecated/invalid claims never become corroboration.
func RegistryClaims(e wikimedia.Entity) []LEIClaim {
	out := []LEIClaim{}
	raw, ok := e.Claims["P1278"]
	if !ok {
		return out
	}
	var rows []json.RawMessage
	if strictjson.Decode(raw, &rows) != nil || rows == nil {
		return []LEIClaim{{Index: -1, State: "unsupported_claim_shape", Raw: raw}}
	}
	for i, raw := range rows {
		c := LEIClaim{Index: i, State: "unsupported_claim_shape", Raw: raw}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if string(fields["qualifiers"]) == "null" || string(fields["qualifiers-order"]) == "null" {
			out = append(out, c)
			continue
		}
		var s struct {
			ID   string `json:"id"`
			Rank string `json:"rank"`
			Type string `json:"type"`
			Main struct {
				Property string `json:"property"`
				SnakType string `json:"snaktype"`
				DataType string `json:"datatype"`
				Hash     string `json:"hash"`
				Value    struct {
					Type  string `json:"type"`
					Value string `json:"value"`
				} `json:"datavalue"`
			} `json:"mainsnak"`
			Qualifiers     map[string]json.RawMessage `json:"qualifiers"`
			QualifierOrder []string                   `json:"qualifiers-order"`
			References     json.RawMessage            `json:"references"`
		}
		if strictjson.Decode(raw, &s) == nil && s.Type == "statement" && strings.HasPrefix(s.ID, e.ID+"$") && len(s.ID) > len(e.ID)+1 && s.Main.Property == "P1278" && s.Main.DataType == "external-id" && s.Main.SnakType == "value" && s.Main.Value.Type == "string" {
			c.StatementID, c.LEI = s.ID, s.Main.Value.Value
			switch {
			case !gleif.ValidLEI(c.LEI):
				c.State = "invalid_lei"
			case s.Rank == "deprecated":
				c.State = "deprecated"
			case s.Rank != "normal" && s.Rank != "preferred":
				c.State = "unsupported_rank"
			case len(s.Qualifiers) > 0 || len(s.QualifierOrder) > 0:
				c.State = "qualified_validity_unassessed"
			default:
				c.State = "unqualified_nondeprecated"
			}
		}
		out = append(out, c)
	}
	return out
}

// Requests are derived from all successfully retrieved search candidates, not
// benchmark labels or only the name winner. Even deprecated valid identifiers
// can be observed; eligibility is a separate calculation. Missing claims do not
// trigger a name-search fallback, parent lookup or fabricated identifier.
func RegistryRequests(replay wikimedia.Replay) (gleif.RequestSet, error) {
	r := gleif.RequestSet{WikimediaCaptureSHA256: replay.CaptureSHA256, SelectionPolicy: "organization-registry-requests.v1", LEIs: []string{}}
	set := map[string]bool{}
	for _, o := range replay.Observations {
		if o.Issue != "" {
			continue
		}
		for _, id := range wikimedia.PageIDs(o.Pages) {
			for _, c := range RegistryClaims(o.Entities[id]) {
				if gleif.ValidLEI(c.LEI) {
					set[c.LEI] = true
				}
			}
		}
	}
	for lei := range set {
		r.LEIs = append(r.LEIs, lei)
	}
	sort.Strings(r.LEIs)
	return r, r.Validate()
}

type RegistryName struct {
	Field             string     `json:"field"`
	Name              gleif.Name `json:"name"`
	ReportedNameExact bool       `json:"reported_name_exact"`
	ItemNameExact     bool       `json:"item_name_exact"`
}
type RegistryComparison struct {
	LEI            string         `json:"lei"`
	RecordObserved bool           `json:"record_observed"`
	SourceIssue    string         `json:"source_issue,omitempty"`
	Names          []RegistryName `json:"names"`
}
type RegistryCandidate struct {
	QID         string               `json:"qid"`
	State       string               `json:"state"`
	Claims      []LEIClaim           `json:"claims"`
	Comparisons []RegistryComparison `json:"comparisons"`
	Blockers    []string             `json:"blockers"`
}
type RegistryCase struct {
	QueryIndex  int                 `json:"query_index"`
	SourceIssue string              `json:"source_issue,omitempty"`
	Candidates  []RegistryCandidate `json:"candidates"`
}
type Corroboration struct {
	Policy                      string         `json:"policy"`
	BuildSHA256                 string         `json:"build_sha256"`
	Proposals                   Result         `json:"proposals"`
	Registry                    gleif.Replay   `json:"registry"`
	Cases                       []RegistryCase `json:"cases"`
	IdentityPublicationApproved bool           `json:"identity_publication_approved"`
	EmploymentVerified          bool           `json:"employment_verified"`
	OwnershipVerified           bool           `json:"ownership_verified"`
	FinancialAttribution        bool           `json:"financial_attribution"`
}

func Corroborate(replay wikimedia.Replay, registry gleif.Replay, build string) (Corroboration, error) {
	want, err := RegistryRequests(replay)
	if err != nil {
		return Corroboration{}, err
	}
	if !reflect.DeepEqual(want, registry.Manifest.Requests) || len(registry.Observations) != len(want.LEIs) || !wikimedia.Digest(build) || !wikimedia.Digest(registry.CaptureSHA256) {
		return Corroboration{}, fmt.Errorf("corroboration source/request binding mismatch")
	}
	byLEI := map[string]gleif.Observation{}
	for i, o := range registry.Observations {
		if o.LEI != want.LEIs[i] || (o.Issue == "" && (o.Record == nil || o.Record.LEI != o.LEI)) {
			return Corroboration{}, fmt.Errorf("corroboration record conservation")
		}
		byLEI[o.LEI] = o
	}
	p, err := ResolveWithPolicy(replay, build, ExpandedPolicy)
	if err != nil {
		return Corroboration{}, err
	}
	out := Corroboration{Policy: CorroborationPolicy, BuildSHA256: build, Proposals: p, Registry: registry, Cases: []RegistryCase{}}
	for i, d := range p.Decisions {
		x := RegistryCase{QueryIndex: i, SourceIssue: d.Evidence.Issue, Candidates: []RegistryCandidate{}}
		for _, candidate := range d.Candidates {
			e := d.Evidence.Entities[candidate.QID]
			c := RegistryCandidate{QID: candidate.QID, State: "no_lei_claim", Claims: RegistryClaims(e), Comparisons: []RegistryComparison{}, Blockers: []string{"fec_input_has_no_independent_lei_binding", "transaction_time_identity_unverified"}}
			ids := map[string]bool{}
			eligible := map[string]bool{}
			uncertain := false
			for _, claim := range c.Claims {
				if gleif.ValidLEI(claim.LEI) {
					ids[claim.LEI] = true
				}
				if claim.State == "unqualified_nondeprecated" {
					eligible[claim.LEI] = true
				} else if claim.State != "deprecated" {
					uncertain = true
				}
			}
			keys := []string{}
			for id := range ids {
				keys = append(keys, id)
			}
			sort.Strings(keys)
			if len(c.Claims) > 0 {
				c.State = "lei_claim_not_corroborated"
			}
			if len(eligible) > 1 {
				c.Blockers = append(c.Blockers, "competing_nondeprecated_identifiers")
			}
			if uncertain {
				c.Blockers = append(c.Blockers, "unsupported_or_qualified_identifier_claim")
			}
			if len(candidate.Matches) == 0 {
				c.Blockers = append(c.Blockers, "not_a_name_proposal")
			}
			if d.State == "ambiguous_name_candidates" || d.State == "candidate_evidence_incomplete" {
				c.Blockers = append(c.Blockers, "name_candidate_set_unresolved")
			}
			correspondence := false
			for _, id := range keys {
				o, exists := byLEI[id]
				if !exists {
					return Corroboration{}, fmt.Errorf("missing required registry observation")
				}
				comparison := RegistryComparison{LEI: id, SourceIssue: o.Issue, Names: []RegistryName{}}
				if o.Issue == "" {
					comparison.RecordObserved = true
					comparison.Names = registryNames(d.Evidence, e, *o.Record)
					c.State = "lei_record_observed_fec_identity_unresolved"
					legal := comparison.Names[0]
					if len(eligible) == 1 && eligible[id] && !uncertain && len(candidate.Matches) > 0 && legal.ReportedNameExact && legal.ItemNameExact {
						correspondence = true
					}
					// Lapsed, inactive and other statuses remain observations. They
					// cannot prove present validity or nonexistence of an entity.
					if o.Record.EntityStatus != "ACTIVE" || o.Record.RegistrationStatus != "ISSUED" {
						c.Blockers = append(c.Blockers, "registry_status_requires_review")
					}
				}
				c.Comparisons = append(c.Comparisons, comparison)
			}
			if len(ids) == 0 {
				c.Blockers = append(c.Blockers, "no_usable_lei_claim")
			}
			if correspondence {
				c.State = "name_and_lei_correspondence_fec_identity_unresolved"
			}
			sort.Strings(c.Blockers)
			c.Blockers = slices.Compact(c.Blockers)
			x.Candidates = append(x.Candidates, c)
		}
		out.Cases = append(out.Cases, x)
	}
	return out, nil
}

func registryNames(o wikimedia.Observation, e wikimedia.Entity, r gleif.Record) []RegistryName {
	names := []string{}
	if v, ok := e.Labels["en"]; ok {
		names = append(names, v.Value)
	}
	for _, v := range e.Aliases["en"] {
		names = append(names, v.Value)
	}
	for _, p := range o.Pages {
		if _, disambig := p.Props["disambiguation"]; !disambig && p.Props["wikibase_item"] == e.ID {
			names = append(names, p.Title)
		}
	}
	out := []RegistryName{}
	add := func(field string, n gleif.Name) {
		form := Normalize(n.Name)
		v := RegistryName{Field: field, Name: n, ReportedNameExact: form != "" && Normalize(o.Query.Text) == form}
		for _, name := range names {
			if form != "" && Normalize(name) == form {
				v.ItemNameExact = true
			}
		}
		out = append(out, v)
	}
	add("entity.legalName", r.LegalName)
	for _, n := range r.OtherNames {
		add("entity.otherNames", n)
	}
	for _, n := range r.TransliteratedNames {
		add("entity.transliteratedOtherNames", n)
	}
	return out
}
