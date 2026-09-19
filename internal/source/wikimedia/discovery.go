package wikimedia

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/vedantadhobley/legal-tender/internal/nameform"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const DiscoveryContract = "wikimedia/affiliation-discovery@1.0.0"
const DiscoveryPolicy = "reported-name-employer-searches.v1"
const DiscoveryVariantsPolicy = "reported-name-employer-searches.v2"

// These are reported appearances, not people. The caller verifies the source
// bytes; pins and locators alone do not authenticate a hand-written input.
type DiscoveryAppearance struct {
	SHA256   string  `json:"source_sha256"`
	Locator  string  `json:"source_locator"`
	Name     *string `json:"reported_name"`
	Employer *string `json:"reported_employer"`
}

type DiscoverySearch struct {
	Kind        string             `json:"kind"`
	Text        string             `json:"search_text"`
	Appearances []int              `json:"appearance_indexes"`
	Derivations []SearchDerivation `json:"derivations,omitempty"`
}

type SearchDerivation struct {
	Appearance int    `json:"appearance_index"`
	Rule       string `json:"rule"`
}

type DiscoveryPlan struct {
	Policy      string                `json:"policy"`
	BuildSHA256 string                `json:"build_sha256"`
	Selection   string                `json:"selection"`
	Appearances []DiscoveryAppearance `json:"appearances"`
	InputStates []string              `json:"input_states"`
	Searches    []DiscoverySearch     `json:"searches"`
}

// PlanDiscovery makes separate name, name+employer and employer windows. Query
// sharing saves requests, not appearances, and never asserts a common identity.
func PlanDiscovery(inputs []DiscoveryAppearance, selection, build string) (DiscoveryPlan, error) {
	return PlanDiscoveryWithPolicy(inputs, selection, build, DiscoveryPolicy)
}

func PlanDiscoveryWithPolicy(inputs []DiscoveryAppearance, selection, build, policy string) (DiscoveryPlan, error) {
	p := DiscoveryPlan{Policy: policy, BuildSHA256: build, Selection: selection, Appearances: slices.Clone(inputs), InputStates: []string{}, Searches: []DiscoverySearch{}}
	if policy != DiscoveryPolicy && policy != DiscoveryVariantsPolicy {
		return p, fmt.Errorf("unsupported discovery query policy")
	}
	if !Digest(build) || strings.TrimSpace(selection) == "" || len(selection) > 2000 || len(inputs) == 0 || len(inputs) > MaxQueries {
		return p, fmt.Errorf("discovery provenance or appearance budget")
	}
	sort.Slice(p.Appearances, func(i, j int) bool {
		a, b := p.Appearances[i], p.Appearances[j]
		if a.SHA256 != b.SHA256 {
			return a.SHA256 < b.SHA256
		}
		return a.Locator < b.Locator
	})
	searches := map[string]*DiscoverySearch{}
	for i, a := range p.Appearances {
		if !Digest(a.SHA256) || !discoveryText(a.Locator, 1000) || strings.TrimSpace(a.Locator) == "" || (i > 0 && a.SHA256 == p.Appearances[i-1].SHA256 && a.Locator == p.Appearances[i-1].Locator) {
			return p, fmt.Errorf("invalid or duplicate discovery source reference")
		}
		for _, s := range []*string{a.Name, a.Employer} {
			if s != nil && !discoveryText(*s, 500) {
				return p, fmt.Errorf("unsupported discovery text")
			}
		}
		name, employer := discoveryTerms(a.Name), discoveryTerms(a.Employer)
		state := "no_searchable_text"
		add := func(kind, text, rule string) {
			key := kind + "\x00" + text
			if searches[key] == nil {
				searches[key] = &DiscoverySearch{Kind: kind, Text: text, Appearances: []int{}}
			}
			s := searches[key]
			if !slices.Contains(s.Appearances, i) {
				s.Appearances = append(s.Appearances, i)
			}
			if policy == DiscoveryVariantsPolicy {
				s.Derivations = append(s.Derivations, SearchDerivation{Appearance: i, Rule: rule})
			}
		}
		if name != "" {
			add("person_name", name, "reported_tokens")
			state = "name_only"
		}
		if employer != "" {
			add("reported_employer", employer, "reported_tokens")
			state = "employer_only"
		}
		if name != "" && employer != "" {
			add("person_name_employer", name+" "+employer, "reported_tokens")
			state = "name_and_employer"
		}
		p.InputStates = append(p.InputStates, state)
		if policy == DiscoveryVariantsPolicy && employer != "" {
			for _, v := range employerSearchVariants(*a.Employer) {
				add("reported_employer", v.text, v.rule)
				if name != "" {
					add("person_name_employer", name+" "+v.text, v.rule)
				}
			}
		}
	}
	keys := make([]string, 0, len(searches))
	for key := range searches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		p.Searches = append(p.Searches, *searches[key])
	}
	if len(p.Searches) > MaxQueries {
		return p, fmt.Errorf("discovery search budget exceeded")
	}
	return p, nil
}

func discoveryText(s string, limit int) bool {
	if len(s) > limit || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// Query rendering only: preserve token order, case, accents and letter/number
// boundaries. Strip punctuation/operators, not source values. No spelling,
// nickname, initial, suffix or comma-name-order inference occurs here.
func discoveryTerms(s *string) string {
	if s == nil {
		return ""
	}
	terms := strings.Fields(discoveryWords(*s))
	for i, term := range terms {
		terms[i] = `"` + term + `"`
	}
	return strings.Join(terms, " ")
}

func discoveryWords(s string) string {
	return strings.Join(strings.Fields(strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) {
			return r
		}
		return ' '
	}, s)), " ")
}

type searchVariant struct{ text, rule string }

// Employer-only retrieval proposals. Never change reported values or person
// names. These transformations share the organization's bounded vocabulary.
func employerSearchVariants(raw string) []searchVariant {
	original := discoveryWords(raw)
	boundary := nameform.DigitLetterBoundaries(original)
	stem, suffix, _ := nameform.SplitLegalSuffix(original)
	combined, combinedSuffix, _ := nameform.SplitLegalSuffix(boundary)
	variants := []searchVariant{}
	add := func(s, rule string) {
		if s != original && strings.ContainsFunc(s, unicode.IsLetter) {
			variants = append(variants, searchVariant{discoveryTerms(&s), rule})
		}
	}
	add(boundary, "employer_digit_letter_boundaries")
	if suffix != "" {
		add(stem, "employer_legal_suffix_omission")
	}
	if boundary != original && combinedSuffix != "" {
		add(combined, "employer_digit_letter_boundaries_and_legal_suffix_omission")
	}
	return variants
}

func decodeDiscovery(raw []byte) (DiscoveryPlan, []Query, error) {
	var p DiscoveryPlan
	if len(raw) > MaxBody {
		return p, nil, fmt.Errorf("discovery input budget")
	}
	if err := strictjson.Decode(raw, &p); err != nil {
		return p, nil, err
	}
	want, err := PlanDiscoveryWithPolicy(p.Appearances, p.Selection, p.BuildSHA256, p.Policy)
	if err != nil || !reflect.DeepEqual(p, want) {
		return p, nil, fmt.Errorf("discovery plan does not match source-derived policy")
	}
	queries := make([]Query, len(p.Searches))
	for i, s := range p.Searches {
		queries[i].Text = s.Text
	}
	return p, queries, nil
}

func CaptureDiscovery(ctx context.Context, raw []byte, o CaptureOptions) (string, error) {
	client, closeClient := captureClient()
	defer closeClient()
	return captureDiscovery(ctx, raw, o, client, time.Second)
}

func captureDiscovery(ctx context.Context, raw []byte, o CaptureOptions, client *http.Client, spacing time.Duration) (string, error) {
	_, queries, err := decodeDiscovery(raw)
	if err != nil {
		return "", err
	}
	texts := make([]string, len(queries))
	for i, q := range queries {
		texts[i] = q.Text
	}
	return captureSearches(ctx, raw, o, client, spacing, DiscoveryContract, texts)
}

type DiscoveryObservation struct {
	Search            DiscoverySearch      `json:"search"`
	SearchSHA256      string               `json:"search_sha256"`
	SearchShapeSHA256 string               `json:"search_shape_sha256"`
	Pages             []Page               `json:"pages"`
	Candidates        []DiscoveryCandidate `json:"candidates"`
	State             string               `json:"state"`
	Issue             string               `json:"issue,omitempty"`
	Roles             *RoleEvidence        `json:"role_evidence,omitempty"`
}

type DiscoveryCandidate struct {
	PageID                 int64  `json:"page_id"`
	QID                    string `json:"qid,omitempty"`
	State                  string `json:"state"`
	HumanStatementObserved bool   `json:"human_statement_observed"`
}

type DiscoveryResult struct {
	CaptureSHA256            string                 `json:"capture_sha256"`
	Manifest                 Manifest               `json:"manifest"`
	Plan                     DiscoveryPlan          `json:"plan"`
	Observations             []DiscoveryObservation `json:"observations"`
	CaptureUsable            bool                   `json:"capture_usable"`
	DiscoveryComplete        bool                   `json:"exhaustive_discovery"`
	IdentityApproved         bool                   `json:"identity_approved"`
	GraphPublicationApproved bool                   `json:"graph_publication_approved"`
	FinancialAttribution     bool                   `json:"financial_attribution"`
}

// ReadDiscovery follows no new links. Person- and organization-side statements
// are extracted within each verified response, never merged across revisions.
func ReadDiscovery(directory, pin string) (DiscoveryResult, error) {
	r := DiscoveryResult{CaptureUsable: true, Observations: []DiscoveryObservation{}}
	base, err := readSearches(directory, pin, DiscoveryContract, func(raw []byte) ([]Query, error) {
		p, q, err := decodeDiscovery(raw)
		r.Plan = p
		return q, err
	})
	if err != nil {
		return r, err
	}
	r.CaptureSHA256, r.Manifest = base.CaptureSHA256, base.Manifest
	for i, o := range base.Observations {
		d := DiscoveryObservation{Search: r.Plan.Searches[i], SearchSHA256: o.SearchSHA256, SearchShapeSHA256: o.SearchShapeSHA256, Pages: o.Pages, Candidates: []DiscoveryCandidate{}, Issue: o.Issue, State: "retrieved_page_candidates_not_identities"}
		for _, page := range o.Pages {
			c := DiscoveryCandidate{PageID: page.ID, QID: page.Props["wikibase_item"], State: "type_unverified"}
			entity, loaded := o.Entities[c.QID]
			switch {
			case o.Issue != "":
				c.State = "entity_response_unusable"
			case c.QID == "":
				c.State = "page_has_no_qid"
			case !loaded:
				c.State = "entity_not_loaded"
			case entity.Missing != nil:
				c.State = "source_entity_missing"
			default:
				c.HumanStatementObserved = humanStatement(entity)
			}
			if _, disambiguation := page.Props["disambiguation"]; disambiguation {
				c.State = "disambiguation_page"
			}
			d.Candidates = append(d.Candidates, c)
		}
		if o.Issue != "" {
			d.State, r.CaptureUsable = "source_unusable_or_unattempted", false
		} else if len(o.Pages) == 0 {
			d.State = "no_pages_in_search_window"
		} else if ids := PageIDs(o.Pages); len(ids) > 0 {
			roles, err := ExtractRoles(o.rawEntities, o.EntitiesSHA256, ids)
			if err != nil {
				d.State, d.Issue, r.CaptureUsable = "role_source_unusable", err.Error(), false
			} else {
				d.Roles = &roles
			}
		}
		r.Observations = append(r.Observations, d)
	}
	return r, nil
}
