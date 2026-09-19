package personaffiliation

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

const ProsePolicy = "prose-relationship-syntax-candidates.v1"

// ProseText offsets address UTF-8 bytes in an Entry.Text projection, NOT bytes
// in HTML. The containing original HTML span is recorded separately.
type ProseText struct {
	Text  string `json:"text"`
	Start int    `json:"text_start"`
	End   int    `json:"text_end"`
}

type ProseCitation struct {
	Entry   int              `json:"entry_index"`
	HTML    companypage.Span `json:"html_span"`
	Matched ProseText        `json:"matched_text"`
}

type ProseRole struct {
	Evidence     ProseCitation `json:"evidence"`
	Pattern      string        `json:"pattern"`
	Person       ProseText     `json:"person_text"`
	Predicate    ProseText     `json:"predicate_text"`
	Role         ProseText     `json:"role_text"`
	Organization ProseText     `json:"organization_text"`
	TimeWording  *ProseText    `json:"time_wording,omitempty"`
	TimeState    string        `json:"time_state"`
	Meaning      string        `json:"meaning"`
}

type ProseAlias struct {
	Evidence      ProseCitation `json:"evidence"`
	LeadingName   ProseText     `json:"leading_name_text"`
	Parenthetical ProseText     `json:"parenthetical_name_text"`
	FollowingName ProseText     `json:"following_name_text"`
	Meaning       string        `json:"meaning"`
}

type ProseEntry struct {
	Entry   int    `json:"entry_index"`
	State   string `json:"state"`
	Roles   int    `json:"role_candidates"`
	Aliases int    `json:"alias_candidates"`
}

type ProseResult struct {
	Policy                   string             `json:"policy"`
	Source                   companypage.Source `json:"source"`
	LexicalContract          string             `json:"lexical_contract"`
	Entries                  []ProseEntry       `json:"text_entry_outcomes"`
	OtherEntries             int                `json:"other_entries_not_interpreted"`
	Roles                    []ProseRole        `json:"role_candidates"`
	Aliases                  []ProseAlias       `json:"alias_candidates"`
	Limitations              []string           `json:"limitations"`
	IdentityApproved         bool               `json:"identity_approved"`
	GraphPublicationApproved bool               `json:"graph_publication_approved"`
	FinancialAttribution     bool               `json:"financial_attribution"`
}

// This is a deliberately bounded English grammar, not general NER or an
// identity policy. Vocabulary has no person, employer or website exceptions.
const proseNameWord = `(?:\p{Lu}\.|\p{Lu}[\p{L}\p{M}'’-]*(?:\.[\p{L}\p{M}]+)*)`
const prosePerson = proseNameWord + `(?: ` + proseNameWord + `){1,5}`
const proseRoleAtom = `(?:chief executive officer|chief financial officer|general manager|territory manager|sales representative|member at large|vice president|board member|co-founder|co-CEO|CEO|CFO|president|founder|chairman|chair|director|secretary|treasurer|owner|employee|engineer)`
const proseRole = `(?i:(?:former )?` + proseRoleAtom + `(?: (?:and|&) ` + proseRoleAtom + `){0,3})`
const proseOrgWord = `[\p{Lu}\p{N}][\p{L}\p{M}\p{N}'’&/-]*(?:\.[\p{L}\p{N}]+)*`
const proseOrganization = proseOrgWord + `(?: (?:` + proseOrgWord + `|of|the|and|for|&)){0,11}`

var proseCopula = regexp.MustCompile(`^(?P<person>` + prosePerson + `) (?P<predicate>(?i:is|was)(?: (?i:not))?) (?i:(?:the|a|an) )?(?P<role>` + proseRole + `) (?i:of|at|for|with) (?i:the )?(?P<organization>` + proseOrganization + `?)(?: (?P<time>(?i:since|from|until|in|as of) [^.!?]{1,100}))?(?P<ending>\.|$)`)
var proseApposition = regexp.MustCompile(`^(?P<person>` + prosePerson + `), (?P<role>` + proseRole + `) (?i:of|at|for|with) (?i:the )?(?P<organization>` + proseOrganization + `),`)
var proseParenthetical = regexp.MustCompile(`(?P<leading>` + proseNameWord + `) \((?P<alias>\p{Lu}[\p{Ll}\p{M}'’-]{1,39})\) (?P<following>` + proseNameWord + `(?: ` + proseNameWord + `){0,3})`)
var proseRoleOnly = regexp.MustCompile(`^(?i:` + proseRoleAtom + `)$`)

// ProposeProse consumes only verified companypage.Extract output. The caller
// retains it alongside this result. This function does not authenticate a
// caller-constructed Evidence value or copy a page title into a relation.
// Only text-block prefixes are eligible for role syntax; aliases can occur
// within a block. Every text block gets an outcome, including unsupported prose.
func ProposeProse(ctx context.Context, evidence companypage.Evidence) (ProseResult, error) {
	if err := ctx.Err(); err != nil {
		return ProseResult{}, err
	}
	if evidence.Contract != companypage.Contract || evidence.Source.Validate() != nil || evidence.Bytes <= 0 || evidence.Bytes > companypage.MaxBody || len(evidence.Entries) > 10_000 || evidence.IdentityApproved || evidence.GraphPublicationApproved || evidence.FinancialAttribution {
		return ProseResult{}, fmt.Errorf("prose candidates require verified bounded lexical evidence")
	}
	r := ProseResult{Policy: ProsePolicy, Source: evidence.Source, LexicalContract: evidence.Contract,
		Entries: []ProseEntry{}, Roles: []ProseRole{}, Aliases: []ProseAlias{},
		Limitations: []string{
			"syntax_candidates_not_verified_person_organization_or_role_assertions",
			"bounded_english_block_prefix_grammar_not_exhaustive_extraction",
			"capitalized_name_shapes_are_not_entity_type_proof",
			"full_entry_context_and_publisher_truth_not_interpreted",
			"no_cross_block_heading_card_pronoun_or_website_identity_inference",
			"parenthetical_name_forms_not_canonical_aliases_or_nickname_expansion",
			"time_wording_retained_without_role_interval_inference",
			"page_observation_and_metadata_dates_not_role_dates",
			"lexical_text_not_browser_visibility_or_independent_corroboration",
		}}
	for i, e := range evidence.Entries {
		if err := ctx.Err(); err != nil {
			return ProseResult{}, err
		}
		if e.Kind != "text" {
			r.OtherEntries++
			continue
		}
		if e.Span.Start < 0 || e.Span.End > evidence.Bytes || e.Span.End < e.Span.Start {
			return ProseResult{}, fmt.Errorf("invalid lexical entry span")
		}
		out := ProseEntry{Entry: i, State: "no_supported_syntax_not_negative_evidence"}
		for _, pattern := range []struct {
			name string
			re   *regexp.Regexp
		}{{"name_copula_role_preposition_organization", proseCopula}, {"name_appositive_role_preposition_organization", proseApposition}} {
			m := pattern.re.FindStringSubmatchIndex(e.Text)
			if m == nil {
				continue
			}
			person := proseField(e.Text, pattern.re, m, "person")
			// Capitalized conjunctions/articles do not turn multiple people or a
			// conditional sentence into one name-shaped subject.
			if proseAmbiguousName(person.Text) {
				continue
			}
			candidate := ProseRole{Evidence: proseCitation(i, e, m[0], m[1]), Pattern: pattern.name,
				Person: person, Predicate: proseField(e.Text, pattern.re, m, "predicate"),
				Role: proseField(e.Text, pattern.re, m, "role"), Organization: proseField(e.Text, pattern.re, m, "organization"),
				TimeState: "role_validity_unknown", Meaning: "unverified_relation_mention"}
			if candidate.Predicate.Text == "" {
				// The comma itself is evidence of the syntactic operator, not an
				// invented present-tense statement or employment start date.
				candidate.Predicate = proseSlice(e.Text, person.End, person.End+1)
			}
			if time := proseField(e.Text, pattern.re, m, "time"); time.Text != "" {
				candidate.TimeWording = &time
				candidate.TimeState = "explicit_time_wording_uninterpreted"
			}
			r.Roles = append(r.Roles, candidate)
			out.Roles++
		}
		for _, m := range proseParenthetical.FindAllStringSubmatchIndex(e.Text, -1) {
			leading := proseField(e.Text, proseParenthetical, m, "leading")
			alias := proseField(e.Text, proseParenthetical, m, "alias")
			following := proseField(e.Text, proseParenthetical, m, "following")
			before, _ := utf8.DecodeLastRuneInString(e.Text[:m[0]])
			if proseAmbiguousName(leading.Text+" "+following.Text) || proseRoleOnly.MatchString(alias.Text) || (m[0] > 0 && !strings.ContainsRune(" ,;:—–-", before)) {
				continue
			}
			r.Aliases = append(r.Aliases, ProseAlias{Evidence: proseCitation(i, e, m[0], m[1]), LeadingName: leading, Parenthetical: alias, FollowingName: following, Meaning: "unverified_parenthetical_name_form"})
			out.Aliases++
		}
		if out.Roles+out.Aliases > 0 {
			out.State = "syntax_candidates_context_unassessed"
		}
		r.Entries = append(r.Entries, out)
	}
	return r, nil
}

func proseAmbiguousName(s string) bool {
	for _, word := range strings.Fields(strings.ToLower(s)) {
		switch word {
		case "the", "a", "an", "and", "or", "if", "when", "whether":
			return true
		}
	}
	return false
}

func proseField(text string, pattern *regexp.Regexp, match []int, name string) ProseText {
	i := pattern.SubexpIndex(name)
	if i < 0 || match[2*i] < 0 {
		return ProseText{}
	}
	return proseSlice(text, match[2*i], match[2*i+1])
}

func proseSlice(text string, start, end int) ProseText {
	return ProseText{Text: text[start:end], Start: start, End: end}
}

func proseCitation(index int, entry companypage.Entry, start, end int) ProseCitation {
	return ProseCitation{Entry: index, HTML: entry.Span, Matched: proseSlice(entry.Text, start, end)}
}
