package organizationresolution

import (
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const IssuerPolicy = "organization-issuer-candidates.v1"

type IssuerMatch struct {
	Row  sec.Row   `json:"row"`
	Name NameMatch `json:"name_match"`
}

type IssuerCandidate struct {
	CIK     string        `json:"cik"`
	Matches []IssuerMatch `json:"matches"`
}

type IssuerDecision struct {
	Query      wikimedia.Query   `json:"query"`
	State      string            `json:"state"`
	Candidates []IssuerCandidate `json:"candidates"`
	Blockers   []string          `json:"blockers"`
}

type IssuerResult struct {
	Policy                      string            `json:"policy"`
	NamePolicy                  string            `json:"name_policy"`
	BuildSHA256                 string            `json:"build_sha256"`
	QueriesSHA256               string            `json:"queries_sha256"`
	Queries                     wikimedia.Queries `json:"queries"`
	CaptureSHA256               string            `json:"capture_sha256"`
	Source                      sec.Manifest      `json:"source"`
	SourceUsable                bool              `json:"source_usable"`
	SourceIssue                 string            `json:"source_issue,omitempty"`
	DirectoryRows               int               `json:"directory_rows"`
	DirectoryCIKs               int               `json:"directory_ciks"`
	Decisions                   []IssuerDecision  `json:"decisions"`
	IdentityPublicationApproved bool              `json:"identity_publication_approved"`
	EmploymentVerified          bool              `json:"employment_verified"`
	OwnershipVerified           bool              `json:"ownership_verified"`
	FinancialAttribution        bool              `json:"financial_attribution"`
}

// DiscoverIssuers verifies both pinned inputs locally. It needs neither a
// Wikimedia search result nor a candidate-supplied identifier. Every query is
// assessed against every row under the same explained name-proposal rules.
func DiscoverIssuers(raw []byte, queryPin, directory, capturePin, build string) (IssuerResult, error) {
	var out IssuerResult
	if len(raw) > wikimedia.MaxBody || !wikimedia.Digest(queryPin) || wikimedia.Hash(raw) != queryPin || !wikimedia.Digest(build) {
		return out, fmt.Errorf("issuer query/build digest or byte budget")
	}
	var queries wikimedia.Queries
	if err := strictjson.Decode(raw, &queries); err != nil {
		return out, err
	}
	if err := queries.Validate(); err != nil {
		return out, err
	}
	source, err := sec.Read(directory, capturePin)
	if err != nil {
		return out, err
	}
	return matchIssuers(queries, queryPin, source, build), nil
}

func matchIssuers(queries wikimedia.Queries, queryPin string, source sec.Replay, build string) IssuerResult {
	out := IssuerResult{Policy: IssuerPolicy, NamePolicy: ExpandedPolicy, BuildSHA256: build,
		QueriesSHA256: queryPin, Queries: queries, CaptureSHA256: source.CaptureSHA256,
		Source: source.Manifest, SourceUsable: source.Issue == "", SourceIssue: source.Issue,
		DirectoryRows: len(source.Rows), Decisions: []IssuerDecision{}}
	ids := map[string]bool{}
	for _, row := range source.Rows {
		ids[row.CIK] = true
	}
	out.DirectoryCIKs = len(ids)
	for _, q := range queries.Queries {
		d := IssuerDecision{Query: q, Candidates: []IssuerCandidate{}, Blockers: []string{
			"fec_input_has_no_independent_cik_binding", "transaction_time_identity_unverified",
			"issuer_directory_scope_and_accuracy_not_guaranteed"}}
		if source.Issue != "" {
			d.State = "source_unusable"
			out.Decisions = append(out.Decisions, d)
			continue
		}
		groups := map[string][]IssuerMatch{}
		for _, row := range source.Rows {
			if m, ok := matchName(q.Text, row.Source.Title); ok {
				m.Source, m.Name = "sec_directory_title", row.Source.Title
				groups[row.CIK] = append(groups[row.CIK], IssuerMatch{Row: row, Name: m})
			}
		}
		keys := make([]string, 0, len(groups))
		for cik := range groups {
			keys = append(keys, cik)
		}
		sort.Strings(keys)
		for _, cik := range keys {
			d.Candidates = append(d.Candidates, IssuerCandidate{CIK: cik, Matches: groups[cik]})
		}
		switch len(d.Candidates) {
		case 0:
			d.State = "no_name_candidate_in_directory"
		case 1:
			d.State = "single_issuer_candidate_identity_unresolved"
		default:
			d.State = "ambiguous_issuer_candidates"
			d.Blockers = append(d.Blockers, "competing_ciks")
		}
		out.Decisions = append(out.Decisions, d)
	}
	return out
}
