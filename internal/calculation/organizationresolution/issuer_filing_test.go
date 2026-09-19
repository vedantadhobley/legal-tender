package organizationresolution

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestFiledIssuerEvidenceCannotApproveConnections(t *testing.T) {
	body, err := os.ReadFile("../../source/sec/testdata/filing.htm")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, query, state string
		change             func(*sec.FilingIdentity)
	}{
		{"exact", "EXAMPLE CORPORATION", "reported_name_matches_filed_registrant", nil},
		{"format_only", "  Example, Corporation. ", "reported_name_matches_filed_registrant", nil},
		{"no_new_suffix_rule", "EXAMPLE CORP", "filed_registrant_name_does_not_match_reported_text", nil},
		{"bank_not_parent", "EXAMPLE BANK", "filed_registrant_name_does_not_match_reported_text", nil},
		{"missing_cik", "EXAMPLE CORPORATION", "filing_identity_evidence_incomplete_or_conflicting", func(f *sec.FilingIdentity) { f.Facts = f.Facts[:1] }},
		{"missing_name", "EXAMPLE CORPORATION", "filing_identity_evidence_incomplete_or_conflicting", func(f *sec.FilingIdentity) { f.Facts = f.Facts[1:] }},
		{"conflict", "EXAMPLE CORPORATION", "filing_identity_evidence_incomplete_or_conflicting", func(f *sec.FilingIdentity) { f.Facts[1].Issue = "reported_identifier_conflict" }},
		{"context_issue", "EXAMPLE CORPORATION", "filing_identity_evidence_incomplete_or_conflicting", func(f *sec.FilingIdentity) { f.Facts[0].Issue = "context_issuer_conflict" }},
		{"several_names", "EXAMPLE CORPORATION", "multiple_filed_registrant_names", func(f *sec.FilingIdentity) {
			other := f.Facts[0]
			other.Text = "Example Bank"
			other.Ordinal = 3
			f.Facts = append(f.Facts, other)
		}},
		{"duplicate_name_occurrences", "EXAMPLE CORPORATION", "reported_name_matches_filed_registrant", func(f *sec.FilingIdentity) { other := f.Facts[0]; other.Ordinal = 3; f.Facts = append(f.Facts, other) }},
		{"failed_source", "EXAMPLE CORPORATION", "filing_source_unusable", func(f *sec.FilingIdentity) {
			f.SourceUsable = false
			f.Facts = nil
			f.Issues = []string{"http_status_not_ok"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts, err := sec.ParseFilingIdentity(body, "0000000100")
			if err != nil {
				t.Fatal(err)
			}
			filing := sec.FilingIdentity{Reference: sec.FilingReference{CIK: "0000000100", Accession: "0000000100-25-000001", Document: "example.htm"}, SourceUsable: true, Facts: facts}
			if tc.change != nil {
				tc.change(&filing)
			}
			q := observation(t).Query
			q.Text = tc.query
			directory := IssuerResult{Policy: IssuerPolicy, NamePolicy: ExpandedPolicy, SourceUsable: true, Decisions: []IssuerDecision{{Query: q, State: "ambiguous_issuer_candidates", Candidates: []IssuerCandidate{{CIK: "0000000100"}, {CIK: "0000000200"}}, Blockers: []string{"fec_input_has_no_independent_cik_binding", "transaction_time_identity_unverified"}}}}
			build := wikimedia.Hash([]byte("build"))
			r, err := CompareIssuerFiling(directory, filing, build)
			if err != nil || len(r.Comparisons) != 2 || r.Comparisons[0].State != tc.state || r.Comparisons[1].State != "filing_not_inspected" {
				t.Fatal(err, r.Comparisons)
			}
			if !reflect.DeepEqual(r.Directory, directory) || !reflect.DeepEqual(r.Filing, filing) || len(r.Comparisons[0].Blockers) != 3 {
				t.Fatal("lost evidence or ambiguity")
			}
			if r.IdentityPublicationApproved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
				t.Fatal("evidence promoted to connections")
			}
			if tc.name == "duplicate_name_occurrences" && !reflect.DeepEqual(r.Comparisons[0].NameFacts, []int{1, 3}) {
				t.Fatal("occurrences collapsed")
			}
			again, err := CompareIssuerFiling(directory, filing, build)
			if err != nil || !reflect.DeepEqual(r, again) {
				t.Fatal("unstable replay")
			}
			filing.Reference.CIK = "0000000300"
			if _, err := CompareIssuerFiling(directory, filing, build); err == nil {
				t.Fatal("unselected filing accepted")
			}
		})
	}
}

func TestRealFiledNameComparison(t *testing.T) {
	body, err := os.ReadFile("../../../tests/fixtures/organization-resolution/sec-annual-report-v1.htm")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := sec.ParseFilingIdentity(body, "0000034782")
	if err != nil {
		t.Fatal(err)
	}
	// Public filing regression, not an organization whitelist in runtime code.
	q := observation(t).Query
	q.Text = "1ST SOURCE CORPORATION"
	d := IssuerResult{Policy: IssuerPolicy, NamePolicy: ExpandedPolicy, SourceUsable: true, Decisions: []IssuerDecision{{Query: q, Candidates: []IssuerCandidate{{CIK: "0000034782"}}}}}
	f := sec.FilingIdentity{Reference: sec.FilingReference{CIK: "0000034782", Accession: "0000034782-25-000025", Document: "source-20241231.htm"}, SourceUsable: true, Facts: facts}
	r, err := CompareIssuerFiling(d, f, wikimedia.Hash([]byte("build")))
	if err != nil || r.Comparisons[0].State != "reported_name_matches_filed_registrant" || !strings.Contains(strings.Join(r.Comparisons[0].Blockers, " "), "do_not_independently_bind") || r.IdentityPublicationApproved {
		t.Fatal("real evidence boundary", err)
	}
}
