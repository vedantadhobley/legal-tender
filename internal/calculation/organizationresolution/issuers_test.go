package organizationresolution

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func TestIssuerCandidateRulesAndAmbiguity(t *testing.T) {
	for _, tc := range []struct {
		name, query, state string
		names              []string
		ciks               []int64
		want               int
	}{
		{"suffix", "Example Corporation", "single_issuer_candidate_identity_unresolved", []string{"EXAMPLE CORP", "EXAMPLE BANK"}, []int64{1, 2}, 1},
		{"exact_with_broader_rival", "Example Corporation", "ambiguous_issuer_candidates", []string{"EXAMPLE CORPORATION", "EXAMPLE CORP"}, []int64{1, 2}, 2},
		{"multiple_tickers", "Example Corporation", "single_issuer_candidate_identity_unresolved", []string{"EXAMPLE CORP", "EXAMPLE CORP"}, []int64{1, 1}, 1},
		{"parent_not_bank", "Example Bank", "no_name_candidate_in_directory", []string{"EXAMPLE CORP", "EXAMPLE HOLDINGS"}, []int64{1, 2}, 0},
		{"holdings_not_parent", "Example Holdings", "no_name_candidate_in_directory", []string{"EXAMPLE CORP"}, []int64{1}, 0},
		{"typo_not_repaired", "Exampel Corp", "no_name_candidate_in_directory", []string{"EXAMPLE CORP"}, []int64{1}, 0},
		{"unsupported_suffix", "Example Company", "no_name_candidate_in_directory", []string{"EXAMPLE CO"}, []int64{1}, 0},
		{"different_designators", "Example LLC", "no_name_candidate_in_directory", []string{"EXAMPLE CORP"}, []int64{1}, 0},
		{"boundary", "12 AB CORP", "single_issuer_candidate_identity_unresolved", []string{"12AB CORP"}, []int64{1}, 1},
		{"nonissuer_absence_is_scoped", "Example Union", "no_name_candidate_in_directory", []string{"EXAMPLE CORP"}, []int64{1}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := observation(t).Query
			q.Text = tc.query
			queries := wikimedia.Queries{Version: "organization-queries.v1", Selection: "synthetic", BuildSHA256: wikimedia.Hash([]byte("query-build")), Queries: []wikimedia.Query{q}}
			rows := map[string]sec.SourceRow{}
			for i, name := range tc.names {
				rows[fmt.Sprint(i)] = sec.SourceRow{CIK: tc.ciks[i], Title: name, Ticker: fmt.Sprintf("T%d", i)}
			}
			raw, _ := json.Marshal(rows)
			parsed, err := sec.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			s := sec.Replay{Rows: parsed}
			r := matchIssuers(queries, wikimedia.Hash([]byte("queries")), s, "build")
			d := r.Decisions[0]
			if d.State != tc.state || len(d.Candidates) != tc.want || !reflect.DeepEqual(d.Query, q) || len(d.Blockers) < 3 {
				t.Fatal("candidate/evidence boundary", d)
			}
			matchedRows := 0
			for _, candidate := range d.Candidates {
				for _, m := range candidate.Matches {
					matchedRows++
					if m.Name.Source != "sec_directory_title" || m.Name.Name != m.Row.Source.Title || m.Name.Query.Normalized != Normalize(q.Text) {
						t.Fatal("unexplained name transformation")
					}
				}
			}
			if tc.name == "multiple_tickers" && matchedRows != 2 {
				t.Fatal("duplicate-CIK source rows collapsed")
			}
			if r.IdentityPublicationApproved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
				t.Fatal("directory match approved identity or money")
			}
			again := matchIssuers(queries, r.QueriesSHA256, s, "build")
			if !reflect.DeepEqual(r, again) {
				t.Fatal("replay changed")
			}
			s.Issue, s.Rows = "source_schema_not_accepted", nil
			failed := matchIssuers(queries, r.QueriesSHA256, s, "build")
			if failed.SourceUsable || failed.Decisions[0].State != "source_unusable" || len(failed.Decisions[0].Candidates) != 0 {
				t.Fatal("source failure became no match")
			}
		})
	}
}

func TestDiscoverIssuerPinsAndQueryIndependence(t *testing.T) {
	// Read only the raw FEC query snapshot. No Wikimedia capture or labels are
	// consumed, so queries with failed/unattempted Wikipedia calls still run.
	raw, err := os.ReadFile("../../../tests/fixtures/organization-resolution/capture-v1/queries.json")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	body, err := os.ReadFile("../../source/sec/testdata/directory.json")
	if err != nil {
		t.Fatal(err)
	}
	m := sec.Manifest{Contract: sec.Contract, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/synthetic (test@local)", Response: wikimedia.Response{
		URL: sec.DirectoryURL, ObservedAt: "2026-09-15T00:00:00Z", Status: 200, Headers: map[string]string{"Content-Type": "application/json"}, Bytes: len(body), SHA256: wikimedia.Hash(body), Body: "directory.body"}}
	manifest, _ := json.Marshal(m)
	for name, bytes := range map[string][]byte{"capture.json": manifest, "directory.body": body} {
		if err := os.WriteFile(filepath.Join(dir, name), bytes, 0600); err != nil {
			t.Fatal(err)
		}
	}
	qp, cp := wikimedia.Hash(raw), wikimedia.Hash(manifest)
	r, err := DiscoverIssuers(raw, qp, dir, cp, m.BuildSHA256)
	if err != nil || !r.SourceUsable || len(r.Decisions) != 20 {
		t.Fatal("all raw queries must be assessed independent of Wikimedia", err)
	}
	if r.DirectoryRows != 4 || r.DirectoryCIKs != 3 {
		t.Fatal("directory counts")
	}
	for _, pins := range [][3]string{{"bad", cp, m.BuildSHA256}, {cp, cp, m.BuildSHA256}, {qp, qp, m.BuildSHA256}, {qp, cp, "bad"}} {
		if _, err := DiscoverIssuers(raw, pins[0], dir, pins[1], pins[2]); err == nil {
			t.Fatal("unbound input accepted")
		}
	}
}
