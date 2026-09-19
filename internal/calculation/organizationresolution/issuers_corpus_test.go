package organizationresolution

import (
	"os"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func TestRetainedSECDirectoryCandidates(t *testing.T) {
	// The public fixture is the exact response body, not a rewritten HTTP
	// capture. The original capture includes private operator contact metadata.
	root := "../../../tests/fixtures/organization-resolution/"
	body, err := os.ReadFile(root + "sec-company-tickers-v1.json")
	if err != nil || wikimedia.Hash(body) != "82cd5fd9ccffda811b93ba76070460dd41429c02c00e726f83deb71f553c6cff" {
		t.Fatal("retained SEC body changed", err)
	}
	rows, err := sec.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(root + "capture-v1/queries.json")
	if err != nil || wikimedia.Hash(raw) != "9150ee14df2ba7e6da512b3d728d0086c1bad346effe09f56d19985da11cba42" {
		t.Fatal("retained FEC query snapshot changed", err)
	}
	var queries wikimedia.Queries
	if err := strictjson.Decode(raw, &queries); err != nil {
		t.Fatal(err)
	}
	if err := queries.Validate(); err != nil {
		t.Fatal(err)
	}
	// Match parsed source rows without inventing replacement fetch metadata.
	source := sec.Replay{Rows: rows}
	r := matchIssuers(queries, wikimedia.Hash(raw), source, wikimedia.Hash([]byte("corpus-test")))
	if !r.SourceUsable || r.DirectoryRows != 10422 || r.DirectoryCIKs != 8022 || len(r.Decisions) != 20 {
		t.Fatal("retained directory/query conservation")
	}
	for i, d := range r.Decisions {
		if !reflect.DeepEqual(d.Query, queries.Queries[i]) || len(d.Blockers) != 3 {
			t.Fatal("original source references or blockers lost")
		}
		if i != 2 {
			if d.State != "no_name_candidate_in_directory" || len(d.Candidates) != 0 {
				t.Fatal("unexpected proposal on pinned corpus", i)
			}
			continue
		}
		if d.Query.Text != "1ST SOURCE CORPORATION" || d.State != "single_issuer_candidate_identity_unresolved" || len(d.Candidates) != 1 {
			t.Fatal("retained reported-name proposal")
		}
		c := d.Candidates[0]
		if c.CIK != "0000034782" || len(c.Matches) != 1 || c.Matches[0].Row.Key != "2123" || c.Matches[0].Row.Source.Title != "1ST SOURCE CORP" || c.Matches[0].Row.Source.Ticker != "SRCE" || c.Matches[0].Name.Rule != "legal_suffix_variant" {
			t.Fatal("source-backed identifier/name explanation changed")
		}
	}
	// An observed spelling difference remains a miss under the unchanged
	// vocabulary. The test must not introduce a named exception to recover it.
	found := false
	for _, row := range rows {
		if row.CIK == "0000066740" && row.Source.Title == "3M CO" {
			found = true
		}
	}
	if !found || r.Decisions[18].Query.Text != "3M COMPANY" || len(r.Decisions[18].Candidates) != 0 {
		t.Fatal("unsupported CO/COMPANY difference silently accepted")
	}
	if r.IdentityPublicationApproved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution {
		t.Fatal("directory candidate approved identity or money")
	}
	again := matchIssuers(queries, wikimedia.Hash(raw), source, r.BuildSHA256)
	if !reflect.DeepEqual(r, again) {
		t.Fatal("retained source replay changed")
	}
}
