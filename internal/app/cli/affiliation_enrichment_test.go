package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestEnrichmentCLIRealSourcesAndStrictJoin(t *testing.T) {
	base := []string{"pipeline", "entities", "enrich-affiliations", "--corpus", "../../../tests/fixtures/person-affiliation", "--expected-corpus-sha256", "7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4"}
	for _, capture := range []struct{ dir, pin string }{
		{"discovery-v1", "c1232da97bbd9e5f53305c043736cfeff001471a4a0488a59f082848e9274f5c"},
		{"discovery-v2", "5ff30d1c86bd6e4b2b7746f88628057f30d03220a6b27a6853fc8124e04a9c81"},
	} {
		args := append(append([]string{}, base...), "--capture", "../../../tests/fixtures/person-affiliation/"+capture.dir, "--expected-capture-sha256", capture.pin)
		var previous []byte
		for range 2 {
			var out, stderr bytes.Buffer
			if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
				t.Fatal(code, stderr.String())
			}
			var r affiliationReport
			if err := json.Unmarshal(out.Bytes(), &r); err != nil {
				t.Fatal(err)
			}
			if len(r.Report.Appearances) != 4 || !r.Report.CaptureUsable || r.Report.IdentityResolved || r.Report.GraphPublicationApproved || r.Report.FinancialAttribution {
				t.Fatal("conservation or approval")
			}
			for _, a := range r.Report.Appearances {
				name := *a.Appearance.Receipt.Name
				want := 0
				if name == "JOHN CHAMBERS" {
					want = 2
				} else if name == "DAVID DUFFIELD" {
					want = 1
				}
				if len(a.MatchingSourceIDs) != want || len(a.EmployerContextIDs) != 0 {
					t.Fatal(name, a.MatchingSourceIDs, a.EmployerContextIDs)
				}
			}
			if previous != nil && !bytes.Equal(previous, out.Bytes()) {
				t.Fatal("CLI replay changed")
			}
			previous = bytes.Clone(out.Bytes())
		}
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), append(args, "--sample-size", "3"), &out, &stderr); code != 1 || out.Len() != 0 {
			t.Fatal("unrelated capture bound to new appearances", code)
		}
		if code := RunContext(context.Background(), append(args, "--query-policy", "reported-name-employer-searches.v2"), &out, &stderr); code != 2 {
			t.Fatal("live query policy silently ignored during replay", code)
		}
	}
}

func TestEnrichmentCLIArguments(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--unknown"}, 2},
		{[]string{"--sample-size", "6"}, 2},
	} {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), append([]string{"pipeline", "entities", "enrich-affiliations"}, tc.args...), &out, &stderr); code != tc.want {
			t.Fatal(code, stderr.String())
		}
	}
}

func TestEnrichmentCLIAutomaticallySelectedRealSample(t *testing.T) {
	args := []string{"pipeline", "entities", "enrich-affiliations",
		"--corpus", "../../../tests/fixtures/person-affiliation", "--expected-corpus-sha256", "7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4",
		"--sample-size", "3", "--capture", "../../../tests/fixtures/person-affiliation/discovery-sample-v1", "--expected-capture-sha256", "79b2f1b49e422831ce6a704b57ae86ea66285aff4f68782d3e206aee840c52d6"}
	var previous []byte
	for range 2 {
		var out, stderr bytes.Buffer
		if code := RunContext(context.Background(), args, &out, &stderr); code != 0 {
			t.Fatal(code, stderr.String())
		}
		var r affiliationReport
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if !r.Selection.PopulationScanned || r.Selection.EligibleOccurrences != 1581 || r.Selection.OutsideProfileOccurrences != 1394 || r.Selection.DistinctInputs != 549 || len(r.Report.Appearances) != 3 || !r.Report.CaptureUsable {
			t.Fatal("sample scope changed")
		}
		pages, searches := 0, 0
		for _, a := range r.Report.Appearances {
			if len(a.MatchingSourceIDs) != 0 || len(a.EmployerContextIDs) != 0 || a.NameState != "no_name_correspondence_in_search_scope" {
				t.Fatal("real sample coverage changed; review evidence")
			}
			for _, s := range a.Searches {
				if len(s.Pages) != len(s.Candidates) {
					t.Fatal("retrieved page evidence lost")
				}
				for _, page := range s.Pages {
					if page.ID == 0 || page.Title == "" {
						t.Fatal("page title or identity lost")
					}
				}
				pages += len(s.Candidates)
				searches++
			}
		}
		if pages != 25 || searches != 9 || r.Report.IdentityResolved || r.Report.GraphPublicationApproved || r.Report.FinancialAttribution {
			t.Fatal("capture conservation or approval")
		}
		if previous != nil && !bytes.Equal(previous, out.Bytes()) {
			t.Fatal("sample replay changed")
		}
		previous = bytes.Clone(out.Bytes())
	}
}
