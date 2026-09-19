package classic

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExactFixturesMapToCanonicalRecords(t *testing.T) {
	t.Parallel()
	repositoryRoot := filepath.Join("..", "..", "..", "..")
	cases := []struct {
		dataset Dataset
		fixture string
	}{
		{CandidateMaster, "current-candidate"},
		{CandidateMaster, "partial-address"},
		{CommitteeMaster, "connected-organization"},
		{CommitteeMaster, "source-empty-codes"},
		{CandidateCommitteeLinkage, "current-election"},
		{CandidateCommitteeLinkage, "prior-candidate-election"},
		{AllCandidatesSummary, "current-coverage"},
		{AllCandidatesSummary, "prior-coverage-negative-refund"},
		{CurrentCampaignsSummary, "candidate-loan"},
		{CurrentCampaignsSummary, "partial-campaign"},
	}
	for _, test := range cases {
		test := test
		t.Run(string(test.dataset)+"/"+test.fixture, func(t *testing.T) {
			t.Parallel()
			directory := filepath.Join(repositoryRoot, "contracts", "sources", "fec", string(test.dataset), "v1", "fixtures")
			raw, err := os.ReadFile(filepath.Join(directory, test.fixture+".txt"))
			if err != nil {
				t.Fatal(err)
			}
			canonicalBytes, err := os.ReadFile(filepath.Join(directory, test.fixture+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var expected map[string]string
			if err := json.Unmarshal(canonicalBytes, &expected); err != nil {
				t.Fatal(err)
			}
			spec, err := Lookup(string(test.dataset))
			if err != nil {
				t.Fatal(err)
			}
			decoder := NewDecoder(bytes.NewReader(raw))
			if !decoder.Scan() {
				t.Fatalf("fixture has no row: %v", decoder.Err())
			}
			issues := decoder.Row().Validate(spec, "2024")
			if test.dataset == CandidateCommitteeLinkage && test.fixture == "prior-candidate-election" {
				// The candidate election year may be old; the FEC period still equals 2024.
			}
			if len(issues) != 0 {
				t.Fatalf("fixture issues: %+v", issues)
			}
			actual, ok := decoder.Row().CanonicalMap(spec)
			if !ok || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("canonical mismatch\nactual=%v\nexpected=%v", actual, expected)
			}
			if decoder.Scan() {
				t.Fatal("fixture has more than one row")
			}
			if err := decoder.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMalformedRowsRemainInspectable(t *testing.T) {
	t.Parallel()
	spec, err := Lookup(string(CandidateCommitteeLinkage))
	if err != nil {
		t.Fatal(err)
	}
	decoder := NewDecoder(bytes.NewReader([]byte("bad|2024|2022|C00000001|H|P|12")))
	if !decoder.Scan() {
		t.Fatal("missing malformed row")
	}
	issues := decoder.Row().Validate(spec, "2024")
	want := map[string]bool{"missing_line_feed": true, "invalid_candidate_id": true, "period_mismatch": true}
	for _, issue := range issues {
		delete(want, issue.Code)
	}
	if len(want) != 0 {
		t.Fatalf("missing issue codes: %v; got %+v", want, issues)
	}
}

func TestInvalidCalendarDateAndMoney(t *testing.T) {
	t.Parallel()
	spec, err := Lookup(string(AllCandidatesSummary))
	if err != nil {
		t.Fatal(err)
	}
	fields := make([]string, len(spec.Fields))
	fields[0] = "H0AL01097"
	fields[5] = "01.00"
	fields[27] = "02/30/2024"
	var source bytes.Buffer
	for index, value := range fields {
		if index > 0 {
			source.WriteByte('|')
		}
		source.WriteString(value)
	}
	source.WriteByte('\n')
	decoder := NewDecoder(&source)
	if !decoder.Scan() {
		t.Fatal("missing row")
	}
	issues := decoder.Row().Validate(spec, "2024")
	want := map[string]bool{"invalid_money": true, "invalid_coverage_date": true}
	for _, issue := range issues {
		delete(want, issue.Code)
	}
	if len(want) != 0 {
		t.Fatalf("missing issue codes: %v; got %+v", want, issues)
	}
}
