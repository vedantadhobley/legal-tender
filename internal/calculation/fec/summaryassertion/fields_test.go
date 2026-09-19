package summaryassertion

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAssertionFieldsRejectsUnknownAndDuplicateFields(t *testing.T) {
	for _, names := range [][]string{{"invented"}, {"CMTE_ID"}, {"PTY_CMTE_CONTB", "PTY_CMTE_CONTB"}} {
		if _, err := ReadAssertionFields(context.Background(), "", "", WindowComparison{}, names); err == nil || !strings.Contains(err.Error(), "invalid or repeated") {
			t.Fatal(names, err)
		}
	}
}

func TestAssertionFieldsRetainedIdentityGate(t *testing.T) {
	root, id := os.Getenv("LT_FAMILY_SUMMARY_FIELDS_STORAGE"), os.Getenv("LT_FAMILY_SUMMARY_FACT_SET")
	if root == "" || id == "" {
		t.Skip("requires retained read-only summary publication")
	}
	path := filepath.Join(root, "facts/fec/committee-summary/v1/manifests", id+".json")
	s, err := Run(context.Background(), root, path, "2024")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Committees) == 0 {
		t.Fatal("empty retained corpus")
	}
	expected := WindowComparison{Cycle: s.Cycle, SummaryInput: s.Input, CommitteeID: s.Committees[0].CommitteeID, Summary: &s.Committees[0]}
	names := []string{"PTY_CMTE_CONTB", "OTH_CMTE_CONTB", "TRANF_FROM_OTHER_AUTH_CMTE", "CAND_LOAN", "OTH_LOANS"}
	fields, err := ReadAssertionFields(context.Background(), root, path, expected, names)
	if err != nil || len(fields) != len(expected.Summary.Assertions) {
		t.Fatal(fields, err)
	}
	for i, f := range fields {
		if f.AssertionID != expected.Summary.Assertions[i].ID || f.RepresentativeFactID != expected.Summary.Assertions[i].RepresentativeFactID || len(f.Fields) != len(names) {
			t.Fatal(f)
		}
	}
	bad := expected
	bad.SummaryInput.ManifestSHA256 = strings.Repeat("0", 64)
	if _, err = ReadAssertionFields(context.Background(), root, path, bad, names); err == nil {
		t.Fatal("accepted changed manifest pin")
	}
	bad = expected
	bad.Cycle = "2022"
	if _, err = ReadAssertionFields(context.Background(), root, path, bad, names); err == nil {
		t.Fatal("accepted different cycle")
	}
	bad = expected
	group := *expected.Summary
	group.Assertions = append([]Assertion(nil), group.Assertions...)
	group.Assertions[0].Members = append([]Member(nil), group.Assertions[0].Members...)
	group.Assertions[0].Members[0].RawSHA256 = strings.Repeat("0", 64)
	bad.Summary = &group
	if _, err = ReadAssertionFields(context.Background(), root, path, bad, names); err == nil {
		t.Fatal("accepted changed representative")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = ReadAssertionFields(ctx, root, path, expected, names); err == nil {
		t.Fatal("ignored cancellation")
	}
}
