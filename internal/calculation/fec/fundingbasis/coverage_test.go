package fundingbasis

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestCoverageSummaryMoneyDistinguishesBlankZeroAndSigned(t *testing.T) {
	var f SummaryFieldCoverage
	for _, tc := range []struct{ raw, minor string }{{"", ""}, {"0", "0"}, {"-0.00", "0"}, {"0.01", "1"}, {"-1.05", "-105"}, {"92233720368547758.07", "9223372036854775807"}} {
		observation := occ.SummaryMoneyObservation{RawValue: tc.raw, ObservationState: "reported_value", ReportedMinorUnits: &tc.minor, Currency: "USD", MeasurementKind: "summary_value"}
		if tc.raw == "" {
			observation.ObservationState = "source_blank"
			observation.ReportedMinorUnits = nil
		}
		if err := f.observe(tc.raw, observation); err != nil {
			t.Fatal(err)
		}
	}
	if f.Blank != 1 || f.Zero != 2 || f.Positive != 2 || f.Negative != 1 {
		t.Fatal(f)
	}
	for _, tc := range []struct{ raw, minor, state string }{{"", "0", "reported_value"}, {"-2.00", "200", "reported_value"}, {"1.001", "100", "reported_value"}, {"92233720368547758.08", "0", "reported_value"}, {"0", "0", "source_blank"}} {
		before := f
		if err := f.observe(tc.raw, occ.SummaryMoneyObservation{RawValue: tc.raw, ReportedMinorUnits: &tc.minor, ObservationState: tc.state, Currency: "USD", MeasurementKind: "summary_value"}); err == nil || f != before {
			t.Fatal("invalid observation counted", tc, f)
		}
	}
}

func summaryCoverageFixture(t *testing.T, index int, date, opening string) occ.ClassicFact {
	t.Helper()
	spec, _ := classic.Lookup(string(classic.AllCandidatesSummary))
	fields := map[string]string{}
	for _, field := range spec.Fields {
		fields[field] = ""
	}
	fields["CAND_ID"] = fmt.Sprintf("H0AA%05d", index)
	fields["CVG_END_DT"] = date
	fields["COH_BOP"] = opening
	typed := occ.SummaryTypedFields{CandidateID: fields["CAND_ID"], SourceCycle: 2024, Money: map[string]occ.SummaryMoneyObservation{}}
	for _, field := range occ.SummaryMoneyFields() {
		raw := fields[field]
		v := occ.SummaryMoneyObservation{RawValue: raw, ObservationState: "source_blank", Currency: "USD", MeasurementKind: "summary_value"}
		if raw != "" {
			minor, _, issue := money.ParseUSDMinorUnits(raw)
			if issue != "" {
				t.Fatal(issue)
			}
			v.ObservationState = "reported_value"
			v.ReportedMinorUnits = &minor
		}
		typed.Money[field] = v
	}
	if date != "" {
		parsed, err := time.Parse("01/02/2006", date)
		if err != nil {
			t.Fatal(err)
		}
		s := parsed.Format("2006-01-02")
		typed.CoverageThrough = &s
	}
	return occ.ClassicFact{SchemaVersion: occ.ClassicFactSchemaVersion, FactID: fmt.Sprintf("fact-%d", index), FactType: spec.FactType, Dataset: string(spec.Dataset), Cycle: "2024", NaturalKey: fmt.Sprintf("fec:%s:2024:%s", spec.Dataset, fields["CAND_ID"]), OccurrenceSetID: "occurrence-set", OccurrenceID: fmt.Sprintf("occurrence-%d", index), RecordVersionID: "record-version", SourceReleaseID: "fec-" + strings.Repeat("a", 64), SourceContract: spec.SourceContract, State: "valid", IssueCodes: []string{}, SourceFields: fields, TypedFields: typed}
}

func writeSummaryCoverage(t *testing.T, root string, facts []occ.ClassicFact) occ.ClassicFactManifest {
	t.Helper()
	spec, _ := classic.Lookup(string(classic.AllCandidatesSummary))
	w, err := artifact.NewWriter(context.Background(), root, filepath.Join(root, "tmp"), "coverage-fixtures", "facts")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for _, f := range facts {
		if err := w.WriteJSON(f); err != nil {
			t.Fatal(err)
		}
	}
	a, err := w.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	return occ.ClassicFactManifest{FactSetID: "fixture-fact-set", Dataset: string(spec.Dataset), FactType: spec.FactType, Cycle: "2024", SourceContract: spec.SourceContract, SourceReleaseID: "fec-" + strings.Repeat("a", 64), OccurrenceSetID: "occurrence-set", Counts: occ.ClassicFactCounts{Facts: uint64(len(facts)), ValidFacts: uint64(len(facts)), SourceOccurrences: uint64(len(facts))}, Facts: occ.Artifact{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}}
}

func TestSummaryCoverageProfilesEachDateAndNeverAddsCandidates(t *testing.T) {
	root := t.TempDir()
	facts := []occ.ClassicFact{
		summaryCoverageFixture(t, 1, "", ""), summaryCoverageFixture(t, 2, "12/31/2022", "0"), summaryCoverageFixture(t, 3, "01/01/2023", "-1"), summaryCoverageFixture(t, 4, "12/31/2024", "100.50"), summaryCoverageFixture(t, 5, "01/01/2025", "1"),
	}
	m := writeSummaryCoverage(t, root, facts)
	// Upstream exclusions are visible; scanning valid facts does not erase them.
	m.Counts.SourceOccurrences++
	m.Counts.ExcludedOccurrences = 1
	m.Counts.SourceDuplicates = 1
	got, err := profileSummary(context.Background(), root, m, "digest")
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows != 5 || got.Counts.ExcludedOccurrences != 1 || len(got.Money) != len(occ.SummaryMoneyFields()) || len(got.Dates) != 5 {
		t.Fatal(got)
	}
	for _, field := range got.Money {
		if field.Field == "COH_BOP" && field != (SummaryFieldCoverage{Field: "COH_BOP", Blank: 1, Zero: 1, Negative: 1, Positive: 2}) {
			t.Fatal(field)
		}
	}
	want := []string{"source_blank", "before_source_cycle", "within_source_cycle", "within_source_cycle", "after_source_cycle"}
	for i, d := range got.Dates {
		if d.Relation != want[i] || d.Rows != 1 {
			t.Fatal(got.Dates)
		}
	}
	replay, err := profileSummary(context.Background(), root, m, "digest")
	if err != nil || !reflect.DeepEqual(got, replay) {
		t.Fatal("unstable profile", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := profileSummary(ctx, root, m, "digest"); err == nil {
		t.Fatal("cancellation accepted")
	}
	path, _ := artifact.Resolve(root, m.Facts.StorageKey)
	if err := os.WriteFile(path, []byte("changed bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := profileSummary(context.Background(), root, m, "digest"); err == nil || got.Rows != 0 {
		t.Fatal("corruption returned partial success")
	}
}

func TestSummaryCoverageRejectsInvalidSourceAndTypedEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*occ.ClassicFact)
	}{
		{"extra source field", func(f *occ.ClassicFact) { f.SourceFields["UNREVIEWED"] = "" }},
		{"missing source field", func(f *occ.ClassicFact) { delete(f.SourceFields, "COH_BOP") }},
		{"typed amount differs", func(f *occ.ClassicFact) {
			v := f.TypedFields.(occ.SummaryTypedFields)
			money := v.Money["COH_BOP"]
			money.ReportedMinorUnits = ptr("9")
			v.Money["COH_BOP"] = money
			f.TypedFields = v
		}},
		{"invalid date", func(f *occ.ClassicFact) { f.SourceFields["CVG_END_DT"] = "02/30/2024" }},
		{"different typed date", func(f *occ.ClassicFact) {
			v := f.TypedFields.(occ.SummaryTypedFields)
			v.CoverageThrough = ptr("2023-12-31")
			f.TypedFields = v
		}},
		{"missing money field", func(f *occ.ClassicFact) {
			v := f.TypedFields.(occ.SummaryTypedFields)
			delete(v.Money, "COH_BOP")
			f.TypedFields = v
		}},
		{"blank date reported", func(f *occ.ClassicFact) { f.SourceFields["CVG_END_DT"] = "" }},
		{"wrong candidate", func(f *occ.ClassicFact) { f.NaturalKey = "H0AA00009" }},
		{"wrong release", func(f *occ.ClassicFact) { f.SourceReleaseID = "another-release" }},
		{"wrong cycle", func(f *occ.ClassicFact) { f.Cycle = "2026" }},
		{"wrong occurrence ancestry", func(f *occ.ClassicFact) { f.OccurrenceSetID = "different" }},
		{"invalid fact", func(f *occ.ClassicFact) { f.State = "invalid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			f := summaryCoverageFixture(t, 1, "12/31/2024", "1.01")
			tc.change(&f)
			m := writeSummaryCoverage(t, root, []occ.ClassicFact{f})
			if got, err := profileSummary(context.Background(), root, m, "digest"); err == nil || got.Rows != 0 {
				t.Fatal("invalid fact counted")
			}
		})
	}
	root := t.TempDir()
	f := summaryCoverageFixture(t, 1, "", "0")
	m := writeSummaryCoverage(t, root, []occ.ClassicFact{f, f})
	if _, err := profileSummary(context.Background(), root, m, "digest"); err == nil {
		t.Fatal("duplicate summary merged")
	}
	m.Counts.Facts = 100001
	m.Counts.ValidFacts = 100001
	m.Facts.RecordCount = 100001
	if _, err := profileSummary(context.Background(), root, m, "digest"); err == nil {
		t.Fatal("summary index cap ignored")
	}
}

func TestCoverageBindsExactInventoryAndSummaryReferences(t *testing.T) {
	r, _ := inventoryFixture(t)
	b := receipts.FactBundleManifest{Cycle: r.result.Cycle, SourceReleaseID: r.manifest.SourceReleaseID, Counts: receipts.FactBundleCounts{ScheduleAFacts: r.result.Input.Facts}, InputFactSets: []receipts.FactSetReference{{Role: "schedule_a_receipts", FactSetID: r.result.Input.FactSetID, ManifestSHA256: r.result.Input.ManifestSHA256}}}
	if err := r.coverageBundleMatches(b); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*receipts.FactBundleManifest){func(b *receipts.FactBundleManifest) { b.Cycle = "2026" }, func(b *receipts.FactBundleManifest) { b.SourceReleaseID = "different" }, func(b *receipts.FactBundleManifest) { b.Counts.ScheduleAFacts++ }, func(b *receipts.FactBundleManifest) {
		b.InputFactSets = []receipts.FactSetReference{{Role: "schedule_a_receipts", FactSetID: "different"}}
	}, func(b *receipts.FactBundleManifest) { b.InputFactSets = nil }} {
		bad := b
		change(&bad)
		if err := r.coverageBundleMatches(bad); err == nil {
			t.Fatal("mismatched bundle accepted")
		}
	}
	root := t.TempDir()
	m := writeSummaryCoverage(t, root, []occ.ClassicFact{summaryCoverageFixture(t, 1, "", "0")})
	ref := receipts.FactSetReference{Dataset: m.Dataset, FactSetID: m.FactSetID, FactType: m.FactType, ManifestSHA256: "digest"}
	b.Cycle = m.Cycle
	b.SourceReleaseID = m.SourceReleaseID
	if err := coverageSummaryMatches(m, "digest", ref, b, 1); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*occ.ClassicFactManifest){func(m *occ.ClassicFactManifest) { m.Cycle = "2026" }, func(m *occ.ClassicFactManifest) { m.SourceReleaseID = "different" }, func(m *occ.ClassicFactManifest) { m.FactSetID = "different" }, func(m *occ.ClassicFactManifest) { m.Counts.Facts++ }} {
		bad := m
		change(&bad)
		if err := coverageSummaryMatches(bad, "digest", ref, b, 1); err == nil {
			t.Fatal("mixed summary accepted")
		}
	}
	if err := coverageSummaryMatches(m, "wrong-digest", ref, b, 1); err == nil {
		t.Fatal("wrong summary bytes accepted")
	}
	states := map[string]string{}
	for _, req := range coverageRequirements() {
		if states[req.Requirement] != "" || len(req.Evidence) == 0 || req.Reason == "" {
			t.Fatal(req)
		}
		states[req.Requirement] = req.State
	}
	if states["committee_report_opening_balance"] != "scope_incompatible" || states["committee_unitemized_receipts"] != "absent_from_supplied_sources" || states["recipient_cash_availability"] != "unresolved" {
		t.Fatal(states)
	}
	fields := occ.SummaryMoneyFields()
	fields[0] = "MUTATED"
	if occ.SummaryMoneyFields()[0] == "MUTATED" {
		t.Fatal("source money field list is mutable")
	}
}
