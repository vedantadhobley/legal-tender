package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type SummaryCoverage struct {
	Dataset        string                 `json:"dataset"`
	Scope          string                 `json:"scope"`
	State          string                 `json:"state"`
	FactSetID      string                 `json:"fact_set_id"`
	ManifestSHA256 string                 `json:"manifest_sha256"`
	SourceContract string                 `json:"source_contract"`
	Counts         occ.ClassicFactCounts  `json:"publication_counts"`
	FactsArtifact  artifact.Descriptor    `json:"facts_artifact"`
	SourceFields   []string               `json:"source_fields"`
	Money          []SummaryFieldCoverage `json:"money_fields"`
	Dates          []SummaryDateCoverage  `json:"coverage_through"`
	Rows           uint64                 `json:"verified_rows"`
}

// Count field observations without summing candidates or mixing populations.
type SummaryFieldCoverage struct {
	Field    string `json:"field"`
	Blank    uint64 `json:"blank_rows"`
	Positive uint64 `json:"positive_rows"`
	Negative uint64 `json:"negative_rows"`
	Zero     uint64 `json:"zero_rows"`
}

type SummaryDateCoverage struct {
	Date     *string `json:"date"`
	Relation string  `json:"source_cycle_relation"`
	Rows     uint64  `json:"rows"`
}

func profileSummary(ctx context.Context, root string, m occ.ClassicFactManifest, digest string) (SummaryCoverage, error) {
	spec, err := classic.Lookup(m.Dataset)
	if err != nil {
		return SummaryCoverage{}, err
	}
	if spec.Dataset != classic.AllCandidatesSummary && spec.Dataset != classic.CurrentCampaignsSummary {
		return SummaryCoverage{}, fmt.Errorf("candidate-summary publication required")
	}
	if m.FactType != spec.FactType || m.SourceContract != spec.SourceContract || m.Counts.InvalidFacts != 0 || m.Counts.Facts != m.Counts.ValidFacts || m.Facts.RecordCount != m.Counts.Facts {
		return SummaryCoverage{}, fmt.Errorf("coverage audit requires valid summary facts with exact source contract")
	}
	if m.Counts.Facts > 100000 {
		return SummaryCoverage{}, fmt.Errorf("summary uniqueness index exceeds 100000-row limit")
	}
	a := m.Facts
	desc := artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	r, err := artifact.Open[occ.ClassicFact](ctx, root, desc)
	if err != nil {
		return SummaryCoverage{}, err
	}
	defer r.Abort()
	out := SummaryCoverage{Dataset: m.Dataset, Scope: "candidate_summary_not_committee_report", State: "complete_valid_fact_scan", FactSetID: m.FactSetID, ManifestSHA256: digest, SourceContract: m.SourceContract, Counts: m.Counts, FactsArtifact: desc, SourceFields: spec.Fields, Money: []SummaryFieldCoverage{}, Dates: []SummaryDateCoverage{}}
	if m.Counts.ExcludedOccurrences != 0 {
		out.State = "valid_fact_subset_source_exclusions"
	}
	names := occ.SummaryMoneyFields()
	for _, field := range names {
		out.Money = append(out.Money, SummaryFieldCoverage{Field: field})
	}
	dates := map[string]uint64{}
	seen := map[string]bool{}
	factIDs := map[string]bool{}
	for {
		f, ok, err := r.Next()
		if err != nil {
			return SummaryCoverage{}, err
		}
		if !ok {
			break
		}
		if err := ctx.Err(); err != nil {
			return SummaryCoverage{}, err
		}
		if out.Rows >= 100000 {
			return SummaryCoverage{}, fmt.Errorf("summary artifact exceeds bounded row limit")
		}
		typed, err := validateCoverageSummary(f, m, spec)
		if err != nil {
			return SummaryCoverage{}, err
		}
		if seen[typed.CandidateID] || factIDs[f.FactID] {
			return SummaryCoverage{}, fmt.Errorf("duplicate summary candidate or fact identity")
		}
		seen[typed.CandidateID] = true
		factIDs[f.FactID] = true
		for i, field := range names {
			if err := out.Money[i].observe(f.SourceFields[field], typed.Money[field]); err != nil {
				return SummaryCoverage{}, err
			}
		}
		date := ""
		if typed.CoverageThrough != nil {
			date = *typed.CoverageThrough
		}
		dates[date]++
		out.Rows++
	}
	if err := r.Close(); err != nil {
		return SummaryCoverage{}, err
	}
	if out.Rows != m.Counts.Facts {
		return SummaryCoverage{}, fmt.Errorf("summary row conservation failed")
	}
	cycle, err := strconv.Atoi(m.Cycle)
	if err != nil || cycle < 2 || cycle > 9999 {
		return SummaryCoverage{}, fmt.Errorf("invalid source cycle")
	}
	for date, count := range dates {
		d := SummaryDateCoverage{Relation: "source_blank", Rows: count}
		if date != "" {
			d.Date = &date
			d.Relation = "within_source_cycle"
			if date < fmt.Sprintf("%04d-01-01", cycle-1) {
				d.Relation = "before_source_cycle"
			} else if date > fmt.Sprintf("%04d-12-31", cycle) {
				d.Relation = "after_source_cycle"
			}
		}
		out.Dates = append(out.Dates, d)
	}
	sort.Slice(out.Dates, func(i, j int) bool {
		a, b := out.Dates[i].Date, out.Dates[j].Date
		return a == nil && b != nil || a != nil && b != nil && *a < *b
	})
	return out, nil
}

func validateCoverageSummary(f occ.ClassicFact, m occ.ClassicFactManifest, spec classic.Spec) (occ.SummaryTypedFields, error) {
	var t occ.SummaryTypedFields
	if f.SchemaVersion != occ.ClassicFactSchemaVersion || f.FactType != m.FactType || f.Dataset != m.Dataset || f.Cycle != m.Cycle || f.SourceContract != m.SourceContract || f.SourceReleaseID != m.SourceReleaseID || f.OccurrenceSetID != m.OccurrenceSetID || f.FactID == "" || f.OccurrenceID == "" || f.RecordVersionID == "" || f.State != "valid" || len(f.IssueCodes) != 0 {
		return t, fmt.Errorf("summary fact envelope differs from pinned publication")
	}
	if len(f.SourceFields) != len(spec.Fields) {
		return t, fmt.Errorf("summary source layout changed")
	}
	for _, field := range spec.Fields {
		if _, ok := f.SourceFields[field]; !ok {
			return t, fmt.Errorf("missing summary source field %s", field)
		}
	}
	encoded, err := json.Marshal(f.TypedFields)
	if err != nil {
		return t, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&t); err != nil {
		return t, err
	}
	if !spec.ValidNaturalKey(t.CandidateID) || f.NaturalKey != fmt.Sprintf("fec:%s:%s:%s", m.Dataset, m.Cycle, t.CandidateID) || t.CandidateID != f.SourceFields["CAND_ID"] || strconv.Itoa(t.SourceCycle) != m.Cycle {
		return t, fmt.Errorf("summary candidate or cycle identity differs")
	}
	if len(t.Money) != len(occ.SummaryMoneyFields()) {
		return t, fmt.Errorf("summary normalized money layout changed")
	}
	for _, field := range occ.SummaryMoneyFields() {
		if _, ok := t.Money[field]; !ok {
			return t, fmt.Errorf("missing normalized summary money field %s", field)
		}
	}
	raw := f.SourceFields["CVG_END_DT"]
	if raw == "" {
		if t.CoverageThrough != nil {
			return t, fmt.Errorf("blank coverage date became reported")
		}
	} else {
		parsed, err := time.Parse("01/02/2006", raw)
		if err != nil || parsed.Format("01/02/2006") != raw || t.CoverageThrough == nil || parsed.Format("2006-01-02") != *t.CoverageThrough {
			return t, fmt.Errorf("source and normalized coverage date disagree")
		}
	}
	return t, nil
}

func (f *SummaryFieldCoverage) observe(raw string, t occ.SummaryMoneyObservation) error {
	if raw != t.RawValue || t.MeasurementKind != "summary_value" || t.Currency != "USD" {
		return fmt.Errorf("summary money meaning differs from source for %s", f.Field)
	}
	if raw == "" {
		if t.ObservationState != "source_blank" || t.ReportedMinorUnits != nil {
			return fmt.Errorf("blank summary money became reported for %s", f.Field)
		}
		f.Blank++
		return nil
	}
	value, _, issue := money.ParseUSDMinorUnits(raw)
	if issue != "" || t.ObservationState != "reported_value" || t.ReportedMinorUnits == nil || value != *t.ReportedMinorUnits {
		return fmt.Errorf("source and normalized summary money disagree for %s", f.Field)
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}
	if n < 0 {
		f.Negative++
	} else if n > 0 {
		f.Positive++
	} else {
		f.Zero++
	}
	return nil
}
