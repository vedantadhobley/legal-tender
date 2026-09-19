package fundingbasis

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const ReportProfilePolicy = "fec/receipt-report-occurrence-profile@1.0.0"
const maxReportProfileGroups = 500_000

type ReportProfile struct {
	SchemaVersion        string                  `json:"schema_version"`
	ProfileID            string                  `json:"profile_id"`
	Policy               string                  `json:"policy"`
	IndividualPolicy     string                  `json:"individual_policy"`
	Cycle                string                  `json:"cycle"`
	SummaryInput         summaryassertion.Input  `json:"summary_input"`
	SummaryCalculationID string                  `json:"summary_calculation_id"`
	Source               fecrelease.StagedOutput `json:"schedule_a_source"`
	Verification         schedulea.Verification  `json:"verification"`
	Total                Measures                `json:"total_occurrences"`
	Individual           Measures                `json:"individual_predicate_occurrences"`
	Forms                []ReportFormGroup       `json:"form_line_decisions"`
	Reports              []ReportOccurrenceGroup `json:"individual_report_groups"`
	NotEstablished       []string                `json:"not_established"`
	ComparisonReady      bool                    `json:"comparison_ready"`
	TerminalEligible     bool                    `json:"terminal_attribution_eligible"`
}

type ReportFormKey struct {
	Form     Cell   `json:"filing_form"`
	Schedule Cell   `json:"schedule_type"`
	Line     Cell   `json:"line_num"`
	Decision string `json:"individual_decision"`
}
type ReportFormGroup struct {
	Key      ReportFormKey `json:"key"`
	Measures Measures      `json:"measures"`
}
type ReportOccurrenceKey struct {
	Committee  Cell `json:"committee"`
	File       Cell `json:"file_num"`
	Form       Cell `json:"filing_form"`
	Schedule   Cell `json:"schedule_type"`
	Line       Cell `json:"line_num"`
	ReportType Cell `json:"report_type"`
	ReportYear Cell `json:"report_year"`
}
type ReceiptDateProfile struct {
	Missing     uint64 `json:"missing_rows"`
	Invalid     uint64 `json:"invalid_rows"`
	BeforeCycle uint64 `json:"before_cycle_rows"`
	InCycle     uint64 `json:"in_cycle_rows"`
	AfterCycle  uint64 `json:"after_cycle_rows"`
	First       string `json:"first_observed_date"`
	Last        string `json:"last_observed_date"`
}
type ReportOccurrenceGroup struct {
	Key      ReportOccurrenceKey `json:"key"`
	Measures Measures            `json:"measures"`
	Dates    ReceiptDateProfile  `json:"receipt_dates"`
	First    uint64              `json:"first_source_row_ordinal"`
	Last     uint64              `json:"last_source_row_ordinal"`
}

// ProfileReports verifies the summary source, then scans its release-owned A
// relation once. It observes physical rows, not deduplicated/effective facts.
// No summary/detail monetary delta or report coverage inference is made.
func ProfileReports(ctx context.Context, root, summaryPath, cycle string, progress func(string)) (ReportProfile, error) {
	b, err := newReportProfiler(cycle)
	if err != nil {
		return ReportProfile{}, err
	}
	scan, err := scanReportSource(ctx, root, summaryPath, cycle, b.observe, progress)
	if err != nil {
		return ReportProfile{}, err
	}
	b.out.SummaryInput, b.out.SummaryCalculationID = scan.summary, scan.calculationID
	b.out.Source, b.out.Verification = scan.source, scan.verification
	if scan.verification.Rows != b.out.Total.Rows {
		return ReportProfile{}, fmt.Errorf("incomplete report profile")
	}
	return b.finish()
}

type reportSourceScan struct {
	summary       summaryassertion.Input
	calculationID string
	source        fecrelease.StagedOutput
	verification  schedulea.Verification
}

// Both profile versions share this exact source selection and verification gate.
func scanReportSource(ctx context.Context, root, summaryPath, cycle string, observe func(*schedulea.Row) error, progress func(string)) (reportSourceScan, error) {
	summary, err := summaryassertion.Run(ctx, root, summaryPath, cycle)
	if err != nil {
		return reportSourceScan{}, err
	}
	source, err := profileSource(root, summary.Input, cycle)
	if err != nil {
		return reportSourceScan{}, err
	}
	path, err := artifact.Resolve(root, source.StorageKey)
	if err != nil {
		return reportSourceScan{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return reportSourceScan{}, err
	}
	defer f.Close()
	v, err := schedulea.VerifyZstdRows(ctx, f, schedulea.VerifyOptions{
		ExpectedPeriod: cycle, ExpectedRows: *source.RowCount,
		ExpectedCompressedBytes: source.CompressedByteCount, ExpectedCompressedSHA256: source.CompressedSHA256,
		ExpectedUncompressedBytes: source.UncompressedByteCount, ExpectedUncompressedSHA256: source.UncompressedSHA256,
	}, func(row *schedulea.Row) error {
		if err := observe(row); err != nil {
			return err
		}
		if row.Number()%10_000_000 == 0 && progress != nil {
			progress(fmt.Sprintf("profiled %d Schedule A occurrences", row.Number()))
		}
		return nil
	})
	if err != nil {
		return reportSourceScan{}, err
	}
	if !v.Complete {
		return reportSourceScan{}, fmt.Errorf("incomplete report profile")
	}
	// Runtime is operational evidence in stderr, not content identity.
	if progress != nil {
		progress(fmt.Sprintf("complete verified scan: %d rows in %d ms", v.Rows, v.ElapsedMilliseconds))
	}
	v.ElapsedMilliseconds = 0
	if err := ctx.Err(); err != nil {
		return reportSourceScan{}, err
	}
	return reportSourceScan{summary.Input, summary.CalculationID, source, v}, nil
}

func profileSource(root string, input summaryassertion.Input, cycle string) (fecrelease.StagedOutput, error) {
	f, err := os.Open(filepath.Join(root, "releases", "fec", "manifests", input.SourceReleaseID+".json"))
	if err != nil {
		return fecrelease.StagedOutput{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	if err != nil {
		return fecrelease.StagedOutput{}, err
	}
	d := sha256.Sum256(raw)
	if len(raw) > 4<<20 || hex.EncodeToString(d[:]) != input.SourceReleaseSHA256 {
		return fecrelease.StagedOutput{}, fmt.Errorf("summary source release digest mismatch")
	}
	var m fecrelease.ReleaseManifest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return fecrelease.StagedOutput{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fecrelease.StagedOutput{}, fmt.Errorf("trailing source release JSON")
	}
	if issues := fecrelease.ValidateKnownManifest(m); len(issues) > 0 {
		return fecrelease.StagedOutput{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}
	if m.ReleaseID != input.SourceReleaseID {
		return fecrelease.StagedOutput{}, fmt.Errorf("source release identity mismatch")
	}
	var selected []fecrelease.StagedOutput
	for _, output := range m.StagedOutputs {
		if output.SourceID == fecrelease.ScheduleASourceID && output.Period == cycle {
			selected = append(selected, output)
		}
	}
	if len(selected) != 1 || selected[0].RowCount == nil || *selected[0].RowCount == 0 {
		return fecrelease.StagedOutput{}, fmt.Errorf("one nonempty cycle Schedule A relation required")
	}
	return selected[0], nil
}

func (b *reportProfiler) finish() (ReportProfile, error) {
	var all, included Measures
	for key, m := range b.forms {
		if !m.valid() {
			return ReportProfile{}, fmt.Errorf("invalid form measures")
		}
		if err := all.merge(*m); err != nil {
			return ReportProfile{}, err
		}
		b.out.Forms = append(b.out.Forms, ReportFormGroup{key, *m})
	}
	for _, g := range b.reports {
		d := g.Dates
		if !g.Measures.valid() || d.Missing+d.Invalid+d.BeforeCycle+d.InCycle+d.AfterCycle != g.Measures.Rows {
			return ReportProfile{}, fmt.Errorf("report/date conservation failed")
		}
		if err := included.merge(g.Measures); err != nil {
			return ReportProfile{}, err
		}
		b.out.Reports = append(b.out.Reports, *g)
	}
	if all != b.out.Total || included != b.out.Individual {
		return ReportProfile{}, fmt.Errorf("report profile conservation failed")
	}
	// Compare cells directly: null and empty stay distinct without allocating
	// JSON strings for every sort comparison.
	sort.Slice(b.out.Forms, func(i, j int) bool {
		a, c := b.out.Forms[i].Key, b.out.Forms[j].Key
		v := compareProfileCells([]Cell{a.Form, a.Schedule, a.Line}, []Cell{c.Form, c.Schedule, c.Line})
		if v == 0 {
			return a.Decision < c.Decision
		}
		return v < 0
	})
	sort.Slice(b.out.Reports, func(i, j int) bool {
		a, c := b.out.Reports[i].Key, b.out.Reports[j].Key
		return compareProfileCells([]Cell{a.Committee, a.File, a.Form, a.Schedule, a.Line, a.ReportType, a.ReportYear}, []Cell{c.Committee, c.File, c.Form, c.Schedule, c.Line, c.ReportType, c.ReportYear}) < 0
	})
	raw, err := json.Marshal(b.out)
	if err != nil {
		return ReportProfile{}, err
	}
	d := sha256.Sum256(raw)
	b.out.ProfileID = hex.EncodeToString(d[:])
	return b.out, nil
}

func compareProfileCells(a, b []Cell) int {
	for i, x := range a {
		y := b[i]
		if x.Present != y.Present {
			if !x.Present {
				return -1
			}
			return 1
		}
		if v := cmp.Compare(x.Value, y.Value); v != 0 {
			return v
		}
	}
	return 0
}
