package fundingbasis

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

const ReportProfileV2Policy = "fec/receipt-report-occurrence-profile@2.0.0"

type ReportLineProfile struct {
	SchemaVersion        string                   `json:"schema_version"`
	ProfileID            string                   `json:"profile_id"`
	Policy               string                   `json:"policy"`
	IndividualPolicy     string                   `json:"individual_policy"`
	LinePolicy           string                   `json:"line_policy"`
	Cycle                string                   `json:"cycle"`
	SummaryInput         summaryassertion.Input   `json:"summary_input"`
	SummaryCalculationID string                   `json:"summary_calculation_id"`
	Source               fecrelease.StagedOutput  `json:"schedule_a_source"`
	Verification         schedulea.Verification   `json:"verification"`
	Total                Measures                 `json:"total_occurrences"`
	Individual           Measures                 `json:"individual_predicate_occurrences"`
	LinePopulation       Measures                 `json:"reviewed_nonmemo_line_occurrences"`
	Forms                []ReportLineProfileForm  `json:"form_line_groups"`
	Reports              []ReportLineProfileGroup `json:"report_line_groups"`
	NotEstablished       []string                 `json:"not_established"`
	ComparisonReady      bool                     `json:"comparison_ready"`
	TerminalEligible     bool                     `json:"terminal_attribution_eligible"`
}

type ReportLineProfileFormKey struct {
	ReportLineKey
	Decision string `json:"individual_decision"`
}

type ReportLineProfileForm struct {
	Key      ReportLineProfileFormKey `json:"key"`
	Measures Measures                 `json:"measures"`
}

type ReportLineProfileKey struct {
	Committee Cell `json:"committee"`
	File      Cell `json:"file_num"`
	ReportLineProfileFormKey
	ReportType Cell `json:"report_type"`
	ReportYear Cell `json:"report_year"`
}

type ReportLineProfileGroup struct {
	Key      ReportLineProfileKey `json:"key"`
	Measures Measures             `json:"measures"`
	Dates    ReceiptDateProfile   `json:"receipt_dates"`
	First    uint64               `json:"first_source_row_ordinal"`
	Last     uint64               `json:"last_source_row_ordinal"`
}

type reportLineProfiler struct {
	*receiptProfileDecoder
	out     ReportLineProfile
	forms   map[ReportLineProfileFormKey]*Measures
	reports map[ReportLineProfileKey]*ReportLineProfileGroup
	limit   int
}

func newReportLineProfiler(cycle string) (*reportLineProfiler, error) {
	d, err := newReceiptProfileDecoder(cycle)
	if err != nil {
		return nil, err
	}
	return &reportLineProfiler{
		receiptProfileDecoder: d, limit: maxReportProfileGroups,
		forms: map[ReportLineProfileFormKey]*Measures{}, reports: map[ReportLineProfileKey]*ReportLineProfileGroup{},
		out: ReportLineProfile{
			SchemaVersion: "legal-tender.fec.receipt-report-occurrence-profile.v2", Policy: ReportProfileV2Policy,
			IndividualPolicy: receipts.ContractID + "@" + receipts.ContractVersion, LinePolicy: ReportLinePolicy, Cycle: cycle,
			Forms: []ReportLineProfileForm{}, Reports: []ReportLineProfileGroup{},
			NotEstablished: []string{"publisher_key_uniqueness", "transaction_identity_uniqueness", "effective_report_selection", "original_filing_membership", "report_period_coverage", "summary_report_and_account_membership", "other_form_line_equivalence", "cash_funding_basis", "terminal_dollar_allocation"},
		},
	}, nil
}

// ProfileReportLines inventories every physical occurrence, including excluded
// and unresolved populations. It does not select effective transactions/reports.
func ProfileReportLines(ctx context.Context, root, summaryPath, cycle string, progress func(string)) (ReportLineProfile, error) {
	b, err := newReportLineProfiler(cycle)
	if err != nil {
		return ReportLineProfile{}, err
	}
	scan, err := scanReportSource(ctx, root, summaryPath, cycle, b.observe, progress)
	if err != nil {
		return ReportLineProfile{}, err
	}
	b.out.SummaryInput, b.out.SummaryCalculationID = scan.summary, scan.calculationID
	b.out.Source, b.out.Verification = scan.source, scan.verification
	if scan.verification.Rows != b.out.Total.Rows {
		return ReportLineProfile{}, fmt.Errorf("incomplete report-line profile")
	}
	return b.finish(ctx)
}

func (b *reportLineProfiler) observe(row *schedulea.Row) error {
	if row.Number() != b.out.Total.Rows+1 {
		return fmt.Errorf("nonsequential report-profile occurrence")
	}
	r, form, err := b.decode(row)
	if err != nil {
		return err
	}
	k := ReportLineProfileFormKey{ReportLineKey: ReportLineKey{
		Form: form.Form, Schedule: form.Schedule, Line: form.Line, Memo: b.cell(row, "memo_cd"), Individual: "source_null",
	}, Decision: form.Decision}
	if r.Individual != nil {
		k.Individual = strconv.FormatBool(*r.Individual)
	}
	k.Disposition = reportLineDisposition(k.ReportLineKey, r.Amount != nil)
	m := b.forms[k]
	if m == nil {
		if len(b.forms) >= b.limit {
			return fmt.Errorf("form-line profile group limit exceeded")
		}
		m = &Measures{}
		b.forms[k] = m
	}
	key := ReportLineProfileKey{b.cell(row, "cmte_id"), b.cell(row, "file_num"), k, b.cell(row, "rpt_tp"), b.cell(row, "rpt_yr")}
	g := b.reports[key]
	if g == nil {
		if len(b.reports) >= b.limit {
			return fmt.Errorf("report-line profile group limit exceeded")
		}
		g = &ReportLineProfileGroup{Key: key, First: row.Number()}
		b.reports[key] = g
	}
	g.Last = row.Number()
	for _, measures := range []*Measures{m, &g.Measures, &b.out.Total} {
		if err := measures.observe(r); err != nil {
			return err
		}
	}
	if k.Decision == "included" {
		if err := b.out.Individual.observe(r); err != nil {
			return err
		}
	}
	if k.Disposition == "reviewed_nonmemo_line" {
		if err := b.out.LinePopulation.observe(r); err != nil {
			return err
		}
	}
	return b.observeDate(row, &g.Dates)
}

func (b *reportLineProfiler) finish(ctx context.Context) (ReportLineProfile, error) {
	if err := ctx.Err(); err != nil {
		return ReportLineProfile{}, err
	}
	var total, individual, line Measures
	byForm := map[ReportLineProfileFormKey]Measures{}
	for _, g := range b.reports {
		d := g.Dates
		validDates := d.BeforeCycle + d.InCycle + d.AfterCycle
		if !g.Measures.valid() || d.Missing+d.Invalid+validDates != g.Measures.Rows ||
			(validDates == 0) != (d.First == "" && d.Last == "") || d.First > d.Last ||
			g.First == 0 || g.First > g.Last || g.Last > b.out.Total.Rows {
			return ReportLineProfile{}, fmt.Errorf("report-line/date conservation failed")
		}
		key := g.Key.ReportLineProfileFormKey
		m := byForm[key]
		if err := m.merge(g.Measures); err != nil {
			return ReportLineProfile{}, err
		}
		byForm[key] = m
		b.out.Reports = append(b.out.Reports, *g)
	}
	for key, m := range b.forms {
		if !m.valid() || byForm[key] != *m {
			return ReportLineProfile{}, fmt.Errorf("form/report-line conservation failed")
		}
		delete(byForm, key)
		if err := total.merge(*m); err != nil {
			return ReportLineProfile{}, err
		}
		if key.Decision == "included" {
			if err := individual.merge(*m); err != nil {
				return ReportLineProfile{}, err
			}
		}
		if key.Disposition == "reviewed_nonmemo_line" {
			if err := line.merge(*m); err != nil {
				return ReportLineProfile{}, err
			}
		}
		b.out.Forms = append(b.out.Forms, ReportLineProfileForm{key, *m})
	}
	if len(byForm) != 0 || total.Rows == 0 || total != b.out.Total || individual != b.out.Individual || line != b.out.LinePopulation {
		return ReportLineProfile{}, fmt.Errorf("report-line profile conservation failed")
	}
	sort.Slice(b.out.Forms, func(i, j int) bool { return compareLineProfileForms(b.out.Forms[i].Key, b.out.Forms[j].Key) < 0 })
	sort.Slice(b.out.Reports, func(i, j int) bool {
		a, c := b.out.Reports[i].Key, b.out.Reports[j].Key
		if v := compareProfileCells([]Cell{a.Committee, a.File}, []Cell{c.Committee, c.File}); v != 0 {
			return v < 0
		}
		if v := compareLineProfileForms(a.ReportLineProfileFormKey, c.ReportLineProfileFormKey); v != 0 {
			return v < 0
		}
		return compareProfileCells([]Cell{a.ReportType, a.ReportYear}, []Cell{c.ReportType, c.ReportYear}) < 0
	})
	if err := ctx.Err(); err != nil {
		return ReportLineProfile{}, err
	}
	body, err := json.Marshal(b.out)
	if err != nil {
		return ReportLineProfile{}, err
	}
	d := sha256.Sum256(body)
	b.out.ProfileID = hex.EncodeToString(d[:])
	return b.out, nil
}

func compareLineProfileForms(a, b ReportLineProfileFormKey) int {
	if v := compareProfileCells([]Cell{a.Form, a.Schedule, a.Line, a.Memo}, []Cell{b.Form, b.Schedule, b.Line, b.Memo}); v != 0 {
		return v
	}
	for _, pair := range [][2]string{{a.Individual, b.Individual}, {a.Decision, b.Decision}, {a.Disposition, b.Disposition}} {
		if v := cmp.Compare(pair[0], pair[1]); v != 0 {
			return v
		}
	}
	return 0
}
