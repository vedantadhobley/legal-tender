package fundingbasis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

type reportProfiler struct {
	*receiptProfileDecoder
	out     ReportProfile
	forms   map[ReportFormKey]*Measures
	reports map[ReportOccurrenceKey]*ReportOccurrenceGroup
}

type receiptProfileDecoder struct {
	indexes              map[string]int
	cycleStart, cycleEnd string
	dates                map[Cell]string
}

func newReportProfiler(cycle string) (*reportProfiler, error) {
	d, err := newReceiptProfileDecoder(cycle)
	if err != nil {
		return nil, err
	}
	return &reportProfiler{
		receiptProfileDecoder: d,
		out: ReportProfile{SchemaVersion: "legal-tender.fec.receipt-report-occurrence-profile.v1", Policy: ReportProfilePolicy,
			IndividualPolicy: receipts.ContractID + "@" + receipts.ContractVersion, Cycle: cycle,
			Forms: []ReportFormGroup{}, Reports: []ReportOccurrenceGroup{},
			NotEstablished: []string{"publisher_key_uniqueness", "effective_report_selection", "report_period_coverage", "summary_report_and_account_membership", "form_line_equivalence", "cash_funding_basis", "terminal_dollar_allocation"}},
		forms: map[ReportFormKey]*Measures{}, reports: map[ReportOccurrenceKey]*ReportOccurrenceGroup{},
	}, nil
}

func newReceiptProfileDecoder(cycle string) (*receiptProfileDecoder, error) {
	year, err := strconv.Atoi(cycle)
	if err != nil || len(cycle) != 4 || year < 2 || year%2 != 0 {
		return nil, fmt.Errorf("four-digit even cycle required")
	}
	b := &receiptProfileDecoder{
		indexes:    map[string]int{},
		cycleStart: fmt.Sprintf("%04d-01-01", year-1), cycleEnd: fmt.Sprintf("%04d-12-31", year), dates: map[Cell]string{},
	}
	for _, name := range []string{"cmte_id", "file_num", "filing_form", "schedule_type", "line_num", "rpt_tp", "rpt_yr", "is_individual", "memo_cd", "contb_receipt_amt", "contb_receipt_dt", "conduit_cmte_id"} {
		i, ok := schedulea.ColumnIndex(name)
		if !ok {
			return nil, fmt.Errorf("missing contracted column %s", name)
		}
		b.indexes[name] = i
	}
	return b, nil
}

func (b *receiptProfileDecoder) cell(row *schedulea.Row, name string) Cell {
	f, _ := row.Field(b.indexes[name])
	if f.IsNull() {
		return Cell{}
	}
	return Cell{true, f.String()}
}

func (b *receiptProfileDecoder) decode(row *schedulea.Row) (receiptRow, ReportFormKey, error) {
	r := receiptRow{}
	individual := b.cell(row, "is_individual")
	if individual.Present {
		value := individual.Value == "t"
		r.Individual = &value
	}
	memo := b.cell(row, "memo_cd")
	r.Memo = memo.Present && memo.Value == "X"
	amount := b.cell(row, "contb_receipt_amt")
	r.AmountState = "source_null"
	if amount.Present {
		v, _, issue := schedulea.ParseUSDMinorUnits(amount.Value)
		if issue != "" {
			return receiptRow{}, ReportFormKey{}, fmt.Errorf("row %d: amount normalization failed: %s", row.Number(), issue)
		}
		minor, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return receiptRow{}, ReportFormKey{}, fmt.Errorf("row %d: amount exceeds profile int64 range", row.Number())
		}
		r.Amount = &minor
		r.AmountState = "reported_value"
	}
	conduit := b.cell(row, "conduit_cmte_id")
	if conduit.Present {
		r.ConduitID = &conduit.Value
	}
	decision := receipts.ItemizedIndividualDecision(r.Individual, r.Memo, r.AmountState, r.Amount)
	key := ReportFormKey{b.cell(row, "filing_form"), b.cell(row, "schedule_type"), b.cell(row, "line_num"), decision}
	return r, key, nil
}

func (b *reportProfiler) observe(row *schedulea.Row) error {
	r, key, err := b.decode(row)
	if err != nil {
		return err
	}
	m := b.forms[key]
	if m == nil {
		if len(b.forms) >= maxReportProfileGroups {
			return fmt.Errorf("form profile group limit exceeded")
		}
		m = &Measures{}
		b.forms[key] = m
	}
	if err := m.observe(r); err != nil {
		return err
	}
	if err := b.out.Total.observe(r); err != nil {
		return err
	}
	if key.Decision != "included" {
		return nil
	}
	if err := b.out.Individual.observe(r); err != nil {
		return err
	}
	report := ReportOccurrenceKey{b.cell(row, "cmte_id"), b.cell(row, "file_num"), key.Form, key.Schedule, key.Line, b.cell(row, "rpt_tp"), b.cell(row, "rpt_yr")}
	g := b.reports[report]
	if g == nil {
		if len(b.reports) >= maxReportProfileGroups {
			return fmt.Errorf("report profile group limit exceeded")
		}
		g = &ReportOccurrenceGroup{Key: report, First: row.Number()}
		b.reports[report] = g
	}
	g.Last = row.Number()
	if err := g.Measures.observe(r); err != nil {
		return err
	}
	return b.observeDate(row, &g.Dates)
}

func (b *receiptProfileDecoder) observeDate(row *schedulea.Row, dates *ReceiptDateProfile) error {
	raw := b.cell(row, "contb_receipt_dt")
	if !raw.Present {
		dates.Missing++
		return nil
	}
	date, ok := b.dates[raw]
	if !ok {
		if len(b.dates) >= maxReportProfileGroups {
			return fmt.Errorf("receipt date cache limit exceeded")
		}
		parsed, err := time.Parse("2006-01-02 15:04:05.999999999", raw.Value)
		if err == nil {
			date = parsed.Format("2006-01-02")
		}
		b.dates[raw] = date
	}
	if date == "" {
		dates.Invalid++
		return nil
	}
	switch {
	case date < b.cycleStart:
		dates.BeforeCycle++
	case date > b.cycleEnd:
		dates.AfterCycle++
	default:
		dates.InCycle++
	}
	if dates.First == "" || date < dates.First {
		dates.First = date
	}
	if date > dates.Last {
		dates.Last = date
	}
	return nil
}
