// Package reportfield compares one qualified reported field across retained
// representations of the same filing. It never selects effective reports.
package reportfield

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const Version = "legal-tender.fec.report-total-receipts-review.v1"
const Field = "fec.form3x.total_receipts.column_a"

type Period struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type Comparison struct {
	MetadataIndex   int             `json:"metadata_index"` // zero-based into Evidence.Metadata
	Field           string          `json:"field"`
	Raw             json.RawMessage `json:"raw"`
	State           string          `json:"state"` // valid, source_null, invalid, unsupported
	MinorUnits      *string         `json:"minor_units"`
	Comparable      bool            `json:"comparable"`        // same-file reported pair only
	DeltaMinorUnits *string         `json:"delta_minor_units"` // metadata minus cover, never a repair
	Blockers        []string        `json:"blockers"`
}

type Review struct {
	Version                     string                    `json:"version"`
	Field                       string                    `json:"field"`
	Use                         string                    `json:"use"`
	Evidence                    reportscope.Assessment    `json:"evidence"`
	Period                      *Period                   `json:"period"`
	CoverFields                 []reportscope.AmountField `json:"cover_fields"`
	CoverBlockers               []string                  `json:"cover_blockers"`
	Comparisons                 []Comparison              `json:"comparisons"`
	FinancialComponentEligible  bool                      `json:"financial_component_eligible"`  // always false
	CycleComparisonReady        bool                      `json:"cycle_comparison_ready"`        // always false
	TerminalAttributionEligible bool                      `json:"terminal_attribution_eligible"` // always false
}

// CompareTotalReceipts accepts pinned source inputs, not precomputed assessment
// JSON or a caller's qualification flag. Each source is verified on every call.
func CompareTotalReceipts(ctx context.Context, request reportscope.Request) (Review, error) {
	a, err := reportscope.Assess(ctx, request)
	if err != nil {
		return Review{}, err
	}
	r := Review{Version: Version, Field: Field, Use: "same_file_reported_value_comparison", Evidence: a,
		CoverFields: []reportscope.AmountField{}, CoverBlockers: []string{}, Comparisons: []Comparison{}}
	r.qualifyCover()
	for i, assertion := range a.Metadata {
		if err := ctx.Err(); err != nil {
			return Review{}, err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(assertion.Record.Raw, &fields); err != nil {
			return Review{}, errors.New("invalid verified metadata record")
		}
		c := Comparison{MetadataIndex: i, Raw: json.RawMessage("null"), State: "unsupported", Blockers: slices.Clone(r.CoverBlockers)}
		formField, formValue := "", ""
		switch assertion.Endpoint {
		case "/v1/filings/":
			c.Field, formField, formValue = "total_receipts", "form_type", "F3X"
		case "/v1/reports/pac-party/":
			c.Field, formField, formValue = "total_receipts_period", "report_form", "Form 3X"
		default:
			c.Blockers = append(c.Blockers, "unsupported_metadata_endpoint")
		}
		if c.Field != "" {
			c.Raw = fields[c.Field]
			c.State, c.MinorUnits = metadataMoney(c.Raw)
			if c.State != "valid" {
				c.Blockers = append(c.Blockers, "metadata_amount:"+c.State)
			}
			if a.Cover != nil {
				c.Blockers = append(c.Blockers, metadataMatchesCover(a, fields, formField, formValue)...)
			}
		}
		slices.Sort(c.Blockers)
		c.Blockers = slices.Compact(c.Blockers)
		if len(c.Blockers) == 0 {
			left, _ := new(big.Int).SetString(*c.MinorUnits, 10)
			right, _ := new(big.Int).SetString(r.CoverFields[0].MinorUnits, 10)
			delta := new(big.Int).Sub(left, right).String()
			c.Comparable, c.DeltaMinorUnits = true, &delta
		}
		r.Comparisons = append(r.Comparisons, c)
	}
	return r, nil
}

func (r *Review) qualifyCover() {
	a := r.Evidence
	r.Period, r.CoverBlockers = paperCoverScope(a)
	if slices.Contains(r.CoverBlockers, "unqualified_cover_layout") {
		return
	}
	// Form 3X instructions transfer line 19 to line 6(c), same column.
	// Preserve both assertions; a conflict or missing operand cannot choose one.
	for _, field := range a.Cover.Amounts {
		if field.Sequence == 22 || field.Sequence == 44 {
			r.CoverFields = append(r.CoverFields, field)
			if field.State != "valid" {
				r.CoverBlockers = append(r.CoverBlockers, "cover_total_receipts:"+field.State)
			}
		}
	}
	if len(r.CoverFields) != 2 {
		r.CoverBlockers = append(r.CoverBlockers, "missing_cover_total_receipts")
	} else if r.CoverFields[0].State == "valid" && r.CoverFields[1].State == "valid" && r.CoverFields[0].MinorUnits != r.CoverFields[1].MinorUnits {
		r.CoverBlockers = append(r.CoverBlockers, "cover_total_receipts_conflict")
	}
	// Other invalid amounts, cash arithmetic, and amendment-selection assertions
	// remain in Evidence; they cannot manufacture a conflict in this reported pair.
	slices.Sort(r.CoverBlockers)
	r.CoverBlockers = slices.Compact(r.CoverBlockers)
}

func metadataMoney(raw json.RawMessage) (string, *string) {
	if len(raw) == 0 || string(raw) == "null" {
		return "source_null", nil
	}
	minor, _, issue := money.ParseUSDMinorUnits(string(raw))
	if issue != "" {
		return "invalid", nil
	}
	return "valid", &minor
}

func stringValue(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}
