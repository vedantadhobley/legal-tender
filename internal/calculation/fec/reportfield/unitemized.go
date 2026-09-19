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

const UnitemizedVersion = "legal-tender.fec.report-unitemized-review.v1"
const UnitemizedField = "fec.form3x.unitemized_individual_contributions.column_a"

type ReportedAmount struct {
	Field      string          `json:"field"`
	Sequence   int             `json:"sequence,omitempty"` // one-based paper position, absent for JSON
	Raw        json.RawMessage `json:"raw"`                // paper string or original JSON type, not converted number
	State      string          `json:"state"`              // valid, blank, source_null, invalid
	MinorUnits *string         `json:"minor_units"`
}

type IndividualSummary struct {
	Itemized                ReportedAmount `json:"itemized"`
	Unitemized              ReportedAmount `json:"unitemized"`
	Total                   ReportedAmount `json:"total"`
	SubtotalState           string         `json:"subtotal_state"`             // balanced, mismatch, unavailable
	SubtotalDeltaMinorUnits *string        `json:"subtotal_delta_minor_units"` // total - itemized - unitemized
}

type ReportedPair struct {
	Comparable      bool     `json:"comparable"`
	DeltaMinorUnits *string  `json:"delta_minor_units"` // metadata minus cover; never inferred funds
	Blockers        []string `json:"blockers"`
}

type UnitemizedMetadata struct {
	MetadataIndex int                `json:"metadata_index"` // zero-based into Evidence.Metadata
	Availability  string             `json:"availability"`   // supplied, not_supplied_by_endpoint, unsupported_endpoint
	Period        *Period            `json:"period"`         // this metadata assertion's own dates, not copied from cover
	ScopeBlockers []string           `json:"scope_blockers"`
	Summary       *IndividualSummary `json:"summary"`
	Comparison    ReportedPair       `json:"comparison"`
}

type UnitemizedReview struct {
	Version                     string                 `json:"version"`
	Field                       string                 `json:"field"`
	AmountMethod                string                 `json:"amount_method"`
	DonorComposition            string                 `json:"donor_composition"`
	Evidence                    reportscope.Assessment `json:"evidence"`
	CoverPeriod                 *Period                `json:"cover_period"`
	CoverScopeBlockers          []string               `json:"cover_scope_blockers"`
	Cover                       *IndividualSummary     `json:"cover"`
	Metadata                    []UnitemizedMetadata   `json:"metadata"`
	FinancialComponentEligible  bool                   `json:"financial_component_eligible"`
	CycleComparisonReady        bool                   `json:"cycle_comparison_ready"`
	TerminalAttributionEligible bool                   `json:"terminal_attribution_eligible"`
}

// ReviewUnitemized retains explicit report-period observations even when another
// representation is unavailable. Neither arithmetic nor absence supplies money.
func ReviewUnitemized(ctx context.Context, request reportscope.Request) (UnitemizedReview, error) {
	a, err := reportscope.Assess(ctx, request)
	if err != nil {
		return UnitemizedReview{}, err
	}
	r := UnitemizedReview{Version: UnitemizedVersion, Field: UnitemizedField, AmountMethod: "explicit_reported_field_only",
		DonorComposition: "not_identified_by_summary", Evidence: a, Metadata: []UnitemizedMetadata{}}
	r.CoverPeriod, r.CoverScopeBlockers = paperCoverScope(a)
	if !slices.Contains(r.CoverScopeBlockers, "unqualified_cover_layout") {
		// P3.4 column A line 11(a)(i), (ii), (iii). Column B uses 79..81;
		// those fields remain in Evidence but are never period-value fallbacks.
		values := make([]ReportedAmount, 0, 3)
		for _, f := range a.Cover.Amounts {
			if f.Sequence < 29 || f.Sequence > 31 {
				continue
			}
			raw, _ := json.Marshal(f.Raw)
			v := ReportedAmount{Field: []string{"11(a)(i)", "11(a)(ii)", "11(a)(iii)"}[f.Sequence-29], Sequence: f.Sequence, Raw: raw, State: f.State}
			if f.State == "valid" {
				minor := f.MinorUnits
				v.MinorUnits = &minor
			}
			values = append(values, v)
		}
		if len(values) != 3 {
			return UnitemizedReview{}, errors.New("verified cover lacks individual-summary fields")
		}
		r.Cover = individualSummary(values[0], values[1], values[2])
	}
	slices.Sort(r.CoverScopeBlockers)
	r.CoverScopeBlockers = slices.Compact(r.CoverScopeBlockers)
	for i, assertion := range a.Metadata {
		if err := ctx.Err(); err != nil {
			return UnitemizedReview{}, err
		}
		m := UnitemizedMetadata{MetadataIndex: i, Availability: "unsupported_endpoint", ScopeBlockers: []string{},
			Comparison: ReportedPair{Blockers: slices.Clone(r.CoverScopeBlockers)}}
		switch assertion.Endpoint {
		case "/v1/filings/":
			m.Availability = "not_supplied_by_endpoint"
			m.Comparison.Blockers = append(m.Comparison.Blockers, "unitemized_not_supplied_by_endpoint")
		case "/v1/reports/pac-party/":
			m.Availability = "supplied"
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(assertion.Record.Raw, &fields); err != nil {
				return UnitemizedReview{}, errors.New("invalid verified metadata record")
			}
			m.Summary = individualSummary(metadataAmount(fields, "individual_itemized_contributions_period"), metadataAmount(fields, "individual_unitemized_contributions_period"), metadataAmount(fields, "total_individual_contributions_period"))
			m.Period, m.ScopeBlockers = metadataIndividualScope(fields)
			m.Comparison.Blockers = append(m.Comparison.Blockers, m.ScopeBlockers...)
			m.Comparison.Blockers = append(m.Comparison.Blockers, metadataMatchesCover(a, fields, "report_form", "Form 3X")...)
			if m.Summary.Unitemized.State != "valid" {
				m.Comparison.Blockers = append(m.Comparison.Blockers, "metadata_unitemized:"+m.Summary.Unitemized.State)
			}
		default:
			m.Comparison.Blockers = append(m.Comparison.Blockers, "unsupported_metadata_endpoint")
		}
		if r.Cover != nil && r.Cover.Unitemized.State != "valid" {
			m.Comparison.Blockers = append(m.Comparison.Blockers, "cover_unitemized:"+r.Cover.Unitemized.State)
		}
		slices.Sort(m.Comparison.Blockers)
		m.Comparison.Blockers = slices.Compact(m.Comparison.Blockers)
		if len(m.Comparison.Blockers) == 0 {
			left, _ := new(big.Int).SetString(*m.Summary.Unitemized.MinorUnits, 10)
			right, _ := new(big.Int).SetString(*r.Cover.Unitemized.MinorUnits, 10)
			delta := left.Sub(left, right).String()
			m.Comparison.Comparable = true
			m.Comparison.DeltaMinorUnits = &delta
		}
		r.Metadata = append(r.Metadata, m)
	}
	return r, nil
}

func metadataAmount(fields map[string]json.RawMessage, field string) ReportedAmount {
	raw := fields[field]
	v := ReportedAmount{Field: field, Raw: raw, State: "source_null"}
	if len(raw) == 0 || string(raw) == "null" {
		return v
	}
	lexeme := string(raw)
	// This endpoint's pinned shape explicitly permits number OR string. Keep
	// the raw type; do not generalize this to numeric-only total-receipts fields.
	if raw[0] == '"' {
		if json.Unmarshal(raw, &lexeme) != nil {
			v.State = "invalid"
			return v
		}
	}
	if lexeme == "" {
		v.State = "blank"
		return v
	}
	minor, _, issue := money.ParseUSDMinorUnits(lexeme)
	if issue != "" {
		v.State = "invalid"
		return v
	}
	v.State = "valid"
	v.MinorUnits = &minor
	return v
}

func individualSummary(itemized, unitemized, total ReportedAmount) *IndividualSummary {
	s := &IndividualSummary{Itemized: itemized, Unitemized: unitemized, Total: total, SubtotalState: "unavailable"}
	if itemized.State != "valid" || unitemized.State != "valid" || total.State != "valid" {
		return s
	}
	i, _ := new(big.Int).SetString(*itemized.MinorUnits, 10)
	u, _ := new(big.Int).SetString(*unitemized.MinorUnits, 10)
	t, _ := new(big.Int).SetString(*total.MinorUnits, 10)
	delta := t.Sub(t, i).Sub(t, u).String()
	s.SubtotalDeltaMinorUnits = &delta
	s.SubtotalState = "mismatch"
	if delta == "0" {
		s.SubtotalState = "balanced"
	}
	return s // Never solve for or fill the unitemized operand.
}

func metadataIndividualScope(fields map[string]json.RawMessage) (*Period, []string) {
	blockers := []string{}
	if stringValue(fields["report_form"]) != "Form 3X" {
		blockers = append(blockers, "metadata_scope:report_form")
	}
	if stringValue(fields["report_type"]) == "" {
		blockers = append(blockers, "metadata_scope:report_type")
	}
	start, end := metadataDate(fields["coverage_start_date"]), metadataDate(fields["coverage_end_date"])
	if start == "" {
		blockers = append(blockers, "metadata_scope:coverage_start_date")
	}
	if end == "" {
		blockers = append(blockers, "metadata_scope:coverage_end_date")
	}
	if start != "" && end != "" && end < start {
		blockers = append(blockers, "metadata_scope:reversed_period")
	}
	slices.Sort(blockers)
	if start == "" || end == "" || end < start {
		return nil, blockers
	}
	return &Period{start, end}, blockers
}
