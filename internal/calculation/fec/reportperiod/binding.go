package reportperiod

import (
	"context"
	"encoding/json"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const BindingVersion = "legal-tender.fec.report-field-binding.v1"

type BindingRequest struct {
	Membership Request
	Document   reportscope.Request
}

type MetadataAmount struct {
	Raw        json.RawMessage `json:"raw"`
	State      string          `json:"state"`
	MinorUnits *string         `json:"minor_units"`
}

type FieldBinding struct {
	Name               string                    `json:"name"`
	Cover              []reportscope.AmountField `json:"cover"`
	Metadata           MetadataAmount            `json:"metadata"`
	DeltaMinorUnits    *string                   `json:"delta_minor_units"` // metadata minus cover
	ReportedValueBound bool                      `json:"reported_value_bound"`
	Blockers           []string                  `json:"blockers"`
}

type FieldBindingReview struct {
	Version                     string                           `json:"version"`
	Membership                  Review                           `json:"membership"`
	Document                    reportscope.ElectronicAssessment `json:"document"`
	ObservationIndex            *int                             `json:"observation_index"`
	ScopeBlockers               []string                         `json:"scope_blockers"`
	ScopeBound                  bool                             `json:"scope_bound"`
	Fields                      []FieldBinding                   `json:"fields"`
	CycleTotalReady             bool                             `json:"cycle_total_ready"` // no window-level financial membership
	CashBasisReady              bool                             `json:"cash_basis_ready"`  // matching reported cash is not cash continuity
	TerminalAttributionEligible bool                             `json:"terminal_attribution_eligible"`
}

// BindFields re-verifies both input families. A bound field is an exact pair of
// source observations attached to one observed chain candidate, not new money.
func BindFields(ctx context.Context, request BindingRequest) (FieldBindingReview, error) {
	m, err := Inspect(ctx, request.Membership)
	if err != nil {
		return FieldBindingReview{}, err
	}
	return bindFields(ctx, m, request.Document)
}

// Private so batch consumers reuse one verified capture without accepting
// external membership decisions or serializing that capture N times.
func bindFields(ctx context.Context, m Review, document reportscope.Request) (FieldBindingReview, error) {
	return bindElectronicFields(ctx, m, document, reportscope.AssessElectronic, BindingVersion)
}

func bindElectronicFields(ctx context.Context, m Review, document reportscope.Request, assess func(context.Context, reportscope.Request) (reportscope.ElectronicAssessment, error), version string) (FieldBindingReview, error) {
	d, err := assess(ctx, document)
	if err != nil {
		return FieldBindingReview{}, err
	}
	r := FieldBindingReview{Version: version, Membership: m, Document: d, ScopeBlockers: slices.Clone(d.Issues), Fields: []FieldBinding{}}
	var raw map[string]json.RawMessage
	index := 0
	for _, page := range m.Evidence.Pages {
		for _, record := range page.Records {
			if record.FileNumber == d.FileNumber {
				i := index
				r.ObservationIndex = &i
				if err := json.Unmarshal(record.Raw, &raw); err != nil {
					return FieldBindingReview{}, err
				}
			}
			index++
		}
	}
	if r.ObservationIndex == nil {
		r.ScopeBlockers = append(r.ScopeBlockers, "document_not_in_metadata_capture")
	} else {
		r.qualifyScope(raw)
	}
	if d.Cover == nil {
		r.ScopeBlockers = append(r.ScopeBlockers, "unqualified_electronic_cover")
	}
	if m.Evidence.PaginationState != "exact_count_satisfied" && m.Evidence.PaginationState != "empty_page_observed" {
		r.ScopeBlockers = append(r.ScopeBlockers, "partial_metadata_traversal")
	}
	slices.Sort(r.ScopeBlockers)
	r.ScopeBlockers = slices.Compact(r.ScopeBlockers)
	r.ScopeBound = len(r.ScopeBlockers) == 0
	for _, f := range d.PeriodFields {
		if err := ctx.Err(); err != nil {
			return FieldBindingReview{}, err
		}
		b := FieldBinding{Name: f.Name, Cover: f.Amounts, Metadata: bindingAmount(raw[f.Name], bindingAllowsString(f.Name)), Blockers: slices.Clone(r.ScopeBlockers)}
		if len(b.Cover) == 0 {
			b.Blockers = append(b.Blockers, "missing_cover_field")
		}
		for j, v := range b.Cover {
			if v.State != "valid" {
				b.Blockers = append(b.Blockers, "cover_field:"+v.State)
			}
			if j > 0 && v.State == "valid" && b.Cover[0].State == "valid" && v.MinorUnits != b.Cover[0].MinorUnits {
				b.Blockers = append(b.Blockers, "conflicting_cover_positions")
			}
		}
		if b.Metadata.State != "valid" {
			b.Blockers = append(b.Blockers, "metadata_field:"+b.Metadata.State)
		}
		// Retain the signed comparison even if membership or a duplicate conflicts.
		// This is explicitly not a fallback selection of the first position.
		if len(b.Cover) > 0 && b.Cover[0].State == "valid" && b.Metadata.State == "valid" {
			left, _ := new(big.Int).SetString(*b.Metadata.MinorUnits, 10)
			right, _ := new(big.Int).SetString(b.Cover[0].MinorUnits, 10)
			delta := left.Sub(left, right).String()
			b.DeltaMinorUnits = &delta
			if delta != "0" {
				b.Blockers = append(b.Blockers, "reported_value_mismatch")
			}
		}
		slices.Sort(b.Blockers)
		b.Blockers = slices.Compact(b.Blockers)
		b.ReportedValueBound = len(b.Blockers) == 0
		r.Fields = append(r.Fields, b)
	}
	return r, nil
}

var amendmentNumber = regexp.MustCompile(`^[0-9]{1,3}$`)

func (r *FieldBindingReview) qualifyScope(raw map[string]json.RawMessage) {
	i := *r.ObservationIndex
	o := r.Membership.Observations[i]
	if !slices.Contains(r.Membership.ChainCandidateIndexes, i) {
		r.ScopeBlockers = append(r.ScopeBlockers, "not_observed_chain_candidate")
	}
	if o.WindowRelation != "inside" {
		r.ScopeBlockers = append(r.ScopeBlockers, "report_not_inside_requested_window")
	}
	c := r.Document.Cover
	if c == nil {
		return
	}
	form := "Form 3"
	if strings.HasPrefix(c.Form, "F3X") {
		form = "Form 3X"
	}
	start, e1 := time.Parse("20060102", c.CoverageStart)
	end, e2 := time.Parse("20060102", c.CoverageEnd)
	if c.CommitteeID != r.Membership.Evidence.Query.CommitteeID || c.CommitteeID != text(raw["committee_id"]) || form != o.ReportForm || c.ReportCode != o.ReportType || o.Period == nil || e1 != nil || e2 != nil || start.Format("2006-01-02") != o.Period.Start || end.Format("2006-01-02") != o.Period.End {
		r.ScopeBlockers = append(r.ScopeBlockers, "cover_metadata_scope_mismatch")
	}
	suffix := c.Form[len(c.Form)-1:]
	if suffix != text(raw["amendment_indicator"]) {
		r.ScopeBlockers = append(r.ScopeBlockers, "amendment_indicator_mismatch")
	}
	ids, valid := chain(raw["amendment_chain"])
	h := r.Document.Header
	if !valid || len(h) < 7 {
		r.ScopeBlockers = append(r.ScopeBlockers, "missing_header_chain_identity")
		return
	}
	if len(ids) > 1 {
		// The original ID corroborates the root, not an immediately previous
		// amendment. Sequence numbers are retained and shape-checked, not ranked.
		if suffix != "A" || h[5] != "FEC-"+ids[0] || !amendmentNumber.MatchString(h[6]) || strings.TrimLeft(h[6], "0") == "" {
			r.ScopeBlockers = append(r.ScopeBlockers, "header_chain_identity_mismatch")
		}
	} else if suffix == "A" || h[5] != "" || h[6] != "" {
		r.ScopeBlockers = append(r.ScopeBlockers, "header_chain_identity_mismatch")
	}
}

// Exact accepted field names from the pinned endpoint shapes, not substring
// matching or coercion of arbitrary numeric-looking metadata strings.
func bindingAllowsString(name string) bool {
	switch name {
	case "individual_itemized_contributions_period", "individual_unitemized_contributions_period", "total_individual_contributions_period", "offsets_to_operating_expenditures_period":
		return true
	default:
		return false
	}
}

func bindingAmount(raw json.RawMessage, allowString bool) MetadataAmount {
	v := MetadataAmount{Raw: raw, State: "source_null"}
	if len(raw) == 0 || string(raw) == "null" {
		return v
	}
	lexeme := string(raw)
	if raw[0] == '"' {
		if !allowString || json.Unmarshal(raw, &lexeme) != nil {
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
	v.State, v.MinorUnits = "valid", &minor
	return v
}
