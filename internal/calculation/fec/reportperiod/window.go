package reportperiod

import (
	"context"
	"errors"
	"math/big"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const WindowVersion = "legal-tender.fec.report-window.v1"

var windowFields = []string{"individual_itemized_contributions_period", "individual_unitemized_contributions_period", "total_individual_contributions_period", "total_receipts_period", "total_disbursements_period", "cash_on_hand_beginning_period", "cash_on_hand_end_period"}

type WindowRequest struct {
	Membership    Request
	DocumentsPath string
}

// Evidence occurs once. The metadata population lives in WindowReview.Membership.
type WindowBinding struct {
	Document         reportscope.ElectronicAssessment `json:"document"`
	ObservationIndex *int                             `json:"observation_index"`
	ScopeBlockers    []string                         `json:"scope_blockers"`
	ScopeBound       bool                             `json:"scope_bound"`
	Fields           []FieldBinding                   `json:"fields"`
}

type MissingField struct {
	ObservationIndex int      `json:"observation_index"`
	BindingIndex     *int     `json:"binding_index"`
	Blockers         []string `json:"blockers"`
}

type WindowField struct {
	Name                  string         `json:"name"`
	Operation             string         `json:"operation"` // sum_periods, opening_boundary, closing_boundary
	MemberBindingIndexes  []int          `json:"member_binding_indexes"`
	Missing               []MissingField `json:"missing"`
	Coverage              Coverage       `json:"coverage"`
	ObservedSumMinorUnits *string        `json:"observed_sum_minor_units"` // sum only; never a lower bound
	WindowValueMinorUnits *string        `json:"window_value_minor_units"` // null unless exact field/window readiness
	ReportedWindowReady   bool           `json:"reported_window_ready"`
	Blockers              []string       `json:"blockers"`
}

type WindowReview struct {
	Version                     string                  `json:"version"`
	Membership                  Review                  `json:"membership"`
	DocumentSet                 reportmetadata.Artifact `json:"document_set"`
	Bindings                    []WindowBinding         `json:"bindings"` // descriptor order, zero-based references
	Fields                      []WindowField           `json:"fields"`
	Equations                   []Equation              `json:"equations"`
	FinancialCycleTotalReady    bool                    `json:"financial_cycle_total_ready"`
	CashBasisReady              bool                    `json:"cash_basis_ready"`
	TerminalAttributionEligible bool                    `json:"terminal_attribution_eligible"`
}

// ReviewWindow re-verifies raw evidence; no saved binding or caller readiness is trusted.
func ReviewWindow(ctx context.Context, request WindowRequest) (WindowReview, error) {
	r, err := readWindowBindings(ctx, request, bindFields)
	if err != nil {
		return WindowReview{}, err
	}
	m := r.Membership
	byObservation := map[int]int{}
	for j, b := range r.Bindings {
		if b.ObservationIndex != nil {
			byObservation[*b.ObservationIndex] = j
		}
	}
	if err := r.calculateFields(ctx, m, byObservation); err != nil {
		return WindowReview{}, err
	}
	return r, nil
}

// Shared bounded source verification, without accepting caller-supplied decisions.
func readWindowBindings(ctx context.Context, request WindowRequest, bind func(context.Context, Review, reportscope.Request) (FieldBindingReview, error)) (WindowReview, error) {
	identity, documents, err := readDocumentSet(ctx, request.DocumentsPath)
	if err != nil {
		return WindowReview{}, err
	}
	m, err := Inspect(ctx, request.Membership)
	if err != nil {
		return WindowReview{}, err
	}
	r := WindowReview{Version: WindowVersion, Membership: m, DocumentSet: identity, Bindings: []WindowBinding{}, Fields: []WindowField{}, Equations: []Equation{}}
	seenFiles := map[string]bool{}
	var total int64
	for _, d := range documents {
		b, err := bind(ctx, m, d)
		if err != nil {
			return WindowReview{}, err
		}
		total += b.Document.Body.Bytes + b.Document.Headers.Bytes
		if total > MaxWindowDocumentBytes {
			return WindowReview{}, errors.New("verified document bytes exceed window budget")
		}
		if seenFiles[b.Document.FileNumber] {
			return WindowReview{}, errors.New("duplicate document file ID in window request")
		}
		seenFiles[b.Document.FileNumber] = true
		r.Bindings = append(r.Bindings, WindowBinding{b.Document, b.ObservationIndex, b.ScopeBlockers, b.ScopeBound, b.Fields})
	}
	return r, nil
}

func (r *WindowReview) calculateFields(ctx context.Context, m Review, byObservation map[int]int) error {
	for i, name := range windowFields {
		if err := ctx.Err(); err != nil {
			return err
		}
		operation := "sum_periods"
		if i == 5 {
			operation = "opening_boundary"
		}
		if i == 6 {
			operation = "closing_boundary"
		}
		f := WindowField{Name: name, Operation: operation, MemberBindingIndexes: []int{}, Missing: []MissingField{}, Blockers: slices.Clone(m.PartitionBlockers)}
		members := []int{}
		sum := new(big.Int)
		for _, o := range m.ChainCandidateIndexes {
			obs := m.Observations[o]
			if obs.WindowRelation == "outside" {
				continue
			}
			missing := MissingField{ObservationIndex: o, Blockers: []string{}}
			j, found := byObservation[o]
			if found {
				missing.BindingIndex = &j
				b := boundField(r.Bindings[j], name)
				if b != nil && b.ReportedValueBound {
					members = append(members, o)
					f.MemberBindingIndexes = append(f.MemberBindingIndexes, j)
					value, _ := new(big.Int).SetString(*b.Metadata.MinorUnits, 10)
					sum.Add(sum, value)
					continue
				}
				if b != nil {
					missing.Blockers = slices.Clone(b.Blockers)
				} else {
					missing.Blockers = append(slices.Clone(r.Bindings[j].ScopeBlockers), "unqualified_field_layout")
				}
			} else {
				missing.Blockers = append(missing.Blockers, "missing_document")
			}
			f.Missing = append(f.Missing, missing)
		}
		f.Coverage = coverage(m.Window, m.Observations, members)
		if len(f.Missing) > 0 {
			f.Blockers = append(f.Blockers, "unbound_candidate_field")
		}
		if f.Coverage.GapDays > 0 {
			f.Blockers = append(f.Blockers, "field_coverage_gaps")
		}
		if f.Coverage.OverlapDays > 0 {
			f.Blockers = append(f.Blockers, "field_coverage_overlaps")
		}
		if len(members) == 0 {
			f.Blockers = append(f.Blockers, "no_bound_field_members")
		}
		slices.Sort(f.Blockers)
		f.Blockers = slices.Compact(f.Blockers)
		f.ReportedWindowReady = len(f.Blockers) == 0
		if operation == "sum_periods" {
			if len(members) > 0 {
				value := sum.String()
				f.ObservedSumMinorUnits = &value
			}
			if f.ReportedWindowReady {
				f.WindowValueMinorUnits = f.ObservedSumMinorUnits
			}
		} else if f.ReportedWindowReady {
			for _, j := range f.MemberBindingIndexes {
				p := m.Observations[*r.Bindings[j].ObservationIndex].Period
				if (operation == "opening_boundary" && p.Start == m.Window.Start) || (operation == "closing_boundary" && p.End == m.Window.End) {
					value := *boundField(r.Bindings[j], name).Metadata.MinorUnits
					f.WindowValueMinorUnits = &value
				}
			}
		}
		r.Fields = append(r.Fields, f)
	}
	r.equations()
	return nil
}

func boundField(b WindowBinding, name string) *FieldBinding {
	for i := range b.Fields {
		if b.Fields[i].Name == name {
			return &b.Fields[i]
		}
	}
	return nil
}
