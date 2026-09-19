package fundingbasis

import (
	"context"
	"fmt"
	"math/big"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const FamilyWindowVersion = "legal-tender.fec.receipt-family-window.v1"

type FamilyWindowMember struct {
	ObservationIndex     int      `json:"observation_index"`
	ReportIndex          *int     `json:"report_index"`
	FamilyIndex          *int     `json:"family_index"`
	ValueBasis           string   `json:"value_basis"`
	ReportedMinorUnits   *string  `json:"reported_minor_units"`
	ComparisonMinorUnits *string  `json:"comparison_minor_units"`
	DeltaMinorUnits      *string  `json:"delta_minor_units"`
	State                string   `json:"state"`
	ReportedBlockers     []string `json:"reported_blockers"`
	ComparisonBlockers   []string `json:"comparison_blockers"`
}

type FamilyWindow struct {
	Field                        reportscope.ReceiptFamilyField `json:"field"`
	Members                      []FamilyWindowMember           `json:"members"`
	ReportedCoverage             reportperiod.Coverage          `json:"reported_coverage"`
	ComparisonCoverage           reportperiod.Coverage          `json:"comparison_coverage"`
	ObservedReportedMinorUnits   *string                        `json:"observed_reported_sum_minor_units"`
	ObservedComparisonMinorUnits *string                        `json:"observed_comparison_sum_minor_units"`
	ReportedWindowMinorUnits     *string                        `json:"reported_window_minor_units"`
	ComparisonWindowMinorUnits   *string                        `json:"comparison_window_minor_units"`
	DeltaMinorUnits              *string                        `json:"delta_minor_units"`
	State                        string                         `json:"state"`
	ReportedWindowReady          bool                           `json:"reported_window_ready"`
	ComparisonWindowReady        bool                           `json:"comparison_window_ready"`
	ReportedBlockers             []string                       `json:"reported_blockers"`
	ComparisonBlockers           []string                       `json:"comparison_blockers"`
}

type FamilyWindowComparison struct {
	Version              string              `json:"version"`
	Reviewed             FamilyAbsenceReview `json:"reviewed"`
	Window               reportperiod.Period `json:"window"`
	Families             []FamilyWindow      `json:"families"`
	FinancialUseEligible bool                `json:"financial_use_eligible"`
	TerminalEligible     bool                `json:"terminal_attribution_eligible"`
}

// CompareReceiptFamilyWindow re-verifies raw evidence through the existing
// boundary. It accepts no saved absence decisions or caller-selected membership.
func CompareReceiptFamilyWindow(ctx context.Context, request ReceiptWindowRequest) (FamilyWindowComparison, error) {
	a, err := ReviewFamilyAbsence(ctx, request)
	if err != nil {
		return FamilyWindowComparison{}, err
	}
	return compareReceiptFamilyWindow(ctx, a)
}

func compareReceiptFamilyWindow(ctx context.Context, a FamilyAbsenceReview) (FamilyWindowComparison, error) {
	if err := ctx.Err(); err != nil {
		return FamilyWindowComparison{}, err
	}
	m := a.Compared.FamilyReports.Membership
	r := FamilyWindowComparison{Version: FamilyWindowVersion, Reviewed: a, Window: m.Window, Families: []FamilyWindow{}}
	// Same exact endpoint/form scope accepted by reportperiod.Inspect, including
	// an empty document set: missing documents cannot remove expected families.
	form := map[string]string{"/v1/reports/house-senate/": "F3", "/v1/reports/pac-party/": "F3X"}[m.Evidence.Endpoint]
	if form == "" {
		return r, fmt.Errorf("unsupported verified family-window endpoint")
	}
	byObservation := map[int]int{}
	if len(a.Reports) != len(a.Compared.Reports) {
		return r, fmt.Errorf("inconsistent verified absence reports")
	}
	for i, report := range a.Compared.Reports {
		b := a.Compared.FamilyReports.Bindings[report.BindingIndex]
		if a.Reports[i].ReportIndex != i || len(a.Reports[i].Families) != len(report.Families) {
			return r, fmt.Errorf("inconsistent verified absence references")
		}
		if b.ObservationIndex != nil {
			byObservation[*b.ObservationIndex] = i
		}
	}
	for _, spec := range reportscope.ReceiptFamilyFields(form) {
		f := FamilyWindow{Field: spec, Members: []FamilyWindowMember{}, State: "blocked"}
		reportedIndexes, comparisonIndexes := []int{}, []int{}
		reportedSum, comparisonSum := new(big.Int), new(big.Int)
		for _, oi := range m.ChainCandidateIndexes {
			if err := ctx.Err(); err != nil {
				return r, err
			}
			if m.Observations[oi].WindowRelation == "outside" {
				continue
			}
			member := familyWindowMember(a, spec, oi, byObservation)
			if member.ReportedMinorUnits != nil {
				if err := addFamilyWindowAmount(reportedSum, member.ReportedMinorUnits); err != nil {
					return r, err
				}
				reportedIndexes = append(reportedIndexes, oi)
			}
			if member.ComparisonMinorUnits != nil {
				if err := addFamilyWindowAmount(comparisonSum, member.ComparisonMinorUnits); err != nil {
					return r, err
				}
				comparisonIndexes = append(comparisonIndexes, oi)
				var err error
				member.DeltaMinorUnits, member.State, err = receiptDifference(member.ReportedMinorUnits, member.ComparisonMinorUnits)
				if err != nil {
					return r, err
				}
			}
			f.Members = append(f.Members, member)
		}
		f.ReportedCoverage = m.CoverageFor(reportedIndexes)
		f.ComparisonCoverage = m.CoverageFor(comparisonIndexes)
		f.ReportedBlockers = familyWindowBlockers(m, f.ReportedCoverage, len(f.Members), len(reportedIndexes))
		f.ComparisonBlockers = familyWindowBlockers(m, f.ComparisonCoverage, len(f.Members), len(comparisonIndexes))
		f.ReportedWindowReady = len(f.ReportedBlockers) == 0
		f.ComparisonWindowReady = len(f.ComparisonBlockers) == 0
		if len(reportedIndexes) > 0 {
			v := reportedSum.String()
			f.ObservedReportedMinorUnits = &v
		}
		if len(comparisonIndexes) > 0 {
			v := comparisonSum.String()
			f.ObservedComparisonMinorUnits = &v
		}
		if f.ReportedWindowReady {
			f.ReportedWindowMinorUnits = f.ObservedReportedMinorUnits
		}
		if f.ComparisonWindowReady {
			f.ComparisonWindowMinorUnits = f.ObservedComparisonMinorUnits
			var err error
			f.DeltaMinorUnits, f.State, err = receiptDifference(f.ReportedWindowMinorUnits, f.ComparisonWindowMinorUnits)
			if err != nil {
				return r, err
			}
		}
		r.Families = append(r.Families, f)
	}
	return r, nil
}

func familyWindowMember(a FamilyAbsenceReview, spec reportscope.ReceiptFamilyField, oi int, byObservation map[int]int) FamilyWindowMember {
	v := FamilyWindowMember{ObservationIndex: oi, ValueBasis: "unqualified", State: "blocked", ReportedBlockers: []string{}, ComparisonBlockers: []string{}}
	m := a.Compared.FamilyReports.Membership
	if m.Observations[oi].WindowRelation != "inside" {
		v.ReportedBlockers = append(v.ReportedBlockers, "report_not_inside_requested_window")
	}
	ri, found := byObservation[oi]
	var family *ReceiptFamilyComparison
	if !found {
		v.ReportedBlockers = append(v.ReportedBlockers, "missing_document")
	} else {
		v.ReportIndex = &ri
		r := a.Compared.Reports[ri]
		b := a.Compared.FamilyReports.Bindings[r.BindingIndex]
		if !b.ScopeBound {
			v.ReportedBlockers = append(v.ReportedBlockers, "report_scope_unbound")
		}
		v.ReportedBlockers = append(v.ReportedBlockers, b.ScopeBlockers...)
		for fi, f := range r.Families {
			if f.Field != spec {
				continue
			}
			v.FamilyIndex = &fi
			family = &f
			if f.Binding == nil || !f.Binding.ReportedValueBound || f.Binding.Metadata.MinorUnits == nil {
				v.ReportedBlockers = append(v.ReportedBlockers, "reported_family_field_unbound")
			} else if len(v.ReportedBlockers) == 0 {
				v.ReportedMinorUnits = f.Binding.Metadata.MinorUnits
			}
			if f.Binding != nil {
				v.ReportedBlockers = append(v.ReportedBlockers, f.Binding.Blockers...)
			}
			break
		}
		if family == nil {
			v.ReportedBlockers = append(v.ReportedBlockers, "unqualified_family_layout")
		}
	}
	v.ReportedBlockers = sortedFamilyBlockers(v.ReportedBlockers)
	v.ComparisonBlockers = append(v.ComparisonBlockers, v.ReportedBlockers...)
	if len(v.ReportedBlockers) == 0 && family != nil {
		f := *family
		z := a.Reports[ri].Families[*v.FamilyIndex]
		switch {
		case (f.State == "equal" || f.State == "different") && f.DetailMinorUnits != nil && len(f.Blockers) == 0:
			v.ValueBasis = "nonmemo_occurrence_subtotal"
			v.ComparisonMinorUnits = f.DetailMinorUnits
		case z.State == "qualified_reported_zero" && z.ReportedZeroMinorUnits != nil && len(z.Blockers) == 0:
			v.ValueBasis = "qualified_reported_zero_without_occurrences"
			v.ComparisonMinorUnits = z.ReportedZeroMinorUnits
		default:
			v.ComparisonBlockers = append(v.ComparisonBlockers, "unqualified_family_comparison")
			v.ComparisonBlockers = append(v.ComparisonBlockers, f.Blockers...)
			v.ComparisonBlockers = append(v.ComparisonBlockers, z.Blockers...)
		}
	}
	v.ComparisonBlockers = sortedFamilyBlockers(v.ComparisonBlockers)
	return v
}

func familyWindowBlockers(m reportperiod.Review, c reportperiod.Coverage, expected, actual int) []string {
	b := append([]string{}, m.PartitionBlockers...)
	if !m.ObservedPartitionReady {
		b = append(b, "reported_partition_unready")
	}
	if actual < expected {
		b = append(b, "unqualified_candidate_family")
	}
	if actual == 0 {
		b = append(b, "no_qualified_family_members")
	}
	if c.GapDays > 0 {
		b = append(b, "family_coverage_gaps")
	}
	if c.OverlapDays > 0 {
		b = append(b, "family_coverage_overlaps")
	}
	if len(c.CrossBoundaryIndexes) > 0 {
		b = append(b, "cross_boundary_report_amount_not_apportioned")
	}
	return sortedFamilyBlockers(b)
}

func sortedFamilyBlockers(b []string) []string { slices.Sort(b); return slices.Compact(b) }

func addFamilyWindowAmount(sum *big.Int, amount *string) error {
	v, ok := new(big.Int).SetString(*amount, 10)
	if !ok {
		return fmt.Errorf("invalid verified family-window amount")
	}
	sum.Add(sum, v)
	return nil
}
