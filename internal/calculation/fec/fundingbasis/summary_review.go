package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

const SummaryReviewPolicy = "fec/summary-receipt-comparison-readiness@1.0.0"
const SummaryReviewVersion = "legal-tender.fec.summary-receipt-comparison-readiness.v1"

type ReceiptSource struct {
	ReleaseID      string `json:"release_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// ReceiptPopulation has overlapping diagnostic cohorts, not additive ledgers.
// Components alone partition Total. An empty cohort is not reported zero funding.
type ReceiptPopulation struct {
	State      string                `json:"state"`
	Total      Measures              `json:"total"`
	Individual Measures              `json:"individual_predicate"`
	Overlap    Measures              `json:"individual_committee_overlap"`
	Components []ReceiptRoleCoverage `json:"components"`
}

type SummaryFieldReview struct {
	Field        string                      `json:"field"`
	Raw          string                      `json:"raw"`
	Value        committeesummary.MoneyValue `json:"value"`
	Relationship string                      `json:"receipt_relationship"`
	Blockers     []string                    `json:"field_blockers"`
	Delta        *string                     `json:"delta_minor_units"`
}

type SummaryDiagnostic struct {
	State string  `json:"state"`
	Delta *string `json:"delta_minor_units"`
}

type SummaryVariantReview struct {
	AssertionID        string                       `json:"assertion_id"`
	RepresentativeFact string                       `json:"representative_fact_id"`
	CommitteeType      string                       `json:"committee_type_raw"`
	Designation        string                       `json:"designation_raw"`
	Members            []summaryassertion.Member    `json:"members"`
	CoverageStart      committeesummary.Value       `json:"coverage_start"`
	CoverageEnd        committeesummary.Value       `json:"coverage_end"`
	ScopeBlockers      []string                     `json:"scope_blockers"`
	Fields             []SummaryFieldReview         `json:"fields"`
	Diagnostics        map[string]SummaryDiagnostic `json:"summary_diagnostics"`
}

// This version assesses readiness, not numeric differences or financial truth.
// Comparison and funding blockers are separate: cash timing need not block a
// future scoped reported-subtotal comparison, but it does block cash allocation.
type SummaryReview struct {
	SchemaVersion    string                 `json:"schema_version"`
	ReviewID         string                 `json:"review_id"`
	Policy           string                 `json:"policy"`
	State            string                 `json:"state"`
	Cycle            string                 `json:"cycle"`
	CommitteeID      string                 `json:"committee_id"`
	InventoryID      string                 `json:"inventory_calculation_id"`
	ReceiptInput     Input                  `json:"receipt_input"`
	ReceiptSource    ReceiptSource          `json:"receipt_source"`
	SummaryInput     summaryassertion.Input `json:"summary_input"`
	SummaryID        string                 `json:"summary_calculation_id"`
	SourceAlignment  string                 `json:"source_alignment"`
	SummaryState     string                 `json:"summary_state"`
	ConflictFields   []string               `json:"summary_conflict_fields"`
	UnindexedRows    uint64                 `json:"summary_unindexed_source_rows"`
	Receipts         ReceiptPopulation      `json:"receipts"`
	Assertions       []SummaryVariantReview `json:"assertions"`
	ComparisonBlocks []string               `json:"comparison_blockers"`
	FundingBlocks    []string               `json:"funding_blockers"`
	ComparisonReady  bool                   `json:"comparison_ready"`
	FundingEligible  bool                   `json:"complete_committee_funding_basis"`
	TerminalEligible bool                   `json:"terminal_attribution_eligible"`
}

// ReviewSummary reuses this verified inventory and regenerates summary grouping
// from its verified immutable facts. User-supplied calculation JSON is not trusted.
func (r *Reader) ReviewSummary(ctx context.Context, manifestPath, cycle, committee string) (SummaryReview, error) {
	if r.result.Cycle != cycle || !committeeflows.ValidCommitteeID(&committee) {
		return SummaryReview{}, fmt.Errorf("requested cycle or committee does not match review scope")
	}
	summary, err := summaryassertion.Run(ctx, r.root, manifestPath, cycle)
	if err != nil {
		return SummaryReview{}, err
	}
	return r.reviewSummary(ctx, summary, committee)
}

func (r *Reader) reviewSummary(ctx context.Context, summary summaryassertion.Result, committee string) (SummaryReview, error) {
	if err := ctx.Err(); err != nil {
		return SummaryReview{}, err
	}
	if summary.Cycle != r.result.Cycle || !committeeflows.ValidCommitteeID(&committee) {
		return SummaryReview{}, fmt.Errorf("summary/receipt cycle or committee mismatch")
	}
	out := SummaryReview{
		SchemaVersion: SummaryReviewVersion, Policy: SummaryReviewPolicy, State: "reported_observations_comparison_blocked",
		Cycle: r.result.Cycle, CommitteeID: committee, InventoryID: r.result.CalculationID, ReceiptInput: r.result.Input,
		ReceiptSource: ReceiptSource{r.manifest.SourceReleaseID, r.manifest.SourceReleaseManifestSHA256},
		SummaryInput:  summary.Input, SummaryID: summary.CalculationID, SourceAlignment: "different_source_release",
		SummaryState: "no_indexed_summary_in_snapshot", ConflictFields: []string{}, UnindexedRows: summary.Counts.UnindexedRows,
		Assertions: []SummaryVariantReview{}, ComparisonBlocks: []string{},
		FundingBlocks: []string{"cash_versus_valuation_unverified", "inter_report_cash_continuity_unverified", "recipient_cash_availability_unverified", "complete_funding_family_coverage_unverified"},
	}
	if out.ReceiptSource.ReleaseID == summary.Input.SourceReleaseID {
		if out.ReceiptSource.ManifestSHA256 != summary.Input.SourceReleaseSHA256 {
			return SummaryReview{}, fmt.Errorf("same release ID has conflicting manifest digests")
		}
		out.SourceAlignment = "same_source_release"
	} else {
		out.ComparisonBlocks = append(out.ComparisonBlocks, "source_release_mismatch")
	}
	// The inventory does not carry report-period/date coverage or form-line
	// equivalence. Neither matching cycles nor matching totals supplies that proof.
	out.ComparisonBlocks = append(out.ComparisonBlocks, "receipt_reporting_period_coverage_unverified", "summary_account_and_report_scope_unverified")
	population, err := r.summaryPopulation(committee)
	if err != nil {
		return SummaryReview{}, err
	}
	out.Receipts = population
	if population.Total.Rows == 0 {
		out.ComparisonBlocks = append(out.ComparisonBlocks, "no_recipient_rows_in_snapshot")
	}
	for _, group := range summary.Committees {
		if group.CommitteeID != committee {
			continue
		}
		out.SummaryState, out.ConflictFields = group.State, group.ConflictFields
		for _, assertion := range group.Assertions {
			variant, err := reviewVariant(summary.Cycle, group.ConflictFields, assertion)
			if err != nil {
				return SummaryReview{}, err
			}
			out.Assertions = append(out.Assertions, variant)
		}
	}
	if len(out.Assertions) == 0 {
		out.ComparisonBlocks = append(out.ComparisonBlocks, "no_indexed_summary_in_snapshot")
	}
	if err := ctx.Err(); err != nil {
		return SummaryReview{}, err
	}
	body, err := json.Marshal(out)
	if err != nil {
		return SummaryReview{}, err
	}
	sum := sha256.Sum256(body)
	out.ReviewID = hex.EncodeToString(sum[:])
	return out, nil
}

func (r *Reader) summaryPopulation(committee string) (ReceiptPopulation, error) {
	p := ReceiptPopulation{State: "no_rows_in_snapshot", Components: []ReceiptRoleCoverage{}}
	groups := map[[2]string]Measures{}
	for _, b := range r.result.Buckets {
		if !b.Key.Recipient.Present || b.Key.Recipient.Value != committee {
			continue
		}
		if err := p.Total.merge(b.Measures); err != nil {
			return ReceiptPopulation{}, err
		}
		if b.Key.IndividualDecision == "included" {
			if err := p.Individual.merge(b.Measures); err != nil {
				return ReceiptPopulation{}, err
			}
		}
		if b.Key.Component == "overlapping_individual_and_committee" {
			if err := p.Overlap.merge(b.Measures); err != nil {
				return ReceiptPopulation{}, err
			}
		}
		key := [2]string{b.Key.Component, b.Key.ReceiptRole}
		m := groups[key]
		if err := m.merge(b.Measures); err != nil {
			return ReceiptPopulation{}, err
		}
		groups[key] = m
	}
	var conserved Measures
	for key, m := range groups {
		p.Components = append(p.Components, ReceiptRoleCoverage{key[0], key[1], m})
		if err := conserved.merge(m); err != nil {
			return ReceiptPopulation{}, err
		}
	}
	if conserved != p.Total {
		return ReceiptPopulation{}, fmt.Errorf("recipient inventory conservation failed")
	}
	sort.Slice(p.Components, func(i, j int) bool {
		a, b := p.Components[i], p.Components[j]
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Role < b.Role
	})
	if p.Total.Rows != 0 {
		p.State = "reported_rows_not_complete_funding"
	}
	return p, nil
}

func hasAny(fields []string, candidates ...string) bool {
	for _, candidate := range candidates {
		if slices.Contains(fields, candidate) {
			return true
		}
	}
	return false
}
