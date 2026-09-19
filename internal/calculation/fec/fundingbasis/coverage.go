package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const CoveragePolicy = "fec/preserved-funding-coverage-audit@1.0.0"

type FundingCoverage struct {
	SchemaVersion        string                `json:"schema_version"`
	AuditID              string                `json:"audit_id"`
	Policy               string                `json:"policy"`
	Cycle                string                `json:"cycle"`
	InventoryID          string                `json:"inventory_calculation_id"`
	ReceiptInput         Input                 `json:"receipt_input"`
	BundleID             string                `json:"receipt_bundle_id"`
	BundleSHA256         string                `json:"receipt_bundle_manifest_sha256"`
	SourceReleaseID      string                `json:"source_release_id"`
	ReceiptRoles         []ReceiptRoleCoverage `json:"receipt_roles"`
	Summaries            []SummaryCoverage     `json:"candidate_summary_profiles"`
	Requirements         []CoverageRequirement `json:"requirements"`
	CompleteFundingBasis bool                  `json:"complete_committee_funding_basis"`
	TerminalEligible     bool                  `json:"terminal_attribution_eligible"`
}

type ReceiptRoleCoverage struct {
	Component string   `json:"component"`
	Role      string   `json:"receipt_role"`
	Measures  Measures `json:"measures"`
}

type CoverageRequirement struct {
	Requirement string   `json:"requirement"`
	State       string   `json:"state"`
	Evidence    []string `json:"evidence"`
	Reason      string   `json:"reason"`
}

// AuditCoverage reuses a verified receipt inventory and independently scans the
// two candidate-summary publications from the same immutable receipt bundle.
// It does not scan Schedule A rows again or infer missing summary components.
func (r *Reader) AuditCoverage(ctx context.Context, bundlePath string) (FundingCoverage, error) {
	bundle, digest, err := receipts.LoadPublishedFactBundle(r.root, bundlePath)
	if err != nil {
		return FundingCoverage{}, err
	}
	if err := r.coverageBundleMatches(bundle); err != nil {
		return FundingCoverage{}, err
	}
	out := FundingCoverage{SchemaVersion: "legal-tender.fec.funding-coverage-audit.v1", Policy: CoveragePolicy, Cycle: r.result.Cycle, InventoryID: r.result.CalculationID, ReceiptInput: r.result.Input, BundleID: bundle.BundleID, BundleSHA256: digest, SourceReleaseID: bundle.SourceReleaseID, ReceiptRoles: []ReceiptRoleCoverage{}, Summaries: []SummaryCoverage{}}
	roles := map[[2]string]Measures{}
	for _, b := range r.result.Buckets {
		key := [2]string{b.Key.Component, b.Key.ReceiptRole}
		m := roles[key]
		if err := m.merge(b.Measures); err != nil {
			return FundingCoverage{}, err
		}
		roles[key] = m
	}
	var total Measures
	for key, m := range roles {
		out.ReceiptRoles = append(out.ReceiptRoles, ReceiptRoleCoverage{key[0], key[1], m})
		if err := total.merge(m); err != nil {
			return FundingCoverage{}, err
		}
	}
	if total != r.result.Total {
		return FundingCoverage{}, fmt.Errorf("receipt role coverage does not conserve inventory")
	}
	sort.Slice(out.ReceiptRoles, func(i, j int) bool {
		a, b := out.ReceiptRoles[i], out.ReceiptRoles[j]
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Role < b.Role
	})
	for _, ref := range bundle.InputFactSets {
		if ref.Dataset != string(classic.AllCandidatesSummary) && ref.Dataset != string(classic.CurrentCampaignsSummary) {
			continue
		}
		path := filepath.Join(r.root, "facts/fec/classic", ref.Dataset, "manifests", ref.FactSetID+".json")
		m, md, err := occ.LoadPublishedClassicFactManifest(r.root, path, ref.Dataset)
		if err != nil {
			return FundingCoverage{}, err
		}
		expected := bundle.Counts.AllCandidatesSummaries
		if ref.Dataset == string(classic.CurrentCampaignsSummary) {
			expected = bundle.Counts.CurrentCampaignsSummaries
		}
		if err := coverageSummaryMatches(m, md, ref, bundle, expected); err != nil {
			return FundingCoverage{}, err
		}
		profile, err := profileSummary(ctx, r.root, m, md)
		if err != nil {
			return FundingCoverage{}, err
		}
		out.Summaries = append(out.Summaries, profile)
	}
	if len(out.Summaries) != 2 {
		return FundingCoverage{}, fmt.Errorf("two distinct candidate-summary populations required")
	}
	out.Requirements = coverageRequirements()
	if err := ctx.Err(); err != nil {
		return FundingCoverage{}, err
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return FundingCoverage{}, err
	}
	sum := sha256.Sum256(encoded)
	out.AuditID = hex.EncodeToString(sum[:])
	return out, nil
}

func (r *Reader) coverageBundleMatches(b receipts.FactBundleManifest) error {
	if b.Cycle != r.result.Cycle || b.SourceReleaseID != r.manifest.SourceReleaseID || b.Counts.ScheduleAFacts != r.result.Input.Facts {
		return fmt.Errorf("coverage bundle must match inventory cycle, source release, and receipt count")
	}
	for _, ref := range b.InputFactSets {
		if ref.Role == "schedule_a_receipts" {
			if ref.FactSetID != r.result.Input.FactSetID || ref.ManifestSHA256 != r.result.Input.ManifestSHA256 {
				return fmt.Errorf("coverage bundle must bind exact inventory Schedule A facts")
			}
			return nil
		}
	}
	return fmt.Errorf("coverage bundle lacks Schedule A reference")
}

func coverageSummaryMatches(m occ.ClassicFactManifest, digest string, ref receipts.FactSetReference, b receipts.FactBundleManifest, count uint64) error {
	if m.FactSetID != ref.FactSetID || digest != ref.ManifestSHA256 || m.FactType != ref.FactType || m.Dataset != ref.Dataset || m.Cycle != b.Cycle || m.SourceReleaseID != b.SourceReleaseID || m.Counts.Facts != count {
		return fmt.Errorf("summary publication differs from pinned coverage bundle")
	}
	return nil
}

// These are versioned semantic requirements, not heuristics derived from field
// spelling. Exact supported source layouts are checked before emitting them.
func coverageRequirements() []CoverageRequirement {
	return []CoverageRequirement{
		{"published_receipt_occurrence_inventory", "supported", []string{"receipt_input", "receipt_roles"}, "disjoint source components and signed roles conserve the pinned published facts; not a cash denominator"},
		{"candidate_summary_observations", "supported", []string{"candidate_summary_profiles"}, "both exact source populations are scanned separately; not combined or substituted for detail"},
		{"committee_unitemized_receipts", "absent_from_supplied_sources", []string{"candidate_summary_profiles.source_fields"}, "these candidate-summary contracts contain total individual contributions, not a separate unitemized assertion"},
		{"committee_report_opening_balance", "scope_incompatible", []string{"candidate_summary_profiles.COH_BOP"}, "candidate-level balances do not identify a committee, account, or report opening boundary"},
		{"committee_report_coverage_interval", "absent_from_supplied_sources", []string{"candidate_summary_profiles.CVG_END_DT"}, "a candidate coverage-through date is not a per-committee report identifier plus coverage start and end"},
		{"prior_cycle_funding_origin", "unresolved", []string{"candidate_summary_profiles.COH_BOP"}, "an opening balance does not identify its original contributors or transfer ancestry"},
		{"complete_cash_receipt_roles", "unresolved", []string{"receipt_roles"}, "published transaction roles include memo, valuation, loan, refund, and unresolved populations; no complete cash policy accepted"},
		{"recipient_cash_availability", "unresolved", []string{"receipt_input"}, "receipt dates remain in source facts but are not profiled by this audit; donor/conduit dates do not alone establish recipient cash availability"},
		{"negative_adjustment_allocation", "unresolved", []string{"receipt_roles"}, "signed amounts are conserved; their economic correction targets and effective timing are not inferred"},
		{"complete_committee_funding_denominator", "unresolved", []string{"receipt_input", "candidate_summary_profiles"}, "no residual filling, candidate-to-committee balance distribution, or normalization of incomplete funding"},
	}
}
