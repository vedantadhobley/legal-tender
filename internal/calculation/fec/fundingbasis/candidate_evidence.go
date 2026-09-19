package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
)

const CandidateEvidenceVersion = "legal-tender.fec.candidate-evidence.v1"
const CandidateEvidencePolicy = "fec/integrated-candidate-observations@1.0.0"

type CandidateEvidenceOptions struct {
	candidateupstream.Options
	BasisResult, SummaryFacts, ExecutableSHA256 string
}

type EvidenceCommittee struct {
	CommitteeID   string            `json:"committee_id"`
	Authorization string            `json:"candidate_authorization"`
	Reached       bool              `json:"reached_by_selected_committee_trace"`
	Receipts      ReceiptPopulation `json:"reported_receipt_population"`
	Summary       *SummaryReview    `json:"candidate_linked_summary_review"`
	Coverage      []string          `json:"coverage_limits"`
}

type EvidenceOverview struct {
	AuthorizedCommittees       int `json:"authorized_committees"`
	UnresolvedLinkedCommittees int `json:"unresolved_linked_committees"`
	ReachedCommittees          int `json:"reached_committees"`
	ReachedWithoutMaster       int `json:"reached_without_same_cycle_master"`
	ReachedWithoutReceipts     int `json:"reached_without_receipt_rows"`
	CyclicComponents           int `json:"cyclic_components"`
}

// This envelope deliberately has no candidate total or network-wide money sum.
// Components and the nested trace retain their own non-additive scopes.
type CandidateEvidence struct {
	SchemaVersion    string                   `json:"schema_version"`
	ResultID         string                   `json:"result_id"`
	Policy           string                   `json:"policy"`
	ExecutableSHA256 string                   `json:"executable_sha256"`
	CandidateID      string                   `json:"candidate_id"`
	Cycle            string                   `json:"cycle"`
	State            string                   `json:"state"`
	InventoryID      string                   `json:"inventory_calculation_id"`
	InventoryPolicy  string                   `json:"inventory_policy"`
	ReceiptInput     Input                    `json:"receipt_input"`
	SummaryState     string                   `json:"summary_context_state"`
	Overview         EvidenceOverview         `json:"overview"`
	Trace            candidateupstream.Result `json:"committee_trace"`
	Witnesses        []flow.Observation       `json:"connection_witnesses"`
	Committees       []EvidenceCommittee      `json:"committees"`
	TerminalPolicy   *string                  `json:"terminal_policy"`
	AllocationPolicy *string                  `json:"allocation_policy"`
	TerminalAmount   *string                  `json:"terminal_amount_minor_units"`
	TerminalEligible bool                     `json:"terminal_attribution_eligible"`
	Limitations      []string                 `json:"limitations"`
}

// BuildCandidateEvidence verifies retained inputs once per component and joins
// them. It neither rescans Schedule A records nor fetches any new source data.
func BuildCandidateEvidence(ctx context.Context, o CandidateEvidenceOptions) (CandidateEvidence, error) {
	b, err := hex.DecodeString(o.ExecutableSHA256)
	if err != nil || len(b) != sha256.Size || hex.EncodeToString(b) != o.ExecutableSHA256 {
		return CandidateEvidence{}, fmt.Errorf("exact executable SHA-256 required")
	}
	if o.Progress != nil {
		o.Progress("verifying retained receipt inventory and backing")
	}
	r, err := Open(ctx, o.StorageRoot, o.BasisResult)
	if err != nil {
		return CandidateEvidence{}, err
	}
	trace, witnesses, err := candidateupstream.RunWithWitnesses(ctx, o.Options)
	if err != nil {
		return CandidateEvidence{}, err
	}
	var summary *summaryassertion.Result
	if o.SummaryFacts != "" {
		if o.Progress != nil {
			o.Progress("verifying and grouping optional summary context once")
		}
		s, err := summaryassertion.Run(ctx, o.StorageRoot, o.SummaryFacts, o.Cycle)
		if err != nil {
			return CandidateEvidence{}, err
		}
		summary = &s
	}
	return r.candidateEvidence(ctx, trace, witnesses, summary, o.ExecutableSHA256)
}

func (r *Reader) candidateEvidence(ctx context.Context, trace candidateupstream.Result, witnesses []flow.Observation, summary *summaryassertion.Result, executable string) (CandidateEvidence, error) {
	// Reuse the existing exact-source binding; this is not a join by cycle alone.
	if err := r.matchTrace(trace); err != nil {
		return CandidateEvidence{}, err
	}
	out := CandidateEvidence{SchemaVersion: CandidateEvidenceVersion, Policy: CandidateEvidencePolicy, ExecutableSHA256: executable,
		CandidateID: trace.Candidate, Cycle: trace.Cycle, State: "integrated_observations_not_complete_funding",
		InventoryID: r.result.CalculationID, InventoryPolicy: r.result.Policy, ReceiptInput: r.result.Input,
		SummaryState: "not_requested", Trace: trace, Witnesses: witnesses, Committees: []EvidenceCommittee{},
		Limitations: []string{"receipt_populations_are_not_complete_candidate_funding", "upstream_receipts_are_not_additive_candidate_money",
			"connectivity_is_not_chronological_dollar_provenance", "missing_edges_or_masters_do_not_define_terminal_sources",
			"unknown_signed_amounts_do_not_establish_lower_bounds", "person_corporation_and_conduit_identity_unresolved",
			"report_amendment_and_financial_period_selection_not_established", "opening_balances_unitemized_and_cash_basis_not_established",
			"independent_expenditures_and_sender_ledger_not_combined_into_receipts", "terminal_and_allocation_policies_not_selected"}}
	if summary != nil {
		out.SummaryState = "candidate_linked_context_only_not_receipt_replacement"
	}
	byCommittee := map[string][]Bucket{}
	for _, b := range r.result.Buckets {
		if b.Key.Recipient.Present {
			byCommittee[b.Key.Recipient.Value] = append(byCommittee[b.Key.Recipient.Value], b)
		}
	}
	nodes := map[string]candidateupstream.Node{}
	selected := map[string]string{}
	for _, node := range trace.Nodes {
		nodes[node.CommitteeID] = node
		selected[node.CommitteeID] = "not_candidate_linked"
		out.Overview.ReachedCommittees++
		if node.MasterFactID == nil {
			out.Overview.ReachedWithoutMaster++
		}
	}
	for _, rel := range trace.Relationships {
		_, reached := nodes[rel.CommitteeID]
		if reached || rel.State == "authorized" || rel.State == "unresolved" {
			selected[rel.CommitteeID] = rel.State
		}
		if rel.State == "authorized" {
			out.Overview.AuthorizedCommittees++
		} else if rel.State == "unresolved" {
			out.Overview.UnresolvedLinkedCommittees++
		}
	}
	out.Overview.CyclicComponents = len(trace.CyclicComponents)
	ids := make([]string, 0, len(selected))
	for id := range selected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return CandidateEvidence{}, err
		}
		// Only narrow the in-memory population; identity and source manifest stay
		// pinned to the complete verified inventory. No per-node corpus scan.
		local := *r
		local.result.Buckets = byCommittee[id]
		population, err := local.summaryPopulation(id)
		if err != nil {
			return CandidateEvidence{}, err
		}
		_, reached := nodes[id]
		c := EvidenceCommittee{CommitteeID: id, Authorization: selected[id], Reached: reached, Receipts: population,
			Coverage: []string{"reported_rows_not_complete_funding", "resolved_donor_identity_not_established"}}
		if population.Total.Rows == 0 {
			c.Coverage = append(c.Coverage, "no_receipt_rows_is_not_reported_zero")
			if reached {
				out.Overview.ReachedWithoutReceipts++
			}
		}
		if population.Total.Unknown > 0 {
			c.Coverage = append(c.Coverage, "unknown_signed_amounts")
		}
		if population.Overlap.Rows > 0 {
			c.Coverage = append(c.Coverage, "individual_committee_predicates_overlap_do_not_add")
		}
		if summary != nil && (c.Authorization == "authorized" || c.Authorization == "unresolved") {
			review, err := local.reviewSummary(ctx, *summary, id)
			if err != nil {
				return CandidateEvidence{}, err
			}
			c.Summary = &review
		}
		out.Committees = append(out.Committees, c)
	}
	body, err := json.Marshal(out)
	if err != nil {
		return CandidateEvidence{}, err
	}
	id := sha256.Sum256(body)
	out.ResultID = hex.EncodeToString(id[:])
	return out, ctx.Err()
}
