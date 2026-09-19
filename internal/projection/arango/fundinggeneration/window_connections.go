package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

const WindowConnectionsVersion = "legal-tender.funding-window-connections.v1"
const WindowConnectionsPolicy = "fec/dated-receipt-and-authorization-connections@1.0.0"

type WindowConnectionQuery struct {
	PathQuery
	EntryGeneration string      `json:"receipt_generation_id"`
	Window          *DateWindow `json:"reported_date_window"`
	SpendingDate    string      `json:"spending_date_field,omitempty"`
}

func (q WindowConnectionQuery) Validate() error {
	if err := q.PathQuery.Validate(); err != nil {
		return err
	}
	if q.ReceiptOrdinal > 0 {
		if !validDigest(q.EntryGeneration) {
			return fmt.Errorf("receipt entry requires an exact generation ID")
		}
	} else if q.EntryGeneration != "" {
		return fmt.Errorf("receipt generation requires an occurrence entry")
	}
	if q.isSpending() {
		if q.SpendingDate != "expenditure" && q.SpendingDate != "dissemination" {
			return fmt.Errorf("Schedule E requires explicit expenditure or dissemination date field")
		}
	} else if q.SpendingDate != "" {
		return fmt.Errorf("spending date field requires a Schedule E ending")
	}
	if q.Window != nil {
		_, _, err := q.Window.bounds()
		return err
	}
	return nil
}

func (q WindowConnectionQuery) isSpending() bool {
	return q.Ending == "independent_support" || q.Ending == "independent_opposition"
}

type WindowReceiptEntry struct {
	GenerationID string `json:"generation_id"`
	Selection    string `json:"date_selection"`
	receipt.DatedPathEntry
}

type WindowConnectionLink struct {
	ID            string         `json:"link_id"`
	GenerationID  string         `json:"generation_id"`
	Topology      graphread.Link `json:"original_topology"`
	Date          *int32         `json:"reported_date_days"`
	TemporalBasis string         `json:"temporal_basis"`
	Evidence      graphread.Item `json:"evidence"`
}

type WindowConnectionFacet struct {
	GenerationID    string           `json:"generation_id"`
	Receipts        graphread.Facet  `json:"receipt_facet"`
	CommitteeFlow   graphread.Facet  `json:"committee_flow_facet"`
	OutsideSpending *graphread.Facet `json:"outside_spending_facet,omitempty"`
	SharedConduits  *graphread.Facet `json:"shared_conduit_facet,omitempty"`
}

type WindowConnectionVertex struct {
	ID     string                  `json:"entity_id"`
	Kind   string                  `json:"kind"`
	Facets []WindowConnectionFacet `json:"publication_facets"`
}

type WindowAuthorizationContext struct {
	GenerationID  string `json:"generation_id"`
	Links         uint64 `json:"candidate_authorization_links"`
	TemporalBasis string `json:"temporal_basis"`
}

type WindowConnectionsResult struct {
	SchemaVersion        string                       `json:"schema_version"`
	Policy               string                       `json:"policy"`
	ResultID             string                       `json:"result_id"`
	BuildSHA256          string                       `json:"consumer_executable_sha256"`
	Inputs               []WindowPublication          `json:"inputs"`
	Query                WindowConnectionQuery        `json:"query"`
	Entry                *WindowReceiptEntry          `json:"receipt_entry"`
	Coverage             []WindowCoverage             `json:"committee_ledger_coverage"`
	Contexts             []WindowAuthorizationContext `json:"candidate_context"`
	SpendingCoverage     []WindowSpendingCoverage     `json:"spending_source_coverage,omitempty"`
	Search               PathSearch                   `json:"search"`
	Paths                []EvidencePath               `json:"paths"`
	Links                []WindowConnectionLink       `json:"links"`
	Vertices             []WindowConnectionVertex     `json:"vertices"`
	Limitations          []string                     `json:"limitations"`
	FinancialEligibility bool                         `json:"financial_eligibility"`
	TerminalEligible     bool                         `json:"terminal_attribution_eligible"`
}

type connectionRoute struct {
	windowRoute
	original graphread.Link
	spending *ie.DatedMember
}

// ConnectionPaths is additive: the original committee-only Paths contract is
// unchanged. Dates select observations; authorization has unknown validity.
func (r *WindowReader) ConnectionPaths(ctx context.Context, q WindowConnectionQuery) (WindowConnectionsResult, error) {
	if err := q.Validate(); err != nil {
		return WindowConnectionsResult{}, err
	}
	if err := r.verify(ctx); err != nil {
		return WindowConnectionsResult{}, err
	}
	for _, p := range r.partitions {
		if p.receipts == nil {
			return WindowConnectionsResult{}, fmt.Errorf("verified receipt source required")
		}
	}
	if q.Window != nil {
		w := *q.Window
		q.Window = &w
	}
	out := WindowConnectionsResult{SchemaVersion: WindowConnectionsVersion, Policy: WindowConnectionsPolicy, BuildSHA256: r.build, Query: q,
		Inputs: []WindowPublication{}, Coverage: []WindowCoverage{}, Contexts: []WindowAuthorizationContext{}, Paths: []EvidencePath{}, Links: []WindowConnectionLink{}, Vertices: []WindowConnectionVertex{}, Limitations: []string{
			"one_selected_committee_ledger_not_reconciled_payments_or_summed_path_money",
			"receipt_and_conduit_entries_preserve_occurrence_grain_not_resolved_donors",
			"conduit_date_is_underlying_receipt_date_not_relationship_validity_or_additional_money",
			"authorization_is_publication_context_not_a_payment_or_proven_authorization_during_window",
			"different_authorization_publications_are_evidence_variants_not_additional_payments",
			"exact_fec_ids_connect_inputs_without_merging_historical_identity_facets",
			"date_filter_not_chronological_flow_or_same_dollar_provenance",
			"unknown_observation_dates_excluded_from_bounded_windows_without_filling_dates",
			"coverage_counts_only_supplied_selected_committee_ledgers_not_all_receipts_or_world_completeness",
			"all_supplied_dates_is_not_all_history",
			"no_path_or_search_cutoff_is_not_terminal_evidence",
			"schedule_e_group_amounts_not_dated_endings_in_this_contract",
			"single_version_per_source_cycle_no_snapshot_reconciliation",
			"search_budget_excludes_opening_topology_scan_and_evidence_readback",
			"immutable_publications_not_a_cross_database_transaction",
		}}
	if q.isSpending() {
		out.SchemaVersion, out.Policy = WindowSpendingConnectionsVersion, WindowSpendingConnectionsPolicy
		out.Limitations = spendingLimitations(out.Limitations)
	}
	for _, p := range r.partitions {
		b, err := json.Marshal(p.publication)
		if err != nil {
			return WindowConnectionsResult{}, err
		}
		var owned WindowPublication
		if err := json.Unmarshal(b, &owned); err != nil {
			return WindowConnectionsResult{}, err
		}
		out.Inputs = append(out.Inputs, owned)
	}
	for _, p := range r.partitions {
		if p.shared != nil {
			out.Limitations = append(out.Limitations, "shared_associations_preserve_original_dispositions_and_add_zero_money", "shared_entry_date_is_original_receipt_date_not_related_memo_date")
			break
		}
	}
	start, entryPartition := q.From, -1
	if q.ReceiptOrdinal > 0 {
		for i, p := range r.partitions {
			if p.publication.GenerationID == q.EntryGeneration {
				entryPartition = i
				break
			}
		}
		if entryPartition < 0 {
			return WindowConnectionsResult{}, fmt.Errorf("receipt generation is not a supplied publication")
		}
		p := r.partitions[entryPartition]
		var entry receipt.DatedPathEntry
		var err error
		if q.EntryFamily == receipt.SharedFamily {
			if p.shared == nil {
				return WindowConnectionsResult{}, fmt.Errorf("shared entry requires an extended generation")
			}
			entry, err = p.shared.DatedPathEntry(ctx, q.ReceiptOrdinal)
		} else {
			entry, err = p.receipts.DatedPathEntry(ctx, q.EntryFamily, q.ReceiptOrdinal)
		}
		if err != nil {
			return WindowConnectionsResult{}, err
		}
		selection := receiptDateSelection(entry.Date, q.Window)
		out.Entry = &WindowReceiptEntry{q.EntryGeneration, selection, entry}
		if entry.Entry.Link != nil {
			l := entry.Entry.Link
			if entry.Entry.State != "available" || entry.Entry.Item == nil || entry.Entry.Item.Key != l.Key || l.Family != q.EntryFamily || !validDigest(l.Key) || l.From != l.Key || graphread.Kind(l.To) != "committee" {
				return WindowConnectionsResult{}, fmt.Errorf("receipt entry topology/evidence mismatch")
			}
			if selection == "included" || selection == "undated_included" {
				start = l.To
			}
		} else if entry.Entry.State == "available" || entry.Entry.Item != nil {
			return WindowConnectionsResult{}, fmt.Errorf("receipt entry availability mismatch")
		}
	}
	chain, flowRoutes, coverage, err := r.windowTopology(ctx, WindowPathQuery{Ledger: q.Ledger, Window: q.Window})
	if err != nil {
		return WindowConnectionsResult{}, err
	}
	out.Coverage = coverage
	routes := make(map[string]connectionRoute, len(flowRoutes))
	for _, links := range chain {
		for _, link := range links {
			routes[link.ID()] = connectionRoute{windowRoute: flowRoutes[link.ID()], original: link}
		}
	}
	var endings pathTopology
	if q.isSpending() {
		endings, out.SpendingCoverage, err = r.spendingEndings(ctx, q, routes)
	} else {
		endings, out.Contexts, err = r.connectionEndings(ctx, q, routes)
	}
	if err != nil {
		return WindowConnectionsResult{}, err
	}
	selected := [][]graphread.Link{}
	if start == "" {
		out.Search = PathSearch{State: "start_relationship_not_available", MorePaths: "not_searched"}
		if out.Entry != nil && out.Entry.Selection != "included" && out.Entry.Selection != "undated_included" {
			out.Search.State = "receipt_entry_excluded_by_date_window"
		}
	} else {
		selected, out.Search, err = searchPaths(ctx, chain, endings, start, q.Target, q.MaxHops, q.Limit, q.Budget)
		if err != nil {
			return WindowConnectionsResult{}, err
		}
	}
	if err := r.connectionEvidence(ctx, &out, selected, routes, entryPartition, start); err != nil {
		return WindowConnectionsResult{}, err
	}
	if err := r.verify(ctx); err != nil {
		return WindowConnectionsResult{}, err
	}
	out.ResultID = valueID(out)
	return out, nil
}

func receiptDateSelection(date *int32, window *DateWindow) string {
	if date == nil {
		if window == nil {
			return "undated_included"
		}
		return "unknown_date_excluded"
	}
	if window != nil {
		lo, hi, _ := window.bounds() // query was validated before source reads
		if *date < lo {
			return "before_window"
		}
		if *date > hi {
			return "after_window"
		}
	}
	return "included"
}
