package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

const PathsVersion = "legal-tender.funding-paths.v1"
const PathsPolicy = "fec/typed-observation-paths@1.0.0"

type PathQuery struct {
	From           string      `json:"from_committee"`
	ReceiptOrdinal uint64      `json:"receipt_source_row_ordinal"`
	EntryFamily    string      `json:"entry_family"`
	Ledger         flow.Ledger `json:"committee_ledger"`
	Target         string      `json:"target_entity"`
	Ending         string      `json:"candidate_ending_family"`
	MaxHops        int         `json:"max_committee_hops"`
	Limit          int         `json:"max_paths"`
	Budget         uint64      `json:"max_links_examined"`
}

func (q PathQuery) Validate() error {
	if q.Ledger != flow.ScheduleA && q.Ledger != flow.ScheduleB {
		return fmt.Errorf("exactly one selected committee ledger required")
	}
	if q.ReceiptOrdinal > 0 {
		if q.From != "" || q.EntryFamily != "reported_receipt" && q.EntryFamily != "conduit_association" && q.EntryFamily != receipt.SharedFamily {
			return fmt.Errorf("receipt start requires one entry family and no committee override")
		}
	} else if graphread.Kind(q.From) != "committee" || q.EntryFamily != "" {
		return fmt.Errorf("committee start or exact receipt occurrence required")
	}
	switch graphread.Kind(q.Target) {
	case "committee":
		if q.Ending != "" {
			return fmt.Errorf("committee target cannot add a candidate ending")
		}
		if q.ReceiptOrdinal == 0 && q.From == q.Target {
			return fmt.Errorf("same-committee identity or cycle queries require a separate query mode")
		}
	case "candidate":
		if q.Ending != "candidate_authorization_context" && q.Ending != "independent_support" && q.Ending != "independent_opposition" {
			return fmt.Errorf("candidate target requires one explicit ending family")
		}
	default:
		return fmt.Errorf("invalid target entity")
	}
	if q.MaxHops < 0 || q.MaxHops > 8 || q.Limit < 1 || q.Limit > 10 || q.Budget < 1 || q.Budget > 100000 {
		return fmt.Errorf("invalid path bounds")
	}
	return nil
}

type EvidencePath struct {
	ID    string   `json:"path_id"`
	Links []string `json:"link_ids"`
}
type PathLink struct {
	ID       string         `json:"link_id"`
	Topology graphread.Link `json:"topology"`
	Family   Family         `json:"family"`
	Evidence graphread.Item `json:"evidence"`
}
type PathVertex struct {
	ID     string                     `json:"entity_id"`
	Kind   string                     `json:"kind"`
	Facets map[string]graphread.Facet `json:"facets"`
}
type PathsResult struct {
	SchemaVersion        string             `json:"schema_version"`
	Policy               string             `json:"policy"`
	ResultID             string             `json:"result_id"`
	GenerationID         string             `json:"generation_id"`
	GenerationSHA256     string             `json:"generation_sha256"`
	BuildSHA256          string             `json:"consumer_executable_sha256"`
	Cycle                string             `json:"cycle"`
	Query                PathQuery          `json:"query"`
	Entry                *receipt.PathEntry `json:"receipt_entry"`
	Search               PathSearch         `json:"search"`
	Paths                []EvidencePath     `json:"paths"`
	Links                []PathLink         `json:"links"`
	Vertices             []PathVertex       `json:"vertices"`
	Limitations          []string           `json:"limitations"`
	FinancialEligibility bool               `json:"financial_eligibility"`
	TerminalEligible     bool               `json:"terminal_attribution_eligible"`
}

func valueID(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (r *Reader) chainTopology(ctx context.Context, side flow.Ledger) (pathTopology, error) {
	t := pathTopology{}
	err := r.flow.VisitLinks(ctx, side, func(link graphread.Link) error {
		if graphread.Kind(link.To) != "committee" {
			return fmt.Errorf("foreign committee topology endpoint")
		}
		return t.add(link)
	})
	if err != nil {
		return nil, err
	}
	return t, t.order()
}
func (r *Reader) endingLinks(family string) ([]graphread.Link, error) {
	if family == "" {
		return []graphread.Link{}, nil
	}
	if family == "candidate_authorization_context" {
		return r.receipts.AuthorizedLinks(), nil
	}
	return r.outside.CandidateLinks(family)
}

func (r *Reader) Paths(ctx context.Context, q PathQuery) (PathsResult, error) {
	if err := q.Validate(); err != nil {
		return PathsResult{}, err
	}
	if q.EntryFamily == receipt.SharedFamily && r.shared == nil {
		return PathsResult{}, fmt.Errorf("shared entry requires an extended generation")
	}
	out := PathsResult{SchemaVersion: PathsVersion, Policy: PathsPolicy, GenerationID: r.queryGenerationID(), GenerationSHA256: r.manifestSHA, BuildSHA256: r.consumerBuild, Cycle: r.generation.Cycle, Query: q, Paths: []EvidencePath{}, Links: []PathLink{}, Vertices: []PathVertex{}, Limitations: []string{
		"directed_simple_committee_paths_not_all_graph_walks",
		"one_selected_committee_ledger_not_reconciled_payments",
		"connected_observations_do_not_establish_same_dollars_or_chronological_flow",
		"conduit_authorization_and_reconciliation_are_not_additional_money",
		"no_path_or_no_outgoing_edge_is_not_terminal_donor_evidence",
		"cycle_and_hop_frontier_counts_describe_search_not_global_topology",
		"source_population_identity_gaps_and_unprojectable_records_remain",
		"outside_evidence_is_calculation_group_not_enumerated_source_membership",
		"expansion_budget_excludes_generation_open_topology_build_and_source_verification",
		"immutable_publications_required_not_a_cross_database_transaction",
	}}
	if r.shared != nil {
		out.Limitations = append(out.Limitations, "shared_associations_preserve_original_dispositions_and_add_zero_money", "shared_open_verifies_complete_source_membership_not_all_live_document_fields")
	}
	start := q.From
	if q.ReceiptOrdinal > 0 {
		var entry receipt.PathEntry
		var err error
		if q.EntryFamily == receipt.SharedFamily {
			entry, err = r.shared.PathEntry(ctx, q.ReceiptOrdinal)
		} else {
			entry, err = r.receipts.PathEntry(ctx, q.EntryFamily, q.ReceiptOrdinal)
		}
		if err != nil {
			return PathsResult{}, err
		}
		out.Entry = &entry
		if entry.Link != nil {
			start = entry.Link.To
		}
	}
	selected := [][]graphread.Link{}
	if start == "" {
		out.Search = PathSearch{State: "start_relationship_not_available", MorePaths: "not_searched"}
	} else {
		chain, err := r.chainTopology(ctx, q.Ledger)
		if err != nil {
			return PathsResult{}, err
		}
		endings := pathTopology{}
		links, err := r.endingLinks(q.Ending)
		if err != nil {
			return PathsResult{}, err
		}
		for _, link := range links {
			if graphread.Kind(link.To) != "candidate" {
				return PathsResult{}, fmt.Errorf("foreign candidate endpoint")
			}
			if link.To == q.Target {
				if err := endings.add(link); err != nil {
					return PathsResult{}, err
				}
			}
		}
		if err := endings.order(); err != nil {
			return PathsResult{}, err
		}
		selected, out.Search, err = searchPaths(ctx, chain, endings, start, q.Target, q.MaxHops, q.Limit, q.Budget)
		if err != nil {
			return PathsResult{}, err
		}
	}
	vertices := map[string]bool{}
	links := map[string]bool{}
	addVertex := func(id string) error {
		if id == "" || vertices[id] {
			return nil
		}
		vertices[id] = true
		v := PathVertex{ID: id, Kind: graphread.Kind(id), Facets: map[string]graphread.Facet{}}
		if v.Kind == "" {
			if !validDigest(id) {
				return fmt.Errorf("invalid path vertex")
			}
			v.Kind = "reported_contributor_appearance"
		} else {
			var err error
			v.Facets["receipts"], err = r.receipts.Entity(ctx, id)
			if err != nil {
				return err
			}
			v.Facets["committee_flow"], err = r.flow.Facet(ctx, id)
			if err != nil {
				return err
			}
			v.Facets["outside_spending"], err = r.outside.Entity(ctx, id)
			if err != nil {
				return err
			}
			if r.shared != nil {
				v.Facets["shared_conduits"], err = r.shared.Entity(ctx, id)
				if err != nil {
					return err
				}
			}
		}
		out.Vertices = append(out.Vertices, v)
		return nil
	}
	if err := addVertex(start); err != nil {
		return PathsResult{}, err
	}
	if err := addVertex(q.Target); err != nil {
		return PathsResult{}, err
	}
	for _, path := range selected {
		if out.Entry != nil {
			path = append([]graphread.Link{*out.Entry.Link}, path...)
		}
		p := EvidencePath{Links: []string{}}
		for _, link := range path {
			id := link.ID()
			p.Links = append(p.Links, id)
			if links[id] {
				continue
			}
			links[id] = true
			for _, vertex := range []string{link.From, link.To} {
				if err := addVertex(vertex); err != nil {
					return PathsResult{}, err
				}
			}
			item, err := r.pathEvidence(ctx, q.Ledger, link, out.Entry)
			if err != nil {
				return PathsResult{}, err
			}
			family, err := r.pathFamily(link.Family)
			if err != nil {
				return PathsResult{}, err
			}
			out.Links = append(out.Links, PathLink{ID: id, Topology: link, Family: family, Evidence: item})
		}
		p.ID = valueID(struct {
			Generation string
			Links      []string
		}{out.GenerationID, p.Links})
		out.Paths = append(out.Paths, p)
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return PathsResult{}, err
	}
	out.ResultID = valueID(out)
	return out, nil
}

func (r *Reader) pathFamily(kind string) (Family, error) {
	for _, f := range r.queryFamilies() {
		if f.Kind == kind {
			return f, nil
		}
	}
	return Family{}, fmt.Errorf("path family absent from generation")
}
func (r *Reader) pathEvidence(ctx context.Context, side flow.Ledger, link graphread.Link, entry *receipt.PathEntry) (graphread.Item, error) {
	switch link.Family {
	case "reported_receipt", "conduit_association", receipt.SharedFamily:
		if entry == nil || entry.Link == nil || entry.Item == nil || *entry.Link != link {
			return graphread.Item{}, fmt.Errorf("path entry identity mismatch")
		}
		return *entry.Item, nil
	case "receiver_reported_committee_observation", "sender_reported_committee_observation":
		if (link.Family == "receiver_reported_committee_observation") != (side == flow.ScheduleA) {
			return graphread.Item{}, fmt.Errorf("path evidence ledger differs")
		}
		return r.flow.PathEvidence(ctx, side, link.Key)
	case "candidate_authorization_context":
		return r.receipts.AuthorizationEvidence(ctx, link.Key)
	case "independent_support", "independent_opposition":
		return r.outside.PathEvidence(ctx, link.Family, link.Key)
	}
	return graphread.Item{}, fmt.Errorf("unsupported path evidence")
}
