package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

// DateWindow uses inclusive reported calendar dates. Nil means all observations
// in the supplied publications, not all history or complete chronological funding.
type DateWindow struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

func (w DateWindow) bounds() (int32, int32, error) {
	a, errA := time.Parse(time.DateOnly, w.Start)
	b, errB := time.Parse(time.DateOnly, w.End)
	if errA != nil || errB != nil || a.Year() < 1 || b.Year() < 1 || a.Format(time.DateOnly) != w.Start || b.Format(time.DateOnly) != w.End || b.Before(a) {
		return 0, 0, fmt.Errorf("ordered YYYY-MM-DD start and end dates required")
	}
	return int32(a.Unix() / 86400), int32(b.Unix() / 86400), nil
}

type WindowPathQuery struct {
	From    string      `json:"from_committee"`
	Target  string      `json:"target_committee"`
	Ledger  flow.Ledger `json:"ledger"`
	Window  *DateWindow `json:"reported_date_window"`
	MaxHops int         `json:"max_committee_hops"`
	Limit   int         `json:"max_paths"`
	Budget  uint64      `json:"max_links_examined"`
}

func (q WindowPathQuery) Validate() error {
	if graphread.Kind(q.Target) != "committee" {
		return fmt.Errorf("window paths currently support committee targets only")
	}
	if err := (PathQuery{From: q.From, Target: q.Target, Ledger: q.Ledger, MaxHops: q.MaxHops, Limit: q.Limit, Budget: q.Budget}).Validate(); err != nil {
		return err
	}
	if q.Window != nil {
		_, _, err := q.Window.bounds()
		return err
	}
	return nil
}

type WindowCoverage struct {
	GenerationID    string `json:"generation_id"`
	Rows            uint64 `json:"selected_ledger_observations"`
	Included        uint64 `json:"included_observations"`
	Before          uint64 `json:"before_window"`
	After           uint64 `json:"after_window"`
	UnknownExcluded uint64 `json:"unknown_date_excluded"`
	UndatedIncluded uint64 `json:"undated_included"`
}

type WindowPathLink struct {
	ID           string         `json:"link_id"`
	GenerationID string         `json:"generation_id"`
	Topology     graphread.Link `json:"topology"`
	Date         *int32         `json:"reported_date_days"`
	Evidence     graphread.Item `json:"evidence"`
}

type WindowFacet struct {
	GenerationID string          `json:"generation_id"`
	Facet        graphread.Facet `json:"committee_flow_facet"`
}

type WindowVertex struct {
	ID     string        `json:"fec_committee_id"`
	Facets []WindowFacet `json:"publication_facets"`
}

type WindowPathsResult struct {
	SchemaVersion        string              `json:"schema_version"`
	Policy               string              `json:"policy"`
	ResultID             string              `json:"result_id"`
	BuildSHA256          string              `json:"consumer_executable_sha256"`
	Inputs               []WindowPublication `json:"inputs"`
	Query                WindowPathQuery     `json:"query"`
	Coverage             []WindowCoverage    `json:"coverage"`
	Search               PathSearch          `json:"search"`
	Paths                []EvidencePath      `json:"paths"`
	Links                []WindowPathLink    `json:"links"`
	Vertices             []WindowVertex      `json:"vertices"`
	Limitations          []string            `json:"limitations"`
	FinancialEligibility bool                `json:"financial_eligibility"`
	TerminalEligible     bool                `json:"terminal_attribution_eligible"`
}

type windowRoute struct {
	partition int
	date      *int32
}

func (r *WindowReader) windowTopology(ctx context.Context, q WindowPathQuery) (pathTopology, map[string]windowRoute, []WindowCoverage, error) {
	chain, routes := pathTopology{}, map[string]windowRoute{}
	coverage := []WindowCoverage{}
	seen := map[string]bool{}
	var lo, hi int32
	if q.Window != nil {
		var err error
		lo, hi, err = q.Window.bounds()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	for i, p := range r.partitions {
		c := WindowCoverage{GenerationID: p.publication.GenerationID}
		v := p.publication.Generation.CommitteeFlow
		expected, fact, family := v.A.Rows, v.Inputs.A.FactSetID, "receiver_reported_committee_observation"
		if q.Ledger == flow.ScheduleB {
			expected, fact, family = v.B.Rows, v.Inputs.B.FactSetID, "sender_reported_committee_observation"
		}
		err := p.source.VisitDatedLinks(ctx, q.Ledger, func(row flow.DatedLink) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			c.Rows++
			link := row.Link
			if c.Rows > expected || uint64(len(seen)) >= MaxWindowObservations || row.FactSetID != fact || row.Ordinal == 0 || link.Family != family || !validDigest(link.Key) || graphread.Kind(link.From) != "committee" || graphread.Kind(link.To) != "committee" || seen[link.ID()] {
				return fmt.Errorf("window topology source, membership or resource mismatch")
			}
			seen[link.ID()] = true
			if row.Date == nil {
				if q.Window != nil {
					c.UnknownExcluded++
					return nil
				}
				c.UndatedIncluded++
			} else if q.Window != nil {
				if *row.Date < lo {
					c.Before++
					return nil
				}
				if *row.Date > hi {
					c.After++
					return nil
				}
			}
			c.Included++
			route := windowRoute{partition: i}
			if row.Date != nil {
				d := *row.Date
				route.date = &d
			}
			routes[link.ID()] = route
			return chain.add(link)
		})
		if err != nil {
			return nil, nil, nil, err
		}
		if c.Rows != expected || c.Rows != c.Included+c.Before+c.After+c.UnknownExcluded {
			return nil, nil, nil, fmt.Errorf("window topology failed observation conservation")
		}
		coverage = append(coverage, c)
	}
	return chain, routes, coverage, chain.order()
}

// Paths composes one observation ledger through exact FEC committee IDs. It
// filters observation dates, not validity periods, financial membership or the
// temporal order of a route. Candidate/receipt entries remain separate readers.
func (r *WindowReader) Paths(ctx context.Context, q WindowPathQuery) (WindowPathsResult, error) {
	if err := q.Validate(); err != nil {
		return WindowPathsResult{}, err
	}
	if err := r.verify(ctx); err != nil {
		return WindowPathsResult{}, err
	}
	// Own the query value retained in the answer.
	if q.Window != nil {
		w := *q.Window
		q.Window = &w
	}
	out := WindowPathsResult{SchemaVersion: WindowVersion, Policy: WindowPolicy, BuildSHA256: r.build, Query: q,
		Inputs: []WindowPublication{}, Coverage: []WindowCoverage{}, Paths: []EvidencePath{}, Links: []WindowPathLink{}, Vertices: []WindowVertex{}, Limitations: []string{
			"selected_committee_observations_only_not_all_receipts_or_candidate_endings",
			"one_ledger_not_reconciled_economic_payments_or_summed_path_money",
			"exact_fec_committee_ids_not_person_or_corporation_resolution",
			"historical_facets_remain_source_scoped_not_day_level_validity",
			"date_filter_not_chronological_funding_or_relationship_validity",
			"unknown_dates_excluded_from_bounded_windows_and_counted_separately",
			"coverage_counts_describe_supplied_selected_ledgers_not_world_completeness",
			"all_supplied_dates_is_not_all_history",
			"no_outgoing_edge_or_search_cutoff_is_not_terminal_evidence",
			"single_version_per_source_cycle_no_snapshot_reconciliation",
			"search_budget_excludes_opening_topology_scan_and_evidence_readback",
			"immutable_publications_not_a_cross_database_transaction",
		}}
	for _, p := range r.partitions {
		// Own nested maps/slices: callers must not mutate a verified reader by
		// editing a previous response's embedded generation metadata.
		b, err := json.Marshal(p.publication)
		if err != nil {
			return WindowPathsResult{}, err
		}
		var publication WindowPublication
		if err := json.Unmarshal(b, &publication); err != nil {
			return WindowPathsResult{}, err
		}
		out.Inputs = append(out.Inputs, publication)
	}
	chain, routes, coverage, err := r.windowTopology(ctx, q)
	if err != nil {
		return WindowPathsResult{}, err
	}
	out.Coverage = coverage
	paths, state, err := searchPaths(ctx, chain, nil, q.From, q.Target, q.MaxHops, q.Limit, q.Budget)
	if err != nil {
		return WindowPathsResult{}, err
	}
	out.Search = state
	seenLinks, vertices := map[string]bool{}, map[string]bool{q.From: true, q.Target: true}
	for _, path := range paths {
		v := EvidencePath{Links: []string{}}
		for _, link := range path {
			id := link.ID()
			v.Links = append(v.Links, id)
			vertices[link.From], vertices[link.To] = true, true
			if seenLinks[id] {
				continue
			}
			seenLinks[id] = true
			route := routes[id]
			p := r.partitions[route.partition]
			evidence, err := p.source.PathEvidence(ctx, q.Ledger, link.Key)
			if err != nil {
				return WindowPathsResult{}, err
			}
			if evidence.Key != link.Key {
				return WindowPathsResult{}, fmt.Errorf("window source readback returned a different link")
			}
			out.Links = append(out.Links, WindowPathLink{id, p.publication.GenerationID, link, route.date, evidence})
		}
		v.ID = valueID(struct {
			Inputs []WindowPublication
			Links  []string
		}{out.Inputs, v.Links})
		out.Paths = append(out.Paths, v)
	}
	ids := make([]string, 0, len(vertices))
	for id := range vertices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		vertex := WindowVertex{ID: id, Facets: []WindowFacet{}}
		for _, p := range r.partitions {
			facet, err := p.source.Facet(ctx, id)
			if err != nil {
				return WindowPathsResult{}, err
			}
			vertex.Facets = append(vertex.Facets, WindowFacet{p.publication.GenerationID, facet})
		}
		out.Vertices = append(out.Vertices, vertex)
	}
	if err := r.verify(ctx); err != nil {
		return WindowPathsResult{}, err
	}
	out.ResultID = valueID(out)
	return out, nil
}
