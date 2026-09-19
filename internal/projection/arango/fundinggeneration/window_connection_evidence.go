package fundinggeneration

import (
	"context"
	"fmt"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

const authorizationTimeBasis = "source_publication_context_day_level_validity_unknown"

func connectionLinkID(generation string, link graphread.Link) string {
	return generation + ":" + link.ID()
}

func (r *WindowReader) connectionEndings(ctx context.Context, q WindowConnectionQuery, routes map[string]connectionRoute) (pathTopology, []WindowAuthorizationContext, error) {
	endings, contexts := pathTopology{}, []WindowAuthorizationContext{}
	if q.Ending == "" {
		return endings, contexts, nil
	}
	var count uint64
	for i, p := range r.partitions {
		c := WindowAuthorizationContext{GenerationID: p.publication.GenerationID, TemporalBasis: authorizationTimeBasis}
		for _, link := range p.receipts.AuthorizedLinks() {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			count++
			if count > MaxWindowObservations {
				return nil, nil, fmt.Errorf("authorization contexts exceed window memory guard")
			}
			if link.Family != "candidate_authorization_context" || !validDigest(link.Key) || graphread.Kind(link.From) != "committee" || graphread.Kind(link.To) != "candidate" {
				return nil, nil, fmt.Errorf("invalid authorization context topology")
			}
			if link.To != q.Target {
				continue
			}
			// Original authorization keys identify endpoint pairs, not source
			// publications. Qualify search keys without changing stored topology.
			qualified := link
			qualified.Key = valueID(connectionLinkID(p.publication.GenerationID, link))
			if _, exists := routes[qualified.ID()]; exists {
				return nil, nil, fmt.Errorf("duplicate authorization context")
			}
			routes[qualified.ID()] = connectionRoute{windowRoute: windowRoute{partition: i}, original: link}
			if err := endings.add(qualified); err != nil {
				return nil, nil, err
			}
			c.Links++
		}
		contexts = append(contexts, c)
	}
	return endings, contexts, endings.order()
}

func (r *WindowReader) connectionEvidence(ctx context.Context, out *WindowConnectionsResult, selected [][]graphread.Link, routes map[string]connectionRoute, entryPartition int, start string) error {
	spending, err := r.spendingEvidence(ctx, selected, routes)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	vertices := map[string]bool{out.Query.Target: true}
	if start != "" {
		vertices[start] = true
	}
	add := func(route connectionRoute, item graphread.Item, basis string, path *EvidencePath) error {
		link := route.original
		generation := r.partitions[route.partition].publication.GenerationID
		id := connectionLinkID(generation, link)
		path.Links = append(path.Links, id)
		vertices[link.From], vertices[link.To] = true, true
		if seen[id] {
			return nil
		}
		if item.Key != link.Key {
			return fmt.Errorf("connection source readback returned a different link")
		}
		seen[id] = true
		out.Links = append(out.Links, WindowConnectionLink{id, generation, link, route.date, basis, item})
		return nil
	}
	for _, selectedPath := range selected {
		path := EvidencePath{Links: []string{}}
		if out.Entry != nil {
			entry := out.Entry
			if err := add(connectionRoute{windowRoute: windowRoute{entryPartition, entry.Date}, original: *entry.Entry.Link}, *entry.Entry.Item, "underlying_receipt_reported_date", &path); err != nil {
				return err
			}
		}
		for _, link := range selectedPath {
			route, ok := routes[link.ID()]
			if !ok {
				return fmt.Errorf("connection source route absent")
			}
			// Evidence variants can share long committee chains. Verify each
			// returned source occurrence once, not once per candidate assertion.
			id := connectionLinkID(r.partitions[route.partition].publication.GenerationID, route.original)
			if seen[id] {
				path.Links = append(path.Links, id)
				continue
			}
			var item graphread.Item
			var err error
			basis := "selected_committee_observation_reported_date"
			p := r.partitions[route.partition]
			if route.spending != nil {
				var ok bool
				item, ok = spending[id]
				if !ok {
					return fmt.Errorf("spending member source evidence absent")
				}
				basis = "schedule_e_" + out.Query.SpendingDate + "_reported_date"
			} else if route.original.Family == "candidate_authorization_context" {
				item, err = p.receipts.AuthorizationEvidence(ctx, route.original.Key)
				basis = authorizationTimeBasis
			} else {
				item, err = p.source.PathEvidence(ctx, out.Query.Ledger, route.original.Key)
			}
			if err != nil {
				return err
			}
			if err := add(route, item, basis, &path); err != nil {
				return err
			}
		}
		path.ID = valueID(struct {
			Inputs []WindowPublication
			Links  []string
		}{out.Inputs, path.Links})
		out.Paths = append(out.Paths, path)
	}
	ids := make([]string, 0, len(vertices))
	for id := range vertices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		v := WindowConnectionVertex{ID: id, Kind: graphread.Kind(id), Facets: []WindowConnectionFacet{}}
		if v.Kind == "" {
			if !validDigest(id) {
				return fmt.Errorf("invalid connection vertex")
			}
			v.Kind = "reported_contributor_appearance"
		} else {
			for _, p := range r.partitions {
				receiptFacet, err := p.receipts.Entity(ctx, id)
				if err != nil {
					return err
				}
				flowFacet, err := p.source.Facet(ctx, id)
				if err != nil {
					return err
				}
				facet := WindowConnectionFacet{GenerationID: p.publication.GenerationID, Receipts: receiptFacet, CommitteeFlow: flowFacet}
				if p.shared != nil {
					shared, err := p.shared.Entity(ctx, id)
					if err != nil {
						return err
					}
					facet.SharedConduits = &shared
				}
				if out.Query.isSpending() {
					outside, err := p.outside.Entity(ctx, id)
					if err != nil {
						return err
					}
					facet.OutsideSpending = &outside
				}
				v.Facets = append(v.Facets, facet)
			}
		}
		out.Vertices = append(out.Vertices, v)
	}
	return nil
}
