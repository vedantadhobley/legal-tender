package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

type PathEntry struct {
	State  string          `json:"state"`
	Link   *graphread.Link `json:"link"`
	Item   *graphread.Item `json:"item"`
	Source json.RawMessage `json:"source"`
}

func (r *CycleReader) PathEntry(ctx context.Context, family string, ordinal uint64) (PathEntry, error) {
	if family != "reported_receipt" && family != "conduit_association" {
		return PathEntry{}, fmt.Errorf("unsupported receipt entry")
	}
	source, err := r.loaded.inspector.Inspect(ctx, ordinal)
	if err != nil {
		return PathEntry{}, err
	}
	evidence, _ := json.Marshal(source)
	out := PathEntry{State: "reported_recipient_unresolved", Source: evidence}
	var decision *c.Decision
	if family == "conduit_association" {
		decisions, err := r.conduitDecisions(ctx, map[uint64]bool{ordinal: true})
		if err != nil {
			return PathEntry{}, err
		}
		if d, ok := decisions[ordinal]; ok {
			decision = &d
		}
		out.State = "no_qualified_conduit_association"
	}
	appearance, receiptEdge, conduitEdge, err := project(r.completion.Inputs.Facts.ID, source.Participant, decision)
	if err != nil {
		return PathEntry{}, err
	}
	var want any
	collection, target := receipts, ""
	if family == "reported_receipt" && receiptEdge != nil {
		want, target = receiptEdge, receiptEdge.To
	}
	if family == "conduit_association" && conduitEdge != nil {
		want, target, collection = conduitEdge, conduitEdge.To, conduits
	}
	if want == nil {
		return out, nil
	}
	raw, err := verifyConnectionDocument(ctx, r.cl, collection, appearance.Key, want)
	if err != nil {
		return PathEntry{}, err
	}
	out.State = "available"
	out.Link = &graphread.Link{Family: family, Key: appearance.Key, From: appearance.Key, To: strings.TrimPrefix(target, entities+"/")}
	out.Item = &graphread.Item{Key: appearance.Key, Document: raw, EvidenceKind: "complete_schedule_a_source_occurrence", Evidence: evidence}
	return out, nil
}

// Non-authorized context remains inspectable in neighborhoods, but cannot end
// an authorized-candidate path. This does not reclassify any linkage source fact.
func (r *CycleReader) AuthorizedLinks() []graphread.Link {
	out := []graphread.Link{}
	for _, a := range r.loaded.links {
		if a.State == "authorized" {
			out = append(out, graphread.Link{Family: "candidate_authorization_context", Key: a.Key, From: strings.TrimPrefix(a.From, entities+"/"), To: strings.TrimPrefix(a.To, entities+"/")})
		}
	}
	return out
}

func (r *CycleReader) AuthorizationEvidence(ctx context.Context, key string) (graphread.Item, error) {
	for _, a := range r.loaded.links {
		if a.Key == key && a.State == "authorized" {
			raw, err := verifyConnectionDocument(ctx, r.cl, authorizations, key, a)
			if err != nil {
				return graphread.Item{}, err
			}
			evidence, _ := json.Marshal(struct {
				FactSet string   `json:"fact_set_id"`
				Facts   []string `json:"supporting_fact_ids"`
			}{a.FactSet, a.SupportingFacts})
			return graphread.Item{Key: key, Document: raw, EvidenceKind: "verified_linkage_fact_membership", Evidence: evidence}, nil
		}
	}
	return graphread.Item{}, fmt.Errorf("authorized context absent from pinned projection")
}
