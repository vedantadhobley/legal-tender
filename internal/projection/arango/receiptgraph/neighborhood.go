package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	xs "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func (r *CycleReader) Entity(ctx context.Context, id string) (graphread.Facet, error) {
	if graphread.Kind(id) == "" {
		return graphread.Facet{}, fmt.Errorf("invalid FEC entity")
	}
	var out json.RawMessage
	n := 0
	err := r.cl.query(ctx, "FOR d IN entities FILTER d._key == @key LIMIT 2 RETURN UNSET(d, '_id', '_rev')", map[string]any{"key": id}, func(raw json.RawMessage) error {
		n++
		if n > 1 {
			return fmt.Errorf("duplicate receipt entity")
		}
		want := r.loaded.entity(id)
		b, _ := json.Marshal(want)
		if !equalJSON(raw, b) {
			return fmt.Errorf("receipt entity differs from exact master evidence")
		}
		out = b
		return nil
	})
	if err != nil {
		return graphread.Facet{}, err
	}
	if n == 0 {
		return graphread.Facet{State: "not_present_in_projection"}, nil
	}
	return graphread.Facet{State: "present", Document: out}, nil
}

func (r *CycleReader) Page(ctx context.Context, kind, id, after string, limit int) (graphread.Page, error) {
	if err := graphread.ValidPage(id, after, limit); err != nil {
		return graphread.Page{}, err
	}
	if kind == "candidate_authorization_context" {
		return r.authorizationPage(ctx, id, after, limit)
	}
	collection := receipts
	if kind == "conduit_association" {
		collection = conduits
	} else if kind != "reported_receipt" {
		return graphread.Page{}, fmt.Errorf("unsupported receipt family")
	}
	if graphread.Kind(id) != "committee" {
		return graphread.Empty("not_applicable"), nil
	}
	// Only the edge endpoint is filtered. No interpretation of donor identity or
	// effective money is introduced. Server runtime/memory caps fail the page.
	var rawRows []json.RawMessage
	last := after
	err := r.cl.query(ctx, "FOR d IN @@collection FILTER d._to == @entity AND d._key > @after SORT d._key LIMIT @limit RETURN UNSET(d, '_id', '_rev')", map[string]any{"@collection": collection, "entity": entities + "/" + id, "after": after, "limit": limit + 1}, func(raw json.RawMessage) error {
		if len(rawRows) >= limit+1 {
			return fmt.Errorf("receipt page exceeds bound")
		}
		var key struct {
			Key string `json:"_key"`
		}
		if json.Unmarshal(raw, &key) != nil || !validDigest(key.Key) || key.Key <= last {
			return fmt.Errorf("invalid receipt page order")
		}
		last = key.Key
		rawRows = append(rawRows, append(json.RawMessage(nil), raw...))
		return nil
	})
	if err != nil {
		return graphread.Page{}, err
	}
	page := graphread.Empty("available")
	page.HasMore = len(rawRows) > limit
	// Verify lookahead too; a foreign lookahead cannot manufacture has_more.
	ordinals := map[uint64]bool{}
	for _, raw := range rawRows {
		if collection == receipts {
			var v receipt
			if strictjson.Decode(raw, &v) != nil {
				return page, fmt.Errorf("invalid receipt document")
			}
			ordinals[v.Ordinal] = true
		} else {
			var v conduit
			if strictjson.Decode(raw, &v) != nil {
				return page, fmt.Errorf("invalid conduit document")
			}
			ordinals[v.Decision.Ordinal] = true
		}
	}
	decisions := map[uint64]c.Decision{}
	if collection == conduits && len(ordinals) > 0 {
		decisions, err = r.conduitDecisions(ctx, ordinals)
		if err != nil {
			return graphread.Page{}, err
		}
	}
	for index, raw := range rawRows {
		var ordinal uint64
		if collection == receipts {
			var v receipt
			_ = json.Unmarshal(raw, &v)
			ordinal = v.Ordinal
		} else {
			var v conduit
			_ = json.Unmarshal(raw, &v)
			ordinal = v.Decision.Ordinal
		}
		source, err := r.loaded.inspector.Inspect(ctx, ordinal)
		if err != nil {
			return graphread.Page{}, err
		}
		var d *c.Decision
		if collection == conduits {
			v, ok := decisions[ordinal]
			if !ok {
				return graphread.Page{}, fmt.Errorf("conduit has no published decision")
			}
			d = &v
		}
		a, receiptEdge, conduitEdge, err := project(r.completion.Inputs.Facts.ID, source.Participant, d)
		if err != nil {
			return graphread.Page{}, err
		}
		var want any = receiptEdge
		if collection == conduits {
			want = conduitEdge
			if conduitEdge == nil || conduitEdge.To != entities+"/"+id {
				return graphread.Page{}, fmt.Errorf("foreign conduit endpoint")
			}
		} else if receiptEdge == nil || receiptEdge.To != entities+"/"+id {
			return graphread.Page{}, fmt.Errorf("foreign receipt endpoint")
		}
		b, _ := json.Marshal(want)
		if !equalJSON(raw, b) {
			return graphread.Page{}, fmt.Errorf("receipt relationship differs from source evidence")
		}
		if index < limit {
			evidence, _ := json.Marshal(source)
			page.Items = append(page.Items, graphread.Item{Key: a.Key, Document: b, EvidenceKind: "complete_schedule_a_source_occurrence", Evidence: evidence})
			page.NextAfter = a.Key
		}
	}
	return page, nil
}

func (r *CycleReader) conduitDecisions(ctx context.Context, wanted map[uint64]bool) (map[uint64]c.Decision, error) {
	return readConduitDecisions(ctx, r.options.Conduits, r.loaded.c, wanted)
}

func readConduitDecisions(ctx context.Context, path string, publication c.Result, wanted map[uint64]bool) (map[uint64]c.Decision, error) {
	reader, err := xs.Open(ctx, filepath.Join(filepath.Dir(path), "data"), publication.Decisions)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	out := map[uint64]c.Decision{}
	var last uint64
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		d, err := c.DecodeDecision(record, publication.SourceRows)
		if err != nil {
			return nil, err
		}
		if d.Ordinal <= last {
			return nil, fmt.Errorf("non-increasing conduit decisions")
		}
		last = d.Ordinal
		if wanted[d.Ordinal] {
			out[d.Ordinal] = d
		}
	}
	return out, nil
}

func (r *CycleReader) authorizationPage(ctx context.Context, id, after string, limit int) (graphread.Page, error) {
	selected := []authorization{}
	for _, a := range r.loaded.links {
		if a.Key > after && (a.From == entities+"/"+id || a.To == entities+"/"+id) {
			selected = append(selected, a)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Key < selected[j].Key })
	page := graphread.Empty("available")
	page.HasMore = len(selected) > limit
	for index, a := range selected[:min(len(selected), limit+1)] {
		b, err := verifyConnectionDocument(ctx, r.cl, authorizations, a.Key, a)
		if err != nil {
			return graphread.Page{}, err
		}
		if index >= limit {
			continue
		}
		evidence, _ := json.Marshal(struct {
			FactSet string   `json:"fact_set_id"`
			Facts   []string `json:"supporting_fact_ids"`
		}{a.FactSet, a.SupportingFacts})
		page.Items = append(page.Items, graphread.Item{Key: a.Key, Document: b, EvidenceKind: "verified_linkage_fact_membership", Evidence: evidence})
		page.NextAfter = a.Key
	}
	return page, nil
}

func (r *CycleReader) AuthorizedCandidateIDs() []string {
	set := map[string]bool{}
	for _, a := range r.loaded.links {
		if a.State == "authorized" {
			set[a.To[len(entities)+1:]] = true
		}
	}
	out := []string{}
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ConduitWitnessID verifies the complete compact artifact and selects the
// lexicographically first conduit ID. This is a test case, not a donor ranking.
func (r *CycleReader) ConduitWitnessID(ctx context.Context) (string, error) {
	reader, err := xs.Open(ctx, filepath.Join(filepath.Dir(r.options.Conduits), "data"), r.loaded.c.Decisions)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	id := ""
	var last uint64
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		d, err := c.DecodeDecision(record, r.completion.SourceRows)
		if err != nil {
			return "", err
		}
		if d.Ordinal <= last {
			return "", fmt.Errorf("non-increasing conduit decisions")
		}
		last = d.Ordinal
		if d.ConduitID != nil && (id == "" || *d.ConduitID < id) {
			id = *d.ConduitID
		}
	}
	return id, nil
}
