package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	xs "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type SharedEvidence struct {
	BaseProjection   Reference     `json:"base_receipt_projection"`
	Calculation      Reference     `json:"conduit_calculation"`
	OriginalDecision c.Decision    `json:"original_decision"`
	Decision         c.Decision    `json:"shared_decision"`
	Group            c.GroupRecord `json:"complete_group_decision"`
	Source           p.Inspection  `json:"source"`
	Related          p.Inspection  `json:"related_memo_source"`
}

func (r *SharedReader) Entity(ctx context.Context, id string) (graphread.Facet, error) {
	if graphread.Kind(id) == "" {
		return graphread.Facet{}, fmt.Errorf("invalid FEC entity")
	}
	if !slices.Contains(r.ids, id) {
		return graphread.Facet{State: "not_present_in_projection"}, nil
	}
	b, err := verifyConnectionDocument(ctx, r.cl, entities, id, r.base.loaded.entity(id))
	if err != nil {
		return graphread.Facet{}, err
	}
	return graphread.Facet{State: "present", Document: b}, nil
}

func (r *SharedReader) groups(ctx context.Context, decisions map[uint64]c.Decision) (map[uint64]c.GroupRecord, error) {
	wanted := map[uint64]bool{}
	for _, d := range decisions {
		if d.State == policy.SharedAssociation {
			wanted[d.Related] = true
		}
	}
	rd, err := xs.Open(ctx, filepath.Join(filepath.Dir(r.calculation), "data"), r.updated.Groups.Decisions)
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	out := map[uint64]c.GroupRecord{}
	for {
		record, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		g, err := c.DecodeGroup(record, r.updated.SourceRows)
		if err != nil {
			return nil, err
		}
		if wanted[g.Related] {
			out[g.Related] = g
		}
	}
	return out, nil
}

func (r *SharedReader) item(ctx context.Context, d c.Decision, group c.GroupRecord) (graphread.Item, error) {
	a, e, err := sharedDocuments(r.base.View(), d)
	if err != nil {
		return graphread.Item{}, err
	}
	if group.Related != d.Related || group.Decision.State != policy.SharedAssociation || group.Decision.ConduitID == nil || *group.Decision.ConduitID != *d.ConduitID {
		return graphread.Item{}, fmt.Errorf("shared source group differs")
	}
	raw, err := verifyConnectionDocument(ctx, r.cl, conduits, e.Key, e)
	if err != nil {
		return graphread.Item{}, err
	}
	if _, err = verifyConnectionDocument(ctx, r.cl, appearances, a.Key, a); err != nil {
		return graphread.Item{}, err
	}
	if _, err = r.Entity(ctx, *d.ConduitID); err != nil {
		return graphread.Item{}, err
	}
	if err = r.base.verifySharedBaseMembers(ctx, batch{keys: []string{e.Key}, data: append(append([]byte(nil), raw...), '\n')}); err != nil {
		return graphread.Item{}, err
	}
	source, err := r.base.loaded.inspector.Inspect(ctx, d.Ordinal)
	if err != nil {
		return graphread.Item{}, err
	}
	related, err := r.base.loaded.inspector.Inspect(ctx, d.Related)
	if err != nil {
		return graphread.Item{}, err
	}
	old := d
	old.State, old.ConduitID = "shared_related_record_unresolved", nil
	evidence, err := json.Marshal(SharedEvidence{r.view.Base, r.view.Calculation, old, d, group, source, related})
	if err != nil {
		return graphread.Item{}, err
	}
	return graphread.Item{Key: e.Key, Document: raw, EvidenceKind: "complete_shared_group_and_original_schedule_a_occurrences", Evidence: evidence}, nil
}

func (r *SharedReader) PathEntry(ctx context.Context, ordinal uint64) (PathEntry, error) {
	source, err := r.base.loaded.inspector.Inspect(ctx, ordinal)
	if err != nil {
		return PathEntry{}, err
	}
	b, _ := json.Marshal(source)
	out := PathEntry{State: "no_qualified_shared_conduit_association", Source: b}
	decisions, err := readConduitDecisions(ctx, r.calculation, r.updated, map[uint64]bool{ordinal: true})
	if err != nil {
		return PathEntry{}, err
	}
	d, ok := decisions[ordinal]
	if !ok || d.State != policy.SharedAssociation {
		return out, nil
	}
	groups, err := r.groups(ctx, decisions)
	if err != nil {
		return PathEntry{}, err
	}
	item, err := r.item(ctx, d, groups[d.Related])
	if err != nil {
		return PathEntry{}, err
	}
	out.State, out.Item = "available", &item
	out.Link = &graphread.Link{Family: SharedFamily, Key: item.Key, From: item.Key, To: *d.ConduitID}
	return out, nil
}

func (r *SharedReader) Page(ctx context.Context, id, after string, limit int) (graphread.Page, error) {
	if err := graphread.ValidPage(id, after, limit); err != nil {
		return graphread.Page{}, err
	}
	if graphread.Kind(id) != "committee" {
		return graphread.Empty("not_applicable"), nil
	}
	rows := []conduit{}
	last := after
	err := r.cl.query(ctx, "FOR d IN reported_conduit_associations FILTER d._to == @entity AND d._key > @after SORT d._key LIMIT @limit RETURN UNSET(d, '_id', '_rev')", map[string]any{"entity": entities + "/" + id, "after": after, "limit": limit + 1}, func(raw json.RawMessage) error {
		var d conduit
		if len(rows) >= limit+1 || strictjson.Decode(raw, &d) != nil || !validDigest(d.Key) || d.Key <= last || d.Decision.ConduitID == nil || *d.Decision.ConduitID != id {
			return fmt.Errorf("invalid shared page document or order")
		}
		last = d.Key
		rows = append(rows, d)
		return nil
	})
	if err != nil {
		return graphread.Page{}, err
	}
	page := graphread.Empty("available")
	if len(rows) == 0 {
		return page, nil
	}
	wanted := map[uint64]bool{}
	for _, e := range rows {
		wanted[e.Decision.Ordinal] = true
	}
	decisions, err := readConduitDecisions(ctx, r.calculation, r.updated, wanted)
	if err != nil {
		return graphread.Page{}, err
	}
	groups, err := r.groups(ctx, decisions)
	if err != nil {
		return graphread.Page{}, err
	}
	for i, e := range rows {
		d, ok := decisions[e.Decision.Ordinal]
		if !ok {
			return graphread.Page{}, fmt.Errorf("shared edge lacks published decision")
		}
		item, err := r.item(ctx, d, groups[d.Related])
		if err != nil {
			return graphread.Page{}, err
		}
		b, _ := json.Marshal(e)
		if !equalJSON(b, item.Document) {
			return graphread.Page{}, fmt.Errorf("shared page differs from source-backed edge")
		}
		// Lookahead gets the same checks as returned items.
		if i < limit {
			page.Items = append(page.Items, item)
			page.NextAfter = item.Key
		}
	}
	page.HasMore = len(rows) > limit
	return page, nil
}
